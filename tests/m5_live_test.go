package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/cluster"
	"agent-runtime-lab/src/clustertls"
	"agent-runtime-lab/src/taskevidence"
)

func startM5Process(t *testing.T, binary, logPath string, args ...string) *liveProcess {
	t.Helper()
	log, err := os.OpenFile(logPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, args...)
	cmd.Stdout, cmd.Stderr = log, log
	if err = cmd.Start(); err != nil {
		log.Close()
		t.Fatal(err)
	}
	p := &liveProcess{cmd: cmd, done: make(chan struct{})}
	go func() { p.err = cmd.Wait(); log.Close(); close(p.done) }()
	t.Cleanup(func() { stopM5Process(t, p, false) })
	return p
}
func stopM5Process(t *testing.T, p *liveProcess, crash bool) {
	t.Helper()
	if p == nil {
		return
	}
	select {
	case <-p.done:
		return
	default:
	}
	if crash {
		p.cmd.Process.Kill()
	} else {
		p.cmd.Process.Signal(syscall.SIGTERM)
	}
	select {
	case <-p.done:
	case <-time.After(15 * time.Second):
		p.cmd.Process.Kill()
		<-p.done
		t.Error("cluster service required forced termination")
	}
}

// Real E2B commands, three actual fleetd/agent processes, two coordinators,
// three real etcd members. This single-host gate proves process failover;
// physical-server HA requires deploying those roles across failure domains.
func TestM5Cluster(t *testing.T) {
	if os.Getenv("LAB_M5_E2B_INTEGRATION") != "1" {
		t.Skip("set LAB_M5_E2B_INTEGRATION=1 for live M5 cluster release gate")
	}
	if os.Getenv("E2B_API_KEY") == "" {
		t.Fatal("E2B_API_KEY required")
	}
	q := newM5Quorum(t)
	fleets := make([]*liveFleet, 3)
	for i := range fleets {
		fleets[i] = newLiveFleet(t)
		fleets[i].start(t, "m5-initial", false)
	}
	build := fleets[0]
	build.build(t, "clusterd", "./cmd/lab-clusterd", false)
	build.build(t, "node-agent", "./cmd/lab-node-agent", false)
	common := []string{"--execute", "--cluster", "m5-verify", "--etcd", q.endpoints, "--pki", q.pki, "--lease-ttl", "6"}
	agents := make([]*liveProcess, 3)
	agentArgs := make([][]string, 3)
	for i := range agents {
		address := availablePort(t)
		id := fmt.Sprintf("node-%c", 'a'+i)
		agentArgs[i] = append(append([]string{}, common...), "--id", id, "--listen", address, "--advertise", "https://"+address, "--state-file", filepath.Join(q.dir, id, "node.json"), "--fleet-socket", fleets[i].socket, "--labels", "pin="+id, "--test-hold-claim-job", "pending-move")
		agents[i] = startM5Process(t, filepath.Join(build.dir, "node-agent"), filepath.Join(q.dir, id+".log"), agentArgs[i]...)
	}
	coordinators := map[string]*liveProcess{}
	var servers []string
	for i := 0; i < 2; i++ {
		address := availablePort(t)
		id := fmt.Sprintf("coord-%c", 'a'+i)
		args := append(append([]string{}, common...), "--id", id, "--listen", address, "--advertise", "https://"+address)
		servers = append(servers, "https://"+address)
		coordinators[id] = startM5Process(t, filepath.Join(build.dir, "clusterd"), filepath.Join(q.dir, id+".log"), args...)
	}
	ctx := context.Background()
	tlsConfig, err := clustertls.Config(filepath.Join(q.pki, "cluster-ca.pem"), filepath.Join(q.pki, "operator.pem"), filepath.Join(q.pki, "operator.key"))
	if err != nil {
		t.Fatal(err)
	}
	client := cluster.Client{HTTP: clustertls.ClientRole(tlsConfig, "coordinator"), Servers: servers}
	defer client.HTTP.CloseIdleConnections()
	ready := func() bool { s, err := q.repo.Read(ctx); return err == nil && len(s.Live) == 3 && s.Leader.ID != "" }
	eventuallyM5(t, "three nodes and elected coordinator", ready)
	request := func(id string, pin bool) cluster.Request {
		r := cluster.Request{ID: id, Tenant: "alpha", Resources: admission.Resources{CPU: 2, MemoryMiB: 2048}, Timeout: 30, Command: "echo marker >> /tmp/" + id + "; wc -l /tmp/" + id, Artifacts: []string{"/tmp/" + id}}
		if pin {
			r.Labels = map[string]string{"pin": "node-a"}
		}
		return r
	}
	submit := func(req cluster.Request) {
		t.Helper()
		if _, _, err := client.Request(ctx, http.MethodPost, "/v1/jobs", req); err != nil {
			t.Fatal(err)
		}
	}
	job := func(id, state string) cluster.Job {
		t.Helper()
		var result cluster.Job
		eventuallyM5(t, id+" "+state, func() bool {
			s, err := q.repo.Read(ctx)
			if err != nil {
				return false
			}
			result = s.Jobs[id]
			return result.State == state
		})
		return result
	}
	artifact := func(id string) {
		t.Helper()
		data, headers, err := client.Request(ctx, http.MethodGet, "/v1/jobs/"+id+"/artifacts/0", nil)
		if err != nil || string(data) != "marker\n" || headers.Get("X-Artifact-SHA256") != taskevidence.Hash(data) {
			t.Fatalf("surviving artifact: %v %q", err, data)
		}
	}
	if !t.Run("pending_assignment_moves_after_worker_crash", func(t *testing.T) {
		submit(request("pending-move", false))
		assigned := job("pending-move", "assigned")
		if assigned.Node != "node-a" {
			t.Fatal("deterministic initial assignment changed", assigned)
		}
		stopM5Process(t, agents[0], true)
		done := job("pending-move", "succeeded")
		if done.Node == "node-a" || done.Epoch <= assigned.Epoch {
			t.Fatal("never-dispatched assignment did not move", done)
		}
		artifact("pending-move")
		if _, err := cluster.NewFleetClient(fleets[0].socket).Job(ctx, assigned.LocalID); !errors.Is(err, cluster.ErrNotFound) {
			t.Fatal("old owner dispatched moved job", err)
		}
		agents[0] = startM5Process(t, filepath.Join(build.dir, "node-agent"), filepath.Join(q.dir, "node-a-rejoin.log"), agentArgs[0]...)
		eventuallyM5(t, "node-a rejoined with same incarnation", ready)
	}) {
		return
	}
	if !t.Run("coordinator_crash_elects_successor", func(t *testing.T) {
		r := request("leader-failover", false)
		r.Command = "sleep 2; " + r.Command
		submit(r)
		job(r.ID, "dispatched")
		s, err := q.repo.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		old := s.Leader
		stopM5Process(t, coordinators[old.ID], true)
		eventuallyM5(t, "new coordinator leader", func() bool { s, err := q.repo.Read(ctx); return err == nil && s.Leader.ID != "" && s.Leader != old })
		job(r.ID, "succeeded")
		artifact(r.ID)
		// Future mutations deliberately target the surviving coordinator once.
		s, _ = q.repo.Read(ctx)
		client.Servers = []string{s.Leader.URL}
	}) {
		return
	}
	if !t.Run("dispatched_owner_loss_requires_original_evidence", func(t *testing.T) {
		eventuallyM5(t, "earlier jobs released their reservations", func() bool {
			s, err := q.repo.Read(ctx)
			if err != nil {
				return false
			}
			for _, j := range s.Jobs {
				if !j.Released {
					return false
				}
			}
			return true
		})
		if err := fleets[0].stop(); err != nil {
			t.Fatal(err)
		}
		encoded, _ := json.Marshal([]any{"m5-verify", "pinned-proof", uint64(1)})
		localID := "m5-" + taskevidence.Hash(encoded)[:32]
		fleets[0].start(t, "m5-proof-crash", false, "--test-crash-after-artifacts-job", localID)
		// The node agent's Unix destination stays fixed across fleetd restarts.
		stopM5Process(t, agents[0], false)
		for i := range agentArgs[0] {
			if agentArgs[0][i] == "--fleet-socket" {
				agentArgs[0][i+1] = fleets[0].socket
			}
		}
		agents[0] = startM5Process(t, filepath.Join(build.dir, "node-agent"), filepath.Join(q.dir, "node-a-proof.log"), agentArgs[0]...)
		eventuallyM5(t, "proof node ready", ready)
		r := request("pinned-proof", true)
		submit(r)
		select {
		case <-fleets[0].process.done:
		case <-time.After(90 * time.Second):
			t.Fatal("durable-artifact crash point not reached")
		}
		var exit *exec.ExitError
		if !errors.As(fleets[0].process.err, &exit) || exit.ExitCode() != 87 {
			t.Fatal("wrong crash outcome", fleets[0].process.err)
		}
		stopM5Process(t, agents[0], true)
		unknown := job(r.ID, "uncertain")
		eventuallyM5(t, "original node heartbeat expired", func() bool { s, err := q.repo.Read(ctx); _, live := s.Live["node-a"]; return err == nil && !live })
		if unknown.Node != "node-a" || unknown.Epoch != 1 || unknown.Released {
			t.Fatal("unknown execution moved or reservation was released", unknown)
		}
		for _, f := range fleets[1:] {
			if _, err := cluster.NewFleetClient(f.socket).Job(ctx, localID); !errors.Is(err, cluster.ErrNotFound) {
				t.Fatal("dispatched command appeared on another node", err)
			}
		}
		fleets[0].start(t, "m5-proof-recovery", false)
		for i := range agentArgs[0] {
			if agentArgs[0][i] == "--fleet-socket" {
				agentArgs[0][i+1] = fleets[0].socket
			}
		}
		agents[0] = startM5Process(t, filepath.Join(build.dir, "node-agent"), filepath.Join(q.dir, "node-a-recover.log"), agentArgs[0]...)
		eventuallyM5(t, "original journal node restored", ready)
		if _, _, err := client.Request(ctx, http.MethodPost, "/v1/jobs/"+r.ID+"/recover", struct{}{}); err != nil {
			t.Fatal(err)
		}
		done := job(r.ID, "succeeded")
		if done.Epoch != unknown.Epoch || done.LocalID != localID || len(done.Replicas) < 2 {
			t.Fatal("recovery changed execution or lacks independent receipts", done)
		}
		// Exact request replay returns the cached identity even after the original VM/node disappear.
		if _, _, err := client.Request(ctx, http.MethodPost, "/v1/jobs", r); err != nil {
			t.Fatal(err)
		}
		eventuallyM5(t, "recovered VM confirmed deleted and reservation released", func() bool { s, err := q.repo.Read(ctx); return err == nil && s.Jobs[r.ID].Released })
		stopM5Process(t, agents[0], true)
		if err := fleets[0].stop(); err != nil {
			t.Fatal(err)
		}
		artifact(r.ID)
		data, _, err := client.Request(ctx, http.MethodGet, "/v1/jobs/"+r.ID+"/result", nil)
		if err != nil || !strings.Contains(string(data), "1 /tmp/pinned-proof") {
			t.Fatal("verified one-marker result unavailable", err)
		}
	}) {
		return
	}
	if _, _, err := client.Request(ctx, http.MethodPost, "/v1/drain", struct{}{}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := client.Request(ctx, http.MethodPost, "/v1/jobs", request("after-drain", false)); err == nil {
		t.Fatal("cluster drain accepted new work")
	}
	t.Log("PASS: real E2B process failover, pinned uncertain execution, evidence-only recovery, replicated artifacts; selected-journal cleanup follows")
}
