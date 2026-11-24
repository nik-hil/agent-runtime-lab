package tests

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/artifactstore"
	"agent-runtime-lab/src/cluster"
	"agent-runtime-lab/src/clustertls"
	"agent-runtime-lab/src/fleet"
	"agent-runtime-lab/src/fleetdaemon"
	"agent-runtime-lab/src/m4exec"
	"agent-runtime-lab/src/taskevidence"
	"agent-runtime-lab/src/taskjournal"
)

// This backend deliberately synthesizes a task result. The test proves the
// real etcd + TLS scheduling/archive protocol, not sandbox command execution.
type protocolFleet struct {
	mu    sync.Mutex
	vms   map[string]fleet.JobStatus
	jobs  map[string]taskjournal.Entry
	calls int
}

func (f *protocolFleet) Capabilities(context.Context) (cluster.Capabilities, error) {
	return cluster.Capabilities{Capacity: admission.Resources{CPU: 2, MemoryMiB: 2048}, Template: admission.Resources{CPU: 2, MemoryMiB: 2048}, Helper: taskevidence.Hash([]byte("protocol-helper")), Workers: 1}, nil
}
func (f *protocolFleet) Status(context.Context) (fleetdaemon.Status, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	jobs := map[string]fleet.JobStatus{}
	for id, j := range f.vms {
		jobs[id] = j
	}
	return fleetdaemon.Status{Fleet: fleet.Status{Jobs: jobs}}, nil
}
func (f *protocolFleet) SubmitVM(_ context.Context, id, tenant string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, exists := f.vms[id]; exists {
		return cluster.ErrConflict
	}
	f.vms[id] = fleet.JobStatus{Entry: admission.Entry{Request: admission.Request{ID: id, Tenant: tenant, Resources: admission.Resources{CPU: 2, MemoryMiB: 2048}}, State: admission.Running}, Workload: fleet.Workload{SandboxID: "protocol-" + id, Desired: "running"}}
	return nil
}
func (f *protocolFleet) DeleteVM(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	j := f.vms[id]
	j.Entry.State = admission.Deleted
	j.Workload.Desired = "deleted"
	f.vms[id] = j
	return nil
}
func (f *protocolFleet) Job(_ context.Context, id string) (taskjournal.Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	j, ok := f.jobs[id]
	if !ok {
		return j, cluster.ErrNotFound
	}
	return j, nil
}
func (f *protocolFleet) SubmitJob(_ context.Context, r fleetdaemon.JobRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if _, exists := f.jobs[r.ID]; exists {
		return cluster.ErrConflict
	}
	intent, err := taskevidence.NewIntent(taskevidence.Hash([]byte("protocol-helper")), r.Artifacts)
	if err != nil {
		return err
	}
	identity := taskevidence.Identity{TaskID: r.ID, WorkloadID: r.WorkloadID, SandboxID: f.vms[r.WorkloadID].Workload.SandboxID, CommandSHA256: taskevidence.Hash([]byte(r.Command)), Timeout: r.Timeout, Intent: intent}
	manifest := taskevidence.Manifest{Identity: identity, CompletedAt: time.Now().UTC(), Stdout: "protocol completion\n", StdoutSHA256: taskevidence.Hash([]byte("protocol completion\n")), StderrSHA256: taskevidence.Hash(nil), Artifacts: []taskevidence.Artifact{}}
	for _, p := range intent.Artifacts {
		manifest.Artifacts = append(manifest.Artifacts, taskevidence.Artifact{Path: p, Size: 7, SHA256: taskevidence.Hash([]byte("marker\n"))})
	}
	evidence, err := taskevidence.Seal(manifest)
	if err != nil {
		return err
	}
	f.jobs[r.ID] = taskjournal.Entry{ID: r.ID, WorkloadID: r.WorkloadID, SandboxID: identity.SandboxID, CommandHash: identity.CommandSHA256, Timeout: r.Timeout, State: taskjournal.Succeeded, Async: true, EvidenceIntent: &intent, Evidence: &evidence, ArtifactsStored: true, Result: &m4exec.Result{SandboxID: identity.SandboxID, ExitCode: 0, Stdout: manifest.Stdout}}
	return nil
}
func (*protocolFleet) Cancel(context.Context, string) error  { return nil }
func (*protocolFleet) Recover(context.Context, string) error { return nil }
func (*protocolFleet) Artifact(context.Context, string, int) ([]byte, error) {
	return []byte("marker\n"), nil
}

func TestM5Protocol(t *testing.T) {
	if os.Getenv("LAB_CLUSTER_INTEGRATION") != "1" {
		t.Skip("set LAB_CLUSTER_INTEGRATION=1 for real etcd/TLS node protocol")
	}
	q := newM5Quorum(t)
	ctx := context.Background()
	if err := q.repo.Bootstrap(ctx, cluster.DefaultConfig("m5-verify")); err != nil {
		t.Fatal(err)
	}
	load := func(role string) *http.Client {
		tlsConfig, err := clustertls.Config(filepath.Join(q.pki, "cluster-ca.pem"), filepath.Join(q.pki, role+".pem"), filepath.Join(q.pki, role+".key"))
		if err != nil {
			t.Fatal(err)
		}
		return clustertls.ClientRole(tlsConfig, "node")
	}
	stopNode := make([]context.CancelFunc, 3)
	nodeServers := make([]*httptest.Server, 3)
	backends := make([]*protocolFleet, 3)
	for i := range stopNode {
		id := fmt.Sprintf("node-%c", 'a'+i)
		dir := filepath.Join(q.dir, id)
		state, err := cluster.OpenLocal(filepath.Join(dir, "node.json"), "m5-verify", id)
		if err != nil {
			t.Fatal(err)
		}
		archive, err := artifactstore.Open(filepath.Join(dir, "archive"), 32<<20)
		if err != nil {
			t.Fatal(err)
		}
		f := &protocolFleet{vms: map[string]fleet.JobStatus{}, jobs: map[string]taskjournal.Entry{}}
		backends[i] = f
		a := &cluster.Agent{Repo: q.repo, State: state, Archive: archive, Fleet: f, Node: cluster.Node{ID: id, Incarnation: state.Incarnation()}, TTL: 6}
		tlsConfig, err := clustertls.Config(filepath.Join(q.pki, "cluster-ca.pem"), filepath.Join(q.pki, "node.pem"), filepath.Join(q.pki, "node.key"))
		if err != nil {
			t.Fatal(err)
		}
		server := httptest.NewUnstartedServer(a.Handler())
		server.TLS = clustertls.Server(tlsConfig, "coordinator")
		server.StartTLS()
		a.Node.URL = server.URL
		nodeServers[i] = server
		run, cancel := context.WithCancel(ctx)
		stopNode[i] = cancel
		done := make(chan error, 1)
		go func() { done <- a.Run(run) }()
		t.Cleanup(func() {
			cancel()
			if err := <-done; err != nil {
				t.Error(err)
			}
			server.Close()
			archive.Close()
			state.Close()
		})
	}
	coordStops := map[string]context.CancelFunc{}
	var coordURLs []string
	for i := 0; i < 2; i++ {
		id := fmt.Sprintf("coord-%d", i)
		c := &cluster.Coordinator{Repo: q.repo, ID: id, TTL: 6, Client: load("coordinator")}
		tlsConfig, err := clustertls.Config(filepath.Join(q.pki, "cluster-ca.pem"), filepath.Join(q.pki, "coordinator.pem"), filepath.Join(q.pki, "coordinator.key"))
		if err != nil {
			t.Fatal(err)
		}
		server := httptest.NewUnstartedServer(c.Handler())
		server.TLS = clustertls.Server(tlsConfig, "operator", "coordinator")
		server.StartTLS()
		c.URL = server.URL
		coordURLs = append(coordURLs, server.URL)
		run, cancel := context.WithCancel(ctx)
		coordStops[id] = cancel
		done := make(chan error, 1)
		go func() { done <- c.Run(run) }()
		t.Cleanup(func() {
			cancel()
			if err := <-done; err != nil {
				t.Error(err)
			}
			server.Close()
		})
	}
	eventuallyM5(t, "real node leases and coordinator", func() bool { s, err := q.repo.Read(ctx); return err == nil && len(s.Live) == 3 && s.Leader.ID != "" })
	operatorTLS, err := clustertls.Config(filepath.Join(q.pki, "cluster-ca.pem"), filepath.Join(q.pki, "operator.pem"), filepath.Join(q.pki, "operator.key"))
	if err != nil {
		t.Fatal(err)
	}
	client := cluster.Client{HTTP: clustertls.ClientRole(operatorTLS, "coordinator"), Servers: coordURLs}
	defer client.HTTP.CloseIdleConnections()
	r := cluster.Request{ID: "protocol-once", Tenant: "alpha", Command: "protocol-only", Timeout: 30, Resources: admission.Resources{CPU: 2, MemoryMiB: 2048}, Artifacts: []string{"/tmp/protocol"}}
	if _, _, err := client.Request(ctx, http.MethodPost, "/v1/jobs", r); err != nil {
		t.Fatal(err)
	}
	var done cluster.Job
	eventuallyM5(t, "replicated protocol completion and confirmed reservation release", func() bool {
		s, err := q.repo.Read(ctx)
		done = s.Jobs[r.ID]
		return err == nil && done.State == "succeeded" && done.Released
	})
	if len(done.Replicas) != 2 {
		t.Fatal("missing replica receipts")
	}
	if _, _, err := client.Request(ctx, http.MethodPost, "/v1/jobs", r); err != nil {
		t.Fatal(err)
	}
	for i := range stopNode {
		if done.Node == fmt.Sprintf("node-%c", 'a'+i) {
			stopNode[i]()
			nodeServers[i].Close()
		}
	}
	s, err := q.repo.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	old := s.Leader
	coordStops[old.ID]()
	eventuallyM5(t, "standby coordinator elected", func() bool { s, err := q.repo.Read(ctx); return err == nil && s.Leader.ID != "" && s.Leader != old })
	data, headers, err := client.Request(ctx, http.MethodGet, "/v1/jobs/"+r.ID+"/artifacts/0", nil)
	if err != nil || string(data) != "marker\n" || headers.Get("X-Artifact-SHA256") != taskevidence.Hash(data) {
		t.Fatal("archive not served after owner loss", err)
	}
	calls := 0
	for _, f := range backends {
		f.mu.Lock()
		calls += f.calls
		f.mu.Unlock()
	}
	if calls != 1 {
		t.Fatal("protocol admission repeated", calls)
	}
	// Operator certificate is rejected by the node API even for read requests.
	if response, err := clustertls.Client(operatorTLS).Get(nodeServers[1].URL + "/v1/node/status"); err == nil {
		response.Body.Close()
		t.Fatal("operator certificate accepted at coordinator-only node endpoint")
	}
}
