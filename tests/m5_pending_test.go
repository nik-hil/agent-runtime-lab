package tests

import (
	"context"
	"flag"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/artifactstore"
	"agent-runtime-lab/src/cluster"
	"agent-runtime-lab/src/clustertls"
	"agent-runtime-lab/src/fleet"
	"agent-runtime-lab/src/taskjournal"
)

func pendingProtocolNode(t *testing.T, q *m5Quorum, id string) (*protocolFleet, func()) {
	t.Helper()
	f := flag.NewFlagSet("pending-node", flag.ContinueOnError)
	hold := f.String("test-hold-claim-job", "", "shared live fixture fault")
	if err := f.Parse(m5PendingFaultArgs(id)); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(q.dir, id)
	state, err := cluster.OpenLocal(filepath.Join(dir, "node.json"), "m5-verify", id)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { state.Close() })
	archive, err := artifactstore.Open(filepath.Join(dir, "archive"), 32<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { archive.Close() })
	backend := &protocolFleet{vms: map[string]fleet.JobStatus{}, jobs: map[string]taskjournal.Entry{}}
	a := &cluster.Agent{Repo: q.repo, State: state, Archive: archive, Fleet: backend, Node: cluster.Node{ID: id, Incarnation: state.Incarnation()}, TTL: 6, HoldClaim: *hold}
	tlsConfig, err := clustertls.Config(filepath.Join(q.pki, "cluster-ca.pem"), filepath.Join(q.pki, "node.pem"), filepath.Join(q.pki, "node.key"))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(a.Handler())
	server.TLS = clustertls.Server(tlsConfig, "coordinator")
	server.StartTLS()
	a.Node.URL = server.URL
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			if err := <-done; err != nil {
				t.Error(err)
			}
			server.Close()
		})
	}
	t.Cleanup(stop)
	return backend, stop
}

// Reproduce the live fixture's pending-move scenario with real etcd/TLS and
// the same fault arguments. Synthetic fleet results do not prove E2B execution.
func TestM5PendingFailover(t *testing.T) {
	if os.Getenv("LAB_CLUSTER_INTEGRATION") != "1" {
		t.Skip("set LAB_CLUSTER_INTEGRATION=1 for pending reassignment regression")
	}
	q := newM5Quorum(t)
	ctx := context.Background()
	if err := q.repo.Bootstrap(ctx, cluster.DefaultConfig("m5-verify")); err != nil {
		t.Fatal(err)
	}
	backends := make([]*protocolFleet, 3)
	stops := make([]func(), 3)
	for i := range backends {
		backends[i], stops[i] = pendingProtocolNode(t, q, fmt.Sprintf("node-%c", 'a'+i))
	}
	eventuallyM5(t, "three pending-failover workers", func() bool {
		s, err := q.repo.Read(ctx)
		return err == nil && len(s.Live) == 3
	})
	leader, _, won, err := q.repo.Campaign(ctx, "pending-coordinator", "https://127.0.0.1:7443", 120)
	if err != nil || !won {
		t.Fatalf("campaign: won=%t err=%v", won, err)
	}
	r := cluster.Request{ID: "pending-move", Tenant: "alpha", Command: "protocol-only", Timeout: 30, Resources: admission.Resources{CPU: 2, MemoryMiB: 2048}, Artifacts: []string{"/tmp/pending-move"}}
	if _, _, err := q.repo.Submit(ctx, r); err != nil {
		t.Fatal(err)
	}
	var assigned cluster.Job
	eventuallyM5(t, "node-a held before dispatch", func() bool {
		if _, err := q.repo.Schedule(ctx, leader); err != nil {
			return false
		}
		s, err := q.repo.Read(ctx)
		assigned = s.Jobs[r.ID]
		return err == nil && assigned.State == "assigned" && assigned.Node == "node-a"
	})
	stops[0]()
	tlsConfig, err := clustertls.Config(filepath.Join(q.pki, "cluster-ca.pem"), filepath.Join(q.pki, "coordinator.pem"), filepath.Join(q.pki, "coordinator.key"))
	if err != nil {
		t.Fatal(err)
	}
	c := &cluster.Coordinator{Repo: q.repo, Client: clustertls.ClientRole(tlsConfig, "node")}
	defer c.Client.CloseIdleConnections()
	var completed cluster.Job
	eventuallyM5(t, "surviving worker claims and replicates pending-move", func() bool {
		if _, err := q.repo.Schedule(ctx, leader); err != nil {
			return false
		}
		if err := c.Replicate(ctx, leader); err != nil {
			return false
		}
		s, err := q.repo.Read(ctx)
		completed = s.Jobs[r.ID]
		return err == nil && completed.State == "succeeded" && completed.Released
	})
	if completed.Node == "node-a" || completed.Epoch <= assigned.Epoch || len(completed.Replicas) < 2 {
		t.Fatal("assignment did not complete on a surviving owner", completed)
	}
	calls := 0
	for i, backend := range backends {
		backend.mu.Lock()
		count, vms := backend.calls, len(backend.vms)
		backend.mu.Unlock()
		if i == 0 && (count != 0 || vms != 0) {
			t.Fatal("held original owner performed a provider mutation")
		}
		calls += count
	}
	if calls != 1 {
		t.Fatalf("expected one command admission, got %d", calls)
	}
}
