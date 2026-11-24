package cluster

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/artifactstore"
	"agent-runtime-lab/src/coordkv"
	"agent-runtime-lab/src/fleetdaemon"
	"agent-runtime-lab/src/taskevidence"
	"agent-runtime-lab/src/taskjournal"
)

// memoryKV models authoritative lease deletion and atomic revision compares.
// No clock/heartbeat on the worker can extend a revoked server-side lease.
type memoryKV struct {
	mu              sync.Mutex
	revision, lease int64
	data            map[string]coordkv.Value
	leases          map[int64]bool
	down            bool
}

func newMemory() *memoryKV {
	return &memoryKV{data: map[string]coordkv.Value{}, leases: map[int64]bool{}}
}
func (m *memoryKV) Range(ctx context.Context, prefix string) ([]coordkv.Value, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.down {
		return nil, coordkv.ErrUnconfirmed
	}
	var out []coordkv.Value
	for key, v := range m.data {
		if strings.HasPrefix(key, prefix) {
			out = append(out, v)
		}
	}
	return out, ctx.Err()
}
func (m *memoryKV) Txn(ctx context.Context, cmp []coordkv.Compare, puts []coordkv.Put) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.down {
		return false, coordkv.ErrUnconfirmed
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	for _, c := range cmp {
		v := m.data[c.Key]
		if c.Mod != nil {
			if v.Mod != *c.Mod {
				return false, nil
			}
		} else if !bytes.Equal(v.Data, c.Data) {
			return false, nil
		}
	}
	for _, p := range puts {
		if p.Lease != 0 && !m.leases[p.Lease] {
			return false, errors.New("lease expired")
		}
	}
	m.revision++
	for _, p := range puts {
		v := m.data[p.Key]
		if v.Create == 0 {
			v.Create = m.revision
		}
		v.Key = []byte(p.Key)
		v.Mod = m.revision
		v.Data = append([]byte{}, p.Data...)
		v.Lease = p.Lease
		m.data[p.Key] = v
	}
	return true, nil
}
func (m *memoryKV) Grant(context.Context, int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lease++
	m.leases[m.lease] = true
	return m.lease, nil
}
func (m *memoryKV) KeepAlive(_ context.Context, id int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.down || !m.leases[id] {
		return coordkv.ErrUnconfirmed
	}
	return nil
}
func (m *memoryKV) Revoke(_ context.Context, id int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.leases, id)
	for key, v := range m.data {
		if v.Lease == id {
			delete(m.data, key)
			m.revision++
		}
	}
	return nil
}

var shape = admission.Resources{CPU: 2, MemoryMiB: 2048}

func testRepo(t *testing.T) (*Repository, *memoryKV, Leader) {
	t.Helper()
	kv := newMemory()
	r, _ := NewRepository(kv, "test")
	if err := r.Bootstrap(context.Background(), DefaultConfig("test")); err != nil {
		t.Fatal(err)
	}
	l, _, ok, err := r.Campaign(context.Background(), "leader-a", "https://leader-a:7443", 6)
	if err != nil || !ok {
		t.Fatal(err)
	}
	return r, kv, l
}
func snapshot(t *testing.T, r *Repository) Snapshot {
	t.Helper()
	s, err := r.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func register(t *testing.T, r *Repository, name string, labels map[string]string) (Node, Live, int64) {
	t.Helper()
	n := Node{ID: name, Incarnation: Token(), URL: "https://" + name + ":7443", Capacity: shape, Template: shape, Labels: labels}
	l := Live{Node: n.ID, Incarnation: n.Incarnation, Token: Token()}
	lease, err := r.KV.Grant(context.Background(), 6)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Register(context.Background(), n, l, lease); err != nil {
		t.Fatal(err)
	}
	return n, l, lease
}
func submit(t *testing.T, r *Repository, id string) Request {
	t.Helper()
	q := Request{ID: id, Tenant: "alpha", Command: "echo marker", Timeout: 30, Resources: shape, Artifacts: []string{}}
	if _, _, err := r.Submit(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	return q
}
func schedule(t *testing.T, r *Repository, l Leader) {
	t.Helper()
	if _, err := r.Schedule(context.Background(), l); err != nil {
		t.Fatal(err)
	}
}

func TestOwnershipAndSafeFailover(t *testing.T) {
	r, kv, l := testRepo(t)
	_, la, leaseA := register(t, r, "node-a", nil)
	register(t, r, "node-b", nil)
	request := submit(t, r, "once")
	schedule(t, r, l)
	first := snapshot(t, r).Jobs["once"]
	if first.Node != "node-a" {
		t.Fatal(first)
	}
	kv.Revoke(context.Background(), leaseA)
	schedule(t, r, l)
	schedule(t, r, l)
	second := snapshot(t, r).Jobs["once"]
	if second.Node != "node-b" || second.Epoch <= first.Epoch {
		t.Fatal("never-dispatched work did not move", second)
	}
	if ok, _ := r.Claim(context.Background(), "once", first.Epoch, la); ok {
		t.Fatal("stale owner claimed moved assignment")
	}
	lbData := snapshot(t, r).Live["node-b"]
	var lb Live
	decode(lbData.Data, &lb)
	if ok, err := r.Claim(context.Background(), "once", second.Epoch, lb); err != nil || !ok {
		t.Fatal(err)
	}
	kv.Revoke(context.Background(), lbData.Lease)
	schedule(t, r, l)
	unknown := snapshot(t, r).Jobs["once"]
	if unknown.State != "uncertain" || unknown.Node != "node-b" || unknown.Released || usage(snapshot(t, r), "node-b", "") != shape {
		t.Fatal("lost heartbeat released or moved dispatched work", unknown)
	}
	for range 5 {
		schedule(t, r, l)
	}
	if got := snapshot(t, r).Jobs["once"]; got.Epoch != second.Epoch {
		t.Fatal("unknown execution was replayed")
	}
	if _, created, err := r.Submit(context.Background(), request); err != nil || created {
		t.Fatal("exact replay not cached", err)
	}
	request.Command = "echo different"
	if _, _, err := r.Submit(context.Background(), request); !errors.Is(err, ErrConflict) {
		t.Fatal("ID accepted different command")
	}
	s := snapshot(t, r)
	kv.Revoke(context.Background(), s.LeaderValue.Lease)
	next, _, won, err := r.Campaign(context.Background(), "leader-b", "https://leader-b:7443", 6)
	if err != nil || !won {
		t.Fatal(err)
	}
	if _, err = r.Schedule(context.Background(), l); !errors.Is(err, ErrFenced) {
		t.Fatal("stale leader accepted", err)
	}
	if ok, err := r.Commit(context.Background(), s, &l, nil); err != nil || ok {
		if !errors.Is(err, ErrFenced) {
			t.Fatal("snapshot fence failed", err)
		}
	}
	schedule(t, r, next)
}

func TestGlobalQuotaLabelsDrainAndQuorum(t *testing.T) {
	r, kv, l := testRepo(t)
	register(t, r, "a", map[string]string{"pool": "slow"})
	register(t, r, "b", map[string]string{"pool": "fast"})
	q := Request{ID: "labelled", Tenant: "alpha", Command: "true", Timeout: 30, Resources: shape, Labels: map[string]string{"pool": "fast"}}
	if _, _, err := r.Submit(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	schedule(t, r, l)
	if snapshot(t, r).Jobs[q.ID].Node != "b" {
		t.Fatal("labels ignored")
	}
	if err := r.DrainNode(context.Background(), "b"); err != nil {
		t.Fatal(err)
	}
	schedule(t, r, l)
	if snapshot(t, r).Jobs[q.ID].State != "queued" {
		t.Fatal("undispatched drain did not return assignment")
	}
	kv.mu.Lock()
	kv.down = true
	kv.mu.Unlock()
	if _, err := r.Schedule(context.Background(), l); err == nil {
		t.Fatal("scheduled without quorum")
	}
	kv.mu.Lock()
	kv.down = false
	kv.mu.Unlock()
	if _, err := r.Cancel(context.Background(), q.ID); err != nil {
		t.Fatal(err)
	}
	// Concurrent admission uses one gate, so tenant logical and per-node budgets cannot oversubscribe.
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := Request{ID: string(rune('A' + i)), Tenant: "alpha", Command: "true", Timeout: 30, Resources: shape}
			r.Submit(context.Background(), req)
		}(i)
	}
	wg.Wait()
	s := snapshot(t, r)
	count := 0
	for _, j := range s.Jobs {
		if logical(j) {
			count++
		}
	}
	if count > 8 {
		t.Fatal("tenant admission oversubscribed", count)
	}
	for range 10 {
		schedule(t, r, l)
	}
	if usage(snapshot(t, r), "a", "") != shape {
		t.Fatal("node capacity accounting failed")
	}
	old := s.Nodes["a"]
	old.Incarnation = Token()
	lease, _ := kv.Grant(context.Background(), 6)
	if err := r.Register(context.Background(), old, Live{Node: "a", Incarnation: old.Incarnation, Token: Token()}, lease); err == nil {
		t.Fatal("new disk adopted old node ownership")
	}
}

type deadFleet struct{ vmCalls, jobCalls int }

func (*deadFleet) Capabilities(context.Context) (Capabilities, error) { return Capabilities{}, nil }
func (*deadFleet) Status(context.Context) (fleetdaemon.Status, error) {
	return fleetdaemon.Status{}, nil
}
func (f *deadFleet) SubmitVM(context.Context, string, string) error {
	f.vmCalls++
	return io.ErrUnexpectedEOF
}
func (*deadFleet) DeleteVM(context.Context, string) error { return nil }
func (*deadFleet) Job(context.Context, string) (taskjournal.Entry, error) {
	return taskjournal.Entry{}, ErrNotFound
}
func (f *deadFleet) SubmitJob(context.Context, fleetdaemon.JobRequest) error {
	f.jobCalls++
	return io.ErrUnexpectedEOF
}
func (*deadFleet) Cancel(context.Context, string) error                  { return nil }
func (*deadFleet) Recover(context.Context, string) error                 { return nil }
func (*deadFleet) Artifact(context.Context, string, int) ([]byte, error) { return nil, ErrNotFound }

func TestLocalOutboxNeverReplaysAmbiguousVM(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "private", "state.json")
	state, err := OpenLocal(statePath, "test", "node-a")
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	// State incarnation is independently persisted; set the registered identity to it before claiming.
	r2, _, l2 := testRepo(t)
	n := Node{ID: "node-a", Incarnation: state.Incarnation(), URL: "https://node-a:7443", Capacity: shape, Template: shape}
	live := Live{Node: n.ID, Incarnation: n.Incarnation, Token: Token()}
	lease, _ := r2.KV.Grant(context.Background(), 6)
	if err = r2.Register(context.Background(), n, live, lease); err != nil {
		t.Fatal(err)
	}
	submit(t, r2, "lost-ack")
	schedule(t, r2, l2)
	job := snapshot(t, r2).Jobs["lost-ack"]
	if err = state.Prepare(job); err != nil {
		t.Fatal(err)
	}
	if ok, err := r2.Claim(context.Background(), job.Request.ID, job.Epoch, live); !ok || err != nil {
		t.Fatal(err)
	}
	fleet := &deadFleet{}
	a := Agent{Repo: r2, State: state, Fleet: fleet, Node: n}
	a.setSession(live, true)
	for range 4 {
		if err = a.Step(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if fleet.vmCalls != 1 || fleet.jobCalls != 0 || snapshot(t, r2).Jobs["lost-ack"].State != "uncertain" {
		t.Fatal("ambiguous create replayed", fleet)
	}
	if _, err = OpenLocal(state.path, "test", "node-a"); err == nil {
		t.Fatal("second local owner acquired journal")
	}
	incarnation := state.Incarnation()
	state.Close()
	reopened, err := OpenLocal(statePath, "test", "node-a")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	a.State = reopened
	if reopened.Incarnation() != incarnation {
		t.Fatal("restart changed node incarnation")
	}
	if err = a.Step(context.Background()); err != nil || fleet.vmCalls != 1 {
		t.Fatal("restart replayed uncertain RPC", err)
	}
	if err = reopened.Prepare(Job{Request: Request{ID: job.Request.ID}, Epoch: job.Epoch + 1, LocalID: localID("test", job.Request.ID, job.Epoch+1)}); err == nil {
		t.Fatal("sent intent permitted new epoch")
	}
}

type routeTransport map[string]http.Handler

func (r routeTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	h := r[req.URL.Hostname()]
	if h == nil {
		return nil, errors.New("node offline")
	}
	out := httptest.NewRecorder()
	h.ServeHTTP(out, req)
	return out.Result(), nil
}

func TestReplicatedArtifactsSurviveOriginalNodeLoss(t *testing.T) {
	r, kv, l := testRepo(t)
	na, la, _ := register(t, r, "node-a", nil)
	nb, _, _ := register(t, r, "node-b", nil)
	nc, _, _ := register(t, r, "node-c", nil)
	q := Request{ID: "artifact", Tenant: "alpha", Command: "echo marker", Timeout: 30, Resources: shape, Artifacts: []string{"/tmp/marker"}}
	if _, _, err := r.Submit(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	schedule(t, r, l)
	job := snapshot(t, r).Jobs[q.ID]
	if ok, err := r.Claim(context.Background(), q.ID, job.Epoch, la); err != nil || !ok {
		t.Fatal(err)
	}
	job = snapshot(t, r).Jobs[q.ID]
	intent, _ := taskevidence.NewIntent(taskevidence.Hash([]byte("helper")), q.Artifacts)
	identity := taskevidence.Identity{TaskID: job.LocalID, WorkloadID: job.LocalID, SandboxID: "sandbox-a", CommandSHA256: job.CommandHash, Timeout: q.Timeout, Intent: intent}
	data := []byte("marker\n")
	envelope, err := taskevidence.Seal(taskevidence.Manifest{Identity: identity, CompletedAt: time.Now().UTC(), Stdout: "one\n", StdoutSHA256: taskevidence.Hash([]byte("one\n")), StderrSHA256: taskevidence.Hash(nil), Artifacts: []taskevidence.Artifact{{Path: q.Artifacts[0], Size: int64(len(data)), SHA256: taskevidence.Hash(data)}}})
	if err != nil {
		t.Fatal(err)
	}
	job.State = "collecting"
	job.Identity = &identity
	job.ManifestHash = envelope.SHA256
	job.Accepted = true
	job.Request.Command = ""
	if ok, err := r.UpdateOwned(context.Background(), job, la); err != nil || !ok {
		t.Fatal(err)
	}
	routes := routeTransport{}
	stores := map[string]*artifactstore.Store{}
	for _, n := range []Node{na, nb, nc} {
		store, err := artifactstore.Open(filepath.Join(t.TempDir(), "archive"), 16<<20)
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		stores[n.ID] = store
		a := &Agent{Repo: r, Archive: store, Node: n, Fleet: &deadFleet{}}
		routes[n.ID] = a.Handler()
	}
	if err := stores[na.ID].Put(context.Background(), identity, envelope, func(context.Context, taskevidence.Artifact) ([]byte, error) { return data, nil }); err != nil {
		t.Fatal(err)
	}
	c := Coordinator{Repo: r, Client: &http.Client{Transport: routes}}
	for range 2 {
		if err := c.Replicate(context.Background(), l); err != nil {
			t.Fatal(err)
		}
	}
	done := snapshot(t, r).Jobs[q.ID]
	if done.State != "succeeded" || len(done.Replicas) != 2 {
		t.Fatal(done)
	}
	delete(routes, na.ID)
	lease := snapshot(t, r).Live[na.ID].Lease
	kv.Revoke(context.Background(), lease)
	if transfer, err := Fetch(context.Background(), c.Client, snapshot(t, r), done); err != nil || len(transfer.Files) != 1 || !bytes.Equal(transfer.Files[0], data) {
		t.Fatal("surviving copy unavailable", err)
	}
	if err := c.Replicate(context.Background(), l); err != nil {
		t.Fatal(err)
	}
	repaired := snapshot(t, r).Jobs[q.ID]
	if len(repaired.Replicas) != 3 {
		t.Fatal("artifact replicas not repaired", repaired)
	}
	// The worker cannot undo a published result with an unavailable-fleet report.
	a := Agent{Repo: r}
	if err := a.unknown(context.Background(), repaired, la, "lost connection"); err != nil {
		t.Fatal(err)
	}
	if snapshot(t, r).Jobs[q.ID].State != "succeeded" {
		t.Fatal("completion regressed")
	}
	if err := SaveDownload(filepath.Join(t.TempDir(), "bad"), []byte("bad"), taskevidence.Hash([]byte("good"))); err == nil {
		t.Fatal("corrupt download published")
	}
}
