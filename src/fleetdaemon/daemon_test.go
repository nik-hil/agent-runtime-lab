package fleetdaemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/fleet"
	"agent-runtime-lab/src/lifecycle"
)

type runtime struct {
	mu        sync.Mutex
	states    map[string]lifecycle.ObservedState
	keys      map[string]string
	creates   int
	loseReply bool
}

func newRuntime() *runtime {
	return &runtime{states: map[string]lifecycle.ObservedState{}, keys: map[string]string{}}
}

func (b *runtime) Create(_ context.Context, key string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.creates++
	id := "vm-" + key[len(key)-10:]
	b.keys[id] = key
	b.states[id] = lifecycle.Running
	if b.loseReply {
		return "", context.DeadlineExceeded
	}
	return id, nil
}

func (b *runtime) Describe(_ context.Context, id string) (fleet.Details, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	st, ok := b.states[id]
	if !ok {
		return fleet.Details{State: lifecycle.Absent}, nil
	}
	return fleet.Details{State: st, Resources: admission.Resources{CPU: 1, MemoryMiB: 256}}, nil
}

func (b *runtime) Observe(ctx context.Context, id string) (lifecycle.ObservedState, error) {
	info, err := b.Describe(ctx, id)
	return info.State, err
}

func (b *runtime) Pause(_ context.Context, id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.states[id] != lifecycle.Running {
		return errors.New("VM not running")
	}
	b.states[id] = lifecycle.Paused
	return nil
}

func (b *runtime) Resume(_ context.Context, id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.states[id] != lifecycle.Paused {
		return errors.New("VM not paused")
	}
	b.states[id] = lifecycle.Running
	return nil
}

func (b *runtime) Delete(_ context.Context, id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.states, id)
	return nil
}

func (b *runtime) FindByLogicalKey(_ context.Context, key string) ([]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var ids []string
	for id, storedKey := range b.keys {
		if storedKey == key && b.states[id] != "" {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

func testConfig() fleet.Config {
	cap := admission.Resources{CPU: 2, MemoryMiB: 512}
	return fleet.Config{TemplateSize: admission.Resources{CPU: 1, MemoryMiB: 256}, Admission: admission.Config{
		Capacity: cap, MaxPending: 6, Tenants: map[string]admission.Policy{
			"alpha": {MaxLogical: 2, MaxActive: cap},
			"beta":  {MaxLogical: 2, MaxActive: cap},
		},
	}}
}

type serverFixture struct {
	client *http.Client
	server *http.Server
	cancel context.CancelFunc
	done   chan struct{}
	close  func() error
}

func start(t *testing.T, ctrl *fleet.Controller) *serverFixture {
	t.Helper()
	d, err := New(ctrl, 100*time.Millisecond, 12)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "private", "daemon.sock")
	listener, cleanup, err := ListenUnix(path)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: d.Handler()}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); d.Run(ctx) }()
	go func() { _ = server.Serve(listener) }()
	client := &http.Client{Timeout: 4 * time.Second, Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", path)
	}}}
	return &serverFixture{client: client, server: server, cancel: cancel, done: done, close: cleanup}
}

func (f *serverFixture) stop(t *testing.T) {
	t.Helper()
	f.cancel()
	if err := f.server.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-f.done
	if err := f.close(); err != nil {
		t.Fatal(err)
	}
}

func request(t *testing.T, client *http.Client, method, path string, payload any, want int) Status {
	t.Helper()
	var body *bytes.Reader
	if payload == nil {
		body = bytes.NewReader(nil)
	} else {
		b, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, "http://local"+path, body)
	if err != nil {
		t.Fatal(err)
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != want {
		var detail any
		json.NewDecoder(res.Body).Decode(&detail)
		t.Fatalf("%s %s: HTTP %d, want %d, body=%v", method, path, res.StatusCode, want, detail)
	}
	if path != "/v1/status" {
		return Status{}
	}
	var snapshot Status
	if err := json.NewDecoder(res.Body).Decode(&snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func waitState(t *testing.T, f *serverFixture, id string, want admission.State) Status {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		st := request(t, f.client, http.MethodGet, "/v1/status", nil, 200)
		if entry, found := st.Fleet.Jobs[id]; found && entry.Entry.State == want {
			return st
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("job %q did not reach %q", id, want)
	return Status{}
}

func TestUnixWorkerAdmissionPauseResumeDeleteAndRestart(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "fleet.json")
	b := newRuntime()
	store, err := fleet.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	c, err := fleet.New(store, b, testConfig(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	f := start(t, c)
	for _, row := range []struct{ id, tenant string }{{"a1", "alpha"}, {"b1", "beta"}, {"a2", "alpha"}} {
		request(t, f.client, "POST", "/v1/submit", map[string]string{"id": row.id, "tenant": row.tenant}, 202)
	}
	waitState(t, f, "a1", admission.Running)
	waitState(t, f, "b1", admission.Running)
	if st := request(t, f.client, "GET", "/v1/status", nil, 200); st.Fleet.Jobs["a2"].Entry.State != admission.Queued {
		t.Fatalf("third job exceeded capacity: %+v", st.Fleet.Admission)
	}
	request(t, f.client, "POST", "/v1/intent", map[string]string{"id": "a1", "desired": "paused"}, 202)
	waitState(t, f, "a1", admission.Paused)
	waitState(t, f, "a2", admission.Running)
	oldID := c.Status().Jobs["a1"].Workload.SandboxID
	request(t, f.client, "POST", "/v1/intent", map[string]string{"id": "a1", "desired": "running"}, 202)
	waitState(t, f, "a1", admission.ResumeQueued)
	request(t, f.client, "POST", "/v1/intent", map[string]string{"id": "b1", "desired": "deleted"}, 202)
	waitState(t, f, "b1", admission.Deleted)
	waitState(t, f, "a1", admission.Running)
	if c.Status().Jobs["a1"].Workload.SandboxID != oldID || b.creates != 3 {
		t.Fatal("resumed or restart created duplicate sandbox")
	}
	f.stop(t)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart must preserve the same VM identities and budget.
	store, err = fleet.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	c, err = fleet.New(store, b, testConfig(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if c.Status().Jobs["a1"].Workload.SandboxID != oldID || c.Status().Admission.Used.CPU != 2 || b.creates != 3 {
		t.Fatal("restart lost reservations or identity")
	}
	f = start(t, c)
	for _, id := range []string{"a1", "a2"} {
		request(t, f.client, "POST", "/v1/intent", map[string]string{"id": id, "desired": "deleted"}, 202)
	}
	waitState(t, f, "a1", admission.Deleted)
	waitState(t, f, "a2", admission.Deleted)
	st := request(t, f.client, "GET", "/v1/status", nil, 200)
	if st.Fleet.Admission.Used.CPU != 0 || st.Fleet.Admission.Pending != 0 {
		t.Fatalf("resources not reclaimed: %+v", st.Fleet.Admission)
	}
	f.stop(t)
}

func TestDaemonAmbiguousCreateNeedsExplicitRecover(t *testing.T) {
	store, err := fleet.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	b := newRuntime()
	b.loseReply = true
	c, err := fleet.New(store, b, testConfig(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	f := start(t, c)
	request(t, f.client, "POST", "/v1/submit", map[string]string{"id": "lost", "tenant": "alpha"}, 202)
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if c.Status().Jobs["lost"].Workload.CreatePending {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !c.Status().Jobs["lost"].Workload.CreatePending || b.creates != 1 {
		t.Fatal("ambiguous create not fenced")
	}
	request(t, f.client, "POST", "/v1/recover", map[string]string{"id": "lost"}, 202)
	waitState(t, f, "lost", admission.Running)
	if b.creates != 1 {
		t.Fatal("ambiguous create was duplicated")
	}
	request(t, f.client, "POST", "/v1/intent", map[string]string{"id": "lost", "desired": "deleted"}, 202)
	waitState(t, f, "lost", admission.Deleted)
	f.stop(t)
}

func TestRejectUnsafeHTTPRequests(t *testing.T) {
	store, err := fleet.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	c, err := fleet.New(store, newRuntime(), testConfig(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	f := start(t, c)
	request(t, f.client, "POST", "/v1/submit", map[string]string{"id": "a/b", "tenant": "alpha"}, 400)
	request(t, f.client, "POST", "/v1/submit", map[string]string{"id": "a1", "tenant": "alpha"}, 202)
	request(t, f.client, "POST", "/v1/intent", map[string]string{"id": "a1", "desired": "wrong"}, 400)
	request(t, f.client, "POST", "/v1/submit", map[string]string{"id": "b1", "tenant": "unknown"}, 400)
	request(t, f.client, "POST", "/v1/submit", map[string]string{"id": "a1", "tenant": "alpha", "unknown": "x"}, 400)
	f.stop(t)
}

func TestUnixSocketRequiresPrivateDirectoryAndNoOverwrite(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "public")
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatal(err)
	}
	// Mkdir's mode is filtered by the caller's umask. Set the actual fixture
	// permission explicitly so this rejection check also works with umask 077.
	if err := os.Chmod(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ListenUnix(filepath.Join(dir, "fleet.sock")); err == nil {
		t.Fatal("world-readable socket directory accepted")
	}
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "fleet.sock")
	listener, cleanup, err := ListenUnix(path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("socket mode not private: %v %v", info, err)
	}
	if _, _, err := ListenUnix(path); err == nil {
		t.Fatal("overwrote live socket")
	}
	_ = listener
	if err := cleanup(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("socket not removed")
	}
}
