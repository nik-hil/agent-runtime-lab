package durable

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"agent-runtime-lab/src/lifecycle"
)

type fakeRuntime struct {
	mu         sync.Mutex
	states     map[string]lifecycle.ObservedState
	keys       map[string]string
	creates    int
	pauses     int
	resumes    int
	deletes    int
	createHook func()
	pauseHook  func() error
}

func newFake() *fakeRuntime {
	return &fakeRuntime{states: map[string]lifecycle.ObservedState{}, keys: map[string]string{}}
}

func (f *fakeRuntime) Create(_ context.Context, key string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.creates++
	id := "sbx-" + key
	f.keys[id] = key
	f.states[id] = lifecycle.Running
	if f.createHook != nil {
		f.createHook()
	}
	return id, nil
}

func (f *fakeRuntime) Observe(_ context.Context, id string) (lifecycle.ObservedState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if state, found := f.states[id]; found {
		return state, nil
	}
	return lifecycle.Absent, nil
}

func (f *fakeRuntime) Pause(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pauses++
	if f.pauseHook != nil {
		return f.pauseHook()
	}
	f.states[id] = lifecycle.Paused
	return nil
}

func (f *fakeRuntime) Resume(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resumes++
	f.states[id] = lifecycle.Running
	return nil
}

func (f *fakeRuntime) Delete(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletes++
	delete(f.states, id)
	return nil
}

func (f *fakeRuntime) FindByLogicalKey(_ context.Context, key string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var ids []string
	for id, entryKey := range f.keys {
		if key == entryKey && f.states[id] != "" {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

func openController(t *testing.T, path string, b *fakeRuntime, key string) (*Controller, *FileStore) {
	t.Helper()
	s, err := OpenFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	c, err := New(s, b, key, time.Second)
	if err != nil {
		s.Close()
		t.Fatal(err)
	}
	return c, s
}

func converge(t *testing.T, c *Controller) {
	t.Helper()
	for range 5 {
		if err := c.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
		if c.Converged() {
			return
		}
	}
	t.Fatalf("not converged: %+v", c.Record())
}

func TestRestartAfterRunningAndPaused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	f := newFake()
	c, store := openController(t, path, f, "tenant-a/task-42")
	converge(t, c)
	if err := c.SetDesired(lifecycle.WantPaused); err != nil {
		t.Fatal(err)
	}
	converge(t, c)
	id := c.Record().SandboxID
	store.Close()

	c, store = openController(t, path, f, "")
	defer store.Close()
	if c.Record().Desired != "paused" || c.Record().SandboxID != id {
		t.Fatalf("lost persisted state: %+v", c.Record())
	}
	converge(t, c)
	if err := c.SetDesired(lifecycle.WantRunning); err != nil {
		t.Fatal(err)
	}
	converge(t, c)
	if err := c.SetDesired(lifecycle.WantDeleted); err != nil {
		t.Fatal(err)
	}
	converge(t, c)
	if !c.Record().Terminated || f.creates != 1 || f.pauses != 1 || f.resumes != 1 || f.deletes != 1 {
		t.Fatalf("wrong lifecycle %+v, runtime %+v", c.Record(), f)
	}
}

func TestCrashAfterRemoteCreateBeforeRecordingID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	f := newFake()
	c, store := openController(t, path, f, "unique-task")
	f.createHook = func() { panic("simulated SIGKILL after remote create") }
	func() {
		defer func() {
			if recover() == nil {
				t.Error("expected simulated crash")
			}
		}()
		_ = c.Reconcile(context.Background())
	}()
	store.Close()
	f.createHook = nil

	c, store = openController(t, path, f, "")
	defer store.Close()
	if !c.Record().CreatePending || c.Record().SandboxID != "" {
		t.Fatalf("no durable pre-create fence: %+v", c.Record())
	}
	if err := c.Reconcile(context.Background()); !errors.Is(err, ErrCreatePending) {
		t.Fatalf("should block duplicate create: %v", err)
	}
	if f.creates != 1 {
		t.Fatalf("duplicate remote create: %d", f.creates)
	}
	if err := c.RecoverCreate(context.Background(), f); err != nil {
		t.Fatal(err)
	}
	converge(t, c)
	if f.creates != 1 || c.Record().CreatePending {
		t.Fatalf("recovery created duplicate: %+v", c.Record())
	}
}

func TestRecoveryFailsClosedOnZeroOrMultipleMatches(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	f := newFake()
	c, s := openController(t, path, f, "key-1")
	defer s.Close()
	r := c.Record()
	r.CreatePending = true
	if err := c.persist(r); err != nil {
		t.Fatal(err)
	}
	if err := c.RecoverCreate(context.Background(), f); !errors.Is(err, ErrNoMatch) {
		t.Fatalf("zero candidates should not clear fence: %v", err)
	}
	f.keys["sbx-a"] = "key-1"
	f.keys["sbx-b"] = "key-1"
	f.states["sbx-a"] = lifecycle.Running
	f.states["sbx-b"] = lifecycle.Paused
	if err := c.RecoverCreate(context.Background(), f); !errors.Is(err, ErrMultipleMatches) {
		t.Fatalf("duplicates must fail closed: %v", err)
	}
	if !c.Record().CreatePending {
		t.Fatal("cleared ambiguous create despite duplicate")
	}
}

func TestLostPauseResponseReobservedAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	f := newFake()
	c, s := openController(t, path, f, "key-2")
	converge(t, c)
	if err := c.SetDesired(lifecycle.WantPaused); err != nil {
		t.Fatal(err)
	}
	f.pauseHook = func() error {
		f.states[c.record.SandboxID] = lifecycle.Paused
		return context.DeadlineExceeded
	}
	if err := c.Reconcile(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected lost response: %v", err)
	}
	s.Close()
	f.pauseHook = nil
	c, s = openController(t, path, f, "")
	defer s.Close()
	converge(t, c)
	if f.pauses != 1 {
		t.Fatalf("duplicate pause despite remote success: %d", f.pauses)
	}
}

func TestUnexpectedDeletionDoesNotRecreate(t *testing.T) {
	f := newFake()
	c, s := openController(t, filepath.Join(t.TempDir(), "state.json"), f, "key-3")
	defer s.Close()
	converge(t, c)
	delete(f.states, c.Record().SandboxID)
	if err := c.Reconcile(context.Background()); !errors.Is(err, ErrUnexpectedLoss) {
		t.Fatalf("missing unexpected loss: %v", err)
	}
	if f.creates != 1 {
		t.Fatal("unexpected recreation")
	}
	if err := c.SetDesired(lifecycle.WantDeleted); err != nil {
		t.Fatal(err)
	}
	converge(t, c)
}

func TestStoreLockAndCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s, err := OpenFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenFileStore(path); err == nil {
		t.Fatal("second controller got exclusive lock")
	}
	if err := s.Save(Record{Version: 1, LogicalKey: "key", Desired: "running", Observed: "unknown"}); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("state file permissions: %v %v", info, err)
	}
	s.Close()
	if err := os.WriteFile(path, []byte(`{"truncated"`), 0600); err != nil {
		t.Fatal(err)
	}
	s, err = OpenFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, _, err := s.Load(); err == nil {
		t.Fatal("corrupt state silently accepted")
	}
}
