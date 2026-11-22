package fleet

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/lifecycle"
)

type fakeVMs struct {
	mu              sync.Mutex
	states          map[string]lifecycle.ObservedState
	sizes           map[string]admission.Resources
	keys            map[string]string
	creates         int
	createLostReply bool
	pauseLostReply  bool
	shape           admission.Resources
}

func newFake() *fakeVMs {
	return &fakeVMs{states: map[string]lifecycle.ObservedState{}, sizes: map[string]admission.Resources{},
		keys: map[string]string{}, shape: admission.Resources{CPU: 1, MemoryMiB: 256}}
}

func (f *fakeVMs) Create(_ context.Context, key string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.creates++
	id := "sbx-test-" + key[len(key)-6:]
	f.keys[id] = key
	f.states[id] = lifecycle.Running
	f.sizes[id] = f.shape
	if f.createLostReply {
		return "", context.DeadlineExceeded
	}
	return id, nil
}

func (f *fakeVMs) Describe(_ context.Context, id string) (Details, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	st, ok := f.states[id]
	if !ok {
		return Details{State: lifecycle.Absent}, nil
	}
	return Details{State: st, Resources: f.sizes[id]}, nil
}

func (f *fakeVMs) Observe(ctx context.Context, id string) (lifecycle.ObservedState, error) {
	d, err := f.Describe(ctx, id)
	return d.State, err
}

func (f *fakeVMs) Pause(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.states[id] != lifecycle.Running {
		return errors.New("not running")
	}
	f.states[id] = lifecycle.Paused
	if f.pauseLostReply {
		return context.DeadlineExceeded
	}
	return nil
}

func (f *fakeVMs) Resume(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.states[id] != lifecycle.Paused {
		return errors.New("not paused")
	}
	f.states[id] = lifecycle.Running
	return nil
}

func (f *fakeVMs) Delete(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.states, id)
	return nil
}

func (f *fakeVMs) FindByLogicalKey(_ context.Context, key string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := []string{}
	for id, k := range f.keys {
		if k == key && f.states[id] != "" {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

func testConfig() Config {
	return Config{TemplateSize: admission.Resources{CPU: 1, MemoryMiB: 256}, Admission: admission.Config{
		Capacity: admission.Resources{CPU: 2, MemoryMiB: 512}, MaxPending: 10,
		Tenants: map[string]admission.Policy{
			"alpha": {MaxLogical: 3, MaxActive: admission.Resources{CPU: 2, MemoryMiB: 512}},
			"beta":  {MaxLogical: 2, MaxActive: admission.Resources{CPU: 2, MemoryMiB: 512}},
		},
	}}
}

func opened(t *testing.T, path string, runtime *fakeVMs, cfg Config) (*Controller, *Store) {
	t.Helper()
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	c, err := New(s, runtime, cfg, time.Second)
	if err != nil {
		s.Close()
		t.Fatal(err)
	}
	return c, s
}

func tick(t *testing.T, c *Controller) bool {
	t.Helper()
	ok, err := c.Tick(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return ok
}

func state(t *testing.T, c *Controller, id string) admission.State {
	t.Helper()
	e, ok := c.Status().Jobs[id]
	if !ok {
		t.Fatalf("missing request %q", id)
	}
	return e.Entry.State
}

func TestDurableAdmissionFairnessPauseResumeAndDelete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fleet.json")
	b := newFake()
	c, store := opened(t, path, b, testConfig())
	for _, req := range []struct{ id, tenant string }{{"a1", "alpha"}, {"a2", "alpha"}, {"b1", "beta"}} {
		if err := c.Submit(req.tenant, req.id); err != nil {
			t.Fatal(err)
		}
	}
	for range 4 {
		tick(t, c)
	} // create a1, observe, create b1, observe
	if state(t, c, "a1") != admission.Running || state(t, c, "b1") != admission.Running ||
		state(t, c, "a2") != admission.Queued || c.Status().Admission.Used.CPU != 2 {
		t.Fatal("incorrect initial admission", c.Status())
	}
	if progress := tick(t, c); progress {
		t.Fatal("admitted beyond host budget")
	}
	initial := c.Status().Jobs["a1"].Workload.SandboxID
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	c, store = opened(t, path, b, testConfig())
	defer store.Close()
	if c.Status().Jobs["a1"].Workload.SandboxID != initial {
		t.Fatal("sandbox identity lost on restart")
	}
	if b.creates != 2 {
		t.Fatal("restart created extra VM")
	}
	if err := c.SetDesired("a1", lifecycle.WantPaused); err != nil {
		t.Fatal(err)
	}
	tick(t, c) // pause request
	if c.Status().Admission.Used.CPU != 2 {
		t.Fatal("released before remote observation")
	}
	tick(t, c) // observes paused, releases
	if c.Status().Admission.Used.CPU != 1 {
		t.Fatal("didn't free CPU after confirmed pause")
	}
	tick(t, c) // create a2
	tick(t, c) // observe a2 running
	if err := c.SetDesired("a1", lifecycle.WantRunning); err != nil {
		t.Fatal(err)
	}
	if state(t, c, "a1") != admission.ResumeQueued {
		t.Fatal("resume did not queue")
	}
	if progress := tick(t, c); progress {
		t.Fatal("resume exceeded capacity")
	}
	if err := c.SetDesired("b1", lifecycle.WantDeleted); err != nil {
		t.Fatal(err)
	}
	tick(t, c) // send delete
	if c.Status().Admission.Used.CPU != 2 {
		t.Fatal("released reservation merely on DELETE ACK")
	}
	tick(t, c) // observe ABSENT -> release
	if c.Status().Admission.Used.CPU != 1 {
		t.Fatal("delete did not release after observe")
	}
	tick(t, c) // reserve resume a1 and send Resume
	if c.Status().Admission.Used.CPU != 2 {
		t.Fatal("resume did not reserve before VM action")
	}
	tick(t, c) // confirm resumed running
	if state(t, c, "a1") != admission.Running || b.creates != 3 {
		t.Fatal("unexpected final state", c.Status())
	}
	for _, id := range []string{"a1", "a2"} {
		if err := c.SetDesired(id, lifecycle.WantDeleted); err != nil {
			t.Fatal(err)
		}
	}
	for range 4 {
		tick(t, c)
	} // delete and confirm each
	if c.Status().Admission.Used.CPU != 0 {
		t.Fatal("resources leaked after cleanup", c.Status())
	}
}

func TestLostCreateResponseRetainsCapacityAndRecoversAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fleet.json")
	b := newFake()
	b.createLostReply = true
	c, store := opened(t, path, b, testConfig())
	if err := c.Submit("alpha", "one"); err != nil {
		t.Fatal(err)
	}
	progress, err := c.Tick(context.Background())
	if !progress || !errors.Is(err, ErrCreateUncertain) {
		t.Fatalf("wrong ambiguity fence: %v %t", err, progress)
	}
	status := c.Status()
	if !status.Jobs["one"].Workload.CreatePending || status.Admission.Used.CPU != 1 {
		t.Fatal("fence lost reservation", status)
	}
	store.Close()
	b.createLostReply = false
	c, store = opened(t, path, b, testConfig())
	defer store.Close()
	if b.creates != 1 {
		t.Fatal("restart repeated Create")
	}
	if err := c.Recover(context.Background(), "one"); err != nil {
		t.Fatal(err)
	}
	tick(t, c)
	if state(t, c, "one") != admission.Running || b.creates != 1 {
		t.Fatal("recovery duplicated creation")
	}
	if err := c.SetDesired("one", lifecycle.WantDeleted); err != nil {
		t.Fatal(err)
	}
	tick(t, c)
	tick(t, c)
	if c.Status().Admission.Used.CPU != 0 {
		t.Fatal("reserved capacity not freed")
	}
}

func TestLostPauseResponseDoesNotFreeCapacityUntilObserved(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fleet.json")
	b := newFake()
	c, store := opened(t, path, b, testConfig())
	if err := c.Submit("alpha", "one"); err != nil {
		t.Fatal(err)
	}
	tick(t, c)
	tick(t, c)
	if err := c.SetDesired("one", lifecycle.WantPaused); err != nil {
		t.Fatal(err)
	}
	b.pauseLostReply = true
	if _, err := c.Tick(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected timeout %v", err)
	}
	if c.Status().Admission.Used.CPU != 1 {
		t.Fatal("premature release after timeout")
	}
	store.Close()
	c, store = opened(t, path, b, testConfig())
	defer store.Close()
	tick(t, c)
	if state(t, c, "one") != admission.Paused || c.Status().Admission.Used.CPU != 0 {
		t.Fatal("paused VM not reconciled", c.Status())
	}
	if err := c.SetDesired("one", lifecycle.WantDeleted); err != nil {
		t.Fatal(err)
	}
	tick(t, c)
	tick(t, c)
}

func TestOverProvisionedTemplateBlocksAdmissionButAllowsCleanup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fleet.json")
	b := newFake()
	b.shape = admission.Resources{CPU: 3, MemoryMiB: 1024}
	c, store := opened(t, path, b, testConfig())
	if err := c.Submit("alpha", "one"); err != nil {
		t.Fatal(err)
	}
	tick(t, c)
	if _, err := c.Tick(context.Background()); !errors.Is(err, ErrUnsafeCapacity) {
		t.Fatalf("expected fail closed, got %v", err)
	}
	if !c.Status().Unsafe || c.Status().Admission.Used.CPU != 1 {
		t.Fatal("unsafe VM freed reservation", c.Status())
	}
	if err := c.Submit("beta", "two"); !errors.Is(err, ErrUnsafeCapacity) {
		t.Fatal("admitted new job after mis-sized VM")
	}
	if err := c.SetDesired("one", lifecycle.WantDeleted); err != nil {
		t.Fatal(err)
	}
	tick(t, c)
	tick(t, c)
	if c.Status().Admission.Used.CPU != 0 {
		t.Fatal("cannot clean up oversize VM")
	}
	store.Close()
	c, store = opened(t, path, b, testConfig())
	defer store.Close()
	if !c.Status().Unsafe {
		t.Fatal("unsafe flag vanished on restart")
	}
}

func TestStoreLockAndCorruptCheckpointRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fleet.json")
	b := newFake()
	c, store := opened(t, path, b, testConfig())
	if _, err := Open(path); err == nil {
		t.Fatal("second process can mutate same journal")
	}
	if err := c.Submit("alpha", "one"); err != nil {
		t.Fatal(err)
	}
	store.Close()
	if err := os.WriteFile(path, []byte(`{"version":1,"scheduler":{},"workloads":{}}`), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := New(s, b, testConfig(), time.Second); err == nil {
		t.Fatal("accepted corrupt journal")
	}
}
