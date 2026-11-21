package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type fakeBackend struct {
	mu        sync.Mutex
	next      int
	states    map[string]ObservedState
	creates   int
	pauses    int
	resumes   int
	deletes   int
	createErr error
	pauseErr  error
	onPause   func(context.Context) error
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{states: make(map[string]ObservedState)}
}

func (b *fakeBackend) Create(_ context.Context, _ string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.creates++
	if b.createErr != nil {
		return "", b.createErr
	}
	b.next++
	id := fmt.Sprintf("sbx-%d", b.next)
	b.states[id] = Running
	return id, nil
}

func (b *fakeBackend) Observe(_ context.Context, id string) (ObservedState, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	state, ok := b.states[id]
	if !ok {
		return Absent, nil
	}
	return state, nil
}

func (b *fakeBackend) Pause(ctx context.Context, id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pauses++
	if b.onPause != nil {
		return b.onPause(ctx)
	}
	if b.pauseErr != nil {
		return b.pauseErr
	}
	if b.states[id] != Running {
		return errors.New("not running")
	}
	b.states[id] = Paused
	return nil
}

func (b *fakeBackend) Resume(_ context.Context, id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.resumes++
	if b.states[id] != Paused {
		return errors.New("not paused")
	}
	b.states[id] = Running
	return nil
}

func (b *fakeBackend) Delete(_ context.Context, id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.deletes++
	delete(b.states, id)
	return nil
}

func mustController(t *testing.T, b Backend) *Controller {
	t.Helper()
	c, err := New(b, "tenant-a/task-42", 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func reconcileUntil(t *testing.T, c *Controller) {
	t.Helper()
	for range 5 {
		if err := c.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
		if c.Status().Converged {
			return
		}
	}
	t.Fatalf("controller failed to converge: %+v", c.Status())
}

func TestLifecycleCreatePauseResumeDelete(t *testing.T) {
	b := newFakeBackend()
	c := mustController(t, b)
	reconcileUntil(t, c)
	initialID := c.Status().SandboxID
	if initialID == "" {
		t.Fatal("no sandbox ID")
	}
	if err := c.SetDesired(WantPaused); err != nil {
		t.Fatal(err)
	}
	reconcileUntil(t, c)
	if err := c.SetDesired(WantRunning); err != nil {
		t.Fatal(err)
	}
	reconcileUntil(t, c)
	if err := c.SetDesired(WantDeleted); err != nil {
		t.Fatal(err)
	}
	reconcileUntil(t, c)
	if s := c.Status(); !s.Terminated || s.SandboxID != "" || !s.Converged {
		t.Fatalf("unexpected terminal status: %+v", s)
	}
	if b.creates != 1 || b.pauses != 1 || b.resumes != 1 || b.deletes != 1 {
		t.Fatalf("wrong backend calls: %+v", b)
	}
	if err := c.SetDesired(WantRunning); !errors.Is(err, ErrTerminal) {
		t.Fatalf("recreated terminated lifecycle: %v", err)
	}
}

func TestRetryPauseAfterTransientFailure(t *testing.T) {
	b := newFakeBackend()
	c := mustController(t, b)
	reconcileUntil(t, c)
	if err := c.SetDesired(WantPaused); err != nil {
		t.Fatal(err)
	}
	b.pauseErr = errors.New("network timeout")
	if err := c.Reconcile(context.Background()); err == nil {
		t.Fatal("expected pause error")
	}
	if c.Status().Observed != Running {
		t.Fatal("should not assume pause succeeded")
	}
	b.pauseErr = nil
	reconcileUntil(t, c)
	if b.creates != 1 || b.pauses != 2 {
		t.Fatalf("unexpected retries: creates=%d pauses=%d", b.creates, b.pauses)
	}
}

func TestAmbiguousCreateDoesNotDuplicate(t *testing.T) {
	b := newFakeBackend()
	b.createErr = context.DeadlineExceeded
	c := mustController(t, b)
	if err := c.Reconcile(context.Background()); !errors.Is(err, ErrCreateUncertain) {
		t.Fatalf("expected create fence: %v", err)
	}
	if err := c.Reconcile(context.Background()); !errors.Is(err, ErrCreateUncertain) {
		t.Fatalf("expected create fence again: %v", err)
	}
	if b.creates != 1 {
		t.Fatalf("duplicated ambiguous create (%d)", b.creates)
	}
	// Simulate independently recovering the remote ID from an audit index.
	b.states["recovered-id"] = Running
	if err := c.AttachAfterAmbiguousCreate("recovered-id"); err != nil {
		t.Fatal(err)
	}
	reconcileUntil(t, c)
	if c.Status().SandboxID != "recovered-id" || b.creates != 1 {
		t.Fatal("recovered create unexpectedly duplicated")
	}
}

func TestUnexpectedRemoteDeletionIsNotAutoRecreated(t *testing.T) {
	b := newFakeBackend()
	c := mustController(t, b)
	reconcileUntil(t, c)
	id := c.Status().SandboxID
	delete(b.states, id)
	if err := c.Reconcile(context.Background()); !errors.Is(err, ErrUnexpectedLoss) {
		t.Fatalf("expected loss fence: %v", err)
	}
	if b.creates != 1 {
		t.Fatalf("undesired recreation: %d", b.creates)
	}
	if err := c.SetDesired(WantDeleted); err != nil {
		t.Fatal(err)
	}
	reconcileUntil(t, c)
}

func TestOperationTimeoutAndRetry(t *testing.T) {
	b := newFakeBackend()
	c := mustController(t, b)
	reconcileUntil(t, c)
	if err := c.SetDesired(WantPaused); err != nil {
		t.Fatal(err)
	}
	b.onPause = func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}
	if err := c.Reconcile(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline, got: %v", err)
	}
	b.onPause = nil
	reconcileUntil(t, c)
}

func TestConcurrentReconcileOnlyCreatesOnce(t *testing.T) {
	b := newFakeBackend()
	c := mustController(t, b)
	var wg sync.WaitGroup
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.Reconcile(context.Background()); err != nil {
				t.Errorf("reconcile: %v", err)
			}
		}()
	}
	wg.Wait()
	if b.creates != 1 || !c.Status().Converged {
		t.Fatalf("expected one create and convergence, got creates=%d status=%+v", b.creates, c.Status())
	}
}

func TestValidation(t *testing.T) {
	if _, err := New(nil, "a", time.Second); err == nil {
		t.Fatal("accepted nil backend")
	}
	if _, err := New(newFakeBackend(), " ", time.Second); err == nil {
		t.Fatal("accepted empty logical key")
	}
	if _, err := New(newFakeBackend(), "a", 0); err == nil {
		t.Fatal("accepted zero timeout")
	}
	c := mustController(t, newFakeBackend())
	if err := c.SetDesired("invalid"); err == nil {
		t.Fatal("accepted invalid desired state")
	}
	if err := c.AttachAfterAmbiguousCreate("fake"); err == nil {
		t.Fatal("attached without ambiguous create")
	}
}
