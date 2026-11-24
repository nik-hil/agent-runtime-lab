package fleet

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func TestDurableUnsafeFenceDoesNotTurnIdleReconcileIntoFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fleet.json")
	backend := newFake()
	c, store := opened(t, path, backend, testConfig())
	if err := c.Submit("alpha", "one"); err != nil {
		t.Fatal(err)
	}
	tick(t, c) // Create the sandbox.
	tick(t, c) // Confirm it RUNNING.

	if ok, err := c.FenceIfSnapshotCurrent(c.SnapshotJournal(), "confirmed unexpected sandbox absence"); err != nil || !ok {
		t.Fatalf("failed to persist safety fence: ok=%t err=%v", ok, err)
	}
	for i := 0; i < 3; i++ {
		progress, err := c.Tick(context.Background())
		if progress || err != nil {
			t.Fatalf("a stable unsafe fence is not a reconcile error: progress=%t err=%v", progress, err)
		}
	}
	if err := c.Submit("alpha", "two"); !errors.Is(err, ErrUnsafeCapacity) ||
		!strings.Contains(err.Error(), "unsafe") {
		t.Fatalf("unsafe admission was not rejected with an accurate explanation: %v", err)
	}
	if _, err := c.BeginDrain(); err != nil {
		t.Fatal(err)
	}
	tick(t, c) // Delete request; reservation still held.
	if got := c.Status().Admission.Used.CPU; got != 1 {
		t.Fatalf("DELETE acknowledgment released reservation: %d", got)
	}
	tick(t, c) // Provider absence confirmed.
	if got := c.Status(); got.Admission.Used.CPU != 0 || !got.Unsafe {
		t.Fatalf("cleanup should preserve fence but release confirmed absence: %+v", got)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	c, store = opened(t, path, backend, testConfig())
	defer store.Close()
	if _, err := c.Tick(context.Background()); err != nil {
		t.Fatalf("restart replayed unsafe admission as an error: %v", err)
	}
}
