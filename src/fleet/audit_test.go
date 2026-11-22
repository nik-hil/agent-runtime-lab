package fleet

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"agent-runtime-lab/src/admission"
)

func TestAuditFenceSurvivesRestartWithoutFreeingResources(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit-journal.json")
	b := newFake()
	c, store := opened(t, path, b, testConfig())
	if err := c.Submit("alpha", "one"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := c.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if c.Status().Jobs["one"].Entry.State != admission.Running {
		t.Fatal("expected running")
	}
	snap := c.SnapshotJournal()
	if snap.Scheduler.Used.CPU != 1 {
		t.Fatal("snapshot does not contain reservation")
	}
	// Mutating a snapshot must not mutate live journal ownership.
	w := snap.Workloads["one"]
	w.SandboxID = "fake-identity"
	snap.Workloads["one"] = w
	if c.Status().Jobs["one"].Workload.SandboxID == "fake-identity" {
		t.Fatal("snapshot aliases live journal")
	}
	if ok, err := c.FenceIfSnapshotCurrent(snap, "stale audit"); err != nil || ok {
		t.Fatalf("stale audit fenced: %t %v", ok, err)
	}
	snap = c.SnapshotJournal()
	if ok, err := c.FenceIfSnapshotCurrent(snap, "critical drift confirmed"); err != nil || !ok {
		t.Fatalf("valid audit not fenced: %t %v", ok, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	c, store = opened(t, path, b, testConfig())
	defer store.Close()
	if !c.Status().Unsafe || c.Status().Admission.Used.CPU != 1 {
		t.Fatalf("restart lost fence or capacity: %+v", c.Status())
	}
	if err := c.Submit("alpha", "two"); !errors.Is(err, ErrUnsafeCapacity) {
		t.Fatalf("admission bypassed persisted fence: %v", err)
	}
	if _, err := c.BeginDrain(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4 && c.Status().Jobs["one"].Entry.State != admission.Deleted; i++ {
		if _, err := c.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if c.Status().Jobs["one"].Entry.State != admission.Deleted || c.Status().Admission.Used.CPU != 0 {
		t.Fatalf("cannot drain safely fenced workload: %+v", c.Status())
	}
}
