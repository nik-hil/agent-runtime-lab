package fleetdaemon

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"agent-runtime-lab/src/admission"
)

func TestRepeatedAuditsLogFenceOnlyOnFirstTransition(t *testing.T) {
	c, provider, monitor := monitorFixture(t)
	var logs bytes.Buffer
	monitor.opts.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
	if err := c.Submit("alpha", "expiring"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := c.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	vm := c.Status().Jobs["expiring"].Workload.SandboxID
	provider.mu.Lock()
	delete(provider.states, vm)
	provider.mu.Unlock()

	for i := 0; i < 5; i++ {
		monitor.Audit(context.Background())
	}
	if got := strings.Count(logs.String(), `"msg":"fleet_inventory_admission_fenced"`); got != 1 {
		t.Fatalf("expected one fence transition event, got %d: %s", got, logs.String())
	}
	if !monitor.Status().Fenced || !c.Status().Unsafe {
		t.Fatal("safety fence did not persist")
	}
	if _, err := c.BeginDrain(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := c.Status().Jobs["expiring"].Entry.State; got != admission.Deleted {
		t.Fatalf("drain did not confirm provider absence: %s", got)
	}
	monitor.Audit(context.Background())
	if got := strings.Count(logs.String(), `"msg":"fleet_inventory_admission_fenced"`); got != 1 {
		t.Fatalf("fence was re-announced after cleanup: %s", logs.String())
	}
}
