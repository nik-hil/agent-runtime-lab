package fleetaudit

import (
	"context"
	"testing"

	"agent-runtime-lab/src/admission"
)

func TestPersistedFenceDoesNotCreateNewCriticalInventoryDrift(t *testing.T) {
	j := journal(t, admission.Deleted, false)
	j.Unsafe = true
	j.UnsafeNote = "previously confirmed unexpected absence"
	p := normalProvider()
	p.matches = map[string][]string{}
	report, err := Run(context.Background(), j, p)
	if err != nil {
		t.Fatal(err)
	}
	if !report.RequiresReview || len(report.Findings) != 1 ||
		report.Findings[0].Code != "journal_unsafe" || report.Findings[0].Severity != "warning" {
		t.Fatalf("old unsafe state should remain visible, not count as fresh critical drift: %+v", report)
	}
}
