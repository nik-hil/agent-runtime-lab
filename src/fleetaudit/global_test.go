package fleetaudit

import (
	"context"
	"errors"
	"testing"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/e2bapi"
)

type providerWithGlobal struct {
	*fakeProvider
	list []e2bapi.LabSandbox
	err  error
}

func (p *providerWithGlobal) ListLabSandboxes(context.Context) ([]e2bapi.LabSandbox, error) {
	return p.list, p.err
}

func TestRunGlobalDetectsLabTaggedOrphansOutsideJournal(t *testing.T) {
	j := journal(t, admission.Running, false)
	p := &providerWithGlobal{fakeProvider: normalProvider(), list: []e2bapi.LabSandbox{
		{SandboxID: "known", LogicalKey: testKey},
		{SandboxID: "foreign-journal", LogicalKey: "agent-runtime-lab/m2-9/from-other-journal"},
	}}
	r, err := RunGlobal(context.Background(), j, p)
	if err != nil || !r.RequiresReview || !hasFinding(r, "untracked_lab_sandbox") ||
		r.Scope != "team_active_lab_metadata_prefix_and_current_journal" {
		t.Fatalf("global audit: %+v, %v", r, err)
	}
	p.err = errors.New("E2B 503")
	if _, err := RunGlobal(context.Background(), j, p); err == nil {
		t.Fatal("unavailable global list silently accepted")
	}
}
