package fleetaudit

import (
	"context"
	"fmt"
	"sort"

	"agent-runtime-lab/src/e2bapi"
	"agent-runtime-lab/src/fleet"
)

// GlobalProvider is an opt-in read-only inventory provider. Scanning all
// sandboxes under the API key can also discover OTHER lab journals.
type GlobalProvider interface {
	Provider
	ListLabSandboxes(context.Context) ([]e2bapi.LabSandbox, error)
}

// RunGlobal extends the known-key audit with a provider-side scan of lab-tagged
// sandboxes, including remote keys absent from this particular journal. Such
// findings are advisory: another journal may own the VM legitimately.
func RunGlobal(ctx context.Context, j fleet.Journal, p GlobalProvider) (Report, error) {
	r, err := Run(ctx, j, p)
	if err != nil {
		return Report{}, err
	}
	list, err := p.ListLabSandboxes(ctx)
	if err != nil {
		return Report{}, fmt.Errorf("provider-wide inventory incomplete: %w", err)
	}
	keys := make(map[string]string, len(j.Workloads))
	for id, w := range j.Workloads {
		keys[w.LogicalKey] = id
	}
	for _, remote := range list {
		if _, owned := keys[remote.LogicalKey]; !owned {
			r.Findings = append(r.Findings, Finding{Code: "untracked_lab_sandbox", Severity: "warning",
				SandboxID: remote.SandboxID, Detail: "lab-tagged sandbox not in this journal; another journal may own it"})
			r.RequiresReview = true
		}
	}
	sort.Slice(r.Findings, func(i, k int) bool {
		a, b := r.Findings[i], r.Findings[k]
		if a.JobID != b.JobID {
			return a.JobID < b.JobID
		}
		if a.Code != b.Code {
			return a.Code < b.Code
		}
		return a.SandboxID < b.SandboxID
	})
	r.Scope = "team_active_lab_metadata_prefix_and_current_journal"
	return r, nil
}
