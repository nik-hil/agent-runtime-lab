package fleetdaemon

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/fleet"
	"agent-runtime-lab/src/taskjournal"
)

// recoverTask inspects the original VM, never executes the user's command, and
// only commits valid evidence while the fleet still owns that same healthy VM.
func recoverTask(w http.ResponseWriter, r *http.Request, c *fleet.Controller, executor Executor, tasks *taskjournal.Store) {
	id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/tasks/"), "/recover")
	if invalidID(id) {
		fail(w, http.StatusBadRequest, "invalid task ID")
		return
	}
	var input struct{}
	if !parse(w, r, &input) {
		return
	}
	entry, err := tasks.Get(id)
	if errors.Is(err, taskjournal.ErrNotFound) {
		fail(w, http.StatusNotFound, "task not found")
		return
	}
	if err != nil {
		fail(w, http.StatusServiceUnavailable, "task journal unavailable")
		return
	}
	if entry.Async {
		fail(w, http.StatusConflict, "async jobs require /v1/jobs/ID/recover so durable artifacts are verified")
		return
	}
	if entry.State == taskjournal.Succeeded || entry.State == taskjournal.Failed {
		respond(w, http.StatusOK, entry)
		return
	}
	if entry.State != taskjournal.Uncertain || entry.EvidenceIntent == nil {
		fail(w, http.StatusConflict, "recovery requires an uncertain task originally submitted with --evidence; legacy tasks cannot be recovered")
		return
	}
	reader, ok := executor.(EvidenceExecutor)
	if !ok {
		fail(w, http.StatusNotImplemented, "completion evidence reader unavailable")
		return
	}
	initial := c.Status()
	job, found := initial.Jobs[entry.WorkloadID]
	if !found || initial.Unsafe || c.Draining() || job.Entry.State != admission.Running ||
		job.Workload.SandboxID != entry.SandboxID || job.Workload.Desired != "running" || job.Workload.CreatePending {
		fail(w, http.StatusConflict, "recovery requires the original owned healthy RUNNING sandbox; task remains uncertain")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	evidence, err := reader.ReadEvidence(ctx, entry.EvidenceIdentity())
	if err != nil || ctx.Err() != nil || evidence.Validate(entry.EvidenceIdentity()) != nil {
		fail(w, http.StatusConflict, "completion evidence unavailable or invalid; task remains uncertain and command was not rerun")
		return
	}
	current := c.Status()
	active, found := current.Jobs[entry.WorkloadID]
	if !found || current.Unsafe || c.Draining() || active != job {
		fail(w, http.StatusConflict, "workload changed during evidence inspection; task remains uncertain")
		return
	}
	resolved, err := tasks.FinishEvidence(id, evidence, true)
	if err != nil {
		writeTaskError(w, err)
		return
	}
	respond(w, http.StatusOK, resolved)
}
