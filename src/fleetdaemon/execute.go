package fleetdaemon

import (
	"context"
	"errors"
	"net/http"
	"time"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/fleet"
	"agent-runtime-lab/src/m4exec"
)

// Executor performs commands only against a sandbox ID already owned by the
// controller. The transport must never interpret a command as a host action.
type Executor interface {
	Run(context.Context, string, string) (m4exec.Result, error)
}

// ExecuteHandler extends the private fleetd API with one synchronous M4
// execution path. No automatic retries: command side effects are not
// idempotent and a missing response has an unknown outcome.
//
// This endpoint is for trusted local operators, not tenant-facing clients.
// A running workload can be drained concurrently: the sandbox may disappear
// mid-command; such outcomes are never reported as successful task completion.
func ExecuteHandler(base http.Handler, c *fleet.Controller, executor Executor) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/execute" || r.Method != http.MethodPost {
			base.ServeHTTP(w, r)
			return
		}
		if executor == nil || c == nil {
			fail(w, http.StatusNotImplemented, "E2B command executor unavailable")
			return
		}
		var input struct {
			ID             string `json:"id"`
			Command        string `json:"command"`
			TimeoutSeconds int    `json:"timeout_seconds"`
		}
		if !parse(w, r, &input) {
			return
		}
		if invalidID(input.ID) || input.Command == "" || len(input.Command) > 4096 ||
			input.TimeoutSeconds < 1 || input.TimeoutSeconds > 60 {
			fail(w, http.StatusBadRequest, "valid workload ID, command <=4096 bytes and timeout_seconds 1..60 required")
			return
		}
		if c.Draining() {
			fail(w, http.StatusConflict, "fleet is draining")
			return
		}
		st := c.Status()
		job, ok := st.Jobs[input.ID]
		if st.Unsafe || !ok || job.Entry.State != admission.Running || job.Workload.Desired != "running" ||
			job.Workload.SandboxID == "" || job.Workload.CreatePending {
			fail(w, http.StatusConflict, "execution requires an owned, healthy RUNNING workload")
			return
		}
		taskCtx, cancel := context.WithTimeout(r.Context(), time.Duration(input.TimeoutSeconds)*time.Second)
		defer cancel()
		result, err := executor.Run(taskCtx, job.Workload.SandboxID, input.Command)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				fail(w, http.StatusGatewayTimeout, "command completion unconfirmed; do not retry blindly")
				return
			}
			fail(w, http.StatusBadGateway, "sandbox command failed or has unknown outcome; do not retry blindly")
			return
		}
		// A task result cannot be committed against a workload that has since
		// been drained, replaced, paused, or fenced by provider drift.
		current := c.Status()
		active, found := current.Jobs[input.ID]
		if !found || current.Unsafe || c.Draining() || active.Workload != job.Workload ||
			active.Entry.State != admission.Running || active.Workload.Desired != "running" ||
			result.SandboxID != job.Workload.SandboxID {
			fail(w, http.StatusConflict, "workload changed during execution; result not accepted")
			return
		}
		respond(w, http.StatusOK, result)
	})
}
