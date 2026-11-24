package fleetdaemon

import (
	"context"
	"errors"
	"net/http"
	"sort"
	"strings"
	"time"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/fleet"
	"agent-runtime-lab/src/taskjournal"
)

// TaskHandler provides bounded, synchronous command execution with a durable
// write-ahead task intent and a stable task ID. It is only for the private
// fleetd Unix socket; it does not provide tenant authentication.
func TaskHandler(base http.Handler, c *fleet.Controller, executor Executor, tasks *taskjournal.Store) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if tasks == nil || c == nil || executor == nil {
			if strings.HasPrefix(r.URL.Path, "/v1/tasks") {
				fail(w, http.StatusNotImplemented, "durable task execution unavailable")
				return
			}
			base.ServeHTTP(w, r)
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/v1/tasks" {
			entries, err := tasks.List()
			if err != nil {
				fail(w, http.StatusServiceUnavailable, "task journal unavailable")
				return
			}
			sort.Slice(entries, func(i, j int) bool { return entries[i].ID < entries[j].ID })
			summaries := make([]map[string]any, 0, len(entries))
			for _, entry := range entries {
				summaries = append(summaries, map[string]any{
					"id": entry.ID, "workload_id": entry.WorkloadID,
					"sandbox_id": entry.SandboxID, "state": entry.State,
					"started_at": entry.StartedAt, "result_sha256": entry.ResultHash,
				})
			}
			respond(w, http.StatusOK, map[string]any{"tasks": summaries})
			return
		}
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/tasks/") {
			id := strings.TrimPrefix(r.URL.Path, "/v1/tasks/")
			if invalidID(id) {
				fail(w, http.StatusBadRequest, "invalid task ID")
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
			respond(w, http.StatusOK, entry)
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/v1/tasks" {
			base.ServeHTTP(w, r)
			return
		}
		var req struct {
			TaskID         string `json:"task_id"`
			WorkloadID     string `json:"workload_id"`
			Command        string `json:"command"`
			TimeoutSeconds int    `json:"timeout_seconds"`
		}
		if !parse(w, r, &req) {
			return
		}
		if invalidID(req.TaskID) || invalidID(req.WorkloadID) || req.Command == "" ||
			len(req.Command) > 4000 || strings.IndexByte(req.Command, 0) >= 0 ||
			req.TimeoutSeconds < 1 || req.TimeoutSeconds > 60 {
			fail(w, http.StatusBadRequest, "task ID, workload ID, command <=4000 bytes, and timeout 1..60 required")
			return
		}
		// A repeated task ID is never executed again, even after a VM has been
		// drained. The digest/binding check detects accidental ID reuse.
		if old, err := tasks.Get(req.TaskID); err == nil {
			entry, _, err := tasks.Begin(req.TaskID, req.WorkloadID, old.SandboxID, req.Command, req.TimeoutSeconds)
			if err != nil {
				fail(w, taskErrorStatus(err), "task ID conflict or journal unavailable")
				return
			}
			respond(w, http.StatusOK, entry)
			return
		} else if !errors.Is(err, taskjournal.ErrNotFound) {
			fail(w, http.StatusServiceUnavailable, "task journal unavailable")
			return
		}
		if c.Draining() {
			fail(w, http.StatusConflict, "fleet is draining")
			return
		}
		initial := c.Status()
		job, ok := initial.Jobs[req.WorkloadID]
		if initial.Unsafe || !ok || job.Entry.State != admission.Running ||
			job.Workload.Desired != "running" || job.Workload.CreatePending || job.Workload.SandboxID == "" {
			fail(w, http.StatusConflict, "task requires owned, healthy RUNNING workload")
			return
		}
		entry, run, err := tasks.Begin(req.TaskID, req.WorkloadID, job.Workload.SandboxID, req.Command, req.TimeoutSeconds)
		if err != nil {
			fail(w, taskErrorStatus(err), "task ID conflict, VM busy, or task journal unavailable")
			return
		}
		if !run {
			respond(w, http.StatusOK, entry)
			return
		}
		// Check the intended VM is still owned before calling the provider;
		// a concurrent drain may otherwise have raced with the fsync.
		current := c.Status()
		active, found := current.Jobs[req.WorkloadID]
		if !found || current.Unsafe || c.Draining() || active != job || active.Entry.State != admission.Running {
			entry, err = tasks.Finish(req.TaskID, nil, "workload changed after durable intent; remote outcome must be investigated")
			if err != nil {
				fail(w, http.StatusServiceUnavailable, "task intent persisted but outcome journal uncertain")
				return
			}
			respond(w, http.StatusConflict, entry)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), time.Duration(req.TimeoutSeconds)*time.Second)
		defer cancel()
		result, execErr := executor.Run(ctx, job.Workload.SandboxID, req.Command)
		current = c.Status()
		active, found = current.Jobs[req.WorkloadID]
		if execErr != nil || ctx.Err() != nil || !found || current.Unsafe || c.Draining() || active != job ||
			active.Entry.State != admission.Running || result.SandboxID != job.Workload.SandboxID {
			entry, err = tasks.Finish(req.TaskID, nil, "command or workload completion unconfirmed; never retry automatically")
		} else {
			entry, err = tasks.Finish(req.TaskID, &result, "")
		}
		if err != nil {
			fail(w, http.StatusServiceUnavailable, "task result persistence uncertain; inspect journal before retry")
			return
		}
		// An explicit transport error is still an uncertain task state, not
		// a process exit. Persist and return the record for operator review.
		respond(w, http.StatusOK, entry)
	})
}

func taskErrorStatus(err error) int {
	if errors.Is(err, taskjournal.ErrConflict) || errors.Is(err, taskjournal.ErrBusy) || errors.Is(err, taskjournal.ErrFull) {
		return http.StatusConflict
	}
	return http.StatusServiceUnavailable
}
