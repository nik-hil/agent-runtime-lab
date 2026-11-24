package fleetdaemon

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"agent-runtime-lab/src/artifactstore"
	"agent-runtime-lab/src/taskevidence"
	"agent-runtime-lab/src/taskjournal"
)

func jobError(w http.ResponseWriter, err error) {
	code, message := http.StatusServiceUnavailable, "job journal or storage unavailable; inspect daemon state"
	switch {
	case errors.Is(err, taskjournal.ErrNotFound), errors.Is(err, artifactstore.ErrNotFound):
		code, message = http.StatusNotFound, "job or artifact not found"
	case errors.Is(err, taskjournal.ErrConflict), errors.Is(err, taskjournal.ErrBusy):
		code, message = http.StatusConflict, "job identity conflict, busy execution or unavailable completion evidence; inspect status"
	case errors.Is(err, taskjournal.ErrFull), errors.Is(err, artifactstore.ErrQuota):
		code, message = http.StatusConflict, "job journal or artifact storage capacity reached"
	case errors.Is(err, artifactstore.ErrInvalid):
		code, message = http.StatusInternalServerError, "durable artifact integrity verification failed"
	}
	fail(w, code, message)
}

func (j *Jobs) submit(w http.ResponseWriter, r *http.Request) {
	j.admissionGate.Lock()
	defer j.admissionGate.Unlock()
	var req JobRequest
	if !parseBounded(w, r, &req, 64<<10) {
		return
	}
	if invalidID(req.ID) || req.ID == "." || req.ID == ".." || invalidID(req.WorkloadID) || req.Command == "" || len(req.Command) > 4000 || strings.IndexByte(req.Command, 0) >= 0 || req.Timeout < 1 || req.Timeout > 60 {
		fail(w, http.StatusBadRequest, "job_id, workload_id, command <=4000 bytes and timeout_seconds 1..60 required")
		return
	}
	paths, err := taskevidence.NormalizeArtifacts(req.Artifacts)
	if err != nil {
		fail(w, http.StatusBadRequest, "invalid declared artifact paths")
		return
	}
	req.Artifacts = paths
	// Replay remains read-only even after the VM is deleted or the fleet drains.
	if old, err := j.tasks.Get(req.ID); err == nil {
		if !old.Async {
			jobError(w, taskjournal.ErrConflict)
			return
		}
		entry, _, err := j.tasks.Enqueue(req.ID, req.WorkloadID, old.SandboxID, req.Command, req.Timeout, old.EvidenceIntent.HelperSHA256, req.Artifacts)
		if err != nil {
			jobError(w, err)
			return
		}
		respond(w, http.StatusOK, entry)
		return
	} else if !errors.Is(err, taskjournal.ErrNotFound) {
		jobError(w, err)
		return
	}
	state := j.controller.Status()
	vm, ok := state.Jobs[req.WorkloadID]
	if !ok || j.controller.Draining() || state.Unsafe || vm.Entry.State != "running" || vm.Workload.Desired != "running" || vm.Workload.CreatePending || vm.Workload.SandboxID == "" {
		fail(w, http.StatusConflict, "new job requires an owned healthy RUNNING workload")
		return
	}
	if j.executor.EvidenceHelperHash() == "" {
		fail(w, http.StatusNotImplemented, "new jobs require fleetd --task-helper")
		return
	}
	entry, _, err := j.tasks.Enqueue(req.ID, req.WorkloadID, vm.Workload.SandboxID, req.Command, req.Timeout, j.executor.EvidenceHelperHash(), req.Artifacts)
	if err != nil {
		// Request validation above is user input, not a persistence failure.
		if !errors.Is(err, taskjournal.ErrConflict) && !errors.Is(err, taskjournal.ErrFull) && !errors.Is(err, taskjournal.ErrPoisoned) {
			fail(w, http.StatusBadRequest, "invalid declared artifact paths")
		} else {
			jobError(w, err)
		}
		return
	}
	respond(w, http.StatusAccepted, entry)
}

func (j *Jobs) Handler(base http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/v1/cluster-capabilities" {
			respond(w, http.StatusOK, map[string]any{"capacity": j.controller.Status().Admission.Capacity, "template_size": j.controller.TemplateResources(), "helper_sha256": j.executor.EvidenceHelperHash(), "workers": j.opts.Workers})
			return
		}
		if r.Method == http.MethodPost && r.URL.Path == "/v1/drain" {
			j.admissionGate.Lock()
			defer j.admissionGate.Unlock()
			// Fence fleet admission first. Cancel queued work even when worker
			// dispatch is paused; a partial cancellation cannot restart work.
			if _, err := j.controller.BeginDrain(); err != nil {
				jobError(w, err)
				return
			}
			entries, err := j.tasks.List()
			if err != nil {
				jobError(w, err)
				return
			}
			for _, entry := range entries {
				if entry.Async && (entry.State == taskjournal.Queued || entry.State == taskjournal.Running) {
					if _, err := j.Cancel(entry.ID); err != nil {
						jobError(w, err)
						return
					}
				}
			}
			base.ServeHTTP(w, r)
			return
		}
		if r.URL.Path != "/v1/jobs" && !strings.HasPrefix(r.URL.Path, "/v1/jobs/") {
			base.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		if r.URL.Path == "/v1/jobs" {
			if r.Method == http.MethodPost {
				j.submit(w, r)
				return
			}
			if r.Method != http.MethodGet {
				fail(w, http.StatusMethodNotAllowed, "use GET or POST")
				return
			}
			entries, err := j.tasks.List()
			if err != nil {
				jobError(w, err)
				return
			}
			sort.Slice(entries, func(a, b int) bool { return entries[a].QueueSequence < entries[b].QueueSequence })
			jobs := make([]map[string]any, 0)
			counts := make(map[taskjournal.State]int)
			busy := make(map[string]taskjournal.State)
			for _, e := range entries {
				if e.State == taskjournal.Uncertain || e.State == taskjournal.Running && busy[e.SandboxID] != taskjournal.Uncertain {
					busy[e.SandboxID] = e.State
				}
			}
			for _, e := range entries {
				if e.Async {
					counts[e.State]++
					reason := ""
					if e.State == taskjournal.Queued {
						switch {
						case j.opts.Workers == 0:
							reason = "dispatch_paused"
						case j.executor.EvidenceHelperHash() != e.EvidenceIntent.HelperSHA256:
							reason = "helper_changed"
						case !j.owned(e):
							reason = "workload_not_ready_or_fleet_fenced"
						case busy[e.SandboxID] == taskjournal.Uncertain:
							reason = "vm_outcome_uncertain"
						case busy[e.SandboxID] == taskjournal.Running:
							reason = "vm_busy"
						default:
							reason = "waiting_for_worker"
						}
					}
					jobs = append(jobs, map[string]any{"id": e.ID, "workload_id": e.WorkloadID, "sandbox_id": e.SandboxID,
						"state": e.State, "started_at": e.StartedAt, "dispatched_at": e.DispatchedAt,
						"cancel_requested": e.CancelRequested, "artifacts_stored": e.ArtifactsStored, "queue_reason": reason, "queue_sequence": e.QueueSequence})
				}
			}
			respond(w, http.StatusOK, map[string]any{"jobs": jobs, "state_counts": counts, "workers": j.opts.Workers, "storage": j.artifacts.Stats()})
			return
		}
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/jobs/"), "/")
		if len(parts) > 3 || invalidID(parts[0]) {
			fail(w, http.StatusBadRequest, "invalid job route")
			return
		}
		entry, err := j.tasks.Get(parts[0])
		if err != nil || !entry.Async {
			if err == nil {
				err = taskjournal.ErrNotFound
			}
			jobError(w, err)
			return
		}
		switch {
		case len(parts) == 1 && r.Method == http.MethodGet:
			respond(w, http.StatusOK, entry)
		case len(parts) == 2 && r.Method == http.MethodPost && (parts[1] == "cancel" || parts[1] == "recover"):
			var empty struct{}
			if !parse(w, r, &empty) {
				return
			}
			if parts[1] == "cancel" {
				entry, err = j.Cancel(entry.ID)
			} else {
				ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
				defer cancel()
				entry, err = j.Recover(ctx, entry.ID)
			}
			if err != nil {
				jobError(w, err)
				return
			}
			respond(w, http.StatusOK, entry)
		case (len(parts) == 2 || len(parts) == 3) && parts[1] == "artifacts" && r.Method == http.MethodGet:
			if !entry.ArtifactsStored || entry.Evidence == nil {
				fail(w, http.StatusConflict, "job has no committed artifact bundle; inspect or recover it")
				return
			}
			if len(parts) == 2 {
				respond(w, http.StatusOK, map[string]any{"artifacts": entry.Evidence.Manifest.Artifacts})
				return
			}
			index, err := strconv.Atoi(parts[2])
			if err != nil || index < 0 || strconv.Itoa(index) != parts[2] {
				fail(w, http.StatusBadRequest, "artifact index must be a nonnegative integer")
				return
			}
			if index >= len(entry.Evidence.Manifest.Artifacts) {
				jobError(w, artifactstore.ErrNotFound)
				return
			}
			data, meta, err := j.artifacts.Read(entry.EvidenceIdentity(), index)
			if err != nil {
				jobError(w, err)
				return
			}
			if meta != entry.Evidence.Manifest.Artifacts[index] {
				jobError(w, artifactstore.ErrInvalid)
				return
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=artifact-%d", index))
			w.Header().Set("X-Artifact-SHA256", meta.SHA256)
			w.Header().Set("ETag", "\""+meta.SHA256+"\"")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data)
		default:
			fail(w, http.StatusNotFound, "unknown job operation or method")
		}
	})
}
