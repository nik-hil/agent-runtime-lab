package fleetdaemon

import (
	"fmt"
	"net/http"
	"sort"
	"time"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/fleet"
)

type trackedJob struct {
	fingerprint string
	since       time.Time
	alerted     bool
}

// retryDelay is exponential and capped. We do not retry Create blindly:
// the fleet journal fences ambiguous remote creation independently.
func (d *Daemon) retryDelay(failures uint64) time.Duration {
	delay := d.interval
	for i := uint64(1); i < failures; i++ {
		if delay >= d.maxBackoff/2 {
			return d.maxBackoff
		}
		delay *= 2
	}
	if delay > d.maxBackoff {
		return d.maxBackoff
	}
	return delay
}

// A queued sandbox waiting for capacity is a *potentially* stalled workload;
// it is not proof that its microVM is unhealthy. We intentionally avoid
// detecting lack of progress for converged running or paused sandboxes.
func needsProgress(job fleet.JobStatus) bool {
	switch job.Entry.State {
	case admission.Queued, admission.Reserved, admission.ResumeQueued, admission.ResumeHeld, admission.Uncertain:
		return true
	case admission.Running:
		return job.Workload.Desired != "running"
	case admission.Paused:
		return job.Workload.Desired != "paused"
	default:
		return false
	}
}

func (d *Daemon) observeJobs(now time.Time) {
	fleetState := d.controller.Status()
	d.mu.Lock()
	stuck := make([]StuckJob, 0)
	newAlerts := make([]StuckJob, 0)
	for id, job := range fleetState.Jobs {
		if !needsProgress(job) {
			delete(d.tracked, id)
			continue
		}
		fingerprint := string(job.Entry.State) + ":" + job.Workload.Desired + ":" +
			fmt.Sprint(job.Workload.CreatePending) + ":" + job.Workload.SandboxID
		tracked, ok := d.tracked[id]
		if !ok || tracked.fingerprint != fingerprint {
			tracked = trackedJob{fingerprint: fingerprint, since: now}
		}
		age := now.Sub(tracked.since)
		if age >= d.stuckAfter {
			item := StuckJob{ID: id, State: string(job.Entry.State), Desired: job.Workload.Desired,
				AgeSeconds: int64(age / time.Second)}
			stuck = append(stuck, item)
			if !tracked.alerted {
				newAlerts = append(newAlerts, item)
				tracked.alerted = true
			}
		}
		d.tracked[id] = tracked
	}
	for id := range d.tracked {
		if _, ok := fleetState.Jobs[id]; !ok {
			delete(d.tracked, id)
		}
	}
	sort.Slice(stuck, func(i, j int) bool { return stuck[i].ID < stuck[j].ID })
	d.status.StuckJobs = stuck
	d.mu.Unlock()
	for _, item := range newAlerts {
		d.logger.Warn("fleet_workload_stalled", "job_id", item.ID, "state", item.State,
			"desired", item.Desired, "age_seconds", item.AgeSeconds)
	}
}

// writeMetrics intentionally exposes only bounded-cardinality aggregate
// measurements over the private Unix socket. No tenant, sandbox IDs, API keys
// or user-supplied labels enter the Prometheus exposition format.
func writeMetrics(w http.ResponseWriter, r ReconcilerStatus, f fleet.Status) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	counter := func(name, help string, value uint64) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", name, help, name, name, value)
	}
	gauge := func(name, help string, value int64) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n%s %d\n", name, help, name, name, value)
	}
	counter("fleetd_reconcile_cycles_total", "Completed reconciliation cycles.", r.Cycles)
	counter("fleetd_reconcile_failures_total", "Reconciliation cycles ending in error.", r.Failures)
	counter("fleetd_reconcile_transitions_total", "Controller state transitions and remote mutations attempted.", r.Transitions)
	gauge("fleetd_reconcile_consecutive_failures", "Current consecutive error count.", int64(r.ConsecutiveFailures))
	gauge("fleetd_stuck_jobs", "Pending or nonconverged jobs past the local detection threshold.", int64(len(r.StuckJobs)))
	gauge("fleetd_admission_used_cpu", "Reserved CPU cores in this fleet journal.", f.Admission.Used.CPU)
	gauge("fleetd_admission_used_memory_mib", "Reserved memory MiB in this fleet journal.", f.Admission.Used.MemoryMiB)
	gauge("fleetd_admission_pending", "Pending admission requests.", int64(f.Admission.Pending))
	unsafe := int64(0)
	if f.Unsafe {
		unsafe = 1
	}
	gauge("fleetd_admission_unsafe", "Whether new admission is blocked by detected unsafe capacity.", unsafe)
	backoff := int64(0)
	if until := time.Until(r.BackoffUntil); until > 0 {
		backoff = int64((until + time.Second - 1) / time.Second)
	}
	gauge("fleetd_reconcile_backoff_seconds", "Seconds until the next permitted timer-driven attempt.", backoff)
	count := map[admission.State]int64{}
	for _, job := range f.Jobs {
		count[job.Entry.State]++
	}
	fmt.Fprint(w, "# HELP fleetd_workloads Workloads by admission state.\n# TYPE fleetd_workloads gauge\n")
	for _, state := range []admission.State{admission.Queued, admission.Reserved, admission.Running,
		admission.Paused, admission.ResumeQueued, admission.ResumeHeld, admission.Uncertain, admission.Deleted} {
		fmt.Fprintf(w, "fleetd_workloads{state=%q} %d\n", string(state), count[state])
	}
}
