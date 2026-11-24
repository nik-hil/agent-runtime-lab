package tests

import (
	"errors"
	"net/http"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/fleetdaemon"
	"agent-runtime-lab/src/taskevidence"
	"agent-runtime-lab/src/taskjournal"
)

// TestE2BFullLifecycle is deliberately opt-in. Default CI compiles it but cannot
// claim live E2B validation without the local Embed API, proxy and credentials.
// It creates at most two VM identities, with one active VM reservation at a time.
func TestE2BFullLifecycle(t *testing.T) {
	if os.Getenv("LAB_E2B_INTEGRATION") != "1" {
		t.Skip("set LAB_E2B_INTEGRATION=1 to require the live E2B release gate")
	}
	if runtime.GOOS != "linux" {
		t.Fatal("the E2B release gate requires its Linux server")
	}
	v := newLiveFleet(t)
	v.start(t, "armed", true)
	var originalSandbox string
	stage := func(name string, run func(*testing.T)) bool {
		return t.Run(name, run)
	}
	if !stage("admission_and_quotas", func(t *testing.T) {
		v.call(t, http.MethodPost, "/v1/submit", map[string]string{
			"ID": "one", "Tenant": "alpha",
		}, http.StatusAccepted, nil)
		state := v.jobs(t, map[string]admission.State{"one": admission.Running})
		originalSandbox = state.Fleet.Jobs["one"].Workload.SandboxID
		if originalSandbox == "" || state.Fleet.Admission.Used != (admission.Resources{CPU: 2, MemoryMiB: 2048}) {
			t.Fatal("running VM did not hold its full reservation")
		}
		v.call(t, http.MethodPost, "/v1/submit", map[string]string{
			"ID": "queued", "Tenant": "alpha",
		}, http.StatusAccepted, nil)
		state = v.jobs(t, map[string]admission.State{"one": admission.Running, "queued": admission.Queued})
		if state.Fleet.Admission.Pending != 1 || state.Fleet.Jobs["queued"].Workload.SandboxID != "" {
			t.Fatal("queued workload allocated a VM or was not counted")
		}
		v.call(t, http.MethodPost, "/v1/submit", map[string]string{
			"ID": "over-quota", "Tenant": "alpha",
		}, http.StatusConflict, nil)
		if _, ok := v.status(t).Fleet.Jobs["over-quota"]; ok {
			t.Fatal("rejected quota request was admitted")
		}
	}) {
		return
	}
	if !stage("pause_resume_and_identity", func(t *testing.T) {
		v.intent(t, "one", "paused")
		v.jobs(t, map[string]admission.State{"one": admission.Paused, "queued": admission.Running})
		v.intent(t, "one", "running")
		v.jobs(t, map[string]admission.State{"one": admission.ResumeQueued, "queued": admission.Running})
		v.intent(t, "queued", "deleted")
		state := v.jobs(t, map[string]admission.State{"one": admission.Running, "queued": admission.Deleted})
		if state.Fleet.Jobs["one"].Workload.SandboxID != originalSandbox {
			t.Fatal("resume changed sandbox identity")
		}
	}) {
		return
	}
	if !stage("command_exit_codes", func(t *testing.T) {
		if result := v.command(t, "echo COMMAND_OK", 0); result.Stdout != "COMMAND_OK\n" {
			t.Fatal("successful stdout not captured")
		}
		if result := v.command(t, "echo BEFORE_EXIT; exit 17", 17); result.Stdout != "BEFORE_EXIT\n" {
			t.Fatal("nonzero process exit was not captured as a process result")
		}
	}) {
		return
	}
	if !stage("evidence_replay_and_conflict", func(t *testing.T) {
		request := liveTask("verify-once", "echo marker >> /tmp/verify-once; wc -l /tmp/verify-once", "/tmp/verify-once")
		entry := v.task(t, "/v1/tasks", request)
		requireEvidence(t, entry, taskjournal.Succeeded, 0)
		if entry.Result.Stdout != "1 /tmp/verify-once\n" || len(entry.Evidence.Manifest.Artifacts) != 1 {
			t.Fatal("unexpected marker result or artifact count")
		}
		artifact := entry.Evidence.Manifest.Artifacts[0]
		if artifact.Size != 7 || artifact.SHA256 != taskevidence.Hash([]byte("marker\n")) {
			t.Fatal("artifact bytes were not captured correctly")
		}
		replay := v.task(t, "/v1/tasks", request)
		if !reflect.DeepEqual(entry, replay) {
			t.Fatal("replay changed the stored task result")
		}
		request["command"] = "echo DIFFERENT_COMMAND"
		v.call(t, http.MethodPost, "/v1/tasks", request, http.StatusConflict, nil)
		if result := v.command(t, "wc -l /tmp/verify-once", 0); result.Stdout != "1 /tmp/verify-once\n" {
			t.Fatal("replay duplicated the side effect")
		}
	}) {
		return
	}
	if !stage("failed_task_evidence", func(t *testing.T) {
		entry := v.task(t, "/v1/tasks", liveTask("verify-fail", "echo BEFORE_EXIT; exit 17"))
		requireEvidence(t, entry, taskjournal.Failed, 17)
		saved := v.task(t, "/v1/tasks/verify-fail", nil)
		if !reflect.DeepEqual(entry, saved) || entry.Result.Stdout != "BEFORE_EXIT\n" {
			t.Fatal("failed task result was not durably preserved")
		}
	}) {
		return
	}
	if !stage("audit_metrics_and_ttl_renewal", func(t *testing.T) {
		var monitor fleetdaemon.MonitorStatus
		v.wait(t, "successful inventory audit and TTL renewal", func() bool {
			v.call(t, http.MethodGet, "/v1/audit", nil, http.StatusOK, &monitor)
			return monitor.HasReport && monitor.RenewAttempts > 0
		})
		if monitor.Fenced || monitor.Failures != 0 || monitor.RenewFailures != 0 {
			t.Fatalf("monitor failed: %+v", monitor)
		}
		for _, finding := range monitor.Report.Findings {
			if finding.Severity == "critical" {
				t.Fatalf("critical inventory drift: %+v", finding)
			}
		}
		code, body, err := v.request(http.MethodGet, "/v1/metrics", nil)
		if err != nil || code != http.StatusOK || !strings.Contains(string(body), "fleetd_ttl_renew_attempts_total") {
			t.Fatalf("monitor metrics missing: status=%d error=%v", code, err)
		}
		v.status(t)
	}) {
		return
	}
	crashRequest := liveTask("verify-crash",
		"echo marker >> /tmp/verify-crash-once; printf artifact > /tmp/verify-crash-artifact; wc -l /tmp/verify-crash-once",
		"/tmp/verify-crash-once", "/tmp/verify-crash-artifact")
	if !stage("crash_before_host_result", func(t *testing.T) {
		_, _, err := v.request(http.MethodPost, "/v1/tasks", crashRequest)
		if err == nil {
			t.Fatal("named crash returned a confirmed response")
		}
		select {
		case <-v.process.done:
		case <-time.After(15 * time.Second):
			t.Fatal("daemon did not exit at the evidence crash point")
		}
		var exit *exec.ExitError
		if !errors.As(v.process.err, &exit) || exit.ExitCode() != 86 {
			t.Fatalf("expected named crash exit 86, got %v", v.process.err)
		}
		if err := v.stop(); err != nil {
			t.Fatal(err)
		}
		v.start(t, "recovery", false)
		state := v.jobs(t, map[string]admission.State{"one": admission.Running})
		if state.Fleet.Jobs["one"].Workload.SandboxID != originalSandbox {
			t.Fatal("restart changed the original VM identity")
		}
		entry := v.task(t, "/v1/tasks/verify-crash", nil)
		requireUncertain(t, entry)
		replay := v.task(t, "/v1/tasks", crashRequest)
		if !reflect.DeepEqual(entry, replay) {
			t.Fatal("uncertain submission replay changed the record")
		}
		v.call(t, http.MethodPost, "/v1/tasks",
			liveTask("verify-blocked", "echo MUST_NOT_RUN"), http.StatusConflict, nil)
	}) {
		return
	}
	if !stage("reject_missing_or_changed_evidence", func(t *testing.T) {
		entry := v.task(t, "/v1/tasks/verify-crash", nil)
		requireUncertain(t, entry)
		marker := taskevidence.ManifestPath(*entry.EvidenceIntent)
		// All mutations below target this test's disposable evidence, never
		// the append-only side-effect counter or another fleet's files.
		v.command(t, "mv "+marker+" "+marker+".saved", 0)
		v.call(t, http.MethodPost, "/v1/tasks/verify-crash/recover", struct{}{}, http.StatusConflict, nil)
		requireUncertain(t, v.task(t, "/v1/tasks/verify-crash", nil))
		v.command(t, "printf corrupted > "+marker, 0)
		v.call(t, http.MethodPost, "/v1/tasks/verify-crash/recover", struct{}{}, http.StatusConflict, nil)
		requireUncertain(t, v.task(t, "/v1/tasks/verify-crash", nil))
		v.command(t, "mv "+marker+".saved "+marker, 0)
		v.command(t, "mv /tmp/verify-crash-artifact /tmp/verify-crash-artifact.saved", 0)
		v.call(t, http.MethodPost, "/v1/tasks/verify-crash/recover", struct{}{}, http.StatusConflict, nil)
		requireUncertain(t, v.task(t, "/v1/tasks/verify-crash", nil))
		v.command(t, "printf changed > /tmp/verify-crash-artifact", 0)
		v.call(t, http.MethodPost, "/v1/tasks/verify-crash/recover", struct{}{}, http.StatusConflict, nil)
		requireUncertain(t, v.task(t, "/v1/tasks/verify-crash", nil))
		v.command(t, "mv /tmp/verify-crash-artifact.saved /tmp/verify-crash-artifact", 0)
	}) {
		return
	}
	if !stage("recover_without_rerunning", func(t *testing.T) {
		entry := v.task(t, "/v1/tasks/verify-crash/recover", struct{}{})
		requireEvidence(t, entry, taskjournal.Succeeded, 0)
		if entry.Result.Stdout != "1 /tmp/verify-crash-once\n" ||
			entry.Note != "recovered from verified VM completion evidence; command was not rerun" {
			t.Fatal("did not recover the original completion evidence")
		}
		repeated := v.task(t, "/v1/tasks/verify-crash/recover", struct{}{})
		if !reflect.DeepEqual(entry, repeated) {
			t.Fatal("repeated recovery changed terminal evidence")
		}
		if result := v.command(t, "wc -l /tmp/verify-crash-once", 0); result.Stdout != "1 /tmp/verify-crash-once\n" {
			t.Fatal("crash recovery duplicated the side effect")
		}
	}) {
		return
	}
	stage("timeout_stays_uncertain", func(t *testing.T) {
		request := liveTask("verify-timeout", "sleep 3; echo AFTER_TIMEOUT")
		request["timeout_seconds"] = 1
		entry := v.task(t, "/v1/tasks", request)
		requireUncertain(t, entry)
		v.call(t, http.MethodPost, "/v1/tasks",
			liveTask("verify-timeout-blocked", "echo MUST_NOT_RUN"), http.StatusConflict, nil)
	})
	// cleanup is registered before any allocation. It stops this test's
	// daemon, invokes standalone journal cleanup, and independently observes
	// absence for every VM ID seen by this run. Failure fails the parent test.
}

func liveTask(id, command string, artifacts ...string) map[string]any {
	return map[string]any{
		"task_id": id, "workload_id": "one", "command": command,
		"timeout_seconds": 30, "evidence": true, "artifacts": artifacts,
	}
}

func requireUncertain(t *testing.T, entry taskjournal.Entry) {
	t.Helper()
	if entry.State != taskjournal.Uncertain || entry.Result != nil || entry.Evidence != nil || entry.EvidenceIntent == nil {
		t.Fatalf("expected uncertain evidence task with no claimed result: %+v", entry)
	}
}

func requireEvidence(t *testing.T, entry taskjournal.Entry, state taskjournal.State, exit int) {
	t.Helper()
	if entry.State != state || entry.Result == nil || entry.Result.ExitCode != exit ||
		entry.Evidence == nil || entry.EvidenceIntent == nil || entry.FinishedAt == nil ||
		!taskevidence.IsHash(entry.ResultHash) {
		t.Fatalf("invalid terminal evidence task: %+v", entry)
	}
	if err := entry.Evidence.Validate(entry.EvidenceIdentity()); err != nil {
		t.Fatalf("invalid evidence binding: %v", err)
	}
}
