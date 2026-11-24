package tests

import (
	"errors"
	"net/http"
	"os"
	"os/exec"
	"reflect"
	"testing"
	"time"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/fleetdaemon"
	"agent-runtime-lab/src/taskevidence"
	"agent-runtime-lab/src/taskjournal"
)

func TestE2BAsyncJobs(t *testing.T) {
	if os.Getenv("LAB_E2B_INTEGRATION") != "1" {
		t.Skip("set LAB_E2B_INTEGRATION=1 to require live async jobs and durable artifacts")
	}
	v := newLiveFleet(t)
	v.start(t, "queue-paused", false, "--job-workers", "0")
	v.call(t, http.MethodPost, "/v1/submit", map[string]string{"ID": "one", "Tenant": "alpha"}, http.StatusAccepted, nil)
	v.jobs(t, map[string]admission.State{"one": admission.Running})
	request := func(id, command string, paths ...string) fleetdaemon.JobRequest {
		return fleetdaemon.JobRequest{ID: id, WorkloadID: "one", Command: command, Timeout: 30, Artifacts: paths}
	}
	submit := func(t *testing.T, req fleetdaemon.JobRequest) taskjournal.Entry {
		t.Helper()
		var e taskjournal.Entry
		v.call(t, http.MethodPost, "/v1/jobs", req, http.StatusAccepted, &e)
		if !e.Async || e.State != taskjournal.Queued {
			t.Fatalf("submit did not return queued acceptance: %+v", e)
		}
		return e
	}
	status := func(t *testing.T, id string, want taskjournal.State) taskjournal.Entry {
		t.Helper()
		var e taskjournal.Entry
		v.wait(t, "async job "+id+" "+string(want), func() bool {
			v.call(t, http.MethodGet, "/v1/jobs/"+id, nil, http.StatusOK, &e)
			return e.State == want
		})
		return e
	}
	once := request("async-once", "echo marker >> /tmp/m44-once; while [ ! -e /tmp/m44-release ]; do sleep 0.1; done; wc -l /tmp/m44-once", "/tmp/m44-once")
	var first taskjournal.Entry
	if !t.Run("durable_acceptance_and_queued_cancel", func(t *testing.T) {
		first = submit(t, once)
		submit(t, request("async-cancel", "echo MUST_NOT_RUN > /tmp/m44-cancel"))
		var cancelled taskjournal.Entry
		v.call(t, http.MethodPost, "/v1/jobs/async-cancel/cancel", struct{}{}, http.StatusOK, &cancelled)
		if cancelled.State != taskjournal.Cancelled || cancelled.DispatchedAt != nil {
			t.Fatal("queued cancellation claimed a dispatched job")
		}
		submit(t, request("async-next", "printf next > /tmp/m44-next", "/tmp/m44-next"))
		if err := v.stop(); err != nil {
			t.Fatal(err)
		}
		v.start(t, "queue-restarted", false)
	}) {
		return
	}
	if !t.Run("restart_dispatch_vm_serialization_and_replay", func(t *testing.T) {
		running := status(t, "async-once", taskjournal.Running)
		if running.EvidenceIntent.Nonce != first.EvidenceIntent.Nonce {
			t.Fatal("restart changed queued identity")
		}
		var queued taskjournal.Entry
		v.call(t, http.MethodGet, "/v1/jobs/async-next", nil, http.StatusOK, &queued)
		if queued.State != taskjournal.Queued {
			t.Fatal("two async jobs dispatched on one VM")
		}
		v.call(t, http.MethodPost, "/v1/tasks", liveTask("async-sync-blocked", "echo MUST_NOT_RUN"), http.StatusConflict, nil)
		v.command(t, "touch /tmp/m44-release", 0)
		done := status(t, "async-once", taskjournal.Succeeded)
		requireEvidence(t, done, taskjournal.Succeeded, 0)
		if !done.ArtifactsStored || done.Result.Stdout != "1 /tmp/m44-once\n" {
			t.Fatal("completed job lacks durable one-marker evidence")
		}
		var replay taskjournal.Entry
		v.call(t, http.MethodPost, "/v1/jobs", once, http.StatusOK, &replay)
		if !reflect.DeepEqual(done, replay) {
			t.Fatal("replay changed completed job")
		}
		changed := once
		changed.Command = "echo DIFFERENT"
		v.call(t, http.MethodPost, "/v1/jobs", changed, http.StatusConflict, nil)
		status(t, "async-next", taskjournal.Succeeded)
		if result := v.command(t, "test ! -e /tmp/m44-cancel; wc -l /tmp/m44-once", 0); result.Stdout != "1 /tmp/m44-once\n" {
			t.Fatal("cancelled job ran or replay duplicated marker")
		}
	}) {
		return
	}
	if !t.Run("failed_job_has_durable_evidence", func(t *testing.T) {
		submit(t, request("async-fail", "echo BEFORE_EXIT; exit 17"))
		done := status(t, "async-fail", taskjournal.Failed)
		requireEvidence(t, done, taskjournal.Failed, 17)
		if !done.ArtifactsStored {
			t.Fatal("failed job lost its durable proof")
		}
	}) {
		return
	}
	if !t.Run("crash_after_artifacts_before_result", func(t *testing.T) {
		if err := v.stop(); err != nil {
			t.Fatal(err)
		}
		v.start(t, "artifact-crash-armed", false, "--test-crash-after-artifacts-job", "async-offline-proof")
		submit(t, request("async-offline-proof", "sleep 1; echo marker >> /tmp/m44-offline-proof; wc -l /tmp/m44-offline-proof", "/tmp/m44-offline-proof"))
		select {
		case <-v.process.done:
		case <-time.After(45 * time.Second):
			t.Fatal("artifact crash point was not reached")
		}
		var exit *exec.ExitError
		if !errors.As(v.process.err, &exit) || exit.ExitCode() != 87 {
			t.Fatalf("expected stored-artifact crash exit 87: %v", v.process.err)
		}
		if err := v.stop(); err != nil {
			t.Fatal(err)
		}
		v.start(t, "offline-recovery", false, "--job-workers", "0")
		uncertain := status(t, "async-offline-proof", taskjournal.Uncertain)
		if uncertain.Result != nil || uncertain.ArtifactsStored {
			t.Fatal("crash did not leave a genuinely uncertain host record")
		}
	}) {
		return
	}
	if !t.Run("delete_vm_then_recover_and_download", func(t *testing.T) {
		v.intent(t, "one", "deleted")
		v.jobs(t, map[string]admission.State{"one": admission.Deleted})
		var recovered taskjournal.Entry
		v.call(t, http.MethodPost, "/v1/jobs/async-offline-proof/recover", struct{}{}, http.StatusOK, &recovered)
		requireEvidence(t, recovered, taskjournal.Succeeded, 0)
		if !recovered.ArtifactsStored || recovered.Result.Stdout != "1 /tmp/m44-offline-proof\n" {
			t.Fatal("offline recovery lost committed evidence")
		}
		for _, id := range []string{"async-once", "async-offline-proof"} {
			code, body, err := v.request(http.MethodGet, "/v1/jobs/"+id+"/artifacts/0", nil)
			if err != nil || code != http.StatusOK || string(body) != "marker\n" {
				t.Fatalf("artifact did not survive VM deletion: %s status=%d err=%v body=%q", id, code, err, body)
			}
		}
		var replay taskjournal.Entry
		v.call(t, http.MethodPost, "/v1/jobs", once, http.StatusOK, &replay)
		if replay.ResultHash == "" || !replay.ArtifactsStored {
			t.Fatal("deleted VM prevented read-only replay")
		}
	}) {
		return
	}
	t.Run("running_cancel_fences_new_work", func(t *testing.T) {
		if err := v.stop(); err != nil {
			t.Fatal(err)
		}
		v.start(t, "cancellation", false)
		v.call(t, http.MethodPost, "/v1/submit", map[string]string{"ID": "cancel-vm", "Tenant": "alpha"}, http.StatusAccepted, nil)
		v.jobs(t, map[string]admission.State{"cancel-vm": admission.Running})
		req := request("async-running-cancel", "echo BEFORE_CANCEL; sleep 10; echo AFTER_CANCEL")
		req.WorkloadID = "cancel-vm"
		submit(t, req)
		status(t, req.ID, taskjournal.Running)
		v.call(t, http.MethodPost, "/v1/jobs/"+req.ID+"/cancel", struct{}{}, http.StatusOK, nil)
		uncertain := status(t, req.ID, taskjournal.Uncertain)
		if !uncertain.CancelRequested || uncertain.Result != nil {
			t.Fatal("running cancellation claimed remote rollback")
		}
		next := request("async-held", "echo MUST_NOT_RUN")
		next.WorkloadID = "cancel-vm"
		submit(t, next)
		var held taskjournal.Entry
		v.call(t, http.MethodGet, "/v1/jobs/async-held", nil, http.StatusOK, &held)
		if held.State != taskjournal.Queued {
			t.Fatal("uncertain job failed to fence queued work")
		}
		v.call(t, http.MethodPost, "/v1/jobs/async-held/cancel", struct{}{}, http.StatusOK, nil)
		if uncertain.EvidenceIntent.HelperSHA256 != taskevidence.Hash(mustReadHelper(t, v)) {
			t.Fatal("job was not bound to source-built helper")
		}
	})
}

func mustReadHelper(t *testing.T, v *liveFleet) []byte {
	t.Helper()
	data, err := os.ReadFile(v.dir + "/task-agent")
	if err != nil {
		t.Fatal(err)
	}
	return data
}
