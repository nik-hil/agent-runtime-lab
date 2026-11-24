package fleetdaemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"agent-runtime-lab/src/fleet"
	"agent-runtime-lab/src/taskevidence"
	"agent-runtime-lab/src/taskjournal"
)

type evidenceFake struct {
	durableFake
	executions int
	reads      int
	helperHash string
	proof      taskevidence.Envelope
	lostReply  bool
	exitCode   int
	readError  error
	onRead     func()
}

func (f *evidenceFake) EvidenceHelperHash() string { return f.helperHash }

func (f *evidenceFake) RunEvidence(_ context.Context, identity taskevidence.Identity, _ string) (taskevidence.Envelope, error) {
	f.executions++
	f.proof, _ = taskevidence.Seal(taskevidence.Manifest{Identity: identity, CompletedAt: time.Now().UTC(), ExitCode: f.exitCode,
		Stdout: "completed\n", StdoutSHA256: taskevidence.Hash([]byte("completed\n")), StderrSHA256: taskevidence.Hash(nil),
		Artifacts: []taskevidence.Artifact{}})
	if f.lostReply {
		return taskevidence.Envelope{}, errors.New("provider lost response after marker committed")
	}
	return f.proof, nil
}

func (f *evidenceFake) ReadEvidence(_ context.Context, _ taskevidence.Identity) (taskevidence.Envelope, error) {
	f.reads++
	if f.onRead != nil {
		f.onRead()
	}
	return f.proof, f.readError
}

func evidenceHandlerFixture(t *testing.T) (*fleet.Controller, *taskjournal.Store, *evidenceFake, http.Handler) {
	t.Helper()
	c, store, _, _ := taskHandlerFixture(t)
	backend := &evidenceFake{helperHash: taskevidence.Hash([]byte("helper"))}
	return c, store, backend, TaskHandler(http.NewServeMux(), c, backend, store)
}

func sendEvidenceTask(t *testing.T, handler http.Handler, id string) *httptest.ResponseRecorder {
	t.Helper()
	data, err := json.Marshal(map[string]any{"task_id": id, "workload_id": "work", "command": "echo completed",
		"timeout_seconds": 30, "evidence": true})
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/tasks", bytes.NewReader(data))
	r.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(w, r)
	return w
}

func recoverEvidence(t *testing.T, handler http.Handler, id string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/tasks/"+id+"/recover", strings.NewReader("{}"))
	r.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(w, r)
	return w
}

func TestLostEvidenceReplyRequiresExplicitRecoveryAndNeverReexecutes(t *testing.T) {
	_, store, backend, handler := evidenceHandlerFixture(t)
	backend.lostReply = true
	response := sendEvidenceTask(t, handler, "task")
	entry, err := store.Get("task")
	if response.Code != http.StatusOK || err != nil || entry.State != taskjournal.Uncertain || backend.executions != 1 {
		t.Fatalf("lost response didn't preserve uncertainty: %d %+v %v", response.Code, entry, err)
	}
	response = sendEvidenceTask(t, handler, "task")
	if response.Code != http.StatusOK || backend.executions != 1 || backend.reads != 0 {
		t.Fatal("ordinary resubmit executed or implicitly recovered uncertain work")
	}
	response = sendTask(t, handler, "other", "echo next")
	if response.Code != http.StatusConflict || backend.calls != 0 {
		t.Fatal("uncertainty failed to fence VM")
	}
	response = recoverEvidence(t, handler, "task")
	entry, err = store.Get("task")
	if response.Code != http.StatusOK || err != nil || entry.State != taskjournal.Succeeded || entry.Evidence == nil ||
		backend.executions != 1 || backend.reads != 1 || !strings.Contains(entry.Note, "not rerun") {
		t.Fatalf("explicit evidence recovery failed: %d %s %+v %v", response.Code, response.Body.String(), entry, err)
	}
	response = recoverEvidence(t, handler, "task")
	if response.Code != http.StatusOK || backend.reads != 1 || backend.executions != 1 {
		t.Fatal("terminal recovery mutated or inspected remote work again")
	}
	response = sendTask(t, handler, "other", "echo next")
	if response.Code != http.StatusOK || backend.calls != 1 {
		t.Fatalf("verified recovery didn't release VM: %d %s", response.Code, response.Body.String())
	}
}

func TestInvalidOrMissingEvidenceCannotReleaseVM(t *testing.T) {
	_, store, backend, handler := evidenceHandlerFixture(t)
	backend.lostReply = true
	_ = sendEvidenceTask(t, handler, "task")
	backend.readError = errors.New("missing marker")
	response := recoverEvidence(t, handler, "task")
	if response.Code != http.StatusConflict {
		t.Fatalf("missing marker accepted: %d", response.Code)
	}
	backend.readError = nil
	backend.proof.Manifest.Identity.Intent.Nonce = taskevidence.Hash([]byte("wrong attempt"))
	backend.proof, _ = taskevidence.Seal(backend.proof.Manifest)
	response = recoverEvidence(t, handler, "task")
	entry, _ := store.Get("task")
	if response.Code != http.StatusConflict || entry.State != taskjournal.Uncertain || backend.executions != 1 {
		t.Fatalf("wrong marker resolved task: %d %+v", response.Code, entry)
	}
}

func TestRecoveryRejectsConcurrentDrain(t *testing.T) {
	c, store, backend, handler := evidenceHandlerFixture(t)
	backend.lostReply = true
	_ = sendEvidenceTask(t, handler, "task")
	backend.onRead = func() {
		if _, err := c.BeginDrain(); err != nil {
			t.Error(err)
		}
	}
	response := recoverEvidence(t, handler, "task")
	entry, _ := store.Get("task")
	if response.Code != http.StatusConflict || entry.State != taskjournal.Uncertain {
		t.Fatalf("recovery ignored drain: %d %+v", response.Code, entry)
	}
}

func TestLegacyUncertaintyCannotBeRecoveredAndHelperIsOptIn(t *testing.T) {
	_, store, backend, handler := evidenceHandlerFixture(t)
	backend.durableFake.err = errors.New("lost legacy response")
	_ = sendTask(t, handler, "legacy", "echo legacy")
	response := recoverEvidence(t, handler, "legacy")
	entry, _ := store.Get("legacy")
	if response.Code != http.StatusConflict || entry.State != taskjournal.Uncertain || backend.reads != 0 {
		t.Fatal("legacy uncertainty gained invented completion evidence")
	}
	_, freshStore, freshBackend, freshHandler := evidenceHandlerFixture(t)
	freshBackend.helperHash = ""
	response = sendEvidenceTask(t, freshHandler, "no-helper")
	if response.Code != http.StatusNotImplemented || freshBackend.executions != 0 {
		t.Fatal("evidence execution accepted without helper configuration")
	}
	if _, err := freshStore.Get("no-helper"); !errors.Is(err, taskjournal.ErrNotFound) {
		t.Fatalf("unsupported request wrote task intent: %v", err)
	}
}

func TestEvidenceModeSuccessfulCompletionAndDuplicateOptions(t *testing.T) {
	_, store, backend, handler := evidenceHandlerFixture(t)
	response := sendEvidenceTask(t, handler, "task")
	entry, _ := store.Get("task")
	if response.Code != http.StatusOK || entry.State != taskjournal.Succeeded || entry.Evidence == nil {
		t.Fatalf("successful evidence task lost manifest: %d %+v", response.Code, entry)
	}
	backend.helperHash = "" // stored repeats still work after helper removal
	response = sendEvidenceTask(t, handler, "task")
	if response.Code != http.StatusOK || backend.executions != 1 {
		t.Fatal("duplicate task required helper or executed again")
	}
	response = sendTask(t, handler, "task", "echo completed")
	if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), "evidence options") {
		t.Fatalf("evidence option change wasn't an explicit conflict: %d %s", response.Code, response.Body.String())
	}
}

func TestEvidenceRecoveryRetainsNonzeroProcessExit(t *testing.T) {
	_, store, backend, handler := evidenceHandlerFixture(t)
	backend.lostReply, backend.exitCode = true, 17
	_ = sendEvidenceTask(t, handler, "failed")
	response := recoverEvidence(t, handler, "failed")
	entry, _ := store.Get("failed")
	if response.Code != http.StatusOK || entry.State != taskjournal.Failed || entry.Result.ExitCode != 17 || backend.executions != 1 {
		t.Fatalf("failed completion was lost or repeated: %d %+v", response.Code, entry)
	}
}

func TestTaskJSONBudgetIncludesEscapingAndArtifactOverhead(t *testing.T) {
	_, _, backend, handler := evidenceHandlerFixture(t)
	command := "echo " + strings.Repeat("\t", 3995)
	data, _ := json.Marshal(map[string]any{"task_id": "large", "workload_id": "work", "command": command, "timeout_seconds": 30})
	if len(data) <= 4096 {
		t.Fatal("fixture must exceed the old fleet JSON limit")
	}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/tasks", bytes.NewReader(data))
	r.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(w, r)
	if w.Code != http.StatusOK || backend.calls != 1 {
		t.Fatalf("valid bounded command rejected due to JSON escaping overhead: %d %s", w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodPost, "/v1/tasks", strings.NewReader(string(data)+strings.Repeat(" ", 64<<10)))
	r.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest || backend.calls != 1 {
		t.Fatalf("oversized trailing data accepted: %d %s", w.Code, w.Body.String())
	}
}
