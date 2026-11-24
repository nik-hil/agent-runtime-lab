package fleetdaemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"agent-runtime-lab/src/fleet"
	"agent-runtime-lab/src/m4exec"
	"agent-runtime-lab/src/taskjournal"
)

type durableFake struct {
	calls  int
	result m4exec.Result
	err    error
	onRun  func()
}

func (f *durableFake) Run(_ context.Context, id, command string) (m4exec.Result, error) {
	f.calls++
	if f.onRun != nil {
		f.onRun()
	}
	result := f.result
	result.SandboxID = id
	return result, f.err
}

func taskHandlerFixture(t *testing.T) (*fleet.Controller, *taskjournal.Store, *durableFake, http.Handler) {
	t.Helper()
	c, _ := runnable(t)
	s, err := taskjournal.Open(filepath.Join(t.TempDir(), "private", "tasks.json"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	backend := &durableFake{result: m4exec.Result{Stdout: "hello\n"}}
	return c, s, backend, TaskHandler(http.NewServeMux(), c, backend, s)
}

func sendTask(t *testing.T, h http.Handler, id, command string) *httptest.ResponseRecorder {
	t.Helper()
	request := map[string]any{
		"task_id": id, "workload_id": "work", "command": command, "timeout_seconds": 30,
	}
	data, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, "/v1/tasks", bytes.NewReader(data))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestDurableTaskCompletionAndReplayNeverReexecutes(t *testing.T) {
	_, store, backend, handler := taskHandlerFixture(t)
	response := sendTask(t, handler, "task-1", "echo hello")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"state":"succeeded"`) || backend.calls != 1 {
		t.Fatalf("task result: HTTP %d %s calls=%d", response.Code, response.Body.String(), backend.calls)
	}
	first, err := store.Get("task-1")
	if err != nil || first.Result == nil || first.Result.Stdout != "hello\n" || len(first.ResultHash) != 64 {
		t.Fatalf("missing durable evidence: %+v %v", first, err)
	}
	response = sendTask(t, handler, "task-1", "echo hello")
	if response.Code != http.StatusOK || backend.calls != 1 {
		t.Fatalf("duplicate task re-executed: HTTP %d calls=%d", response.Code, backend.calls)
	}
	response = sendTask(t, handler, "task-1", "echo DIFFERENT")
	if response.Code != http.StatusConflict || backend.calls != 1 {
		t.Fatalf("same ID bound to a new payload: HTTP %d calls=%d", response.Code, backend.calls)
	}
	get := httptest.NewRecorder()
	handler.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/v1/tasks/task-1", nil))
	if get.Code != http.StatusOK || !strings.Contains(get.Body.String(), first.ResultHash) {
		t.Fatalf("GET task returned wrong evidence: %d %s", get.Code, get.Body.String())
	}
}

func TestNonzeroExitStoredAsFailedNotUncertain(t *testing.T) {
	_, store, backend, handler := taskHandlerFixture(t)
	backend.result.ExitCode = 17
	backend.result.Stdout = "BEFORE_EXIT\n"
	resp := sendTask(t, handler, "task-fail", "echo BEFORE_EXIT; exit 17")
	entry, err := store.Get("task-fail")
	if resp.Code != http.StatusOK || err != nil || entry.State != taskjournal.Failed || entry.Result.ExitCode != 17 {
		t.Fatalf("nonzero exit marked transport error: %d %s %+v %v", resp.Code, resp.Body.String(), entry, err)
	}
}

func TestTransportLossIsDurableUncertainAndBlocksVM(t *testing.T) {
	_, store, backend, handler := taskHandlerFixture(t)
	backend.err = errors.New("broken stream")
	response := sendTask(t, handler, "first", "echo effects")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"state":"uncertain"`) {
		t.Fatalf("lost result not treated as uncertain: HTTP %d %s", response.Code, response.Body.String())
	}
	entry, err := store.Get("first")
	if err != nil || entry.State != taskjournal.Uncertain || entry.Result != nil || entry.Note == "" {
		t.Fatalf("lost response gained a fake result: %+v %v", entry, err)
	}
	response = sendTask(t, handler, "first", "echo effects")
	if response.Code != http.StatusOK || backend.calls != 1 {
		t.Fatalf("replayed ambiguous task: HTTP %d calls=%d", response.Code, backend.calls)
	}
	response = sendTask(t, handler, "second", "echo more")
	if response.Code != http.StatusConflict || backend.calls != 1 {
		t.Fatalf("ambiguous VM allowed another command: HTTP %d calls=%d", response.Code, backend.calls)
	}
}

func TestDrainDuringTaskNeverCommitsSuccessfulCompletion(t *testing.T) {
	c, store, backend, handler := taskHandlerFixture(t)
	backend.onRun = func() {
		if _, err := c.BeginDrain(); err != nil {
			t.Error(err)
		}
	}
	response := sendTask(t, handler, "drained", "touch /tmp/side-effect")
	entry, err := store.Get("drained")
	if response.Code != http.StatusOK || err != nil || entry.State != taskjournal.Uncertain || backend.calls != 1 {
		t.Fatalf("completed task despite concurrent drain: HTTP %d %+v %v", response.Code, entry, err)
	}
	response = sendTask(t, handler, "new", "echo denied")
	if response.Code != http.StatusConflict || backend.calls != 1 {
		t.Fatalf("accepted new task after drain: HTTP %d", response.Code)
	}
}
