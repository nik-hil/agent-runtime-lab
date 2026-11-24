package fleetdaemon

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"agent-runtime-lab/src/fleet"
	"agent-runtime-lab/src/m4exec"
)

type execFake struct {
	calls       int
	id, command string
	onRun       func()
	exitCode    int
}

func (f *execFake) Run(_ context.Context, id, command string) (m4exec.Result, error) {
	f.calls++
	f.id, f.command = id, command
	if f.onRun != nil {
		f.onRun()
	}
	return m4exec.Result{SandboxID: id, Stdout: "VM:done", ExitCode: f.exitCode}, nil
}

func callExecute(t *testing.T, handler http.Handler, req any) *httptest.ResponseRecorder {
	t.Helper()
	b, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, "/v1/execute", bytes.NewReader(b))
	r.Header.Set("Content-Type", "application/json")
	out := httptest.NewRecorder()
	handler.ServeHTTP(out, r)
	return out
}

func runnable(t *testing.T) (*fleet.Controller, string) {
	t.Helper()
	c, _, _ := monitorFixture(t)
	if err := c.Submit("alpha", "work"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := c.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	return c, c.Status().Jobs["work"].Workload.SandboxID
}

func TestExecuteOnlyOwnedRunningSandboxAndReportsExit(t *testing.T) {
	c, sandboxID := runnable(t)
	exec := &execFake{}
	handler := ExecuteHandler(http.NewServeMux(), c, exec)
	r := callExecute(t, handler, map[string]any{"id": "work", "command": "echo ready", "timeout_seconds": 10})
	if r.Code != http.StatusOK || exec.calls != 1 || exec.id != sandboxID || exec.command != "echo ready" ||
		!bytes.Contains(r.Body.Bytes(), []byte(`"stdout":"VM:done"`)) {
		t.Fatalf("execution did not target the owned VM: HTTP %d body=%s", r.Code, r.Body.String())
	}
	for _, req := range []map[string]any{
		{"id": "missing", "command": "id", "timeout_seconds": 10},
		{"id": "work", "command": "id", "timeout_seconds": 120},
		{"id": "../work", "command": "id", "timeout_seconds": 10},
	} {
		r = callExecute(t, handler, req)
		if r.Code == http.StatusOK {
			t.Fatalf("accepted unsafe task: %+v", req)
		}
	}
	if exec.calls != 1 {
		t.Fatalf("executed unauthorized commands: %d", exec.calls)
	}
}

func TestExecuteRefusesDrainAndConcurrentIntent(t *testing.T) {
	c, _ := runnable(t)
	exec := &execFake{onRun: func() {
		_, _ = c.BeginDrain()
	}}
	handler := ExecuteHandler(http.NewServeMux(), c, exec)
	r := callExecute(t, handler, map[string]any{"id": "work", "command": "echo side-effect", "timeout_seconds": 10})
	if r.Code != http.StatusConflict {
		t.Fatalf("accepted command result after concurrent drain: %d %s", r.Code, r.Body.String())
	}
	r = callExecute(t, handler, map[string]any{"id": "work", "command": "echo again", "timeout_seconds": 10})
	if r.Code != http.StatusConflict || exec.calls != 1 {
		t.Fatal("allowed new execution during drain")
	}
}

func TestExecuteNonzeroProcessExitIsHTTP200WithExitCode(t *testing.T) {
	c, _ := runnable(t)
	exec := &execFake{exitCode: 17}
	handler := ExecuteHandler(http.NewServeMux(), c, exec)
	r := callExecute(t, handler, map[string]any{"id": "work", "command": "exit 17", "timeout_seconds": 10})
	if r.Code != http.StatusOK || exec.calls != 1 || !bytes.Contains(r.Body.Bytes(), []byte(`"exit_code":17`)) {
		t.Fatalf("nonzero VM process exit misclassified as HTTP transport failure: code=%d body=%s", r.Code, r.Body.String())
	}
}
