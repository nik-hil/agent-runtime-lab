package taskjournal

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"agent-runtime-lab/src/m4exec"
)

func openTest(t *testing.T, path string) *Store {
	t.Helper()
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestIntentBeforeExecutionAndReplayReturnsStoredTerminal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "tasks.json")
	s := openTest(t, path)
	entry, run, err := s.Begin("task-1", "work-1", "vm-1", "echo good", 30)
	if err != nil || !run || entry.State != Running || entry.CommandHash != commandHash("echo good") {
		t.Fatalf("durable Begin: %+v %v %v", entry, run, err)
	}
	data, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(data), `"state": "running"`) || strings.Contains(string(data), "echo good") {
		t.Fatalf("journal not flushed before execution or stored raw command: %s %v", data, err)
	}
	stat, _ := os.Stat(path)
	if stat.Mode().Perm() != 0600 {
		t.Fatalf("task journal permissions: %o", stat.Mode().Perm())
	}
	got, run, err := s.Begin("task-1", "work-1", "vm-1", "echo good", 30)
	if err != nil || run || got.State != Running {
		t.Fatalf("duplicate executed again: %+v %v %v", got, run, err)
	}
	if _, _, err := s.Begin("task-1", "work-1", "vm-1", "echo different", 30); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed payload accepted: %v", err)
	}
	if _, _, err := s.Begin("task-2", "work-1", "vm-1", "id", 30); !errors.Is(err, ErrBusy) {
		t.Fatalf("second command on busy VM accepted: %v", err)
	}
	result := &m4exec.Result{SandboxID: "vm-1", Stdout: "good\n", ExitCode: 0}
	finished, err := s.Finish("task-1", result, "")
	if err != nil || finished.State != Succeeded || len(finished.ResultHash) != 64 {
		t.Fatalf("terminal evidence lost: %+v %v", finished, err)
	}
	result.Stdout = "mutated"
	if finished.Result.Stdout != "good\n" {
		t.Fatal("caller changed committed evidence")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openTest(t, path)
	got, run, err = s.Begin("task-1", "work-1", "vm-1", "echo good", 30)
	if err != nil || run || got.State != Succeeded || got.Result.Stdout != "good\n" {
		t.Fatalf("replayed a finished task after restart: %+v %v %v", got, run, err)
	}
	if _, run, err := s.Begin("task-2", "work-1", "vm-1", "id", 30); err != nil || !run {
		t.Fatalf("finished task should not block new task: %v %v", run, err)
	}
}

func TestLostReplyRestartFencesAndNeverReplays(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "tasks.json")
	s := openTest(t, path)
	if _, run, err := s.Begin("side-effect", "work", "vm-1", "touch /tmp/important", 30); err != nil || !run {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openTest(t, path)
	got, run, err := s.Begin("side-effect", "work", "vm-1", "touch /tmp/important", 30)
	if err != nil || run || got.State != Uncertain || got.Note == "" {
		t.Fatalf("interrupted intent reran: %+v %v %v", got, run, err)
	}
	if _, _, err := s.Begin("new-task", "work", "vm-1", "echo hi", 30); !errors.Is(err, ErrBusy) {
		t.Fatalf("uncertainty didn't fence VM: %v", err)
	}
	if _, _, err := s.Begin("new-task", "work", "vm-2", "echo hi", 30); err != nil {
		t.Fatalf("unrelated VM unnecessarily blocked: %v", err)
	}
}

func TestNonzeroExitAndUnknownOutcomePersistAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "tasks.json")
	s := openTest(t, path)
	_, _, _ = s.Begin("failed", "work", "vm-fail", "exit 17", 30)
	result, err := s.Finish("failed", &m4exec.Result{SandboxID: "vm-fail", ExitCode: 17, Stderr: "bad"}, "")
	if err != nil || result.State != Failed || result.Result.ExitCode != 17 {
		t.Fatalf("exit status lost: %+v %v", result, err)
	}
	_, _, _ = s.Begin("ambiguous", "work", "vm-unknown", "write side-effect", 30)
	unknown, err := s.Finish("ambiguous", nil, "provider output unavailable")
	if err != nil || unknown.State != Uncertain || unknown.Result != nil {
		t.Fatalf("lost response treated as complete: %+v %v", unknown, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openTest(t, path)
	failed, err := s.Get("failed")
	if err != nil || failed.State != Failed || failed.Result.ExitCode != 17 {
		t.Fatalf("lost completed failed result: %+v %v", failed, err)
	}
	unknown, err = s.Get("ambiguous")
	if err != nil || unknown.State != Uncertain || unknown.Note == "" {
		t.Fatalf("lost uncertainty: %+v %v", unknown, err)
	}
	if _, err := Open(path); err == nil {
		t.Fatal("second process acquired already-held journal")
	}
}

func TestCorruptionAndModifiedEvidenceFailClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "tasks.json")
	s := openTest(t, path)
	_, _, _ = s.Begin("task", "work", "vm-1", "echo hi", 30)
	_, _ = s.Finish("task", &m4exec.Result{SandboxID: "vm-1", Stdout: "hi\n"}, "")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := json.Unmarshal(content, &raw); err != nil {
		t.Fatal(err)
	}
	entry := raw["tasks"].(map[string]any)["task"].(map[string]any)
	entry["result"].(map[string]any)["stdout"] = "tampered"
	b, _ := json.Marshal(raw)
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	if s, err := Open(path); err == nil {
		_ = s.Close()
		t.Fatal("accepted tampered task evidence")
	}
}
