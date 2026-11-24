package taskjournal

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"agent-runtime-lab/src/m4exec"
	"agent-runtime-lab/src/taskevidence"
)

func proofFor(t *testing.T, entry Entry, exit int) taskevidence.Envelope {
	t.Helper()
	manifest := taskevidence.Manifest{Identity: entry.EvidenceIdentity(), CompletedAt: time.Now().UTC(), ExitCode: exit,
		Stdout: "completed\n", StdoutSHA256: taskevidence.Hash([]byte("completed\n")),
		StderrSHA256: taskevidence.Hash(nil), Artifacts: []taskevidence.Artifact{}}
	for _, path := range entry.EvidenceIntent.Artifacts {
		manifest.Artifacts = append(manifest.Artifacts, taskevidence.Artifact{Path: path, Size: 4, SHA256: taskevidence.Hash([]byte("data"))})
	}
	proof, err := taskevidence.Seal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	return proof
}

func TestEvidenceIntentCrashRecoveryAndRelease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "tasks.json")
	s := openTest(t, path)
	helperHash := taskevidence.Hash([]byte("helper"))
	entry, run, err := s.BeginEvidence("task", "work", "vm-1", "echo completed", 30, helperHash, []string{"/tmp/output"})
	if err != nil || !run || entry.EvidenceIntent == nil {
		t.Fatalf("evidence intent missing: %+v %v", entry, err)
	}
	proof := proofFor(t, entry, 0)
	entry.EvidenceIntent.Artifacts[0] = "/tmp/caller-mutation"
	stored, _ := s.Get("task")
	if stored.EvidenceIntent.Artifacts[0] != "/tmp/output" {
		t.Fatal("caller mutated durable intent")
	}
	if _, err := s.Finish("task", &m4exec.Result{SandboxID: "vm-1"}, ""); err == nil {
		t.Fatal("evidence task completed without its manifest")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openTest(t, path)
	stored, _ = s.Get("task")
	if stored.State != Uncertain || stored.EvidenceIntent.Nonce != proof.Manifest.Identity.Intent.Nonce {
		t.Fatalf("startup lost uncertainty or original identity: %+v", stored)
	}
	if _, _, err := s.Begin("other", "work", "vm-1", "echo new", 30); !errors.Is(err, ErrBusy) {
		t.Fatalf("uncertain task failed to fence VM: %v", err)
	}
	bad := proof
	bad.Manifest.Identity.TaskID = "different-task"
	if _, err := s.FinishEvidence("task", bad, true); !errors.Is(err, ErrConflict) {
		t.Fatalf("cross-task recovery accepted: %v", err)
	}
	resolved, err := s.FinishEvidence("task", proof, true)
	if err != nil || resolved.State != Succeeded || resolved.Evidence == nil || !strings.Contains(resolved.Note, "not rerun") {
		t.Fatalf("valid recovery did not complete task: %+v %v", resolved, err)
	}
	resolved.Evidence.Manifest.Artifacts[0].Path = "/tmp/mutated-proof"
	again, err := s.FinishEvidence("task", proof, true)
	if err != nil || again.Evidence.Manifest.Artifacts[0].Path != "/tmp/output" {
		t.Fatalf("terminal evidence was mutable: %+v %v", again, err)
	}
	if _, run, err := s.Begin("other", "work", "vm-1", "echo new", 30); err != nil || !run {
		t.Fatalf("verified completion didn't release VM: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openTest(t, path)
	stored, err = s.Get("task")
	if err != nil || stored.State != Succeeded || stored.Evidence.SHA256 != proof.SHA256 {
		t.Fatalf("recovered proof did not survive restart: %+v %v", stored, err)
	}
}

func TestEvidenceRequestBindingAndKnownFailure(t *testing.T) {
	s := openTest(t, filepath.Join(t.TempDir(), "private", "tasks.json"))
	helper := taskevidence.Hash([]byte("helper"))
	entry, _, err := s.BeginEvidence("task", "work", "vm-1", "exit 17", 30, helper, nil)
	if err != nil {
		t.Fatal(err)
	}
	duplicate, run, err := s.BeginEvidence("task", "work", "vm-1", "exit 17", 30, taskevidence.Hash([]byte("upgraded helper")), nil)
	if err != nil || run || duplicate.EvidenceIntent.Nonce != entry.EvidenceIntent.Nonce {
		t.Fatalf("repeat request recreated evidence intent: %+v %v", duplicate, err)
	}
	if _, _, err := s.Begin("task", "work", "vm-1", "exit 17", 30); !errors.Is(err, ErrConflict) {
		t.Fatalf("evidence mode removed from existing task: %v", err)
	}
	if _, _, err := s.BeginEvidence("task", "work", "vm-1", "exit 17", 30, helper, []string{"/tmp/new"}); !errors.Is(err, ErrConflict) {
		t.Fatalf("artifact list changed for existing task: %v", err)
	}
	proof := proofFor(t, entry, 17)
	if _, err := s.FinishEvidence("task", proof, true); !errors.Is(err, ErrConflict) {
		t.Fatalf("recovered a still-running task: %v", err)
	}
	finished, err := s.FinishEvidence("task", proof, false)
	if err != nil || finished.State != Failed || finished.Result.ExitCode != 17 {
		t.Fatalf("known nonzero evidence exit lost: %+v %v", finished, err)
	}
}

func TestVersionOneMigrationPreservesRecordsAndWritesCurrentVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "tasks.json")
	s := openTest(t, path)
	_, _, _ = s.Begin("legacy", "work", "vm-1", "echo original", 30)
	original, _ := s.Finish("legacy", &m4exec.Result{SandboxID: "vm-1", Stdout: "original"}, "")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	var old checkpoint
	if err := json.Unmarshal(data, &old); err != nil {
		t.Fatal(err)
	}
	old.Version = 1
	data, _ = json.Marshal(old)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	s = openTest(t, path)
	got, err := s.Get("legacy")
	if err != nil || got.ResultHash != original.ResultHash || got.EvidenceIntent != nil {
		t.Fatalf("legacy record changed during migration: %+v %v", got, err)
	}
	data, _ = os.ReadFile(path)
	var migrated checkpoint
	if err := json.Unmarshal(data, &migrated); err != nil || migrated.Version != version {
		t.Fatalf("migration not persisted: %s %v", data, err)
	}
}
