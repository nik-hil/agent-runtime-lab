package taskjournal

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"agent-runtime-lab/src/m4exec"
	"agent-runtime-lab/src/taskevidence"
)

func enqueueTest(t *testing.T, s *Store, id, vm string) Entry {
	t.Helper()
	e, created, err := s.Enqueue(id, "work", vm, "echo private-command", 30, taskevidence.Hash([]byte("helper")), nil)
	if err != nil || !created || e.State != Queued {
		t.Fatalf("enqueue: %+v %t %v", e, created, err)
	}
	return e
}

func TestQueuedCommandsResumeButClaimedCommandsNeverReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "tasks.json")
	s := openTest(t, path)
	e := enqueueTest(t, s, "one", "vm-one")
	enqueueTest(t, s, "two", "vm-one")
	second := s.journal.Tasks["two"]
	second.StartedAt = e.StartedAt.Add(-time.Hour) // a wall-clock step cannot reorder durable FIFO
	s.journal.Tasks["two"] = second
	if err := s.save(); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	public, _ := json.Marshal(e)
	if !strings.Contains(string(data), "echo private-command") || strings.Contains(string(public), "private-command") {
		t.Fatal("queued command was not persisted privately")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openTest(t, path)
	restored, _ := s.Get("one")
	if restored.State != Queued || restored.EvidenceIntent.Nonce != e.EvidenceIntent.Nonce {
		t.Fatal("queued intent changed across restart")
	}
	claimed, command, run, err := s.ClaimNext(e.EvidenceIntent.HelperSHA256)
	if err != nil || !run || command != "echo private-command" || claimed.ID != "one" || claimed.DispatchedAt == nil {
		t.Fatalf("claim: %+v %q %v %v", claimed, command, run, err)
	}
	data, _ = os.ReadFile(path)
	var checkpoint checkpoint
	if json.Unmarshal(data, &checkpoint) != nil || checkpoint.Commands["one"] != "" || checkpoint.Commands["two"] == "" {
		t.Fatal("claim did not consume only its command")
	}
	if _, _, err := s.Begin("sync", "work", "vm-one", "echo sync", 30); !errors.Is(err, ErrBusy) {
		t.Fatal("async claim did not fence synchronous tasks", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openTest(t, path)
	restored, _ = s.Get("one")
	if restored.State != Uncertain {
		t.Fatal("interrupted async dispatch did not become uncertain")
	}
	if _, _, run, err := s.ClaimNext(e.EvidenceIntent.HelperSHA256); err != nil || run {
		t.Fatal("uncertainty did not hold subsequent jobs", err)
	}
	enqueueTest(t, s, "other-vm", "vm-two")
	other, _, run, err := s.ClaimNext(e.EvidenceIntent.HelperSHA256)
	if err != nil || !run || other.ID != "other-vm" {
		t.Fatal("busy VM blocked unrelated VM", err)
	}
}

func TestQueueReplayCancellationAndHelperFences(t *testing.T) {
	s := openTest(t, filepath.Join(t.TempDir(), "private", "tasks.json"))
	e := enqueueTest(t, s, "one", "vm-one")
	replay, created, err := s.Enqueue("one", "work", "vm-one", "echo private-command", 30, e.EvidenceIntent.HelperSHA256, nil)
	if err != nil || created || replay.EvidenceIntent.Nonce != e.EvidenceIntent.Nonce {
		t.Fatal("replay created another intent", err)
	}
	if _, _, err := s.Enqueue("one", "work", "vm-one", "changed", 30, e.EvidenceIntent.HelperSHA256, nil); !errors.Is(err, ErrConflict) {
		t.Fatal("changed input reused ID", err)
	}
	if _, _, err := s.BeginEvidence("one", "work", "vm-one", "echo private-command", 30, e.EvidenceIntent.HelperSHA256, nil); !errors.Is(err, ErrConflict) {
		t.Fatal("sync endpoint could claim async ID", err)
	}
	if _, _, run, _ := s.ClaimNext(taskevidence.Hash([]byte("new-helper"))); run {
		t.Fatal("changed helper dispatched queued command")
	}
	if _, _, run, _ := s.ClaimNextEligible(e.EvidenceIntent.HelperSHA256, map[string]bool{}); run {
		t.Fatal("ineligible workload dispatched")
	}
	cancelled, err := s.Cancel("one")
	if err != nil || cancelled.State != Cancelled || cancelled.DispatchedAt != nil || !cancelled.CancelRequested || s.journal.Commands["one"] != "" {
		t.Fatal("queued cancellation left runnable command", err)
	}
	enqueueTest(t, s, "running", "vm-one")
	claimed, _, _, _ := s.ClaimNext(e.EvidenceIntent.HelperSHA256)
	requested, err := s.Cancel(claimed.ID)
	if err != nil || requested.State != Running || !requested.CancelRequested {
		t.Fatal("running cancellation invented a remote outcome", err)
	}
	if _, err := s.Finish(claimed.ID, nil, "cancellation outcome unconfirmed"); err != nil {
		t.Fatal(err)
	}
	proof := proofFor(t, claimed, 0)
	if _, err := s.FinishEvidence(claimed.ID, proof, true); !errors.Is(err, ErrConflict) {
		t.Fatal("async completion bypassed artifact receipt", err)
	}
	done, err := s.FinishJobEvidence(claimed.ID, proof, true)
	if err != nil || !done.ArtifactsStored || done.State != Succeeded {
		t.Fatal("verified durable completion did not resolve job", err)
	}
}

func TestConcurrentClaimsNeverShareSandbox(t *testing.T) {
	s := openTest(t, filepath.Join(t.TempDir(), "private", "tasks.json"))
	e := enqueueTest(t, s, "one", "vm-one")
	enqueueTest(t, s, "two", "vm-one")
	var wg sync.WaitGroup
	claimed := make(chan bool, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, run, err := s.ClaimNext(e.EvidenceIntent.HelperSHA256)
			if err != nil {
				t.Error(err)
			}
			claimed <- run
		}()
	}
	wg.Wait()
	close(claimed)
	count := 0
	for run := range claimed {
		if run {
			count++
		}
	}
	if count != 1 {
		t.Fatal("concurrent workers claimed the same sandbox", count)
	}
}

func TestInvalidQueuedCommandCheckpointRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "tasks.json")
	s := openTest(t, path)
	enqueueTest(t, s, "one", "vm")
	s.journal.Commands["one"] = "tampered command"
	if err := s.save(); err != nil {
		t.Fatal(err)
	}
	s.Close()
	if corrupt, err := Open(path); err == nil {
		corrupt.Close()
		t.Fatal("tampered queued command was accepted")
	}
}

func TestSynchronousTaskFencesQueuedJob(t *testing.T) {
	s := openTest(t, filepath.Join(t.TempDir(), "private", "tasks.json"))
	if _, _, err := s.Begin("sync", "work", "vm", "echo sync", 30); err != nil {
		t.Fatal(err)
	}
	e := enqueueTest(t, s, "async", "vm")
	if _, _, run, err := s.ClaimNext(e.EvidenceIntent.HelperSHA256); err != nil || run {
		t.Fatal("queued job ignored synchronous VM owner", err)
	}
	if _, err := s.Finish("sync", &m4exec.Result{SandboxID: "vm"}, ""); err != nil {
		t.Fatal(err)
	}
	if claimed, _, run, err := s.ClaimNext(e.EvidenceIntent.HelperSHA256); err != nil || !run || claimed.ID != "async" {
		t.Fatal("terminal sync task failed to release queue", err)
	}
}
