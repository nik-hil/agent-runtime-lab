package fleetdaemon

import (
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"agent-runtime-lab/src/taskevidence"
	"agent-runtime-lab/src/taskjournal"
)

func TestEvidenceCrashHookRunsAfterDurableIntentBeforeHostCompletion(t *testing.T) {
	c, _ := runnable(t)
	path := filepath.Join(t.TempDir(), "private", "tasks.json")
	store, err := taskjournal.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	backend := &evidenceFake{helperHash: taskevidence.Hash([]byte("helper"))}
	crash := errors.New("simulated abrupt process stop")
	called := false
	wrapped := EvidenceHookExecutor{Executor: backend, EvidenceExecutor: backend, AfterVerified: func(id string) {
		called = true
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var checkpoint struct {
			Tasks map[string]taskjournal.Entry
		}
		if err := json.Unmarshal(data, &checkpoint); err != nil {
			t.Fatal(err)
		}
		entry := checkpoint.Tasks[id]
		if entry.State != taskjournal.Running || entry.Result != nil || entry.Evidence != nil ||
			entry.EvidenceIntent == nil || entry.EvidenceIntent.Nonce != backend.proof.Manifest.Identity.Intent.Nonce {
			t.Fatalf("crash window was not after intent and before completion: %+v", entry)
		}
		if err := backend.proof.Validate(entry.EvidenceIdentity()); err != nil {
			t.Fatal(err)
		}
		panic(crash)
	}}
	handler := TaskHandler(http.NewServeMux(), c, wrapped, store)
	func() {
		defer func() {
			if value := recover(); value != crash {
				t.Fatalf("expected simulated stop, got %v", value)
			}
		}()
		_ = sendEvidenceTask(t, handler, "crash")
	}()
	if !called || backend.executions != 1 {
		t.Fatal("verified VM completion did not reach the crash hook exactly once")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := taskjournal.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	entry, err := restarted.Get("crash")
	if err != nil || entry.State != taskjournal.Uncertain {
		t.Fatalf("startup did not preserve uncertainty: %+v %v", entry, err)
	}
	normal := TaskHandler(http.NewServeMux(), c, backend, restarted)
	response := recoverEvidence(t, normal, "crash")
	entry, err = restarted.Get("crash")
	if response.Code != http.StatusOK || err != nil || entry.State != taskjournal.Succeeded ||
		backend.executions != 1 || backend.reads != 1 {
		t.Fatalf("explicit recovery did not resolve without replay: %d %+v %v", response.Code, entry, err)
	}
}

func TestEvidenceCrashHookIgnoresUnverifiedResponsesAndIsOptional(t *testing.T) {
	_, store, backend, _ := evidenceHandlerFixture(t)
	c, _ := runnable(t)
	called := false
	wrapped := EvidenceHookExecutor{Executor: backend, EvidenceExecutor: backend, AfterVerified: func(string) { called = true }}
	backend.lostReply = true
	handler := TaskHandler(http.NewServeMux(), c, wrapped, store)
	response := sendEvidenceTask(t, handler, "lost")
	entry, err := store.Get("lost")
	if response.Code != http.StatusOK || err != nil || entry.State != taskjournal.Uncertain || called {
		t.Fatalf("unverified response invoked crash hook: %d %+v %v", response.Code, entry, err)
	}
	c2, store2, backend2, _ := evidenceHandlerFixture(t)
	normal := EvidenceHookExecutor{Executor: backend2, EvidenceExecutor: backend2}
	response = sendEvidenceTask(t, TaskHandler(http.NewServeMux(), c2, normal, store2), "normal")
	entry, err = store2.Get("normal")
	if response.Code != http.StatusOK || err != nil || entry.State != taskjournal.Succeeded {
		t.Fatalf("disabled hook changed normal execution: %d %+v %v", response.Code, entry, err)
	}
}
