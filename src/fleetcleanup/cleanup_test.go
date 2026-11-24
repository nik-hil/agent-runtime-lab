package fleetcleanup

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/fleet"
	"agent-runtime-lab/src/lifecycle"
)

type testRuntime struct {
	states       map[string]lifecycle.ObservedState
	keys         map[string]string
	creates      int
	deletes      int
	finds        int
	lostCreate   bool
	lostDelete   bool
	keepPresent  bool
	noMatches    bool
	manyMatches  bool
	failDescribe string
	beforeDelete func()
}

func newRuntime() *testRuntime {
	return &testRuntime{states: make(map[string]lifecycle.ObservedState), keys: make(map[string]string)}
}

func (b *testRuntime) Create(_ context.Context, key string) (string, error) {
	b.creates++
	id := "vm-" + key[len(key)-8:]
	b.states[id], b.keys[id] = lifecycle.Running, key
	if b.lostCreate {
		return "", context.DeadlineExceeded
	}
	return id, nil
}

func (b *testRuntime) Describe(_ context.Context, id string) (fleet.Details, error) {
	if id == b.failDescribe {
		return fleet.Details{}, errors.New("provider temporarily unavailable")
	}
	state, found := b.states[id]
	if !found {
		return fleet.Details{State: lifecycle.Absent}, nil
	}
	return fleet.Details{State: state, Resources: admission.Resources{CPU: 1, MemoryMiB: 128}}, nil
}

func (b *testRuntime) Observe(ctx context.Context, id string) (lifecycle.ObservedState, error) {
	details, err := b.Describe(ctx, id)
	return details.State, err
}

func (b *testRuntime) Pause(context.Context, string) error {
	return errors.New("cleanup must never pause")
}

func (b *testRuntime) Resume(context.Context, string) error {
	return errors.New("cleanup must never resume")
}

func (b *testRuntime) Delete(_ context.Context, id string) error {
	if b.beforeDelete != nil {
		b.beforeDelete()
	}
	b.deletes++
	if !b.keepPresent {
		delete(b.states, id)
	}
	if b.lostDelete {
		return context.DeadlineExceeded
	}
	return nil
}

func (b *testRuntime) FindByLogicalKey(_ context.Context, key string) ([]string, error) {
	b.finds++
	if b.noMatches {
		return nil, nil
	}
	if b.manyMatches {
		return []string{"duplicate-a", "duplicate-b"}, nil
	}
	ids := []string{}
	for id, stored := range b.keys {
		if stored == key && b.states[id] != "" {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

func fixture(t *testing.T) (string, *fleet.Controller, *fleet.Store, *testRuntime) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "private", "fleet.json")
	store, err := fleet.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	backend := newRuntime()
	capacity := admission.Resources{CPU: 2, MemoryMiB: 256}
	c, err := fleet.New(store, backend, fleet.Config{TemplateSize: admission.Resources{CPU: 1, MemoryMiB: 128},
		Admission: admission.Config{Capacity: capacity, MaxPending: 4,
			Tenants: map[string]admission.Policy{"alpha": {MaxLogical: 4, MaxActive: capacity}}}}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return path, c, store, backend
}

func start(t *testing.T, c *fleet.Controller, id string) {
	t.Helper()
	if err := c.Submit("alpha", id); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := c.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func quickOptions() Options { return Options{Timeout: time.Second, Poll: 100 * time.Millisecond} }

func TestStoppedDaemonCleanupUsesSavedPoliciesAndPreservesTaskJournal(t *testing.T) {
	path, c, store, backend := fixture(t)
	start(t, c, "first")
	start(t, c, "second")
	if err := c.Submit("alpha", "queued"); err != nil {
		t.Fatal(err)
	}
	backend.states["unrelated-vm"] = lifecycle.Running
	taskPath := path + ".tasks.json"
	taskBytes := []byte("task journal must remain byte-for-byte unchanged")
	if err := os.WriteFile(taskPath, taskBytes, 0600); err != nil {
		t.Fatal(err)
	}
	backend.beforeDelete = func() {
		data, _ := os.ReadFile(path)
		var journal fleet.Journal
		if err := json.Unmarshal(data, &journal); err != nil || !journal.Draining {
			t.Fatal("provider deletion preceded durable drain intent")
		}
		for _, workload := range journal.Workloads {
			if workload.Desired != "deleted" {
				t.Fatal("not all delete intents were persisted before contacting provider")
			}
		}
	}
	_ = store.Close() // simulate the controller no longer running
	report, err := Run(context.Background(), path, backend, quickOptions())
	if err != nil || !report.Complete || report.Fleet.Admission.Used.CPU != 0 || backend.creates != 2 || backend.deletes != 2 {
		t.Fatalf("cleanup failed or created a VM: %+v creates=%d deletes=%d err=%v", report, backend.creates, backend.deletes, err)
	}
	if backend.states["unrelated-vm"] != lifecycle.Running {
		t.Fatal("cleanup deleted a VM outside the selected journal")
	}
	got, _ := os.ReadFile(taskPath)
	if string(got) != string(taskBytes) {
		t.Fatal("cleanup modified task journal")
	}
	deletes := backend.deletes
	report, err = Run(context.Background(), path, backend, quickOptions())
	if err != nil || !report.Complete || backend.deletes != deletes || backend.creates != 2 {
		t.Fatalf("repeat cleanup was not idempotent: %+v %v", report, err)
	}
}

func TestCleanupRefusesLiveJournalOwnerAndMissingState(t *testing.T) {
	path, c, _, backend := fixture(t)
	start(t, c, "running")
	before, _ := os.ReadFile(path)
	if _, err := Run(context.Background(), path, backend, quickOptions()); err == nil || backend.deletes != 0 {
		t.Fatal("cleanup bypassed live daemon journal lock")
	}
	after, _ := os.ReadFile(path)
	if string(before) != string(after) {
		t.Fatal("blocked cleanup changed live journal")
	}
	missing := filepath.Join(t.TempDir(), "typo", "missing.json")
	if _, err := Run(context.Background(), missing, backend, quickOptions()); err == nil {
		t.Fatal("missing journal reported successful cleanup")
	}
	if _, err := os.Stat(filepath.Dir(missing)); !os.IsNotExist(err) {
		t.Fatal("missing journal created a directory or lock")
	}
}

func TestExpiredVMAndLostDeleteReplyReleaseOnlyAfterObservedAbsence(t *testing.T) {
	for _, expired := range []bool{true, false} {
		t.Run(map[bool]string{true: "already_expired", false: "lost_delete_response"}[expired], func(t *testing.T) {
			path, c, store, backend := fixture(t)
			start(t, c, "vm")
			id := c.Status().Jobs["vm"].Workload.SandboxID
			if expired {
				delete(backend.states, id)
			} else {
				backend.lostDelete = true
			}
			_ = store.Close()
			report, err := Run(context.Background(), path, backend, quickOptions())
			if err != nil || !report.Complete || backend.creates != 1 {
				t.Fatalf("absence was not confirmed safely: %+v %v", report, err)
			}
		})
	}
}

func TestMetadataRecoveryNeverCreatesAndUnresolvedMatchesStayReserved(t *testing.T) {
	for _, mode := range []string{"one", "none", "multiple"} {
		t.Run(mode, func(t *testing.T) {
			path, c, store, backend := fixture(t)
			backend.lostCreate = true
			_ = c.Submit("alpha", "ambiguous")
			if _, err := c.Tick(context.Background()); !errors.Is(err, fleet.ErrCreateUncertain) {
				t.Fatalf("failed to create ambiguity fixture: %v", err)
			}
			backend.lostCreate = false
			start(t, c, "known")
			backend.noMatches, backend.manyMatches = mode == "none", mode == "multiple"
			_ = store.Close()
			report, err := Run(context.Background(), path, backend, quickOptions())
			if backend.creates != 2 || backend.finds != 1 {
				t.Fatalf("cleanup repeated Create or metadata ambiguity: creates=%d finds=%d", backend.creates, backend.finds)
			}
			if mode == "one" {
				if err != nil || !report.Complete || backend.deletes != 2 {
					t.Fatalf("unique ownership not recovered for deletion: %+v %v", report, err)
				}
			} else if err == nil || report.Complete || report.Fleet.Admission.Used.CPU != 1 ||
				report.Fleet.Jobs["known"].Entry.State != admission.Deleted || backend.deletes != 1 {
				t.Fatalf("metadata ambiguity released reservations or blocked known cleanup: %+v %v", report, err)
			}
		})
	}
}

func TestCleanupFailureDoesNotStarveOtherVMsOrClaimSuccess(t *testing.T) {
	path, c, store, backend := fixture(t)
	start(t, c, "a-broken")
	start(t, c, "b-healthy")
	backend.failDescribe = c.Status().Jobs["a-broken"].Workload.SandboxID
	_ = store.Close()
	report, err := Run(context.Background(), path, backend, quickOptions())
	if err == nil || report.Complete || report.Fleet.Jobs["b-healthy"].Entry.State != admission.Deleted ||
		report.Fleet.Admission.Used.CPU != 1 || backend.creates != 2 {
		t.Fatalf("failed VM starved cleanup or lost its reservation: %+v %v", report, err)
	}
}

func TestDeleteAcknowledgementIsNotConfirmedCleanup(t *testing.T) {
	path, c, store, backend := fixture(t)
	start(t, c, "vm")
	backend.keepPresent = true
	_ = store.Close()
	report, err := Run(context.Background(), path, backend, quickOptions())
	if err == nil || report.Complete || report.Fleet.Admission.Used.CPU != 1 || report.Fleet.Jobs["vm"].Workload.SandboxID == "" {
		t.Fatalf("DELETE acknowledgement prematurely released resources: %+v %v", report, err)
	}
}

func TestPausedVMIsDeletedWithoutResume(t *testing.T) {
	path, c, store, backend := fixture(t)
	start(t, c, "paused")
	id := c.Status().Jobs["paused"].Workload.SandboxID
	backend.states[id] = lifecycle.Paused
	_ = store.Close()
	report, err := Run(context.Background(), path, backend, quickOptions())
	if err != nil || !report.Complete || backend.creates != 1 || backend.deletes != 1 {
		t.Fatalf("paused VM was not cleaned directly: %+v %v", report, err)
	}
}

func TestPersistenceFailureCannotReportCompleteFromInMemoryCounters(t *testing.T) {
	path, c, store, backend := fixture(t)
	start(t, c, "vm")
	dir := filepath.Dir(path)
	saved := dir + "-saved"
	backend.beforeDelete = func() {
		// Preserve the actual journal/lock in a moved private test directory,
		// then make the old directory pathname unavailable for atomic writes.
		if err := os.Rename(dir, saved); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(dir, []byte("block checkpoint writes"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	_ = store.Close()
	report, err := Run(context.Background(), path, backend, quickOptions())
	if !errors.Is(err, fleet.ErrStoreUncertain) || report.Complete || backend.deletes != 1 {
		t.Fatalf("uncertain persistence gained a success claim: %+v %v", report, err)
	}
	data, err := os.ReadFile(filepath.Join(saved, filepath.Base(path)))
	if err != nil {
		t.Fatal(err)
	}
	var journal fleet.Journal
	if err := json.Unmarshal(data, &journal); err != nil || journal.Workloads["vm"].SandboxID == "" {
		t.Fatal("fixture did not retain the earlier durable reservation")
	}
}
