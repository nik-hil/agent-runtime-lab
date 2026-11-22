package admission

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

func fixture(t *testing.T, cap Resources, pending int, policies map[string]Policy) *Scheduler {
	t.Helper()
	s, err := New(Config{Capacity: cap, MaxPending: pending, Tenants: policies})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func request(id, tenant string, cpu, mem int64) Request {
	return Request{ID: id, Tenant: tenant, Resources: Resources{CPU: cpu, MemoryMiB: mem}}
}

func TestConcurrentAdmissionNeverOvercommits(t *testing.T) {
	s := fixture(t, Resources{CPU: 8, MemoryMiB: 1024}, 1000,
		map[string]Policy{"team": {MaxLogical: 1000, MaxActive: Resources{CPU: 8, MemoryMiB: 1024}}})
	var wg sync.WaitGroup
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if _, err := s.Submit(request(fmt.Sprint(id), "team", 1, 128)); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Dispatch()
		}()
	}
	wg.Wait()
	snap := s.Snapshot()
	if snap.Used != (Resources{CPU: 8, MemoryMiB: 1024}) || snap.Pending != 492 || snap.Tenant["team"].Logical != 500 {
		t.Fatalf("overcommit/under-reserve: %+v", snap)
	}
}

func TestFairRoundRobinAndBlockedTenant(t *testing.T) {
	s := fixture(t, Resources{CPU: 3, MemoryMiB: 300}, 10,
		map[string]Policy{
			"a": {MaxLogical: 10, MaxActive: Resources{CPU: 3, MemoryMiB: 300}},
			"b": {MaxLogical: 10, MaxActive: Resources{CPU: 3, MemoryMiB: 300}},
		})
	for _, req := range []Request{
		request("a1", "a", 1, 100), request("a2", "a", 1, 100),
		request("b1", "b", 1, 100), request("b2", "b", 1, 100),
	} {
		if _, err := s.Submit(req); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []string{"a1", "b1", "a2"} {
		got, ok := s.Dispatch()
		if !ok || got.Request.ID != id {
			t.Fatalf("expected %s, got %+v %t", id, got, ok)
		}
	}
	if _, ok := s.Dispatch(); ok {
		t.Fatal("admitted a fourth VM beyond capacity")
	}
	if err := s.ConfirmDeleted("a1"); err != nil {
		t.Fatal(err)
	}
	got, ok := s.Dispatch()
	if !ok || got.Request.ID != "b2" {
		t.Fatalf("expected queued b2 after capacity release, got %+v", got)
	}
}

func TestTenantQuotaAndReusedRequestID(t *testing.T) {
	s := fixture(t, Resources{CPU: 10, MemoryMiB: 1000}, 5,
		map[string]Policy{"a": {MaxLogical: 1, MaxActive: Resources{CPU: 2, MemoryMiB: 200}}})
	r := request("job", "a", 1, 100)
	first, err := s.Submit(r)
	if err != nil || first.State != Queued {
		t.Fatalf("first submit: %+v %v", first, err)
	}
	second, err := s.Submit(r)
	if err != nil || second != first {
		t.Fatalf("idempotent submit: %+v %v", second, err)
	}
	if _, err := s.Submit(request("job", "a", 2, 100)); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected conflicting ID, got %v", err)
	}
	if _, err := s.Submit(request("job2", "a", 1, 100)); !errors.Is(err, ErrQuota) {
		t.Fatalf("expected logical quota, got %v", err)
	}
	if err := s.ConfirmDeleted("job"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Submit(request("job2", "a", 1, 100)); err != nil {
		t.Fatal(err)
	}
	old, err := s.Submit(r)
	if err != nil || old.State != Deleted {
		t.Fatalf("old ID must remain terminal: %+v %v", old, err)
	}
}

func TestPauseReleasesResourcesButNotLogicalQuotaAndResumeMustWait(t *testing.T) {
	s := fixture(t, Resources{CPU: 2, MemoryMiB: 200}, 4,
		map[string]Policy{
			"a": {MaxLogical: 2, MaxActive: Resources{CPU: 2, MemoryMiB: 200}},
		})
	for _, id := range []string{"one", "two"} {
		if _, err := s.Submit(request(id, "a", 2, 200)); err != nil {
			t.Fatal(err)
		}
	}
	if e, ok := s.Dispatch(); !ok || e.Request.ID != "one" || e.State != Reserved {
		t.Fatalf("wrong first reservation %+v %v", e, ok)
	}
	if err := s.ConfirmRunning("one"); err != nil {
		t.Fatal(err)
	}
	if err := s.ConfirmPaused("one"); err != nil {
		t.Fatal(err)
	}
	snap := s.Snapshot()
	if snap.Used.CPU != 0 || snap.Tenant["a"].Logical != 2 {
		t.Fatalf("pause should free active resources only: %+v", snap)
	}
	if e, ok := s.Dispatch(); !ok || e.Request.ID != "two" {
		t.Fatalf("second VM should fit when first paused %+v %v", e, ok)
	}
	if err := s.ConfirmRunning("two"); err != nil {
		t.Fatal(err)
	}
	if err := s.QueueResume("one"); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Dispatch(); ok {
		t.Fatal("resumed VM overcommitted host")
	}
	if err := s.ConfirmDeleted("two"); err != nil {
		t.Fatal(err)
	}
	if e, ok := s.Dispatch(); !ok || e.Request.ID != "one" || e.State != ResumeHeld {
		t.Fatalf("resume should reserve capacity again: %+v %v", e, ok)
	}
	if err := s.ConfirmRunning("one"); err != nil {
		t.Fatal(err)
	}
	if got := s.Snapshot().Used; got.CPU != 2 || got.MemoryMiB != 200 {
		t.Fatalf("resume accounting invalid: %+v", got)
	}
}

func TestUncertainCreateKeepsCapacityUntilRemoteAbsenceConfirmed(t *testing.T) {
	s := fixture(t, Resources{CPU: 1, MemoryMiB: 64}, 4,
		map[string]Policy{"a": {MaxLogical: 2, MaxActive: Resources{CPU: 1, MemoryMiB: 64}}})
	for _, id := range []string{"unknown", "waiting"} {
		if _, err := s.Submit(request(id, "a", 1, 64)); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok := s.Dispatch(); !ok {
		t.Fatal("expected reservation")
	}
	if err := s.MarkUncertain("unknown"); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Dispatch(); ok {
		t.Fatal("released uncertain capacity prematurely")
	}
	if err := s.ConfirmPaused("unknown"); !errors.Is(err, ErrTransition) {
		t.Fatalf("unknown create cannot be considered paused: %v", err)
	}
	if err := s.ConfirmDeleted("unknown"); err != nil { // only after provider/operator verification
		t.Fatal(err)
	}
	if e, ok := s.Dispatch(); !ok || e.Request.ID != "waiting" {
		t.Fatalf("expected waiting VM to be admitted: %+v %v", e, ok)
	}
}

func TestUncertainCreateCanRecoverRunningWithoutDuplicateReservation(t *testing.T) {
	s := fixture(t, Resources{CPU: 1, MemoryMiB: 64}, 2,
		map[string]Policy{"a": {MaxLogical: 1, MaxActive: Resources{CPU: 1, MemoryMiB: 64}}})
	s.Submit(request("r", "a", 1, 64))
	s.Dispatch()
	if err := s.MarkUncertain("r"); err != nil {
		t.Fatal(err)
	}
	if err := s.ConfirmRunning("r"); err != nil {
		t.Fatal(err)
	}
	if got := s.Snapshot().Used.CPU; got != 1 {
		t.Fatalf("recovery should not charge twice, got %d", got)
	}
}

func TestQueueBoundAndInvalidRequests(t *testing.T) {
	s := fixture(t, Resources{CPU: 1, MemoryMiB: 100}, 1,
		map[string]Policy{"a": {MaxLogical: 10, MaxActive: Resources{CPU: 1, MemoryMiB: 100}}})
	if _, err := s.Submit(request("bad", "a", 2, 100)); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("oversized request: %v", err)
	}
	if _, err := s.Submit(request("missing", "b", 1, 100)); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("unknown tenant: %v", err)
	}
	if _, err := s.Submit(request("one", "a", 1, 100)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Submit(request("two", "a", 1, 100)); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected bounded queue: %v", err)
	}
	if _, err := s.Submit(request("one", "a", 1, 100)); err != nil {
		t.Fatalf("idempotent submit blocked by full queue: %v", err)
	}
	if _, err := New(Config{Capacity: Resources{CPU: 0, MemoryMiB: 100}}); err == nil {
		t.Fatal("accepted invalid capacity")
	}
}

func TestQueueCancellationAndIdempotentDelete(t *testing.T) {
	s := fixture(t, Resources{CPU: 4, MemoryMiB: 512}, 3,
		map[string]Policy{"a": {MaxLogical: 3, MaxActive: Resources{CPU: 4, MemoryMiB: 512}}})
	for _, id := range []string{"x", "y", "z"} {
		s.Submit(request(id, "a", 1, 128))
	}
	if err := s.ConfirmDeleted("y"); err != nil {
		t.Fatal(err)
	}
	if err := s.ConfirmDeleted("y"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"x", "z"} {
		e, ok := s.Dispatch()
		if !ok || e.Request.ID != id {
			t.Fatalf("unexpected queue after deletion: %+v %v", e, ok)
		}
	}
	if snap := s.Snapshot(); snap.Pending != 0 || snap.Tenant["a"].Logical != 2 {
		t.Fatalf("wrong accounting: %+v", snap)
	}
}

func TestTenantActiveQuotaAndPolicyCopy(t *testing.T) {
	policies := map[string]Policy{
		"small": {MaxLogical: 3, MaxActive: Resources{CPU: 1, MemoryMiB: 128}},
		"large": {MaxLogical: 3, MaxActive: Resources{CPU: 4, MemoryMiB: 512}},
	}
	s := fixture(t, Resources{CPU: 4, MemoryMiB: 512}, 6, policies)
	policies["small"] = Policy{MaxLogical: 100, MaxActive: Resources{CPU: 8, MemoryMiB: 1024}}
	for _, r := range []Request{
		request("s1", "small", 1, 128), request("s2", "small", 1, 128),
		request("l1", "large", 2, 256),
	} {
		if _, err := s.Submit(r); err != nil {
			t.Fatal(err)
		}
	}
	a, ok := s.Dispatch()
	if !ok || a.Request.ID != "s1" {
		t.Fatalf("first admission: %+v %t", a, ok)
	}
	b, ok := s.Dispatch()
	if !ok || b.Request.ID != "l1" {
		t.Fatalf("small tenant should be blocked by its active quota: %+v %t", b, ok)
	}
	if _, ok := s.Dispatch(); ok {
		t.Fatal("tenant overcommitted despite remaining host resources")
	}
	if err := s.ConfirmDeleted("s1"); err != nil {
		t.Fatal(err)
	}
	if e, ok := s.Dispatch(); !ok || e.Request.ID != "s2" {
		t.Fatalf("small tenant quota should be released: %+v %t", e, ok)
	}
}

func TestSkipsUnfittableTenantWithoutBlockingOthers(t *testing.T) {
	s := fixture(t, Resources{CPU: 3, MemoryMiB: 300}, 5, map[string]Policy{
		"a": {MaxLogical: 5, MaxActive: Resources{CPU: 3, MemoryMiB: 300}},
		"b": {MaxLogical: 5, MaxActive: Resources{CPU: 3, MemoryMiB: 300}},
	})
	s.Submit(request("first", "a", 2, 200))
	s.Dispatch()
	s.Submit(request("big", "a", 2, 200)) // blocked by remaining capacity
	s.Submit(request("small", "b", 1, 100))
	e, ok := s.Dispatch()
	if !ok || e.Request.ID != "small" {
		t.Fatalf("blocked tenant caused global HOL blocking: %+v %t", e, ok)
	}
}
