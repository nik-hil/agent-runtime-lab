package admission

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestCheckpointRoundTripPreservesTenantFairnessAndReservations(t *testing.T) {
	config := Config{Capacity: Resources{CPU: 2, MemoryMiB: 200}, MaxPending: 6,
		Tenants: map[string]Policy{
			"alpha": {MaxLogical: 3, MaxActive: Resources{CPU: 2, MemoryMiB: 200}},
			"beta":  {MaxLogical: 3, MaxActive: Resources{CPU: 2, MemoryMiB: 200}},
		}}
	s, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	for _, req := range []Request{
		{ID: "a1", Tenant: "alpha", Resources: Resources{1, 100}},
		{ID: "a2", Tenant: "alpha", Resources: Resources{1, 100}},
		{ID: "b1", Tenant: "beta", Resources: Resources{1, 100}},
	} {
		if _, err := s.Submit(req); err != nil {
			t.Fatal(err)
		}
	}
	e, ok := s.Dispatch()
	if !ok || e.Request.ID != "a1" {
		t.Fatal("incorrect first reservation")
	}
	if err := s.ConfirmRunning("a1"); err != nil {
		t.Fatal(err)
	}
	serialized, err := json.Marshal(s.Checkpoint())
	if err != nil {
		t.Fatal(err)
	}
	var cp Checkpoint
	if err := json.Unmarshal(serialized, &cp); err != nil {
		t.Fatal(err)
	}
	restarted, err := Restore(cp)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(restarted.Snapshot(), s.Snapshot()) {
		t.Fatal("resources lost on restart")
	}
	e, ok = restarted.Dispatch()
	if !ok || e.Request.ID != "b1" {
		t.Fatalf("fair next tenant lost: %+v", e)
	}
	cp.Jobs["a1"] = Entry{Request: Request{ID: "a1", Tenant: "alpha", Resources: Resources{2, 200}}, State: Running}
	if got, _ := restarted.Get("a1"); got.Request.Resources.CPU != 1 {
		t.Fatal("checkpoint mutated active scheduler")
	}
}

func TestCheckpointRejectsUnderreportedResourcesAndQueueCorruption(t *testing.T) {
	s, err := New(Config{Capacity: Resources{2, 200}, MaxPending: 2,
		Tenants: map[string]Policy{"a": {MaxLogical: 2, MaxActive: Resources{2, 200}}}})
	if err != nil {
		t.Fatal(err)
	}
	s.Submit(Request{ID: "r", Tenant: "a", Resources: Resources{1, 100}})
	s.Dispatch()
	if err := s.ConfirmRunning("r"); err != nil {
		t.Fatal(err)
	}
	cp := s.Checkpoint()
	cp.Used.CPU = 0
	if _, err := Restore(cp); err == nil {
		t.Fatal("trusted forged under-reporting of CPU")
	}
	cp = s.Checkpoint()
	delete(cp.Usage, "a")
	if _, err := Restore(cp); err == nil {
		t.Fatal("trusted absent tenant accounting")
	}
	cp = s.Checkpoint()
	cp.Jobs["r"] = Entry{Request: Request{ID: "r", Tenant: "a", Resources: Resources{1, 100}}, State: Queued}
	if _, err := Restore(cp); err == nil {
		t.Fatal("accepted missing queue ID")
	}
}
