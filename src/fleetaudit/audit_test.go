package fleetaudit

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/e2bapi"
	"agent-runtime-lab/src/fleet"
	"agent-runtime-lab/src/lifecycle"
)

const testKey = "lab/metadata-1"

type fakeProvider struct {
	matches map[string][]string
	details map[string]fleet.Details
	listErr error
	getErr  error
}

func (p *fakeProvider) FindByLogicalKey(_ context.Context, key string) ([]string, error) {
	return p.matches[key], p.listErr
}
func (p *fakeProvider) Describe(_ context.Context, id string) (fleet.Details, error) {
	if p.getErr != nil {
		return fleet.Details{}, p.getErr
	}
	if v, found := p.details[id]; found {
		return v, nil
	}
	return fleet.Details{State: lifecycle.Absent}, nil
}

func journal(t *testing.T, state admission.State, pendingCreate bool) fleet.Journal {
	t.Helper()
	size := admission.Resources{CPU: 1, MemoryMiB: 256}
	cap := admission.Resources{CPU: 2, MemoryMiB: 512}
	s, err := admission.New(admission.Config{Capacity: cap, MaxPending: 2,
		Tenants: map[string]admission.Policy{"alpha": {MaxLogical: 2, MaxActive: cap}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Submit(admission.Request{ID: "one", Tenant: "alpha", Resources: size}); err != nil {
		t.Fatal(err)
	}
	if state != admission.Queued {
		if _, ok := s.Dispatch(); !ok {
			t.Fatal("dispatch failed")
		}
		if state == admission.Running || state == admission.Deleted {
			if err := s.ConfirmRunning("one"); err != nil {
				t.Fatal(err)
			}
		}
		if state == admission.Uncertain {
			if err := s.MarkUncertain("one"); err != nil {
				t.Fatal(err)
			}
		}
		if state == admission.Deleted {
			if err := s.ConfirmDeleted("one"); err != nil {
				t.Fatal(err)
			}
		}
	}
	w := fleet.Workload{LogicalKey: testKey, Desired: "running", CreatePending: pendingCreate}
	if state == admission.Running {
		w.SandboxID = "known"
	}
	if state == admission.Deleted {
		w.Desired = "deleted"
	}
	return fleet.Journal{Version: 1, TemplateSize: size,
		Scheduler: s.Checkpoint(), Workloads: map[string]fleet.Workload{"one": w}}
}

func normalProvider() *fakeProvider {
	return &fakeProvider{
		matches: map[string][]string{testKey: {"known"}},
		details: map[string]fleet.Details{"known": {
			State: lifecycle.Running, Resources: admission.Resources{CPU: 1, MemoryMiB: 256},
		}},
	}
}

func hasFinding(r Report, code string) bool {
	for _, f := range r.Findings {
		if f.Code == code {
			return true
		}
	}
	return false
}

func TestAuditHappyPathAndUnexpectedAbsencePreservesJournal(t *testing.T) {
	j := journal(t, admission.Running, false)
	p := normalProvider()
	r, err := Run(context.Background(), j, p)
	if err != nil || r.RequiresReview || len(r.Findings) != 0 || r.Workloads != 1 {
		t.Fatalf("healthy audit: %+v %v", r, err)
	}
	checkpoint := j.Scheduler
	p.details = nil // genuine 404 from GET, not a delete intent
	p.matches = map[string][]string{}
	r, err = Run(context.Background(), j, p)
	if err != nil || !r.RequiresReview || !hasFinding(r, "unexpected_absence") {
		t.Fatalf("missing TTL/drift signal: %+v %v", r, err)
	}
	if !reflect.DeepEqual(j.Scheduler, checkpoint) || j.Scheduler.Used.CPU != 1 || j.Workloads["one"].SandboxID != "known" {
		t.Fatal("audit changed journal reservation or VM identity")
	}
	// After an explicit delete intent, absence is informational, but the
	// auditor still cannot mark the journal deleted or release capacity.
	w := j.Workloads["one"]
	w.Desired = "deleted"
	j.Workloads["one"] = w
	r, err = Run(context.Background(), j, p)
	if err != nil || r.RequiresReview || !hasFinding(r, "deletion_not_yet_reconciled") {
		t.Fatalf("pending deletion: %+v %v", r, err)
	}
}

func TestAuditDuplicateKnownKeyAndDeletedOrphan(t *testing.T) {
	j := journal(t, admission.Running, false)
	p := normalProvider()
	p.matches[testKey] = []string{"known", "extra"}
	r, err := Run(context.Background(), j, p)
	if err != nil || !hasFinding(r, "duplicate_metadata_key") || !r.RequiresReview {
		t.Fatalf("duplicate: %+v %v", r, err)
	}
	p.matches[testKey] = []string{"unrelated"}
	r, err = Run(context.Background(), j, p)
	if err != nil || !hasFinding(r, "identity_mismatch") {
		t.Fatalf("identity mismatch: %+v %v", r, err)
	}
	j = journal(t, admission.Deleted, false)
	p.matches[testKey] = []string{"orphan-remote"}
	r, err = Run(context.Background(), j, p)
	if err != nil || !hasFinding(r, "orphan_after_deletion") || !r.RequiresReview {
		t.Fatalf("known-key orphan after journal deletion: %+v %v", r, err)
	}
	p.matches[testKey] = nil
	r, err = Run(context.Background(), j, p)
	if err != nil || r.RequiresReview {
		t.Fatalf("clean deleted job: %+v %v", r, err)
	}
}

func TestAuditAmbiguousCreateDoesNotAssumeAbsence(t *testing.T) {
	j := journal(t, admission.Uncertain, true)
	p := &fakeProvider{matches: map[string][]string{}}
	r, err := Run(context.Background(), j, p)
	if err != nil || !hasFinding(r, "ambiguous_create_no_match") || j.Scheduler.Used.CPU != 1 {
		t.Fatalf("uncertain Create was treated as absent: %+v %v", r, err)
	}
	p.matches[testKey] = []string{"created-but-reply-lost"}
	r, err = Run(context.Background(), j, p)
	if err != nil || !hasFinding(r, "ambiguous_create_match") || r.Findings[0].SandboxID != "created-but-reply-lost" {
		t.Fatalf("recoverable create: %+v %v", r, err)
	}
	if j.Workloads["one"].SandboxID != "" || !j.Workloads["one"].CreatePending {
		t.Fatal("read-only audit unexpectedly recovered a VM")
	}
}

func TestAuditOversizeMissingMetadataAndFailClosedProviderErrors(t *testing.T) {
	j := journal(t, admission.Running, false)
	p := normalProvider()
	p.details["known"] = fleet.Details{State: lifecycle.Running,
		Resources: admission.Resources{CPU: 4, MemoryMiB: 8192}}
	r, err := Run(context.Background(), j, p)
	if err != nil || !hasFinding(r, "unexpected_resource_shape") {
		t.Fatalf("oversized VM: %+v %v", r, err)
	}
	p = normalProvider()
	p.matches = map[string][]string{}
	r, err = Run(context.Background(), j, p)
	if err != nil || !hasFinding(r, "missing_from_metadata_inventory") {
		t.Fatalf("missing metadata match: %+v %v", r, err)
	}
	p.listErr = errors.New("HTTP 429")
	if _, err := Run(context.Background(), j, p); err == nil {
		t.Fatal("429 must not be treated as empty metadata inventory")
	}
	p = normalProvider()
	p.getErr = errors.New("HTTP 503")
	if _, err := Run(context.Background(), j, p); err == nil {
		t.Fatal("503 must not be interpreted as sandbox absence")
	}
	j.Scheduler.Used.CPU = 0
	if _, err := Run(context.Background(), j, normalProvider()); err == nil {
		t.Fatal("corrupt reservation journal accepted")
	}
}

func TestAuditRealE2BHTTPAdapterReadOnly(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Method != http.MethodGet || r.Header.Get("X-API-Key") != "fake-key" {
			t.Errorf("not a read-only authenticated GET: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		switch r.URL.Path {
		case "/v2/sandboxes":
			if r.URL.Query().Get("metadata") != "agent_runtime_lab_key="+testKey {
				t.Errorf("unexpected metadata query: %s", r.URL.RawQuery)
			}
			fmt.Fprint(w, `[{"sandboxID":"known","metadata":{"agent_runtime_lab_key":"lab/metadata-1"}}]`)
		case "/sandboxes/known":
			fmt.Fprint(w, `{"sandboxID":"known","state":"running","cpuCount":1,"memoryMB":256}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	backend, err := e2bapi.New(e2bapi.Config{BaseURL: server.URL, APIKey: "fake-key"})
	if err != nil {
		t.Fatal(err)
	}
	r, err := Run(context.Background(), journal(t, admission.Running, false), backend)
	if err != nil || r.RequiresReview || requests != 2 {
		t.Fatalf("real E2B adapter mismatch: report=%+v err=%v requests=%d", r, err, requests)
	}
}
