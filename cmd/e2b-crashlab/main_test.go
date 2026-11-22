package main

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"agent-runtime-lab/src/durable"
	"agent-runtime-lab/src/lifecycle"
)

type fakeProvider struct {
	createdID string
	createErr error
	creates   int
}

func (f *fakeProvider) Create(context.Context, string) (string, error) {
	f.creates++
	return f.createdID, f.createErr
}

func (f *fakeProvider) Observe(context.Context, string) (lifecycle.ObservedState, error) {
	return lifecycle.Running, nil
}

func (*fakeProvider) Pause(context.Context, string) error  { return nil }
func (*fakeProvider) Resume(context.Context, string) error { return nil }
func (*fakeProvider) Delete(context.Context, string) error { return nil }

type fakeFinder struct{ ids []string }

func (f fakeFinder) FindByLogicalKey(context.Context, string) ([]string, error) {
	return f.ids, nil
}

func TestCrashAfterRemoteCreateLeavesDurableFence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "crash.json")
	store, err := durable.OpenFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	remote := &fakeProvider{createdID: "sbx-test"}
	called := 0
	injected := crashBackend{Backend: remote, afterCreate: func(id string) error {
		called++
		if id != "sbx-test" {
			t.Fatalf("unexpected sandbox %q", id)
		}
		return errors.New("simulated SIGKILL failure")
	}}
	ctrl, err := durable.New(store, injected, "tenant/key", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := ctrl.Reconcile(context.Background()); !errors.Is(err, durable.ErrCreatePending) {
		t.Fatalf("expected creation fence: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if called != 1 || remote.creates != 1 {
		t.Fatalf("crash callback or Create count mismatch: %d %d", called, remote.creates)
	}

	restarted, err := durable.OpenFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	record, exists, err := restarted.Load()
	if err != nil || !exists || !record.CreatePending || record.SandboxID != "" {
		t.Fatalf("unsafe persisted state: %+v exists=%t error=%v", record, exists, err)
	}
	recovered, err := durable.New(restarted, remote, "", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered.Reconcile(context.Background()); !errors.Is(err, durable.ErrCreatePending) {
		t.Fatalf("expected block on duplicate creation: %v", err)
	}
	if err := recovered.RecoverCreate(context.Background(), fakeFinder{ids: []string{"sbx-test"}}); err != nil {
		t.Fatal(err)
	}
	if err := recovered.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if remote.creates != 1 || recovered.Record().SandboxID != "sbx-test" {
		t.Fatalf("recovery duplicated sandbox: calls=%d state=%+v", remote.creates, recovered.Record())
	}
}

func TestCrashBackendOnlyInjectsAfterSuccessfulCreate(t *testing.T) {
	remote := &fakeProvider{createErr: errors.New("provider offline")}
	called := false
	b := crashBackend{Backend: remote, afterCreate: func(string) error {
		called = true
		return nil
	}}
	_, err := b.Create(context.Background(), "key")
	if err == nil || called {
		t.Fatalf("unexpected callback after failed Create: called=%t err=%v", called, err)
	}
	remote.createErr = nil
	remote.createdID = "sbx-test"
	_, err = b.Create(context.Background(), "key")
	if err == nil || !strings.Contains(err.Error(), "unexpectedly returned") {
		t.Fatalf("crash callback return was incorrectly accepted: %v", err)
	}
}

func TestCrashLabRequiresExplicitAcknowledgement(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"--execute"},
		{"--execute", "--confirm-sigkill"},
		{"--confirm-sigkill", "--state-file", "test.json"},
	} {
		if err := run(args); err == nil {
			t.Fatalf("accepted unsafe invocation %q", args)
		}
	}
}
