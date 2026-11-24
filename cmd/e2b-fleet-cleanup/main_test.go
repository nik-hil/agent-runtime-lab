package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/e2bapi"
	"agent-runtime-lab/src/fleet"
)

func TestCleanupCLIUsesOnlyExistingJournalAndWorksWithoutSocket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "fleet.json")
	var present atomic.Bool
	var creates, deletes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-API-Key") != "test-key" {
			t.Error("missing E2B credentials")
			w.WriteHeader(http.StatusForbidden)
			return
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/sandboxes":
			creates.Add(1)
			present.Store(true)
			w.WriteHeader(http.StatusCreated)
			fmt.Fprint(w, `{"sandboxID":"vm-cli"}`)
		case r.Method == http.MethodGet && r.URL.Path == "/sandboxes/vm-cli":
			if !present.Load() {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			fmt.Fprint(w, `{"sandboxID":"vm-cli","state":"running","cpuCount":1,"memoryMB":128}`)
		case r.Method == http.MethodDelete && r.URL.Path == "/sandboxes/vm-cli":
			data, _ := os.ReadFile(path)
			var journal fleet.Journal
			if err := json.Unmarshal(data, &journal); err != nil || !journal.Draining {
				t.Error("CLI contacted provider before persisting drain intent")
			}
			deletes.Add(1)
			present.Store(false)
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected provider operation: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	t.Setenv("E2B_API_URL", server.URL)
	t.Setenv("E2B_API_KEY", "test-key")
	backend, err := e2bapi.New(e2bapi.Config{BaseURL: server.URL, APIKey: "test-key"})
	if err != nil {
		t.Fatal(err)
	}
	store, err := fleet.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	size := admission.Resources{CPU: 1, MemoryMiB: 128}
	c, err := fleet.New(store, backend, fleet.Config{TemplateSize: size,
		Admission: admission.Config{Capacity: size, MaxPending: 1,
			Tenants: map[string]admission.Policy{"alpha": {MaxLogical: 1, MaxActive: size}}}}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Submit("alpha", "job"); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := c.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	_ = store.Close()
	var output, stderr bytes.Buffer
	args := []string{"--execute", "--state-file", path, "--timeout", "2s", "--poll", "100ms"}
	if err := run(context.Background(), args, &output, &stderr); err != nil {
		t.Fatal(err)
	}
	if creates.Load() != 1 || deletes.Load() != 1 || !strings.Contains(output.String(), `"complete": true`) ||
		!strings.Contains(output.String(), "CPU=0 MiB=0 pending=0") || strings.Contains(output.String(), "test-key") {
		t.Fatalf("wrong cleanup result or unexpected creation: %s", output.String())
	}
}

func TestCleanupCLIRequiresExplicitPermissionPathAndBounds(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"--execute"},
		{"--state-file", "some-journal.json"},
		{"--execute", "--state-file", "some-journal.json", "--timeout", "0s"},
		{"--execute", "--state-file", "some-journal.json", "--poll", "0s"},
	} {
		var output, stderr bytes.Buffer
		if err := run(context.Background(), args, &output, &stderr); err == nil {
			t.Fatalf("invalid cleanup options accepted: %v", args)
		}
	}
}
