package e2bapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"agent-runtime-lab/src/lifecycle"
)

func TestLiveAPIContractAgainstFakeHTTP(t *testing.T) {
	var mu sync.Mutex
	state := ""
	created := 0
	var receivedMetadata string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Header.Get("X-API-Key") != "test-key" {
			t.Error("missing API key")
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/sandboxes":
			var req struct {
				TemplateID string            `json:"templateID"`
				Metadata   map[string]string `json:"metadata"`
				TTL        int               `json:"timeout"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			if req.TemplateID != "base" || req.TTL != 300 {
				t.Errorf("bad create: %+v", req)
			}
			receivedMetadata = req.Metadata[metadataKey]
			created++
			state = "running"
			w.WriteHeader(http.StatusCreated)
			fmt.Fprint(w, `{"sandboxID":"sbx_123","envdAccessToken":"secret-dont-log"}`)
		case r.Method == http.MethodGet && r.URL.Path == "/sandboxes/sbx_123":
			if state == "" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			json.NewEncoder(w).Encode(map[string]string{"sandboxID": "sbx_123", "state": state})
		case r.Method == http.MethodPost && r.URL.Path == "/sandboxes/sbx_123/pause":
			var req struct {
				Memory *bool `json:"memory"`
			}
			json.NewDecoder(r.Body).Decode(&req)
			if req.Memory == nil || !*req.Memory {
				t.Error("not full-memory pause")
			}
			state = "paused"
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && r.URL.Path == "/sandboxes/sbx_123/connect":
			var req struct {
				Timeout int `json:"timeout"`
			}
			json.NewDecoder(r.Body).Decode(&req)
			if req.Timeout != 300 {
				t.Errorf("incorrect connect ttl %d", req.Timeout)
			}
			state = "running"
			w.WriteHeader(http.StatusCreated)
		case r.Method == http.MethodDelete && r.URL.Path == "/sandboxes/sbx_123":
			state = ""
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected API route %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	b, err := New(Config{BaseURL: server.URL, APIKey: "test-key"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	id, err := b.Create(ctx, "tenant-a/task-42")
	if err != nil || id != "sbx_123" {
		t.Fatalf("create id %q: %v", id, err)
	}
	got, err := b.Observe(ctx, id)
	if err != nil || got != lifecycle.Running {
		t.Fatalf("observe running=%s: %v", got, err)
	}
	if err := b.Pause(ctx, id); err != nil {
		t.Fatal(err)
	}
	got, err = b.Observe(ctx, id)
	if err != nil || got != lifecycle.Paused {
		t.Fatalf("observe paused=%s: %v", got, err)
	}
	if err := b.Resume(ctx, id); err != nil {
		t.Fatal(err)
	}
	got, err = b.Observe(ctx, id)
	if err != nil || got != lifecycle.Running {
		t.Fatalf("observe resumed=%s: %v", got, err)
	}
	if err := b.Delete(ctx, id); err != nil {
		t.Fatal(err)
	}
	got, err = b.Observe(ctx, id)
	if err != nil || got != lifecycle.Absent {
		t.Fatalf("observe missing=%s: %v", got, err)
	}
	if created != 1 || receivedMetadata != "tenant-a/task-42" {
		t.Fatalf("missing correlation or duplicate creates: %d %q", created, receivedMetadata)
	}
}

func TestRejectUnsafeURLs(t *testing.T) {
	for _, origin := range []string{"https://127.0.0.1:3000", "http://example.org:3000", "http://127.0.0.1:3000/evil", "http://127.0.0.1:3000?token=yes", "http://user:password@127.0.0.1:3000"} {
		if _, err := New(Config{BaseURL: origin, APIKey: "x"}); err == nil {
			t.Errorf("accepted %s", origin)
		}
	}
	if _, err := New(Config{BaseURL: "http://127.0.0.1:3000"}); err == nil {
		t.Fatal("missing key accepted")
	}
	if _, err := New(Config{BaseURL: "http://127.0.0.1:3000", APIKey: "x", TTLSeconds: 5}); err == nil {
		t.Fatal("invalid TTL accepted")
	}
}

func TestUnexpectedStateAndAuthFailureAreErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sandboxes/rejected" {
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, "sensitive secret must not escape")
			return
		}
		fmt.Fprint(w, `{"sandboxID":"unexpected","state":"stopping"}`)
	}))
	defer server.Close()
	b, err := New(Config{BaseURL: server.URL, APIKey: "x"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = b.Observe(context.Background(), "rejected")
	if err == nil || strings.Contains(err.Error(), "sensitive secret") {
		t.Fatalf("bad error message: %v", err)
	}
	_, err = b.Observe(context.Background(), "unexpected")
	if err == nil || !strings.Contains(err.Error(), "unrecognized") {
		t.Fatalf("unexpected state accepted: %v", err)
	}
}

func TestNoCredentialForwardedOnRedirect(t *testing.T) {
	visited := false
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		visited = true
		if r.Header.Get("X-API-Key") != "" {
			t.Error("key forwarded to redirect target")
		}
	}))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer source.Close()
	b, err := New(Config{BaseURL: source.URL, APIKey: "test-key"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Create(context.Background(), "key"); err == nil {
		t.Fatal("accepted redirected create")
	}
	if visited {
		t.Fatal("redirect was followed")
	}
}
