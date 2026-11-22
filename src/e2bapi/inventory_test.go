package e2bapi

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGlobalInventoryPaginatesAndFiltersOtherTenants(t *testing.T) {
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Method != http.MethodGet || r.URL.Path != "/v2/sandboxes" || r.Header.Get("X-API-Key") != "fake" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		switch r.URL.Query().Get("nextToken") {
		case "":
			w.Header().Set("X-Next-Token", "page-2")
			fmt.Fprint(w, `[{"sandboxID":"vm-a","metadata":{"agent_runtime_lab_key":"agent-runtime-lab/m2-9/a"}},{"sandboxID":"other","metadata":{"agent_runtime_lab_key":"other-project/1"}}]`)
		case "page-2":
			fmt.Fprint(w, `[{"sandboxID":"vm-b","metadata":{"agent_runtime_lab_key":"agent-runtime-lab/m2-9/b"}}]`)
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer srv.Close()
	b, err := New(Config{BaseURL: srv.URL, APIKey: "fake"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := b.ListLabSandboxes(context.Background())
	if err != nil || requests != 2 || len(got) != 2 || got[0].SandboxID != "vm-a" || got[1].SandboxID != "vm-b" {
		t.Fatalf("bad scan result: %+v err=%v requests=%d", got, err, requests)
	}
}

func TestGlobalInventoryDoesNotAcceptProviderErrorsOrCursorCycles(t *testing.T) {
	for _, code := range []int{http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(code)
			fmt.Fprint(w, "token-secret-must-not-leak")
		}))
		b, err := New(Config{BaseURL: srv.URL, APIKey: "fake"})
		if err != nil {
			t.Fatal(err)
		}
		_, err = b.ListLabSandboxes(context.Background())
		if err == nil || strings.Contains(err.Error(), "token-secret") {
			t.Fatalf("unsafe API error %v", err)
		}
		srv.Close()
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Next-Token", "loop")
		fmt.Fprint(w, `[]`)
	}))
	defer srv.Close()
	b, err := New(Config{BaseURL: srv.URL, APIKey: "fake"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.ListLabSandboxes(context.Background()); err == nil {
		t.Fatal("accepted repeated page cursor")
	}
}

func TestSetSandboxTTLRouteAndInputValidation(t *testing.T) {
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Method != http.MethodPost || r.URL.Path != "/sandboxes/sbx_1/timeout" || r.Header.Get("X-API-Key") != "fake" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	b, err := New(Config{BaseURL: srv.URL, APIKey: "fake"})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.SetTimeout(context.Background(), "sbx_1", 300); err != nil || requests != 1 {
		t.Fatalf("valid TTL failed: %v requests=%d", err, requests)
	}
	if err := b.SetTimeout(context.Background(), "sbx_1", 1); err == nil {
		t.Fatal("allowed unsafe TTL")
	}
	if err := b.SetTimeout(context.Background(), "../bad", 300); err == nil {
		t.Fatal("allowed invalid sandbox ID")
	}
	if requests != 1 {
		t.Fatal("invalid inputs generated network requests")
	}
}
