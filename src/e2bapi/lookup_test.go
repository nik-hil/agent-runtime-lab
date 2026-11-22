package e2bapi

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestFindByLogicalKeyPaginationAndExactMetadata(t *testing.T) {
	queries := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries++
		if r.URL.Path != "/v2/sandboxes" || r.URL.Query().Get("metadata") != "agent_runtime_lab_key=tenant-a/task-42" {
			t.Errorf("unexpected list request: %s", r.URL.String())
		}
		if r.Header.Get("X-API-Key") != "key" {
			t.Errorf("missing API key")
		}
		if r.URL.Query().Get("nextToken") == "" {
			w.Header().Set("X-Next-Token", "page-two")
			fmt.Fprint(w, `[{"sandboxID":"not-ours","metadata":{"agent_runtime_lab_key":"another-key"}}]`)
		} else {
			fmt.Fprint(w, `[{"sandboxID":"ours","metadata":{"agent_runtime_lab_key":"tenant-a/task-42"}}]`)
		}
	}))
	defer server.Close()
	b, err := New(Config{BaseURL: server.URL, APIKey: "key"})
	if err != nil {
		t.Fatal(err)
	}
	ids, err := b.FindByLogicalKey(context.Background(), "tenant-a/task-42")
	if err != nil || len(ids) != 1 || ids[0] != "ours" || queries != 2 {
		t.Fatalf("lookup failed: ids=%v err=%v queries=%d", ids, err, queries)
	}
}

func TestFindByLogicalKeyRefusesTruncatedOrFailedList(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
				fmt.Fprint(w, "secret should never appear")
			}))
			defer srv.Close()
			b, _ := New(Config{BaseURL: srv.URL, APIKey: "key"})
			_, err := b.FindByLogicalKey(context.Background(), "x")
			if err == nil || strings.Contains(err.Error(), "secret") {
				t.Fatalf("unsafe lookup failure: %v", err)
			}
		})
	}
}

func TestFindByLogicalKeyRejectsCyclicCursor(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Next-Token", "same")
		fmt.Fprint(w, `[]`)
	}))
	defer srv.Close()
	b, _ := New(Config{BaseURL: srv.URL, APIKey: "key"})
	if _, err := b.FindByLogicalKey(context.Background(), "x"); err == nil {
		t.Fatal("accepted cyclic pagination")
	}
}
