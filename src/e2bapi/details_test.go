package e2bapi

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/lifecycle"
)

func TestE2BDescribeUsesObservedTemplateShape(t *testing.T) {
	test := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-API-Key") != "test-key" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/sandboxes/known":
			fmt.Fprint(w, `{"sandboxID":"known","state":"running","cpuCount":2,"memoryMB":512}`)
		case "/sandboxes/missing":
			w.WriteHeader(http.StatusNotFound)
		case "/sandboxes/invalid":
			fmt.Fprint(w, `{"sandboxID":"other","state":"running","cpuCount":2,"memoryMB":512}`)
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer test.Close()
	b, err := New(Config{BaseURL: test.URL, APIKey: "test-key"})
	if err != nil {
		t.Fatal(err)
	}
	d, err := b.Describe(context.Background(), "known")
	if err != nil || d.State != lifecycle.Running || d.Resources != (admission.Resources{CPU: 2, MemoryMiB: 512}) {
		t.Fatalf("bad shape: %+v %v", d, err)
	}
	d, err = b.Describe(context.Background(), "missing")
	if err != nil || d.State != lifecycle.Absent {
		t.Fatalf("not-found handling: %+v %v", d, err)
	}
	if _, err = b.Describe(context.Background(), "invalid"); err == nil {
		t.Fatal("accepted mismatched sandbox ID")
	}
	if _, err = b.Describe(context.Background(), "invalid/../../notallowed"); err == nil {
		t.Fatal("accepted path traversal")
	}
}
