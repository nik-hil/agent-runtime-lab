package m4exec

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func frame(t *testing.T, w http.ResponseWriter, payload any) {
	t.Helper()
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := binary.Write(w, binary.BigEndian, byte(0)); err != nil {
		t.Fatal(err)
	}
	if err := binary.Write(w, binary.BigEndian, uint32(len(encoded))); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(encoded); err != nil {
		t.Fatal(err)
	}
}

func setup(t *testing.T, events []any) (*Runner, func()) {
	t.Helper()
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/sandboxes/vm-1/connect" ||
			r.Header.Get("X-API-Key") != "fake-key" {
			t.Errorf("unexpected E2B control request: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		fmt.Fprint(w, `{"sandboxID":"vm-1","envdAccessToken":"fake-envd-token"}`)
	}))
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/process.Process/Start" ||
			r.Header.Get("E2b-Sandbox-Id") != "vm-1" ||
			r.Header.Get("E2b-Sandbox-Port") != "49983" ||
			r.Header.Get("X-Access-Token") != "fake-envd-token" ||
			r.Header.Get("X-API-Key") != "" ||
			r.Header.Get("Content-Type") != "application/connect+json" {
			t.Errorf("unexpected envd request: %s %s headers=%v", r.Method, r.URL, r.Header)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		header := make([]byte, 5)
		if _, err := r.Body.Read(header); err != nil {
			t.Error(err)
			return
		}
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		process, ok := request["process"].(map[string]any)
		if !ok || process["cmd"] != "/bin/bash" {
			t.Errorf("bad command protocol request: %#v", request)
		}
		for _, e := range events {
			frame(t, w, e)
		}
	}))
	backend, err := New(Config{APIURL: control.URL, ProxyURL: proxy.URL, APIKey: "fake-key", TTL: 300})
	if err != nil {
		t.Fatal(err)
	}
	return backend, func() { control.Close(); proxy.Close() }
}

func TestRunCommandExecutesOnlyThroughSandboxProxy(t *testing.T) {
	backend, closeServers := setup(t, []any{
		map[string]any{"event": map[string]any{"start": map[string]any{"pid": 40}}},
		map[string]any{"event": map[string]any{"data": map[string]any{"stdout": []byte("sandbox-output"), "stderr": []byte("note")}}},
		map[string]any{"event": map[string]any{"end": map[string]any{"exitCode": 0, "exited": true}}},
	})
	defer closeServers()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result, err := backend.Run(ctx, "vm-1", "echo sandbox-output")
	if err != nil || result.ExitCode != 0 || result.Stdout != "sandbox-output" || result.Stderr != "note" || result.Truncated {
		t.Fatalf("unexpected command result: %+v err=%v", result, err)
	}
}

func TestRunRequiresTerminalEventAndDoesNotRetry(t *testing.T) {
	backend, closeServers := setup(t, []any{map[string]any{"start": map[string]any{"pid": 40}}})
	defer closeServers()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := backend.Run(ctx, "vm-1", "echo partial"); err == nil || !strings.Contains(err.Error(), "outcome uncertain") {
		t.Fatalf("lost completion must be ambiguous, got %v", err)
	}
}

func TestRunAcceptsOmittedZeroExitCodeAfterConfirmedExit(t *testing.T) {
	for _, wrapped := range []bool{false, true} {
		t.Run(fmt.Sprintf("wrapped_%t", wrapped), func(t *testing.T) {
			// EndEvent.exit_code is an implicit-presence proto3 scalar.
			// ProtoJSON omits its default zero value on successful exit.
			start := map[string]any{"start": map[string]any{"pid": 40}}
			data := map[string]any{"data": map[string]any{"stdout": []byte("1\n")}}
			end := map[string]any{"end": map[string]any{"exited": true, "status": "exit status 0"}}
			if wrapped {
				start = map[string]any{"event": start}
				data = map[string]any{"event": data}
				end = map[string]any{"event": end}
			}
			backend, closeServers := setup(t, []any{start, data, end})
			defer closeServers()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			result, err := backend.Run(ctx, "vm-1", "wc -l /tmp/m42-once")
			if err != nil || result.SandboxID != "vm-1" || result.ExitCode != 0 || result.Stdout != "1\n" {
				t.Fatalf("successful ProtoJSON exit lost: %+v err=%v", result, err)
			}
		})
	}
}

func TestRunBoundsOutputAndReturnsNonzeroExitCode(t *testing.T) {
	backend, closeServers := setup(t, []any{
		map[string]any{"start": map[string]any{"pid": 40}},
		map[string]any{"data": map[string]any{"stdout": []byte(strings.Repeat("a", 75<<10))}},
		map[string]any{"end": map[string]any{"exitCode": 17, "exited": true, "error": "process exited with code 17"}},
	})
	defer closeServers()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result, err := backend.Run(ctx, "vm-1", "exit 17")
	if err != nil || result.ExitCode != 17 || len(result.Stdout) != 64<<10 || !result.Truncated {
		t.Fatalf("output not bounded or exit code lost: %+v err=%v", result, err)
	}
}

func TestRunRejectsTerminalErrorsWithoutAttestedExitStatus(t *testing.T) {
	for _, tc := range []struct {
		name string
		end  map[string]any
	}{
		{name: "zero_exit_with_error", end: map[string]any{"exitCode": 0, "error": "process failed to launch"}},
		{name: "missing_exit_code_with_error", end: map[string]any{"error": "process failed to launch"}},
		{name: "omitted_zero_with_error", end: map[string]any{"exited": true, "error": "process failed to launch"}},
		{name: "missing_exit_code", end: map[string]any{"exited": false}},
		{name: "empty_end_event", end: map[string]any{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend, closeServers := setup(t, []any{
				map[string]any{"event": map[string]any{"start": map[string]any{"pid": 40}}},
				map[string]any{"event": map[string]any{"end": tc.end}},
			})
			defer closeServers()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if _, err := backend.Run(ctx, "vm-1", "exit 17"); err == nil || !strings.Contains(err.Error(), "ambiguous") {
				t.Fatalf("unattested terminal event treated as completed command: %v", err)
			}
		})
	}
}

func TestRunRejectsInvalidInputBeforeExternalCalls(t *testing.T) {
	for _, url := range []string{"https://127.0.0.1:3002", "http://evil.example:3002", "http://127.0.0.1:3002/path"} {
		if _, err := New(Config{APIURL: url, ProxyURL: "http://127.0.0.1:3000", APIKey: "x", TTL: 300}); err == nil {
			t.Errorf("accepted unsafe E2B URL %s", url)
		}
	}
	backend, err := New(Config{APIURL: "http://127.0.0.1:3000", ProxyURL: "http://127.0.0.1:3002", APIKey: "x", TTL: 300})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, id := range []string{"../danger", "", "vm-1/../../", "foo?bar"} {
		if _, err := backend.Run(ctx, id, "echo hello"); err == nil {
			t.Errorf("accepted sandbox ID %q", id)
		}
	}
	if _, err := backend.Run(context.Background(), "vm-1", "echo hello"); err == nil {
		t.Fatal("unbounded context accepted")
	}
	if _, err := backend.Run(ctx, "vm-1", strings.Repeat("x", maxCommandBytes+1)); err == nil {
		t.Fatal("oversized command accepted")
	}
}
