package m4exec

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"agent-runtime-lab/src/taskevidence"
)

type evidenceFixture struct {
	runner        *Runner
	identity      taskevidence.Identity
	command       string
	artifact      string
	root          string
	starts        int
	uploads       int
	reads         int
	dropResponse  bool
	mutateFile    bool
	oversize      bool
	missingMarker bool
}

func evidenceProxy(t *testing.T) *evidenceFixture {
	t.Helper()
	f := &evidenceFixture{artifact: filepath.Join(t.TempDir(), "output"), root: filepath.Join(t.TempDir(), "evidence")}
	helper := []byte("fake helper bytes for transport fixture; never executed")
	f.command = "printf marker >> '" + f.artifact + "'; printf 'done\\n'"
	intent, err := taskevidence.NewIntent(taskevidence.Hash(helper), []string{f.artifact})
	if err != nil {
		t.Fatal(err)
	}
	f.identity = taskevidence.Identity{TaskID: "task", WorkloadID: "work", SandboxID: "vm-1",
		CommandSHA256: taskevidence.Hash([]byte(f.command)), Timeout: 5, Intent: intent}
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/sandboxes/vm-1/connect" || r.Header.Get("X-API-Key") != "test-key" {
			t.Errorf("unexpected control request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		fmt.Fprint(w, `{"sandboxID":"vm-1","envdAccessToken":"test-token"}`)
	}))
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Access-Token") != "test-token" || r.Header.Get("X-API-Key") != "" ||
			r.Header.Get("E2b-Sandbox-Id") != "vm-1" || r.Header.Get("E2b-Sandbox-Port") != "49983" {
			t.Error("invalid proxy credentials or sandbox binding")
			w.WriteHeader(http.StatusForbidden)
			return
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/files":
			f.uploads++
			if r.URL.Query().Get("username") != "user" || r.URL.Query().Get("path") != "/tmp/agentlab-m43-helper-"+intent.Nonce {
				t.Error("unexpected helper upload path or user")
			}
			reader, err := r.MultipartReader()
			if err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			part, err := reader.NextPart()
			if err != nil {
				t.Error(err)
				return
			}
			data, err := io.ReadAll(part)
			if err != nil || part.FormName() != "file" || !bytes.Equal(data, helper) {
				t.Error("helper upload changed bytes")
			}
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && r.URL.Path == "/process.Process/Start":
			f.starts++
			header := make([]byte, 5)
			if _, err := io.ReadFull(r.Body, header); err != nil {
				t.Error(err)
				return
			}
			var request struct {
				Process struct {
					Args []string `json:"args"`
				} `json:"process"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil || len(request.Process.Args) != 3 {
				t.Errorf("invalid helper process request: %v", err)
				return
			}
			invocation := request.Process.Args[2]
			prefix := "--request-base64 '"
			_, encoded, found := strings.Cut(invocation, prefix)
			if !found || !strings.HasSuffix(encoded, "'") || strings.Contains(invocation, f.command) {
				t.Error("invalid helper invocation or raw command interpolation")
				return
			}
			data, err := base64.RawURLEncoding.DecodeString(strings.TrimSuffix(encoded, "'"))
			if err != nil {
				t.Error(err)
				return
			}
			var task taskevidence.Request
			if err := json.Unmarshal(data, &task); err != nil || task.Command != f.command {
				t.Errorf("helper request changed: %v", err)
				return
			}
			// This is a deterministic integration fixture executing the source
			// helper implementation in a temporary directory, not fleetd work.
			if _, err := taskevidence.Execute(r.Context(), f.root, task); err != nil {
				t.Error(err)
				return
			}
			if f.mutateFile {
				if err := os.WriteFile(f.artifact, []byte("changed"), 0600); err != nil {
					t.Error(err)
				}
			}
			frame(t, w, map[string]any{"event": map[string]any{"start": map[string]any{"pid": 40}}})
			if !f.dropResponse {
				frame(t, w, map[string]any{"event": map[string]any{"end": map[string]any{"exited": true}}})
			}
		case r.Method == http.MethodGet && r.URL.Path == "/files":
			f.reads++
			path := r.URL.Query().Get("path")
			if path == taskevidence.ManifestPath(intent) {
				if f.missingMarker {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				if f.oversize {
					fmt.Fprint(w, strings.Repeat("x", taskevidence.MaxManifestBytes+1))
					return
				}
				path = filepath.Join(f.root, intent.Nonce, "completion.json")
			} else if path != f.artifact {
				t.Error("attempted undeclared artifact read")
				w.WriteHeader(http.StatusForbidden)
				return
			}
			data, err := os.ReadFile(path)
			if err != nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_, _ = w.Write(data)
		default:
			t.Errorf("unexpected sandbox request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(func() { control.Close(); proxy.Close() })
	f.runner, err = New(Config{APIURL: control.URL, ProxyURL: proxy.URL, APIKey: "test-key", TTL: 300})
	if err != nil {
		t.Fatal(err)
	}
	f.runner.helper, f.runner.helperHash = helper, taskevidence.Hash(helper)
	return f
}

func TestEvidenceHelperUploadExecutionAndVerifiedArtifacts(t *testing.T) {
	f := evidenceProxy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	proof, err := f.runner.RunEvidence(ctx, f.identity, f.command)
	if err != nil || proof.Manifest.Stdout != "done\n" || proof.Manifest.Artifacts[0].SHA256 != taskevidence.Hash([]byte("marker")) ||
		f.uploads != 1 || f.starts != 1 || f.reads != 2 {
		t.Fatalf("evidence execution failed: %+v uploads=%d starts=%d reads=%d err=%v", proof, f.uploads, f.starts, f.reads, err)
	}
}

func TestLostResponseRecoveredOnlyByReadsWithoutRerunning(t *testing.T) {
	f := evidenceProxy(t)
	f.dropResponse = true
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := f.runner.RunEvidence(ctx, f.identity, f.command); err == nil {
		t.Fatal("lost terminal response treated as success")
	}
	proof, err := f.runner.ReadEvidence(ctx, f.identity)
	if err != nil || proof.Manifest.ExitCode != 0 || f.uploads != 1 || f.starts != 1 {
		t.Fatalf("evidence recovery re-executed work: uploads=%d starts=%d err=%v", f.uploads, f.starts, err)
	}
	data, _ := os.ReadFile(f.artifact)
	if string(data) != "marker" {
		t.Fatalf("side effect repeated: %q", data)
	}
}

func TestChangedArtifactMissingMarkerAndOversizeFailClosed(t *testing.T) {
	f := evidenceProxy(t)
	f.mutateFile = true
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := f.runner.RunEvidence(ctx, f.identity, f.command); err == nil {
		t.Fatal("changed artifact accepted")
	}
	if _, err := f.runner.ReadEvidence(ctx, f.identity); err == nil {
		t.Fatal("recovery accepted changed artifact")
	}
	f.missingMarker = true
	if _, err := f.runner.ReadEvidence(ctx, f.identity); err == nil {
		t.Fatal("recovery accepted missing marker")
	}
	f.missingMarker, f.oversize = false, true
	if _, err := f.runner.ReadEvidence(ctx, f.identity); err == nil {
		t.Fatal("recovery accepted oversized marker")
	}
	if f.starts != 1 || f.uploads != 1 {
		t.Fatal("failed inspection caused execution")
	}
}

func TestHelperConfigurationRejectsScriptsAndSymlinks(t *testing.T) {
	file := filepath.Join(t.TempDir(), "helper")
	if err := os.WriteFile(file, []byte("#!/bin/sh\necho wrong-helper\n"), 0700); err != nil {
		t.Fatal(err)
	}
	config := Config{APIURL: "http://127.0.0.1:3000", ProxyURL: "http://127.0.0.1:3002", APIKey: "test-key", TTL: 300, HelperPath: file}
	if _, err := New(config); err == nil {
		t.Fatal("accepted shell script as Go helper")
	}
	link := file + "-link"
	if err := os.Symlink(file, link); err != nil {
		t.Fatal(err)
	}
	config.HelperPath = link
	if _, err := New(config); err == nil {
		t.Fatal("accepted symlink helper")
	}
}
