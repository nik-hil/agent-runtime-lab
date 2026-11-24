package taskevidence

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func requestFor(t *testing.T, command string, artifacts []string) Request {
	t.Helper()
	intent, err := NewIntent(Hash([]byte("test-helper")), artifacts)
	if err != nil {
		t.Fatal(err)
	}
	return Request{Identity: Identity{TaskID: "task", WorkloadID: "work", SandboxID: "vm-1",
		CommandSHA256: Hash([]byte(command)), Timeout: 5, Intent: intent}, Command: command}
}

func TestExecutePublishesBoundManifestAndNeverReusesNonce(t *testing.T) {
	root := filepath.Join(t.TempDir(), "evidence")
	artifact := filepath.Join(t.TempDir(), "output.txt")
	command := "printf marker >> '" + artifact + "'; printf 'done\\n'"
	request := requestFor(t, command, []string{artifact})
	envelope, err := Execute(context.Background(), root, request)
	if err != nil {
		t.Fatal(err)
	}
	if err := envelope.Validate(request.Identity); err != nil || envelope.Manifest.ExitCode != 0 || envelope.Manifest.Stdout != "done\n" ||
		envelope.Manifest.Artifacts[0].SHA256 != Hash([]byte("marker")) {
		t.Fatalf("wrong completion evidence: %+v %v", envelope, err)
	}
	marker := filepath.Join(root, request.Identity.Intent.Nonce, "completion.json")
	data, err := os.ReadFile(marker)
	if err != nil || strings.Contains(string(data), command) {
		t.Fatalf("completion record missing or contains raw command: %v", err)
	}
	var persisted Envelope
	if err := json.Unmarshal(data, &persisted); err != nil || persisted.Validate(request.Identity) != nil {
		t.Fatalf("persisted marker failed validation: %v", err)
	}
	info, _ := os.Stat(marker)
	if info.Mode().Perm() != 0600 {
		t.Fatalf("marker permissions: %o", info.Mode().Perm())
	}
	if _, err := Execute(context.Background(), root, request); err == nil {
		t.Fatal("same nonce executed a second time")
	}
	actual, _ := os.ReadFile(artifact)
	if string(actual) != "marker" {
		t.Fatalf("side effect repeated: %q", actual)
	}
}

func TestNonzeroExitStillPublishesCompletion(t *testing.T) {
	request := requestFor(t, "printf BEFORE_EXIT; exit 17", nil)
	envelope, err := Execute(context.Background(), filepath.Join(t.TempDir(), "evidence"), request)
	if err != nil || envelope.Manifest.ExitCode != 17 || envelope.Manifest.Stdout != "BEFORE_EXIT" {
		t.Fatalf("failed command lost its known result: %+v %v", envelope, err)
	}
}

func TestMissingArtifactLeavesUncertainNonceAndCannotReplay(t *testing.T) {
	root := filepath.Join(t.TempDir(), "evidence")
	effect := filepath.Join(t.TempDir(), "effect")
	request := requestFor(t, "printf x >> '"+effect+"'", []string{filepath.Join(t.TempDir(), "missing")})
	if _, err := Execute(context.Background(), root, request); err == nil {
		t.Fatal("missing artifact produced completion evidence")
	}
	if _, err := os.Stat(filepath.Join(root, request.Identity.Intent.Nonce, "completion.json")); !os.IsNotExist(err) {
		t.Fatalf("unexpected completion marker: %v", err)
	}
	if _, err := Execute(context.Background(), root, request); err == nil {
		t.Fatal("absent completion marker authorized replay")
	}
	data, _ := os.ReadFile(effect)
	if string(data) != "x" {
		t.Fatalf("uncertain task repeated side effect: %q", data)
	}
}

func TestOutputBoundAndInvalidUTF8(t *testing.T) {
	request := requestFor(t, "printf '\\377'; printf '%070000d' 0", nil)
	envelope, err := Execute(context.Background(), filepath.Join(t.TempDir(), "evidence"), request)
	if err != nil || !envelope.Manifest.Truncated || len(envelope.Manifest.Stdout) != MaxOutputBytes || envelope.Manifest.Stdout[0] != '?' {
		t.Fatalf("output bound/UTF-8 handling failed: len=%d err=%v", len(envelope.Manifest.Stdout), err)
	}
}

func TestCanceledHelperDoesNotPublishCompletion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	root := filepath.Join(t.TempDir(), "evidence")
	request := requestFor(t, "echo should-not-complete", nil)
	if _, err := Execute(ctx, root, request); err == nil {
		t.Fatal("canceled helper reported completion")
	}
	if _, err := os.Stat(filepath.Join(root, request.Identity.Intent.Nonce, "completion.json")); !os.IsNotExist(err) {
		t.Fatalf("canceled helper left terminal evidence: %v", err)
	}
}

func TestArtifactBoundsAndSymlinkRejection(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "file")
	if err := os.WriteFile(file, []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(file, link); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadArtifact(link); err == nil {
		t.Fatal("accepted symlink artifact")
	}
	if _, err := ReadArtifact(dir); err == nil {
		t.Fatal("accepted directory artifact")
	}
	if err := os.WriteFile(file, make([]byte, MaxArtifactBytes+1), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadArtifact(file); err == nil {
		t.Fatal("accepted oversized artifact")
	}
	for _, paths := range [][]string{{"relative"}, {"/tmp/../etc/passwd"}, {Root + "/marker"}, {"/tmp/a", "/tmp/a"}} {
		if _, err := NormalizeArtifacts(paths); err == nil {
			t.Fatalf("invalid artifact paths accepted: %v", paths)
		}
	}
}

func TestManifestTamperAndCrossTaskMixupRejected(t *testing.T) {
	request := requestFor(t, "echo evidence", nil)
	envelope, err := Execute(context.Background(), filepath.Join(t.TempDir(), "evidence"), request)
	if err != nil {
		t.Fatal(err)
	}
	mutated := envelope
	mutated.Manifest.Stdout = "tampered"
	if mutated.Validate(request.Identity) == nil {
		t.Fatal("accepted modified output")
	}
	wrong := request.Identity
	wrong.TaskID = "other-task"
	if envelope.Validate(wrong) == nil {
		t.Fatal("accepted another task's marker")
	}
	wrong = request.Identity
	wrong.Intent.Nonce = Hash([]byte("another attempt"))
	if envelope.Validate(wrong) == nil {
		t.Fatal("accepted another attempt's marker")
	}
	mutated = envelope
	mutated.Manifest.Artifacts = nil
	mutated, _ = Seal(mutated.Manifest)
	if mutated.Validate(request.Identity) == nil {
		t.Fatal("accepted a noncanonical missing artifact array")
	}
}
