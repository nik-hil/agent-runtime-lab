package artifactstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"agent-runtime-lab/src/taskevidence"
)

func proof(t *testing.T, id string, content []byte) (taskevidence.Identity, taskevidence.Envelope) {
	t.Helper()
	intent, err := taskevidence.NewIntent(taskevidence.Hash([]byte("helper")), []string{"/tmp/result.bin"})
	if err != nil {
		t.Fatal(err)
	}
	identity := taskevidence.Identity{TaskID: id, WorkloadID: "work", SandboxID: "vm", CommandSHA256: taskevidence.Hash([]byte("command")), Timeout: 30, Intent: intent}
	e, err := taskevidence.Seal(taskevidence.Manifest{Identity: identity, CompletedAt: time.Now().UTC(),
		StdoutSHA256: taskevidence.Hash(nil), StderrSHA256: taskevidence.Hash(nil),
		Artifacts: []taskevidence.Artifact{{Path: "/tmp/result.bin", Size: int64(len(content)), SHA256: taskevidence.Hash(content)}}})
	if err != nil {
		t.Fatal(err)
	}
	return identity, e
}

func newStore(t *testing.T, root string, limit int64) *Store {
	t.Helper()
	s, err := Open(root, limit)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func reader(content []byte) func(context.Context, taskevidence.Artifact) ([]byte, error) {
	return func(context.Context, taskevidence.Artifact) ([]byte, error) { return content, nil }
}

func TestBundleIsVerifiedImmutableAndSurvivesReopen(t *testing.T) {
	root := filepath.Join(t.TempDir(), "artifacts")
	s := newStore(t, root, 2<<20)
	content := []byte{0, 1, 2, 255, 3}
	identity, evidence := proof(t, "job", content)
	if err := s.Put(context.Background(), identity, evidence, reader(content)); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(context.Background(), identity, evidence, func(context.Context, taskevidence.Artifact) ([]byte, error) {
		t.Fatal("idempotent put refetched bytes")
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	if competing, err := Open(root, 2<<20); err == nil {
		competing.Close()
		t.Fatal("two stores owned the same directory")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = newStore(t, root, 2<<20)
	got, meta, err := s.Read(identity, 0)
	if err != nil || string(got) != string(content) || meta.SHA256 != evidence.Manifest.Artifacts[0].SHA256 {
		t.Fatal("durable bytes changed across reopen", err)
	}
	if _, err := s.Verify(identity); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Read(identity, -1); !errors.Is(err, ErrNotFound) {
		t.Fatal("negative index accepted", err)
	}
	if _, _, err := s.Read(identity, 1); !errors.Is(err, ErrNotFound) {
		t.Fatal("out of range index accepted", err)
	}
	wrong := identity
	wrong.Intent.Nonce = taskevidence.Hash([]byte("different nonce"))
	if _, err := s.Verify(wrong); !errors.Is(err, ErrInvalid) {
		t.Fatal("bundle was not bound to original intent", err)
	}
}

func TestBadCopiesNeverPublishAndCorruptionIsRejected(t *testing.T) {
	s := newStore(t, filepath.Join(t.TempDir(), "artifacts"), 2<<20)
	identity, evidence := proof(t, "job", []byte("original"))
	if err := s.Put(context.Background(), identity, evidence, reader([]byte("changed"))); !errors.Is(err, ErrInvalid) {
		t.Fatal("changed bytes accepted", err)
	}
	if _, err := s.Verify(identity); !errors.Is(err, ErrNotFound) {
		t.Fatal("failed copy published a bundle", err)
	}
	if err := s.Put(context.Background(), identity, evidence, reader([]byte("original"))); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(s.directory(identity.TaskID), "artifact-0")
	if err := os.WriteFile(path, []byte("corrupt!"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Read(identity, 0); !errors.Is(err, ErrInvalid) {
		t.Fatal("corrupt bytes were served", err)
	}
	if _, err := s.Verify(identity); !errors.Is(err, ErrInvalid) {
		t.Fatal("corrupt bytes were accepted for recovery", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/etc/passwd", path); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Read(identity, 0); !errors.Is(err, ErrInvalid) {
		t.Fatal("symlink artifact was read", err)
	}
}

func TestQuotaAndCancelledCopyReleaseReservations(t *testing.T) {
	s := newStore(t, filepath.Join(t.TempDir(), "artifacts"), 1<<20)
	content := make([]byte, 512<<10)
	identity, evidence := proof(t, "one", content)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Put(ctx, identity, evidence, reader(content)); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled copy published", err)
	}
	if err := s.Put(context.Background(), identity, evidence, reader(content)); err != nil {
		t.Fatal("cancelled copy leaked quota", err)
	}
	other, second := proof(t, "two", content)
	if err := s.Put(context.Background(), other, second, func(context.Context, taskevidence.Artifact) ([]byte, error) {
		t.Fatal("over-quota copy reached provider")
		return nil, nil
	}); !errors.Is(err, ErrQuota) {
		t.Fatal("quota failed", err)
	}
}

func TestPublicOrSymlinkDirectoryRejected(t *testing.T) {
	root := filepath.Join(t.TempDir(), "public")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0755); err != nil {
		t.Fatal(err)
	}
	if s, err := Open(root, 1<<20); err == nil {
		s.Close()
		t.Fatal("public store accepted")
	}
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(root, link); err != nil {
		t.Fatal(err)
	}
	if s, err := Open(link, 1<<20); err == nil {
		s.Close()
		t.Fatal("symlink store accepted")
	}
}
