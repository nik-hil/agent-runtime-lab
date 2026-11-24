// Package artifactstore keeps verified, immutable completion bundles on the
// host disk. It survives VM deletion; host disk loss still requires backups.
package artifactstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"agent-runtime-lab/src/taskevidence"
)

var (
	ErrNotFound = errors.New("durable artifact bundle not found")
	ErrInvalid  = errors.New("durable artifact bundle failed integrity verification")
	ErrQuota    = errors.New("durable artifact storage quota exceeded")
	ErrPoisoned = errors.New("artifact persistence uncertain; restart and inspect storage")
)

type Store struct {
	mu       sync.Mutex
	root     string
	lock     *os.File
	limit    int64
	used     int64
	reserved int64
	poisoned bool
}

type Bundle struct {
	Version  int                   `json:"version"`
	Evidence taskevidence.Envelope `json:"evidence"`
}

type Stats struct {
	UsedBytes     int64 `json:"used_bytes"`
	ReservedBytes int64 `json:"reserved_bytes"`
	LimitBytes    int64 `json:"limit_bytes"`
	Uncertain     bool  `json:"persistence_uncertain"`
}

func (s *Store) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{UsedBytes: s.used, ReservedBytes: s.reserved, LimitBytes: s.limit, Uncertain: s.poisoned || s.lock == nil}
}

func privateDir(path string) error {
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return errors.New("artifact directories must be nonsymlink private directories (0700)")
	}
	return nil
}

// Sync every newly created directory's parent so a custom nested artifact
// root is durable too, independently of task-journal fsyncs elsewhere.
func makeRoot(path string) error {
	info, err := os.Lstat(path)
	if err == nil {
		if !info.IsDir() {
			return errors.New("artifact path contains a symlink or nondirectory")
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := makeRoot(filepath.Dir(path)); err != nil {
		return err
	}
	if err := os.Mkdir(path, 0700); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

func Open(root string, limit int64) (*Store, error) {
	if root == "" || limit < 1<<20 || limit > 1<<30 {
		return nil, errors.New("artifact directory and quota in range 1 MiB..1 GiB required")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err := makeRoot(abs); err != nil {
		return nil, err
	}
	if err := privateDir(abs); err != nil {
		return nil, err
	}
	canonical, err := filepath.EvalSymlinks(abs)
	if err != nil || canonical != abs {
		return nil, errors.New("artifact root must not contain symlink components")
	}
	fd, err := syscall.Open(filepath.Join(abs, ".lock"), syscall.O_CREAT|syscall.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0600)
	if err != nil {
		return nil, err
	}
	lock := os.NewFile(uintptr(fd), "artifact lock")
	info, err := lock.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		lock.Close()
		return nil, errors.New("artifact lock must be a private regular file")
	}
	if err := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, errors.New("artifact directory is already owned by another daemon")
	}
	s := &Store{root: abs, limit: limit, lock: lock}
	err = filepath.WalkDir(abs, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return ErrInvalid
		}
		if !entry.IsDir() && path != filepath.Join(abs, ".lock") {
			info, err := entry.Info()
			if err != nil || !info.Mode().IsRegular() {
				return ErrInvalid
			}
			s.used += info.Size() // includes crash-left staging files; never hide disk usage
		}
		return nil
	})
	if err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lock == nil {
		return nil
	}
	err := s.lock.Close()
	s.lock = nil
	return err
}

func (s *Store) directory(id string) string {
	return filepath.Join(s.root, taskevidence.Hash([]byte(id)))
}

func readRegular(path string, limit int64) ([]byte, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		if errors.Is(err, syscall.ENOENT) {
			return nil, ErrNotFound
		}
		return nil, ErrInvalid
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > limit {
		return nil, ErrInvalid
	}
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil || int64(len(data)) > limit {
		return nil, ErrInvalid
	}
	return data, nil
}

func (s *Store) bundle(identity taskevidence.Identity) (Bundle, error) {
	if err := identity.Validate(); err != nil {
		return Bundle{}, ErrInvalid
	}
	dir := s.directory(identity.TaskID)
	if _, err := os.Lstat(dir); errors.Is(err, os.ErrNotExist) {
		return Bundle{}, ErrNotFound
	}
	if err := privateDir(dir); err != nil {
		return Bundle{}, ErrInvalid
	}
	data, err := readRegular(filepath.Join(dir, "manifest.json"), taskevidence.MaxManifestBytes)
	if err != nil {
		return Bundle{}, ErrInvalid
	}
	var bundle Bundle
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if d.Decode(&bundle) != nil || d.Decode(new(any)) != io.EOF || bundle.Version != 1 || bundle.Evidence.Validate(identity) != nil {
		return Bundle{}, ErrInvalid
	}
	return bundle, nil
}

func (s *Store) artifact(identity taskevidence.Identity, bundle Bundle, index int) ([]byte, error) {
	if index < 0 || index >= len(bundle.Evidence.Manifest.Artifacts) {
		return nil, ErrNotFound
	}
	a := bundle.Evidence.Manifest.Artifacts[index]
	data, err := readRegular(filepath.Join(s.directory(identity.TaskID), fmt.Sprintf("artifact-%d", index)), taskevidence.MaxArtifactBytes)
	if err != nil || int64(len(data)) != a.Size || taskevidence.Hash(data) != a.SHA256 {
		return nil, ErrInvalid
	}
	return data, nil
}

// Read verifies host bytes before returning them, even after their VM is gone.
func (s *Store) Read(identity taskevidence.Identity, index int) ([]byte, taskevidence.Artifact, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lock == nil || s.poisoned {
		return nil, taskevidence.Artifact{}, ErrPoisoned
	}
	b, err := s.bundle(identity)
	if err != nil {
		return nil, taskevidence.Artifact{}, err
	}
	data, err := s.artifact(identity, b, index)
	if err != nil {
		return nil, taskevidence.Artifact{}, err
	}
	return data, b.Evidence.Manifest.Artifacts[index], nil
}

func (s *Store) Verify(identity taskevidence.Identity) (taskevidence.Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lock == nil || s.poisoned {
		return taskevidence.Envelope{}, ErrPoisoned
	}
	b, err := s.bundle(identity)
	if err != nil {
		return taskevidence.Envelope{}, err
	}
	for index := range b.Evidence.Manifest.Artifacts {
		if _, err := s.artifact(identity, b, index); err != nil {
			return taskevidence.Envelope{}, err
		}
	}
	return b.Evidence, nil
}

func writeSynced(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func syncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// Put verifies every fetched byte, fsyncs files and the staging directory, then
// atomically publishes a whole immutable bundle and fsyncs its parent. Network
// reads happen outside the store mutex, under a bounded disk reservation.
func (s *Store) Put(ctx context.Context, identity taskevidence.Identity, evidence taskevidence.Envelope, fetch func(context.Context, taskevidence.Artifact) ([]byte, error)) error {
	if evidence.Validate(identity) != nil || fetch == nil {
		return ErrInvalid
	}
	data, err := json.Marshal(Bundle{Version: 1, Evidence: evidence})
	if err != nil || len(data) > taskevidence.MaxManifestBytes {
		return ErrInvalid
	}
	bytesNeeded := int64(len(data))
	for _, a := range evidence.Manifest.Artifacts {
		bytesNeeded += a.Size
	}
	s.mu.Lock()
	if s.lock == nil || s.poisoned {
		s.mu.Unlock()
		return ErrPoisoned
	}
	if old, err := s.bundle(identity); err == nil {
		s.mu.Unlock()
		if old.Evidence.SHA256 != evidence.SHA256 {
			return ErrInvalid
		}
		_, err = s.Verify(identity)
		return err
	} else if !errors.Is(err, ErrNotFound) {
		s.mu.Unlock()
		return err
	}
	if s.used+s.reserved+bytesNeeded > s.limit {
		s.mu.Unlock()
		return ErrQuota
	}
	s.reserved += bytesNeeded
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.reserved -= bytesNeeded; s.mu.Unlock() }()
	stage, err := os.MkdirTemp(s.root, ".incoming-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stage)
	for index, a := range evidence.Manifest.Artifacts {
		if err := ctx.Err(); err != nil {
			return err
		}
		content, err := fetch(ctx, a)
		if err != nil {
			return err
		}
		if int64(len(content)) != a.Size || taskevidence.Hash(content) != a.SHA256 {
			return ErrInvalid
		}
		if err := writeSynced(filepath.Join(stage, fmt.Sprintf("artifact-%d", index)), content); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := writeSynced(filepath.Join(stage, "manifest.json"), data); err != nil {
		return err
	}
	if err := syncDir(stage); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lock == nil || s.poisoned {
		return ErrPoisoned
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if old, err := s.bundle(identity); err == nil {
		if old.Evidence.SHA256 != evidence.SHA256 {
			return ErrInvalid
		}
		for index := range old.Evidence.Manifest.Artifacts {
			if _, err := s.artifact(identity, old, index); err != nil {
				return err
			}
		}
		return nil
	} else if !errors.Is(err, ErrNotFound) {
		return err
	}
	if err := os.Rename(stage, s.directory(identity.TaskID)); err != nil {
		return err
	}
	s.used += bytesNeeded
	if err := syncDir(s.root); err != nil {
		s.poisoned = true
		return ErrPoisoned
	}
	return nil
}
