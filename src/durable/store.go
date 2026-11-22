// Package durable provides a crash-safe, single-host state journal for a
// sandbox lifecycle reconciler. The storage implementation is intentionally
// standard-library-only so the M2.7 experiment needs no dependency upgrade.
package durable

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

const stateVersion = 1

// Record contains no credentials. A CreatePending record is written *before*
// contacting the remote API, avoiding duplicate creates after a crash.
type Record struct {
	Version       int    `json:"version"`
	LogicalKey    string `json:"logical_key"`
	SandboxID     string `json:"sandbox_id,omitempty"`
	Desired       string `json:"desired"`
	Observed      string `json:"observed"`
	CreatePending bool   `json:"create_pending"`
	Terminated    bool   `json:"terminated"`
}

func (r Record) validate() error {
	if r.Version != stateVersion || r.LogicalKey == "" {
		return errors.New("invalid state version or logical key")
	}
	if r.Desired != "running" && r.Desired != "paused" && r.Desired != "deleted" {
		return fmt.Errorf("invalid desired state %q", r.Desired)
	}
	if r.Observed != "unknown" && r.Observed != "running" && r.Observed != "paused" && r.Observed != "absent" {
		return fmt.Errorf("invalid observed state %q", r.Observed)
	}
	if r.CreatePending && (r.SandboxID != "" || r.Terminated) {
		return errors.New("create-pending record cannot have sandbox ID or be terminated")
	}
	if r.Terminated && (r.Desired != "deleted" || r.SandboxID != "" || r.CreatePending) {
		return errors.New("invalid terminal record")
	}
	return nil
}

// Store is an exclusive, durable record store. It must serialize Save and Load
// across controller processes, not merely concurrent goroutines.
type Store interface {
	Load() (Record, bool, error)
	Save(Record) error
	Close() error
}

// FileStore writes a new file, fsyncs it, renames it atomically and fsyncs the
// parent directory. A persistent .lock file and flock enforce single-process
// ownership. This only targets local Linux filesystems, not NFS.
type FileStore struct {
	path string
	lock *os.File
}

func OpenFileStore(path string) (*FileStore, error) {
	if path == "" {
		return nil, errors.New("state path required")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	dir := filepath.Dir(abs)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(abs+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("open state lock: %w", err)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("state already locked by another process: %w", err)
	}
	return &FileStore{path: abs, lock: lock}, nil
}

func (s *FileStore) Load() (Record, bool, error) {
	if s.lock == nil {
		return Record{}, false, errors.New("store closed")
	}
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return Record{}, false, nil
	}
	if err != nil {
		return Record{}, false, err
	}
	var r Record
	if err := json.Unmarshal(data, &r); err != nil {
		return Record{}, false, fmt.Errorf("corrupt state file: %w", err)
	}
	if err := r.validate(); err != nil {
		return Record{}, false, fmt.Errorf("invalid stored state: %w", err)
	}
	return r, true, nil
}

func (s *FileStore) Save(r Record) error {
	if s.lock == nil {
		return errors.New("store closed")
	}
	if err := r.validate(); err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".state-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(append(encoded, '\n')); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, s.path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (s *FileStore) Close() error {
	if s.lock == nil {
		return nil
	}
	err := s.lock.Close() // closing releases the advisory flock
	s.lock = nil
	return err
}
