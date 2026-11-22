// Package fleet coordinates the E2B control plane and resource reservations
// with one local, crash-consistent journal. It is NOT a distributed scheduler.
package fleet

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"agent-runtime-lab/src/admission"
)

const journalVersion = 1

// Workload is the identity/intent counterpart of one admission.Entry.
// CreatePending is written before POST Create; an unknown outcome remains
// fenced, even if no sandbox appears in the provider's listing.
type Workload struct {
	LogicalKey    string `json:"logical_key"`
	SandboxID     string `json:"sandbox_id,omitempty"`
	Desired       string `json:"desired"`
	CreatePending bool   `json:"create_pending"`
}

type Journal struct {
	Version      int                  `json:"version"`
	TemplateSize admission.Resources  `json:"template_size"`
	Scheduler    admission.Checkpoint `json:"scheduler"`
	Workloads    map[string]Workload  `json:"workloads"`
	Unsafe       bool                 `json:"unsafe"`
	UnsafeNote   string               `json:"unsafe_note,omitempty"`
	Draining     bool                 `json:"draining,omitempty"` // admission fence survives controller restart
}

// Store owns an exclusive advisory local lock and writes a temporary file,
// fsyncs it, renames, then fsyncs its parent. Use only on local Linux storage.
type Store struct {
	path string
	lock *os.File
}

func Open(path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("fleet state-file path required")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(abs+".lock", os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("another fleet process holds this state lock: %w", err)
	}
	return &Store{path: abs, lock: lock}, nil
}

func (s *Store) Load() (Journal, bool, error) {
	if s.lock == nil {
		return Journal{}, false, errors.New("fleet store closed")
	}
	b, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return Journal{}, false, nil
	}
	if err != nil {
		return Journal{}, false, err
	}
	var journal Journal
	if err := json.Unmarshal(b, &journal); err != nil {
		return Journal{}, false, fmt.Errorf("corrupt fleet journal: %w", err)
	}
	if journal.Version != journalVersion {
		return Journal{}, false, errors.New("unknown fleet journal version")
	}
	return journal, true, nil
}

func (s *Store) Save(j Journal) error {
	if s.lock == nil {
		return errors.New("fleet store closed")
	}
	if j.Version != journalVersion {
		return errors.New("invalid fleet journal version")
	}
	b, err := json.MarshalIndent(j, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	f, err := os.CreateTemp(dir, ".fleet-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err := f.Chmod(0600); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(f.Name(), s.path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (s *Store) Close() error {
	if s.lock == nil {
		return nil
	}
	err := s.lock.Close()
	s.lock = nil
	return err
}
