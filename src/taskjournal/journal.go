// Package taskjournal keeps durable, single-host task execution intent and
// results. A task with an unknown outcome is never automatically replayed.
package taskjournal

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"agent-runtime-lab/src/m4exec"
)

const version = 1
const maxTasks = 128

type State string

const (
	Running   State = "running"
	Succeeded State = "succeeded"
	Failed    State = "failed"
	Uncertain State = "uncertain"
)

var (
	ErrConflict = errors.New("task ID already belongs to a different request")
	ErrBusy     = errors.New("a running or uncertain task already owns this sandbox")
	ErrFull     = errors.New("task journal capacity reached")
	ErrNotFound = errors.New("task not found")
	ErrPoisoned = errors.New("task journal persistence uncertain; stop daemon and inspect before proceeding")
)

// Entry contains nonsecret task identity, a command hash, and bounded output.
// This is a local completion record, NOT third-party attestation of side effects.
type Entry struct {
	ID          string         `json:"id"`
	WorkloadID  string         `json:"workload_id"`
	SandboxID   string         `json:"sandbox_id"`
	CommandHash string         `json:"command_sha256"`
	Timeout     int            `json:"timeout_seconds"`
	State       State          `json:"state"`
	StartedAt   time.Time      `json:"started_at"`
	FinishedAt  *time.Time     `json:"finished_at,omitempty"`
	Result      *m4exec.Result `json:"result,omitempty"`
	ResultHash  string         `json:"result_sha256,omitempty"`
	Note        string         `json:"note,omitempty"`
}

type checkpoint struct {
	Version int              `json:"version"`
	Tasks   map[string]Entry `json:"tasks"`
}

// Store is held exclusively by fleetd. Its file is fsynced before a command
// begins, and completed results are committed with the same durable protocol.
type Store struct {
	mu       sync.Mutex
	path     string
	lock     *os.File
	journal  checkpoint
	poisoned bool
}

func validID(id string) bool {
	if len(id) == 0 || len(id) > 128 {
		return false
	}
	for _, c := range id {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
			return false
		}
	}
	return true
}

func hexHash(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func commandHash(command string) string { return hexHash([]byte(command)) }

func resultHash(e Entry, result m4exec.Result) string {
	b, _ := json.Marshal(struct {
		ID          string        `json:"id"`
		WorkloadID  string        `json:"workload_id"`
		CommandHash string        `json:"command_hash"`
		Result      m4exec.Result `json:"result"`
	}{e.ID, e.WorkloadID, e.CommandHash, result})
	return hexHash(b)
}

func validate(j checkpoint) error {
	if j.Version != version || j.Tasks == nil || len(j.Tasks) > maxTasks {
		return errors.New("invalid task journal header or task count")
	}
	for id, e := range j.Tasks {
		if !validID(id) || e.ID != id || !validID(e.WorkloadID) || !validID(e.SandboxID) ||
			len(e.CommandHash) != 64 || e.Timeout < 1 || e.Timeout > 60 || e.StartedAt.IsZero() {
			return fmt.Errorf("invalid task identity: %q", id)
		}
		switch e.State {
		case Running:
			if e.FinishedAt != nil || e.Result != nil || e.ResultHash != "" || e.Note != "" {
				return fmt.Errorf("invalid running task: %q", id)
			}
		case Uncertain:
			if e.Result != nil || e.ResultHash != "" || e.Note == "" {
				return fmt.Errorf("invalid uncertain task: %q", id)
			}
		case Succeeded, Failed:
			if e.Result == nil || e.FinishedAt == nil || e.Result.SandboxID != e.SandboxID ||
				e.ResultHash != resultHash(e, *e.Result) {
				return fmt.Errorf("invalid terminal evidence: %q", id)
			}
			if e.State == Succeeded && e.Result.ExitCode != 0 || e.State == Failed && e.Result.ExitCode == 0 {
				return fmt.Errorf("wrong terminal status: %q", id)
			}
		default:
			return fmt.Errorf("unknown task state: %q", id)
		}
	}
	return nil
}

// Open takes a nonblocking exclusive lock. An interrupted RUNNING task is
// durably marked UNCERTAIN before the daemon can accept any new task.
func Open(path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("task state file required")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	dir := filepath.Dir(abs)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("task directory must exist privately (mode 0700)")
	}
	lock, err := os.OpenFile(abs+".lock", os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("another process owns task journal: %w", err)
	}
	s := &Store{path: abs, lock: lock, journal: checkpoint{Version: version, Tasks: make(map[string]Entry)}}
	data, err := os.ReadFile(abs)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		s.Close()
		return nil, err
	}
	if err == nil {
		if err := json.Unmarshal(data, &s.journal); err != nil {
			s.Close()
			return nil, fmt.Errorf("corrupt task journal: %w", err)
		}
		if err := validate(s.journal); err != nil {
			s.Close()
			return nil, err
		}
		changed := false
		for id, e := range s.journal.Tasks {
			if e.State == Running {
				e.State = Uncertain
				e.Note = "daemon interrupted after task intent was persisted; execution outcome unknown"
				s.journal.Tasks[id] = e
				changed = true
			}
		}
		if changed {
			if err := s.save(); err != nil {
				s.Close()
				return nil, err
			}
		}
	} else if err := s.save(); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) save() error {
	if s.lock == nil || s.poisoned {
		return ErrPoisoned
	}
	data, err := json.MarshalIndent(s.journal, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	f, err := os.CreateTemp(dir, ".tasks-*")
	if err != nil {
		s.poisoned = true
		return fmt.Errorf("%w: %v", ErrPoisoned, err)
	}
	defer os.Remove(f.Name())
	if err := f.Chmod(0600); err != nil {
		f.Close()
		s.poisoned = true
		return fmt.Errorf("%w: %v", ErrPoisoned, err)
	}
	if _, err := f.Write(append(data, '\n')); err != nil {
		f.Close()
		s.poisoned = true
		return fmt.Errorf("%w: %v", ErrPoisoned, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		s.poisoned = true
		return fmt.Errorf("%w: %v", ErrPoisoned, err)
	}
	if err := f.Close(); err != nil {
		s.poisoned = true
		return fmt.Errorf("%w: %v", ErrPoisoned, err)
	}
	if err := os.Rename(f.Name(), s.path); err != nil {
		s.poisoned = true
		return fmt.Errorf("%w: %v", ErrPoisoned, err)
	}
	d, err := os.Open(dir)
	if err != nil {
		s.poisoned = true
		return fmt.Errorf("%w: %v", ErrPoisoned, err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		s.poisoned = true
		return fmt.Errorf("%w: %v", ErrPoisoned, err)
	}
	return nil
}

func (s *Store) Begin(id, workloadID, sandboxID, command string, timeout int) (Entry, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poisoned || s.lock == nil {
		return Entry{}, false, ErrPoisoned
	}
	if !validID(id) || !validID(workloadID) || !validID(sandboxID) ||
		command == "" || len(command) > 4000 || strings.IndexByte(command, 0) >= 0 || timeout < 1 || timeout > 60 {
		return Entry{}, false, errors.New("invalid bounded task request")
	}
	hash := commandHash(command)
	if existing, ok := s.journal.Tasks[id]; ok {
		if existing.WorkloadID != workloadID || existing.SandboxID != sandboxID || existing.CommandHash != hash || existing.Timeout != timeout {
			return Entry{}, false, ErrConflict
		}
		return clone(existing), false, nil
	}
	if len(s.journal.Tasks) >= maxTasks {
		return Entry{}, false, ErrFull
	}
	for _, existing := range s.journal.Tasks {
		if existing.SandboxID == sandboxID && (existing.State == Running || existing.State == Uncertain) {
			return Entry{}, false, ErrBusy
		}
	}
	e := Entry{ID: id, WorkloadID: workloadID, SandboxID: sandboxID, CommandHash: hash,
		Timeout: timeout, State: Running, StartedAt: time.Now().UTC()}
	s.journal.Tasks[id] = e
	if err := s.save(); err != nil {
		return Entry{}, false, err // no remote execution until fsync succeeds
	}
	return clone(e), true, nil
}

// Finish only records a terminal event for the same VM. Any unknown outcome
// persists as UNCERTAIN and blocks future tasks for that VM until investigated.
func (s *Store) Finish(id string, result *m4exec.Result, note string) (Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poisoned || s.lock == nil {
		return Entry{}, ErrPoisoned
	}
	e, ok := s.journal.Tasks[id]
	if !ok || e.State != Running {
		return Entry{}, errors.New("task is not running")
	}
	now := time.Now().UTC()
	e.FinishedAt = &now
	if result == nil || result.SandboxID != e.SandboxID {
		e.State = Uncertain
		e.Note = "command outcome unconfirmed; do not automatically replay"
		if note != "" {
			e.Note = note
		}
	} else {
		copyResult := *result
		e.Result = &copyResult
		e.ResultHash = resultHash(e, copyResult)
		if copyResult.ExitCode == 0 {
			e.State = Succeeded
		} else {
			e.State = Failed
		}
	}
	s.journal.Tasks[id] = e
	if err := s.save(); err != nil {
		return Entry{}, err
	}
	return clone(e), nil
}

func clone(e Entry) Entry {
	if e.Result != nil {
		v := *e.Result
		e.Result = &v
	}
	if e.FinishedAt != nil {
		v := *e.FinishedAt
		e.FinishedAt = &v
	}
	return e
}

func (s *Store) Get(id string) (Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poisoned || s.lock == nil {
		return Entry{}, ErrPoisoned
	}
	e, ok := s.journal.Tasks[id]
	if !ok {
		return Entry{}, ErrNotFound
	}
	return clone(e), nil
}

func (s *Store) List() ([]Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poisoned || s.lock == nil {
		return nil, ErrPoisoned
	}
	out := make([]Entry, 0, len(s.journal.Tasks))
	for _, e := range s.journal.Tasks {
		out = append(out, clone(e))
	}
	return out, nil
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
