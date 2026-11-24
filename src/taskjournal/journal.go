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
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"

	"agent-runtime-lab/src/m4exec"
	"agent-runtime-lab/src/taskevidence"
)

const version = 3
const maxTasks = 128

type State string

const (
	Queued    State = "queued"
	Cancelled State = "cancelled"
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
	ID              string                 `json:"id"`
	WorkloadID      string                 `json:"workload_id"`
	SandboxID       string                 `json:"sandbox_id"`
	CommandHash     string                 `json:"command_sha256"`
	Timeout         int                    `json:"timeout_seconds"`
	State           State                  `json:"state"`
	StartedAt       time.Time              `json:"started_at"`
	FinishedAt      *time.Time             `json:"finished_at,omitempty"`
	Result          *m4exec.Result         `json:"result,omitempty"`
	ResultHash      string                 `json:"result_sha256,omitempty"`
	Note            string                 `json:"note,omitempty"`
	EvidenceIntent  *taskevidence.Intent   `json:"evidence_intent,omitempty"`
	Evidence        *taskevidence.Envelope `json:"evidence,omitempty"`
	Async           bool                   `json:"async,omitempty"`
	DispatchedAt    *time.Time             `json:"dispatched_at,omitempty"`
	CancelRequested bool                   `json:"cancel_requested,omitempty"`
	ArtifactsStored bool                   `json:"artifacts_stored,omitempty"`
	QueueSequence   uint64                 `json:"queue_sequence,omitempty"`
}

type checkpoint struct {
	Version int              `json:"version"`
	Tasks   map[string]Entry `json:"tasks"`
	// Only unclaimed jobs retain command bytes. The journal is private; APIs
	// never expose this map. Claim deletes the command before any remote call.
	Commands map[string]string `json:"queued_commands,omitempty"`
	NextJob  uint64            `json:"next_job_sequence,omitempty"`
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
	if j.Version != version || j.Tasks == nil || len(j.Tasks) > maxTasks || j.NextJob > maxTasks {
		return errors.New("invalid task journal header or task count")
	}
	sequences := make(map[uint64]bool)
	for id, e := range j.Tasks {
		if !validID(id) || e.ID != id || !validID(e.WorkloadID) || !validID(e.SandboxID) ||
			len(e.CommandHash) != 64 || e.Timeout < 1 || e.Timeout > 60 || e.StartedAt.IsZero() {
			return fmt.Errorf("invalid task identity: %q", id)
		}
		if e.EvidenceIntent != nil {
			if err := e.EvidenceIdentity().Validate(); err != nil {
				return fmt.Errorf("invalid task evidence intent: %q", id)
			}
		}
		if e.Evidence != nil {
			if e.EvidenceIntent == nil || e.Evidence.Validate(e.EvidenceIdentity()) != nil ||
				e.Result == nil || !reflect.DeepEqual(*e.Result, evidenceResult(*e.Evidence)) {
				return fmt.Errorf("invalid task completion manifest: %q", id)
			}
		}
		if e.Async && e.EvidenceIntent == nil || !e.Async && (e.DispatchedAt != nil || e.CancelRequested || e.ArtifactsStored) ||
			e.ArtifactsStored && e.Evidence == nil {
			return fmt.Errorf("invalid async task metadata: %q", id)
		}
		if e.Async && e.State != Queued && e.State != Cancelled && (e.DispatchedAt == nil || e.DispatchedAt.IsZero()) {
			return fmt.Errorf("async execution lacks durable dispatch time: %q", id)
		}
		if e.Async {
			if e.QueueSequence == 0 || e.QueueSequence > j.NextJob || sequences[e.QueueSequence] {
				return fmt.Errorf("invalid job queue sequence: %q", id)
			}
			sequences[e.QueueSequence] = true
		} else if e.QueueSequence != 0 {
			return fmt.Errorf("legacy task has job sequence: %q", id)
		}
		switch e.State {
		case Queued, Cancelled:
			if !e.Async || e.DispatchedAt != nil || e.Result != nil || e.Evidence != nil || e.ResultHash != "" || e.ArtifactsStored {
				return fmt.Errorf("invalid queued/cancelled job: %q", id)
			}
			if e.State == Queued && (e.FinishedAt != nil || e.CancelRequested || e.Note != "" || commandHash(j.Commands[id]) != e.CommandHash || j.Commands[id] == "" || len(j.Commands[id]) > 4000 || strings.IndexByte(j.Commands[id], 0) >= 0) ||
				e.State == Cancelled && (e.FinishedAt == nil || !e.CancelRequested) {
				return fmt.Errorf("invalid queued job command or cancellation: %q", id)
			}
		case Running:
			if e.FinishedAt != nil || e.Result != nil || e.ResultHash != "" || e.Note != "" || e.Evidence != nil {
				return fmt.Errorf("invalid running task: %q", id)
			}
		case Uncertain:
			if e.Result != nil || e.ResultHash != "" || e.Note == "" || e.Evidence != nil {
				return fmt.Errorf("invalid uncertain task: %q", id)
			}
		case Succeeded, Failed:
			if e.Async && !e.ArtifactsStored {
				return fmt.Errorf("completed async job lacks durable artifact receipt: %q", id)
			}
			if e.EvidenceIntent != nil && e.Evidence == nil {
				return fmt.Errorf("evidence task is missing its verified manifest: %q", id)
			}
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
	for id := range j.Commands {
		if entry, ok := j.Tasks[id]; !ok || entry.State != Queued || !entry.Async {
			return fmt.Errorf("orphan queued command: %q", id)
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
	s := &Store{path: abs, lock: lock, journal: checkpoint{Version: version, Tasks: make(map[string]Entry), Commands: make(map[string]string)}}
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
		changed := s.journal.Version == 1 || s.journal.Version == 2
		if changed {
			// Older daemons must refuse queued commands and artifact receipts.
			s.journal.Version = version
		}
		if s.journal.Commands == nil {
			s.journal.Commands = make(map[string]string)
		}
		if err := validate(s.journal); err != nil {
			s.Close()
			return nil, err
		}
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
	// A process restart alone is not proof that a previously unconfirmed
	// checkpoint rename reached disk. Sync its current name before dispatch.
	directory, err := os.Open(dir)
	if err != nil {
		s.Close()
		return nil, err
	}
	err = directory.Sync()
	directory.Close()
	if err != nil {
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
	return s.begin(id, workloadID, sandboxID, command, timeout, "", nil)
}

func (s *Store) BeginEvidence(id, workloadID, sandboxID, command string, timeout int, helperHash string, artifacts []string) (Entry, bool, error) {
	if !taskevidence.IsHash(helperHash) {
		return Entry{}, false, errors.New("valid evidence helper hash required")
	}
	return s.begin(id, workloadID, sandboxID, command, timeout, helperHash, artifacts)
}

func (s *Store) begin(id, workloadID, sandboxID, command string, timeout int, helperHash string, artifacts []string) (Entry, bool, error) {
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
	paths, err := taskevidence.NormalizeArtifacts(artifacts)
	if err != nil || helperHash == "" && len(paths) != 0 {
		return Entry{}, false, errors.New("invalid evidence artifact request")
	}
	if existing, ok := s.journal.Tasks[id]; ok {
		if existing.Async {
			return Entry{}, false, ErrConflict
		}
		if existing.WorkloadID != workloadID || existing.SandboxID != sandboxID || existing.CommandHash != hash || existing.Timeout != timeout {
			return Entry{}, false, ErrConflict
		}
		if (existing.EvidenceIntent != nil) != (helperHash != "") ||
			existing.EvidenceIntent != nil && !reflect.DeepEqual(existing.EvidenceIntent.Artifacts, paths) {
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
	if helperHash != "" {
		intent, err := taskevidence.NewIntent(helperHash, paths)
		if err != nil {
			return Entry{}, false, err
		}
		e.EvidenceIntent = &intent
	}
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
	if e.EvidenceIntent != nil && result != nil {
		return Entry{}, errors.New("evidence tasks require a verified completion manifest")
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
	if e.DispatchedAt != nil {
		v := *e.DispatchedAt
		e.DispatchedAt = &v
	}
	if e.Result != nil {
		v := *e.Result
		e.Result = &v
	}
	if e.FinishedAt != nil {
		v := *e.FinishedAt
		e.FinishedAt = &v
	}
	if e.EvidenceIntent != nil {
		v := *e.EvidenceIntent
		v.Artifacts = append([]string{}, v.Artifacts...)
		e.EvidenceIntent = &v
	}
	if e.Evidence != nil {
		v := *e.Evidence
		v.Manifest.Artifacts = append([]taskevidence.Artifact{}, v.Manifest.Artifacts...)
		v.Manifest.Identity.Intent.Artifacts = append([]string{}, v.Manifest.Identity.Intent.Artifacts...)
		e.Evidence = &v
	}
	return e
}

func (e Entry) EvidenceIdentity() taskevidence.Identity {
	identity := taskevidence.Identity{TaskID: e.ID, WorkloadID: e.WorkloadID, SandboxID: e.SandboxID,
		CommandSHA256: e.CommandHash, Timeout: e.Timeout}
	if e.EvidenceIntent != nil {
		identity.Intent = *e.EvidenceIntent
		identity.Intent.Artifacts = append([]string{}, e.EvidenceIntent.Artifacts...)
	}
	return identity
}

func evidenceResult(envelope taskevidence.Envelope) m4exec.Result {
	m := envelope.Manifest
	return m4exec.Result{SandboxID: m.Identity.SandboxID, ExitCode: m.ExitCode,
		Stdout: m.Stdout, Stderr: m.Stderr, Truncated: m.Truncated}
}

// FinishEvidence records independently read VM completion evidence. Recovery
// may resolve an UNCERTAIN record; it never starts the original command.
// The API must also verify current fleet ownership and every artifact's bytes.
func (s *Store) FinishEvidence(id string, envelope taskevidence.Envelope, recovering bool) (Entry, error) {
	return s.finishEvidence(id, envelope, recovering, false)
}

// FinishJobEvidence requires the caller to have committed a verified artifact
// bundle durably before publishing async completion.
func (s *Store) FinishJobEvidence(id string, envelope taskevidence.Envelope, recovering bool) (Entry, error) {
	return s.finishEvidence(id, envelope, recovering, true)
}

func (s *Store) finishEvidence(id string, envelope taskevidence.Envelope, recovering, durableArtifacts bool) (Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poisoned || s.lock == nil {
		return Entry{}, ErrPoisoned
	}
	e, ok := s.journal.Tasks[id]
	if !ok || e.Async != durableArtifacts || e.EvidenceIntent == nil || envelope.Validate(e.EvidenceIdentity()) != nil {
		return Entry{}, ErrConflict
	}
	if e.State == Succeeded || e.State == Failed {
		if e.Evidence == nil || e.Evidence.SHA256 != envelope.SHA256 {
			return Entry{}, ErrConflict
		}
		return clone(e), nil
	}
	if recovering && e.State != Uncertain || !recovering && e.State != Running {
		return Entry{}, ErrConflict
	}
	// Clone the caller-owned slices before keeping them in the journal.
	copyEntry := clone(Entry{Evidence: &envelope})
	e.Evidence = copyEntry.Evidence
	e.ArtifactsStored = durableArtifacts
	result := evidenceResult(envelope)
	e.Result, e.ResultHash = &result, resultHash(e, result)
	now := time.Now().UTC()
	e.FinishedAt = &now
	e.Note = ""
	if recovering {
		e.Note = "recovered from verified VM completion evidence; command was not rerun"
		if durableArtifacts {
			e.Note = "recovered from verified durable completion evidence; command was not rerun"
		}
	}
	e.State = Succeeded
	if result.ExitCode != 0 {
		e.State = Failed
	}
	s.journal.Tasks[id] = e
	if err := s.save(); err != nil {
		return Entry{}, err
	}
	return clone(e), nil
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
