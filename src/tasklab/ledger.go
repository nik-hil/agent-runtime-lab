// Package tasklab contains the deliberately small, test-only task protocol.
package tasklab

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"syscall"
)

const Artifact = "sum(1..10)=55\n"
const PartialArtifact = "sum(1..5)=15\n"
const ArtifactPath = "/tmp/task-artifact.txt"
const ReleasePath = "/tmp/task-release"
const ChildExitPath = "/tmp/task-child-exit"

var identifier = regexp.MustCompile(`^[a-zA-Z0-9-]{1,80}$`)
var ErrConflict = errors.New("task ID or idempotency key reused with different operation")

type Operation struct {
	TaskID  string `json:"task_id"`
	Key     string `json:"idempotency_key"`
	Payload string `json:"payload"`
}

func (o Operation) Validate() error {
	if !identifier.MatchString(o.TaskID) || !identifier.MatchString(o.Key) || o.Payload != Artifact {
		return errors.New("invalid bounded lab operation")
	}
	return nil
}

type Record struct {
	Operation
	ResourceID string `json:"resource_id"`
}

type Receipt struct {
	Record Record `json:"record"`
	Reused bool   `json:"reused"`
}

type Event struct {
	TaskID        string   `json:"task_id"`
	Attempt       int      `json:"attempt"`
	Phase         string   `json:"phase"`
	Receipt       *Receipt `json:"receipt,omitempty"`
	ChildPID      int      `json:"child_pid,omitempty"`
	ChildExitCode *int     `json:"child_exit_code,omitempty"`
}

type ArtifactObservation struct {
	Exists        bool   `json:"exists"`
	Contents      string `json:"contents"`
	ChildExitCode *int   `json:"child_exit_code_before_read,omitempty"`
}

// Ledger uses a separate flock inode so renaming the data file never changes the
// synchronization point. Every reader, including the independent verifier, locks.
// Only the trusted receiver/verifier can access this private Docker volume.
type Ledger struct{ Directory string }

func (l Ledger) locked(fn func([]Record) error) error {
	lock, err := os.OpenFile(filepath.Join(l.Directory, "ledger.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	data, err := os.ReadFile(filepath.Join(l.Directory, "ledger.json"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	records := []Record{}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &records); err != nil {
			return fmt.Errorf("corrupt ledger: %w", err)
		}
	} else if err == nil {
		return errors.New("empty ledger file")
	}
	seenTasks, seenKeys := map[string]bool{}, map[string]bool{}
	for i, r := range records {
		if r.Validate() != nil || seenTasks[r.TaskID] || seenKeys[r.Key] || r.ResourceID != fmt.Sprintf("resource-%04d", i+1) {
			return errors.New("invalid ledger records")
		}
		seenTasks[r.TaskID], seenKeys[r.Key] = true, true
	}
	if len(records) > 128 {
		return errors.New("ledger bound exceeded")
	}
	return fn(records)
}

func (l Ledger) Snapshot() (records []Record, err error) {
	err = l.locked(func(r []Record) error { records = append([]Record{}, r...); return nil })
	return
}

func (l Ledger) Commit(op Operation) (receipt Receipt, err error) {
	if err = op.Validate(); err != nil {
		return
	}
	err = l.locked(func(records []Record) error {
		for _, r := range records {
			if r.Key == op.Key || r.TaskID == op.TaskID {
				if r.Operation != op {
					return ErrConflict
				}
				receipt = Receipt{Record: r, Reused: true}
				// Also complete a directory sync if a previous writer renamed
				// successfully but failed before acknowledging its directory sync.
				return l.syncDirectory()
			}
		}
		if len(records) >= 128 {
			return errors.New("ledger full")
		}
		r := Record{Operation: op, ResourceID: fmt.Sprintf("resource-%04d", len(records)+1)}
		data, err := json.MarshalIndent(append(records, r), "", "  ")
		if err != nil {
			return err
		}
		file, err := os.CreateTemp(l.Directory, ".pending-")
		if err != nil {
			return err
		}
		defer os.Remove(file.Name())
		_, writeErr := file.Write(append(data, '\n'))
		if writeErr == nil {
			writeErr = file.Sync()
		}
		closeErr := file.Close()
		if err := errors.Join(writeErr, closeErr); err != nil {
			return err
		}
		if err := os.Rename(file.Name(), filepath.Join(l.Directory, "ledger.json")); err != nil {
			return err
		}
		if err := l.syncDirectory(); err != nil {
			return err
		}
		receipt = Receipt{Record: r}
		return nil
	})
	return
}

func (l Ledger) syncDirectory() error {
	dir, err := os.Open(l.Directory)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
