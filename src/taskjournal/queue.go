package taskjournal

import (
	"errors"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	"agent-runtime-lab/src/taskevidence"
)

// Enqueue persists command bytes and an immutable VM/evidence binding. It never
// executes work. The bounded private journal is the queue's source of truth.
func (s *Store) Enqueue(id, workloadID, sandboxID, command string, timeout int, helperHash string, artifacts []string) (Entry, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poisoned || s.lock == nil {
		return Entry{}, false, ErrPoisoned
	}
	if !validID(id) || !validID(workloadID) || !validID(sandboxID) || command == "" || len(command) > 4000 || !utf8.ValidString(command) || strings.IndexByte(command, 0) >= 0 || timeout < 1 || timeout > 60 || !taskevidence.IsHash(helperHash) {
		return Entry{}, false, errors.New("invalid bounded job request")
	}
	paths, err := taskevidence.NormalizeArtifacts(artifacts)
	if err != nil {
		return Entry{}, false, err
	}
	if old, ok := s.journal.Tasks[id]; ok {
		if !old.Async || old.WorkloadID != workloadID || old.SandboxID != sandboxID || old.CommandHash != commandHash(command) || old.Timeout != timeout || old.EvidenceIntent == nil || !reflect.DeepEqual(old.EvidenceIntent.Artifacts, paths) {
			return Entry{}, false, ErrConflict
		}
		return clone(old), false, nil
	}
	if len(s.journal.Tasks) >= maxTasks {
		return Entry{}, false, ErrFull
	}
	intent, err := taskevidence.NewIntent(helperHash, paths)
	if err != nil {
		return Entry{}, false, err
	}
	e := Entry{ID: id, WorkloadID: workloadID, SandboxID: sandboxID, CommandHash: commandHash(command), Timeout: timeout,
		State: Queued, StartedAt: time.Now().UTC(), Async: true, EvidenceIntent: &intent}
	s.journal.NextJob++
	e.QueueSequence = s.journal.NextJob
	s.journal.Tasks[id], s.journal.Commands[id] = e, command
	if err := s.save(); err != nil {
		return Entry{}, false, err
	}
	return clone(e), true, nil
}

// ClaimNext atomically fences a VM and consumes its next eligible queued
// command before execution. Running/uncertain synchronous tasks share the
// same fence. Helper changes hold queued jobs until the matching helper returns.
func (s *Store) ClaimNext(helperHash string) (Entry, string, bool, error) {
	return s.ClaimNextEligible(helperHash, nil)
}

func (s *Store) ClaimNextEligible(helperHash string, eligible map[string]bool) (Entry, string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poisoned || s.lock == nil {
		return Entry{}, "", false, ErrPoisoned
	}
	busy := make(map[string]bool)
	for _, e := range s.journal.Tasks {
		if e.State == Running || e.State == Uncertain {
			busy[e.SandboxID] = true
		}
	}
	var selected Entry
	for _, e := range s.journal.Tasks {
		if e.State != Queued || busy[e.SandboxID] || e.EvidenceIntent.HelperSHA256 != helperHash || eligible != nil && !eligible[e.ID] {
			continue
		}
		if selected.ID == "" || e.QueueSequence < selected.QueueSequence {
			selected = e
		}
	}
	if selected.ID == "" {
		return Entry{}, "", false, nil
	}
	command := s.journal.Commands[selected.ID]
	delete(s.journal.Commands, selected.ID)
	now := time.Now().UTC()
	selected.State, selected.DispatchedAt = Running, &now
	s.journal.Tasks[selected.ID] = selected
	if err := s.save(); err != nil {
		return Entry{}, "", false, err
	}
	return clone(selected), command, true, nil
}

// Cancel only claims cancellation for a job that has never been dispatched.
// A running job records a request; its executor must report the observed or
// uncertain outcome. Cancellation does not prove remote rollback.
func (s *Store) Cancel(id string) (Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poisoned || s.lock == nil {
		return Entry{}, ErrPoisoned
	}
	e, ok := s.journal.Tasks[id]
	if !ok || !e.Async {
		return Entry{}, ErrNotFound
	}
	if e.State != Queued && e.State != Running {
		return clone(e), nil
	}
	if e.CancelRequested {
		return clone(e), nil
	}
	e.CancelRequested = true
	if e.State == Queued {
		now := time.Now().UTC()
		e.State, e.FinishedAt, e.Note = Cancelled, &now, "cancelled before dispatch; command was not executed"
		delete(s.journal.Commands, id)
	}
	s.journal.Tasks[id] = e
	if err := s.save(); err != nil {
		return Entry{}, err
	}
	return clone(e), nil
}
