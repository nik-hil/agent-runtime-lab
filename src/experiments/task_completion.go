package experiments

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"agent-runtime-lab/src/sandbox"
	"agent-runtime-lab/src/tasklab"
)

type TaskAttempt struct {
	TaskID                  string                       `json:"task_id"`
	Attempt                 int                          `json:"attempt"`
	Key                     string                       `json:"idempotency_key"`
	Purpose                 string                       `json:"purpose"`
	RunIndex                int                          `json:"run_index"`
	ProcessStarted          bool                         `json:"process_started"`
	Ready                   bool                         `json:"ready"`
	ExitCode                *int                         `json:"exit_code"`
	ChildReportedCompletion bool                         `json:"child_reported_completion"`
	WorkCompleted           bool                         `json:"work_completed"`
	PartialArtifactCreated  bool                         `json:"partial_artifact_created"`
	Artifact                *tasklab.ArtifactObservation `json:"independent_artifact_read,omitempty"`
	ExternalCommitExists    *bool                        `json:"external_commit_exists"`
	Retried                 bool                         `json:"retry_occurred"`
	DuplicateSideEffects    bool                         `json:"duplicate_side_effects"`
	VerifiedState           string                       `json:"verified_task_state"`
	Events                  []tasklab.Event              `json:"events"`
}

type LedgerCheck struct {
	Stage   string           `json:"stage"`
	Records []tasklab.Record `json:"records"`
}

type TaskInvestigation struct {
	Attempts                   []TaskAttempt      `json:"attempts"`
	LedgerChecks               []LedgerCheck      `json:"independent_ledger_checks,omitempty"`
	Receivers                  []ReceiverEvidence `json:"receivers,omitempty"`
	RetryOccurred              bool               `json:"retry_occurred"`
	DuplicateSideEffects       bool               `json:"duplicate_side_effects"`
	FreshSandboxArtifactAbsent bool               `json:"fresh_sandbox_artifact_absent,omitempty"`
	CleanupConfirmed           bool               `json:"cleanup_confirmed"`
	FinalState                 string             `json:"final_verified_task_state"`
}

type taskExecution struct {
	result sandbox.Result
	err    error
}

func taskAttempt(r sandbox.Result, id, key, purpose string, attempt, index int) (TaskAttempt, error) {
	a := TaskAttempt{TaskID: id, Key: key, Attempt: attempt, RunIndex: index, Purpose: purpose, ExitCode: r.ExitCode, ProcessStarted: r.ExitCode != nil, Retried: attempt > 1, VerifiedState: "incomplete", Events: []tasklab.Event{}}
	decoder := json.NewDecoder(strings.NewReader(r.Stdout))
	for {
		var e tasklab.Event
		if err := decoder.Decode(&e); err != nil {
			if err == io.EOF {
				break
			}
			return a, fmt.Errorf("task events: %w", err)
		}
		if e.TaskID != id || e.Attempt != attempt || len(a.Events) >= 8 {
			return a, errors.New("task event identity or bound mismatch")
		}
		a.Events = append(a.Events, e)
		if e.Phase == "ready" {
			a.Ready = true
		}
		if e.Phase == "work_completed" {
			a.ChildReportedCompletion = true
		}
	}
	if !a.ProcessStarted || !a.Ready || r.StdoutDropped != 0 || r.StderrDropped != 0 || r.TimedOut || r.Cancelled {
		return a, errors.New("task did not start/become ready cleanly, or evidence was truncated")
	}
	return a, nil
}

// A zero exit or a self-reported completion cannot substitute for these bytes.
func artifactCompleted(r sandbox.Result, a TaskAttempt) bool {
	return success(r) && a.Ready && a.ChildReportedCompletion && a.Artifact != nil && a.Artifact.Exists && a.Artifact.Contents == tasklab.Artifact
}

func runObservedArtifact(ctx context.Context, backend string, cfg sandbox.Config) (result sandbox.Result, artifact *tasklab.ArtifactObservation, err error) {
	s, err := sandbox.New(backend)
	if err != nil {
		return result, nil, err
	}
	defer func() { err = errors.Join(err, destroy(s)) }()
	if err = s.Create(ctx, cfg); err != nil {
		return result, nil, err
	}
	execCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan taskExecution, 1)
	go func() { r, e := s.Exec(execCtx); done <- taskExecution{r, e} }()
	reader := s.(interface {
		InspectTaskArtifact(context.Context) ([]byte, error)
	})
	deadline := time.Now().Add(5 * time.Second)
	var last error
	for time.Now().Before(deadline) && ctx.Err() == nil {
		select {
		case execution := <-done:
			return execution.result, nil, errors.Join(execution.err, errors.New("task exited before independent artifact read"), last)
		default:
		}
		var data []byte
		data, last = reader.InspectTaskArtifact(ctx)
		if last == nil {
			artifact = &tasklab.ArtifactObservation{}
			last = json.Unmarshal(data, artifact)
			break
		}
		time.Sleep(40 * time.Millisecond)
	}
	if artifact == nil || last != nil {
		cancel()
		err = errors.Join(errors.New("independent artifact read failed"), last, ctx.Err())
	}
	execution := <-done
	return execution.result, artifact, errors.Join(err, execution.err)
}

func runTaskExperiment(ctx context.Context, backend, image, name string) (ob Observation) {
	ob = Observation{Backend: backend, Experiment: name, Status: "failed", Task: &TaskInvestigation{Attempts: []TaskAttempt{}, FinalState: "not-verified"}}
	cfg := sandbox.DefaultConfig()
	cfg.Image = image
	cfg.Timeout = 10 * time.Second
	var failure error
	fixtureClean := true
	check := func(ok bool, message string) {
		if !ok {
			failure = errors.Join(failure, errors.New(message))
		}
	}
	add := func(r sandbox.Result, runErr error, id, key, purpose string, attempt int) int {
		index := len(ob.Runs)
		ob.Runs = append(ob.Runs, r)
		a, err := taskAttempt(r, id, key, purpose, attempt, index)
		failure = errors.Join(failure, runErr, err)
		ob.Task.Attempts = append(ob.Task.Attempts, a)
		return len(ob.Task.Attempts) - 1
	}
	run := func(config sandbox.Config) (sandbox.Result, error) {
		s, err := sandbox.New(backend)
		if err != nil {
			return sandbox.Result{}, err
		}
		return sandbox.Run(ctx, s, config)
	}
	// Every cleanup check also runs after an assertion or infrastructure failure.
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		clean := true
		for _, r := range ob.Runs {
			if r.Container == "" {
				clean = false
				continue
			}
			absent, err := Absent(cleanupCtx, r.Container)
			failure = errors.Join(failure, err)
			clean = clean && err == nil && absent
		}
		check(clean, "task workload cleanup not confirmed")
		ob.Task.CleanupConfirmed = clean && fixtureClean
		if failure != nil {
			ob.Details = failure.Error()
			ob.Status = "failed"
		} else {
			ob.Status = "passed"
		}
	}()
	if name != "task-retry" {
		mode := "success"
		ob.Expected = "A ready child exits 0 AND a separate reader verifies the exact deterministic artifact."
		if name == "task-partial" {
			mode = "partial"
			ob.Expected = "A ready child writes independently observed partial work, exits 42, and its file is absent in a fresh sandbox after destruction."
		}
		id, key := "task-"+mode+"-"+backend, "key-"+mode+"-"+backend
		cfg.Command = []string{"/lab-probe", "task", mode, id, "1", key, "unused"}
		r, artifact, err := runObservedArtifact(ctx, backend, cfg)
		i := add(r, err, id, key, "artifact-"+mode, 1)
		a := &ob.Task.Attempts[i]
		noExternalReceiver := false
		a.ExternalCommitExists = &noExternalReceiver
		a.Artifact = artifact
		a.WorkCompleted = artifactCompleted(r, *a)
		if mode == "success" {
			check(a.WorkCompleted, "successful task missing expected artifact, readiness, completion marker or zero exit")
			if a.WorkCompleted {
				a.VerifiedState = "completed-artifact-verified"
			} else {
				a.VerifiedState = "artifact-verification-failed"
			}
		} else {
			a.PartialArtifactCreated = artifact != nil && artifact.Exists && artifact.Contents == tasklab.PartialArtifact && artifact.ChildExitCode != nil && *artifact.ChildExitCode == 42
			check(a.PartialArtifactCreated && r.ExitCode != nil && *r.ExitCode == 42 && !a.ChildReportedCompletion, "partial task did not fail after verified intermediate work")
			if a.PartialArtifactCreated {
				a.VerifiedState = "failed-partial-work-observed"
			} else {
				a.VerifiedState = "partial-work-not-verified"
			}
		}
		cfg.Command = []string{"/lab-probe", "task-clean"}
		fresh, err := run(cfg)
		ob.Runs = append(ob.Runs, fresh)
		failure = errors.Join(failure, err)
		var absent tasklab.ArtifactObservation
		parseErr := json.Unmarshal([]byte(fresh.Stdout), &absent)
		ob.Task.FreshSandboxArtifactAbsent = parseErr == nil && success(fresh) && !absent.Exists && absent.Contents == ""
		check(ob.Task.FreshSandboxArtifactAbsent, "artifact unexpectedly present in fresh sandbox or check failed")
		ob.Task.FinalState = a.VerifiedState + "; ephemeral-file-absent-after-destruction"
		return
	}
	ob.Expected = "After durable commit and lost acknowledgement, a fresh sandbox retries against a restarted receiver without duplicates; collision, concurrency and pre-commit recovery checks pass."
	f, err := newTaskReceiver(ctx, image)
	if f != nil {
		defer func() {
			closeErr := f.close()
			fixtureClean = closeErr == nil
			failure = errors.Join(failure, closeErr)
			ob.Task.Receivers = f.Evidence
		}()
	}
	if err != nil {
		failure = errors.Join(failure, err)
		return
	}
	if err = f.start(ctx, "lose-ack"); err != nil {
		failure = errors.Join(failure, err)
		return
	}
	cfg.Network, cfg.LabNetwork = sandbox.NetworkLabInternal, f.network
	id, key := "task-commit-"+backend, "key-commit-"+backend
	config := func(mode, task, key string, attempt int) sandbox.Config {
		c := cfg
		c.Command = []string{"/lab-probe", "task", mode, task, strconv.Itoa(attempt), key, f.endpoint}
		return c
	}
	perform := func(mode, task, key, purpose string, attempt int) int {
		r, e := run(config(mode, task, key, attempt))
		return add(r, e, task, key, purpose, attempt)
	}
	snapshot := func(stage string, want int) ([]tasklab.Record, bool) {
		records, e := f.snapshot(ctx)
		failure = errors.Join(failure, e)
		ob.Task.LedgerChecks = append(ob.Task.LedgerChecks, LedgerCheck{Stage: stage, Records: records})
		check(len(records) == want, "unexpected ledger record count at "+stage)
		seen := map[string]bool{}
		for _, record := range records {
			if seen[record.TaskID] || record.Payload != tasklab.Artifact {
				ob.Task.DuplicateSideEffects = true
			}
			seen[record.TaskID] = true
		}
		check(!ob.Task.DuplicateSideEffects, "duplicate or invalid committed side effect")
		return records, e == nil && len(records) == want
	}
	verify := func(index int, records []tasklab.Record, expectCommit bool) {
		a := &ob.Task.Attempts[index]
		if records == nil {
			a.VerifiedState = "commit-state-unavailable"
			check(false, "cannot verify unavailable ledger for "+a.Purpose)
			return
		}
		var match *tasklab.Record
		count := 0
		for i := range records {
			r := &records[i]
			if r.TaskID == a.TaskID && r.Key == a.Key && r.Payload == tasklab.Artifact {
				match = r
				count++
			}
		}
		exists := count > 0
		a.ExternalCommitExists = &exists
		a.DuplicateSideEffects = count > 1
		check(exists == expectCommit && !a.DuplicateSideEffects, "unexpected committed state for "+a.Purpose)
		var receipt *tasklab.Receipt
		for _, event := range a.Events {
			if event.Phase == "work_completed" {
				receipt = event.Receipt
			}
		}
		a.WorkCompleted = success(ob.Runs[a.RunIndex]) && a.ChildReportedCompletion && match != nil && receipt != nil && receipt.Record == *match
		if a.WorkCompleted {
			a.VerifiedState = "completed-commit-verified"
		} else if exists {
			a.VerifiedState = "committed-without-acknowledgement"
		} else {
			a.VerifiedState = "failed-no-commit"
		}
	}
	first := perform("commit", id, key, "acknowledgement-loss", 1)
	check(ob.Task.Attempts[first].ExitCode != nil && *ob.Task.Attempts[first].ExitCode == 45 && !ob.Task.Attempts[first].ChildReportedCompletion, "task did not report lost acknowledgement")
	// The first task's Run has already destroyed its sandbox. The receiver process
	// has deliberately exited 43 AFTER syncing its ledger, without sending a reply.
	if err = f.restartAfterCrash(ctx); err != nil {
		failure = errors.Join(failure, err)
		return
	}
	records, ok := snapshot("receiver-restarted-before-task-retry", 1)
	verify(first, records, true)
	if !ok {
		return
	}
	retry := perform("commit", id, key, "same-key-fresh-sandbox-retry", 2)
	ob.Task.RetryOccurred = true
	records, _ = snapshot("same-key-retry", 1)
	verify(retry, records, true)
	check(ob.Task.Attempts[retry].WorkCompleted && reusedReceipt(ob.Task.Attempts[retry]), "retry did not reuse the durable operation")
	// Two fresh sandboxes race with the same operation. Collect results in stable
	// order without sharing mutable result slices between goroutines.
	channels := []chan taskExecution{make(chan taskExecution, 1), make(chan taskExecution, 1)}
	for i := range channels {
		c := config("commit", id, key, i+3)
		go func(ch chan taskExecution, c sandbox.Config) { r, e := run(c); ch <- taskExecution{r, e} }(channels[i], c)
	}
	indices := []int{}
	for i, ch := range channels {
		execution := <-ch
		indices = append(indices, add(execution.result, execution.err, id, key, "concurrent-same-key-retry", i+3))
	}
	records, _ = snapshot("concurrent-duplicate-attempts", 1)
	for _, index := range indices {
		verify(index, records, true)
		check(ob.Task.Attempts[index].WorkCompleted && reusedReceipt(ob.Task.Attempts[index]), "concurrent retry created a duplicate or failed")
	}
	collision := perform("commit", "task-collision-"+backend, key, "different-task-same-key-conflict", 1)
	records, _ = snapshot("key-collision-rejected", 1)
	verify(collision, records, false)
	check(ob.Task.Attempts[collision].ExitCode != nil && *ob.Task.Attempts[collision].ExitCode == 46, "different task improperly reused another task's key")
	other := perform("commit", "task-independent-"+backend, "key-independent-"+backend, "different-task-different-key", 1)
	records, _ = snapshot("independent-task-committed", 2)
	verify(other, records, true)
	check(ob.Task.Attempts[other].WorkCompleted && !reusedReceipt(ob.Task.Attempts[other]), "independent task was incorrectly deduplicated")
	beforeID, beforeKey := "task-before-"+backend, "key-before-"+backend
	before := perform("before-commit", beforeID, beforeKey, "crash-before-commit", 1)
	records, _ = snapshot("crash-before-commit", 2)
	verify(before, records, false)
	check(ob.Task.Attempts[before].ExitCode != nil && *ob.Task.Attempts[before].ExitCode == 44, "pre-commit failure injection did not fire")
	recovered := perform("commit", beforeID, beforeKey, "retry-after-pre-commit-crash", 2)
	records, _ = snapshot("final", 3)
	verify(recovered, records, true)
	check(ob.Task.Attempts[recovered].WorkCompleted && !reusedReceipt(ob.Task.Attempts[recovered]), "pre-commit retry did not create exactly one new operation")
	if failure == nil {
		ob.Task.FinalState = "three-distinct-operations-verified; retries-reused-existing-commit; no-duplicates"
	} else {
		ob.Task.FinalState = "verification-failed-inspect-evidence"
	}
	return
}

func reusedReceipt(a TaskAttempt) bool {
	for _, e := range a.Events {
		if e.Receipt != nil && e.Phase == "work_completed" {
			return e.Receipt.Reused
		}
	}
	return false
}
