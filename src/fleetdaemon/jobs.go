package fleetdaemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/artifactstore"
	"agent-runtime-lab/src/fleet"
	"agent-runtime-lab/src/taskevidence"
	"agent-runtime-lab/src/taskjournal"
)

type ArtifactReader interface {
	ReadArtifact(context.Context, taskevidence.Identity, taskevidence.Artifact) ([]byte, error)
}

type JobRequest struct {
	ID         string   `json:"job_id"`
	WorkloadID string   `json:"workload_id"`
	Command    string   `json:"command"`
	Timeout    int      `json:"timeout_seconds"`
	Artifacts  []string `json:"artifacts"`
}

type JobOptions struct {
	Workers int
	Logger  *slog.Logger
	// Test-only failure injection after fsynced artifacts, before host result.
	AfterStored func(string)
}

type Jobs struct {
	controller    *fleet.Controller
	tasks         *taskjournal.Store
	artifacts     *artifactstore.Store
	executor      EvidenceExecutor
	reader        ArtifactReader
	opts          JobOptions
	admissionGate sync.Mutex
	mu            sync.Mutex
	active        map[string]context.CancelFunc
	recovering    map[string]bool
}

func NewJobs(c *fleet.Controller, tasks *taskjournal.Store, artifacts *artifactstore.Store, executor EvidenceExecutor, reader ArtifactReader, opts JobOptions) (*Jobs, error) {
	if c == nil || tasks == nil || artifacts == nil || executor == nil || reader == nil || opts.Workers < 0 || opts.Workers > 8 {
		return nil, errors.New("jobs require controller, journals, evidence executor/reader and worker count 0..8")
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	entries, err := tasks.List()
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if entry.Async && entry.ArtifactsStored {
			evidence, err := artifacts.Verify(entry.EvidenceIdentity())
			if err != nil || entry.Evidence == nil || evidence.SHA256 != entry.Evidence.SHA256 {
				return nil, fmt.Errorf("completed job %s lacks its verified artifact bundle; restore the configured artifact directory", entry.ID)
			}
		}
	}
	return &Jobs{controller: c, tasks: tasks, artifacts: artifacts, executor: executor, reader: reader,
		opts: opts, active: make(map[string]context.CancelFunc), recovering: make(map[string]bool)}, nil
}

func (j *Jobs) owned(entry taskjournal.Entry) bool {
	state := j.controller.Status()
	vm, ok := state.Jobs[entry.WorkloadID]
	return ok && !state.Unsafe && !j.controller.Draining() && vm.Entry.State == admission.Running &&
		vm.Workload.Desired == "running" && !vm.Workload.CreatePending && vm.Workload.SandboxID == entry.SandboxID
}

func (j *Jobs) Cancel(id string) (taskjournal.Entry, error) {
	entry, err := j.tasks.Cancel(id)
	if err != nil {
		return entry, err
	}
	j.mu.Lock()
	if cancel := j.active[id]; cancel != nil {
		cancel()
	}
	j.mu.Unlock()
	return entry, nil
}

// Run workers are independent of HTTP request contexts: disconnecting a
// submitting client does not cancel accepted work. Shutdown cancels execution
// contexts and persists unknown outcomes before the journal can be closed.
func (j *Jobs) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if j.opts.Workers == 0 {
		<-ctx.Done()
		return nil
	}
	errorsOut := make(chan error, j.opts.Workers)
	for i := 0; i < j.opts.Workers; i++ {
		go func() { errorsOut <- j.worker(ctx) }()
	}
	var first error
	for i := 0; i < j.opts.Workers; i++ {
		if err := <-errorsOut; err != nil && first == nil {
			first = err
			cancel()
		}
	}
	return first
}

func (j *Jobs) eligible() (map[string]bool, error) {
	if j.artifacts.Stats().Uncertain {
		return nil, artifactstore.ErrPoisoned
	}
	entries, err := j.tasks.List()
	if err != nil {
		return nil, err
	}
	state, draining := j.controller.Status(), j.controller.Draining()
	allowed := make(map[string]bool)
	for _, e := range entries {
		if !e.Async {
			continue
		}
		vm, exists := state.Jobs[e.WorkloadID]
		if e.State == taskjournal.Queued && (draining || !exists || vm.Entry.State == admission.Deleted || vm.Workload.SandboxID != e.SandboxID) ||
			e.State == taskjournal.Running && draining {
			if _, err := j.Cancel(e.ID); err != nil {
				return nil, err
			}
			continue
		}
		allowed[e.ID] = !state.Unsafe && !draining && exists && vm.Entry.State == admission.Running &&
			vm.Workload.Desired == "running" && !vm.Workload.CreatePending && vm.Workload.SandboxID == e.SandboxID
	}
	return allowed, nil
}

func (j *Jobs) worker(ctx context.Context) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return nil
		}
		allowed, err := j.eligible()
		if err != nil {
			return err
		}
		entry, command, run, err := j.tasks.ClaimNextEligible(j.executor.EvidenceHelperHash(), allowed)
		if err != nil {
			return err
		}
		if run {
			if err := j.execute(ctx, entry, command); err != nil {
				return err
			}
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (j *Jobs) uncertain(id, note string) error {
	_, err := j.tasks.Finish(id, nil, note)
	return err
}

func (j *Jobs) execute(parent context.Context, entry taskjournal.Entry, command string) error {
	ctx, cancel := context.WithCancel(parent)
	j.mu.Lock()
	j.active[entry.ID] = cancel
	j.mu.Unlock()
	defer func() { cancel(); j.mu.Lock(); delete(j.active, entry.ID); j.mu.Unlock() }()
	current, err := j.tasks.Get(entry.ID)
	if err != nil {
		return err
	}
	if current.CancelRequested || ctx.Err() != nil || !j.owned(entry) {
		return j.uncertain(entry.ID, "dispatch interrupted or workload changed; command was not sent; no automatic retry")
	}
	runCtx, stopRun := context.WithTimeout(ctx, time.Duration(entry.Timeout)*time.Second)
	evidence, err := j.executor.RunEvidence(runCtx, entry.EvidenceIdentity(), command)
	runErr := runCtx.Err()
	stopRun()
	if err != nil || runErr != nil || ctx.Err() != nil || evidence.Validate(entry.EvidenceIdentity()) != nil || !j.owned(entry) {
		return j.uncertain(entry.ID, "job execution interrupted or completion unconfirmed; recover evidence; never auto-retry")
	}
	copyCtx, stopCopy := context.WithTimeout(ctx, 30*time.Second)
	err = j.capture(copyCtx, entry, evidence)
	stopCopy()
	if err != nil || ctx.Err() != nil || !j.owned(entry) {
		j.opts.Logger.Warn("job_artifact_capture_unconfirmed", "job_id", entry.ID)
		return j.uncertain(entry.ID, "completion or durable artifact copy unconfirmed; inspect storage and recover; never auto-retry")
	}
	if j.opts.AfterStored != nil {
		j.opts.AfterStored(entry.ID)
	}
	_, err = j.tasks.FinishJobEvidence(entry.ID, evidence, false)
	return err
}

func (j *Jobs) capture(ctx context.Context, entry taskjournal.Entry, evidence taskevidence.Envelope) error {
	return j.artifacts.Put(ctx, entry.EvidenceIdentity(), evidence, func(ctx context.Context, a taskevidence.Artifact) ([]byte, error) {
		return j.reader.ReadArtifact(ctx, entry.EvidenceIdentity(), a)
	})
}

// Recover first verifies the durable local bundle, so VM deletion need not
// destroy recoverability. Only a missing local bundle permits inspection of
// the original owned VM; corrupt local data fails closed. It never executes.
func (j *Jobs) Recover(ctx context.Context, id string) (taskjournal.Entry, error) {
	j.mu.Lock()
	if j.recovering[id] || j.active[id] != nil {
		j.mu.Unlock()
		return taskjournal.Entry{}, taskjournal.ErrBusy
	}
	j.recovering[id] = true
	j.mu.Unlock()
	defer func() { j.mu.Lock(); delete(j.recovering, id); j.mu.Unlock() }()
	entry, err := j.tasks.Get(id)
	if err != nil || !entry.Async {
		if err == nil {
			err = taskjournal.ErrNotFound
		}
		return entry, err
	}
	if entry.State == taskjournal.Succeeded || entry.State == taskjournal.Failed {
		return entry, nil
	}
	if entry.State != taskjournal.Uncertain {
		return entry, taskjournal.ErrConflict
	}
	evidence, err := j.artifacts.Verify(entry.EvidenceIdentity())
	if errors.Is(err, artifactstore.ErrNotFound) {
		if !j.owned(entry) {
			return entry, fmt.Errorf("%w: original VM unavailable and no durable bundle", taskjournal.ErrConflict)
		}
		evidence, err = j.executor.ReadEvidence(ctx, entry.EvidenceIdentity())
		if err != nil || evidence.Validate(entry.EvidenceIdentity()) != nil || !j.owned(entry) {
			return entry, fmt.Errorf("%w: original completion evidence unavailable", taskjournal.ErrConflict)
		}
		if err = j.capture(ctx, entry, evidence); err != nil {
			return entry, err
		}
		if !j.owned(entry) {
			return entry, fmt.Errorf("%w: workload changed during artifact capture; local bundle retained for inspection", taskjournal.ErrConflict)
		}
	}
	if err != nil || ctx.Err() != nil {
		if err == nil {
			err = ctx.Err()
		}
		return entry, err
	}
	return j.tasks.FinishJobEvidence(id, evidence, true)
}
