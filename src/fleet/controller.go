package fleet

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/durable"
	"agent-runtime-lab/src/lifecycle"
)

var (
	ErrCreateUncertain = errors.New("E2B Create outcome uncertain: recover by metadata; never retry Create blindly")
	ErrUnexpectedLoss  = errors.New("sandbox absent without delete intent: reservation retained")
	ErrUnsafeCapacity  = errors.New("fleet is unsafe: new admissions blocked; inspect unsafe_note")
	ErrStoreUncertain  = errors.New("fleet journal outcome uncertain: stop and reopen state")
	ErrIdle            = errors.New("no eligible fleet transition")
)

// Details includes actual VM shape reported by E2B. The scheduler reserves
// an upper bound for the template, rather than trusting a caller-provided size.
type Details struct {
	State     lifecycle.ObservedState
	Resources admission.Resources
}

// Runtime combines existing E2B lifecycle and ambiguity lookup operations
// with resource verification. All mutating operations are explicit.
type Runtime interface {
	lifecycle.Backend
	durable.Finder
	Describe(context.Context, string) (Details, error)
}

// Config fixes the reservation per VM template. Every Submit uses TemplateSize;
// clients cannot understate the template's CPU/RAM footprint.
type Config struct {
	Admission    admission.Config    `json:"admission"`
	TemplateSize admission.Resources `json:"template_size"`
}

// Status is a nonsecret durable snapshot suitable for CLI output.
type Status struct {
	Unsafe     bool                 `json:"unsafe"`
	UnsafeNote string               `json:"unsafe_note,omitempty"`
	Admission  admission.Snapshot   `json:"admission"`
	Jobs       map[string]JobStatus `json:"jobs"`
}

type JobStatus struct {
	Entry    admission.Entry `json:"entry"`
	Workload Workload        `json:"workload"`
}

// Controller requires an exclusive Store lock for its entire lifetime. Its
// mutex serializes callers in this process; the lock excludes other processes.
type Controller struct {
	mu      sync.Mutex
	store   *Store
	backend Runtime
	sched   *admission.Scheduler
	j       Journal
	poison  bool
	timeout time.Duration
}

func New(store *Store, backend Runtime, cfg Config, timeout time.Duration) (*Controller, error) {
	if store == nil || backend == nil || timeout <= 0 {
		return nil, errors.New("store, runtime and positive timeout required")
	}
	if cfg.TemplateSize.CPU <= 0 || cfg.TemplateSize.MemoryMiB <= 0 ||
		cfg.TemplateSize.CPU > cfg.Admission.Capacity.CPU || cfg.TemplateSize.MemoryMiB > cfg.Admission.Capacity.MemoryMiB {
		return nil, errors.New("invalid or oversized E2B template resource reservation")
	}
	j, exists, err := store.Load()
	if err != nil {
		return nil, err
	}
	var sched *admission.Scheduler
	if exists {
		if !reflect.DeepEqual(cfg.Admission, j.Scheduler.Config) {
			return nil, errors.New("fleet policies differ from persisted config; migration required")
		}
		if cfg.TemplateSize != j.TemplateSize {
			return nil, errors.New("E2B template resource reservation changed across restart")
		}
		sched, err = admission.Restore(j.Scheduler)
		if err != nil {
			return nil, fmt.Errorf("invalid persisted scheduler: %w", err)
		}
		if err := validateWorkloads(j, cfg.TemplateSize); err != nil {
			return nil, err
		}
	} else {
		sched, err = admission.New(cfg.Admission)
		if err != nil {
			return nil, err
		}
		j = Journal{Version: journalVersion, TemplateSize: cfg.TemplateSize, Scheduler: sched.Checkpoint(), Workloads: map[string]Workload{}}
		if err := store.Save(j); err != nil {
			return nil, err
		}
	}
	return &Controller{store: store, backend: backend, sched: sched, j: j, timeout: timeout}, nil
}

func validateWorkloads(j Journal, size admission.Resources) error {
	if len(j.Workloads) != len(j.Scheduler.Jobs) {
		return errors.New("fleet journal workloads and admission jobs disagree")
	}
	seenKeys := make(map[string]bool)
	for id, w := range j.Workloads {
		e, ok := j.Scheduler.Jobs[id]
		if !ok || e.Request.Resources != size || w.LogicalKey == "" || seenKeys[w.LogicalKey] {
			return errors.New("invalid workload identity or resource reservation")
		}
		seenKeys[w.LogicalKey] = true
		if w.Desired != "running" && w.Desired != "paused" && w.Desired != "deleted" {
			return errors.New("invalid workload desired state")
		}
		if w.CreatePending && (w.SandboxID != "" || e.State != admission.Reserved && e.State != admission.Uncertain) {
			return errors.New("create-pending record has invalid admission state")
		}
		if (e.State == admission.Running || e.State == admission.Paused || e.State == admission.ResumeHeld || e.State == admission.ResumeQueued) && w.SandboxID == "" {
			return errors.New("running/paused/resuming admission missing sandbox ID")
		}
		if e.State == admission.Deleted && (w.SandboxID != "" || w.CreatePending) {
			return errors.New("deleted entry retains remote sandbox ownership")
		}
	}
	return nil
}

func (c *Controller) persist() error {
	if c.poison {
		return ErrStoreUncertain
	}
	c.j.Scheduler = c.sched.Checkpoint()
	if err := c.store.Save(c.j); err != nil {
		// fsync may fail after rename. Do not continue remote mutations.
		c.poison = true
		return fmt.Errorf("%w: %v", ErrStoreUncertain, err)
	}
	return nil
}

func (c *Controller) Status() Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	jobs := make(map[string]JobStatus, len(c.j.Workloads))
	for id, w := range c.j.Workloads {
		e, _ := c.sched.Get(id)
		jobs[id] = JobStatus{Entry: e, Workload: w}
	}
	return Status{Unsafe: c.j.Unsafe, UnsafeNote: c.j.UnsafeNote, Admission: c.sched.Snapshot(), Jobs: jobs}
}

func (c *Controller) Submit(tenant, id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.poison {
		return ErrStoreUncertain
	}
	if c.j.Unsafe || c.j.Draining {
		return ErrUnsafeCapacity
	}
	if _, exists := c.j.Workloads[id]; exists {
		return admission.ErrConflict
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	key := "agent-runtime-lab/m2-9/" + hex.EncodeToString(nonce[:])
	vm := c.templateSize()
	if _, err := c.sched.Submit(admission.Request{ID: id, Tenant: tenant, Resources: vm}); err != nil {
		return err
	}
	c.j.Workloads[id] = Workload{LogicalKey: key, Desired: "running"}
	return c.persist()
}

func (c *Controller) templateSize() admission.Resources {
	return c.j.TemplateSize
}

func (c *Controller) TemplateResources() admission.Resources {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.j.TemplateSize
}

// setDesired persists user intent independently of the remote API. The next
// Tick observes the actual VM before releasing resources or issuing retries.
func (c *Controller) SetDesired(id string, want lifecycle.DesiredState) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.poison {
		return ErrStoreUncertain
	}
	w, ok := c.j.Workloads[id]
	if !ok {
		return admission.ErrNotFound
	}
	e, _ := c.sched.Get(id)
	if e.State == admission.Deleted {
		if want == lifecycle.WantDeleted {
			return nil
		}
		return lifecycle.ErrTerminal
	}
	if w.Desired == "deleted" && want != lifecycle.WantDeleted {
		return lifecycle.ErrTerminal // a remote Delete may already be in flight
	}
	if want != lifecycle.WantRunning && want != lifecycle.WantPaused && want != lifecycle.WantDeleted {
		return errors.New("invalid desired state")
	}
	if want == lifecycle.WantPaused && e.State != admission.Running && e.State != admission.Paused {
		return errors.New("pause requires a confirmed running or paused sandbox")
	}
	if want == lifecycle.WantRunning && e.State == admission.Paused {
		if err := c.sched.QueueResume(id); err != nil {
			return err
		}
	}
	w.Desired = string(want)
	if want == lifecycle.WantDeleted && e.State == admission.Queued && w.SandboxID == "" && !w.CreatePending {
		if err := c.sched.ConfirmDeleted(id); err != nil {
			return err
		}
	}
	c.j.Workloads[id] = w
	return c.persist()
}

// Recover does not create a VM. A missing or ambiguous metadata match keeps
// the reservation held, and operators must not clear it based on a transient list.
func (c *Controller) Recover(ctx context.Context, id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.poison {
		return ErrStoreUncertain
	}
	w, ok := c.j.Workloads[id]
	if !ok {
		return admission.ErrNotFound
	}
	if !w.CreatePending || w.SandboxID != "" {
		return errors.New("no ambiguous Create to recover")
	}
	op, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	ids, err := c.backend.FindByLogicalKey(op, w.LogicalKey)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return durable.ErrNoMatch
	}
	if len(ids) > 1 {
		return durable.ErrMultipleMatches
	}
	if ids[0] == "" {
		return errors.New("provider returned blank sandbox ID")
	}
	w.SandboxID, w.CreatePending = ids[0], false
	c.j.Workloads[id] = w
	return c.persist()
}

// Tick performs at most one provider mutation or one confirmed state change.
// It always reobserves provider state after a previous response, preventing
// eager resource release on API acknowledgment alone.
func (c *Controller) Tick(ctx context.Context) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.poison {
		return false, ErrStoreUncertain
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	ids := make([]string, 0, len(c.j.Workloads))
	for id := range c.j.Workloads {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		work := c.j.Workloads[id]
		e, _ := c.sched.Get(id)
		if e.State == admission.Deleted {
			continue
		}
		if work.SandboxID == "" {
			if work.Desired == "deleted" && !work.CreatePending {
				if err := c.sched.ConfirmDeleted(id); err != nil {
					return false, err
				}
				return true, c.persist()
			}
			continue // a pending Create remains fenced until Recover
		}
		if work.Desired == "deleted" || e.State == admission.Reserved || e.State == admission.Uncertain ||
			e.State == admission.ResumeHeld || (e.State == admission.Running && work.Desired == "paused") {
			return c.reconcileKnown(ctx, id, e, work)
		}
	}
	// A persisted unsafe admission fence is an expected steady state, not
	// a reconciliation failure. Cleanup above remains allowed; do not retry
	// the idle fence as a provider error or trigger endless backoff logs.
	if c.j.Unsafe {
		return false, nil
	}
	e, ok := c.sched.Dispatch()
	if !ok {
		return false, nil
	}
	w := c.j.Workloads[e.Request.ID]
	if e.State == admission.Reserved {
		w.CreatePending = true
	}
	c.j.Workloads[e.Request.ID] = w
	// Durable reservation + intent BEFORE any external API call.
	if err := c.persist(); err != nil {
		return false, err
	}
	op, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	if e.State == admission.Reserved {
		id, err := c.backend.Create(op, w.LogicalKey)
		if err != nil || id == "" {
			if markErr := c.sched.MarkUncertain(e.Request.ID); markErr != nil {
				return true, markErr
			}
			if saveErr := c.persist(); saveErr != nil {
				return true, saveErr
			}
			return true, fmt.Errorf("%w: %v", ErrCreateUncertain, err)
		}
		w.SandboxID, w.CreatePending = id, false
		c.j.Workloads[e.Request.ID] = w
		return true, c.persist()
	}
	if e.State != admission.ResumeHeld || w.SandboxID == "" {
		return true, errors.New("invalid resume reservation")
	}
	if err := c.backend.Resume(op, w.SandboxID); err != nil {
		if markErr := c.sched.MarkUncertain(e.Request.ID); markErr != nil {
			return true, markErr
		}
		if saveErr := c.persist(); saveErr != nil {
			return true, saveErr
		}
		return true, fmt.Errorf("resume outcome uncertain; reservation held: %w", err)
	}
	return true, nil
}

func (c *Controller) reconcileKnown(ctx context.Context, id string, e admission.Entry, w Workload) (bool, error) {
	op, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	info, err := c.backend.Describe(op, w.SandboxID)
	if err != nil {
		return false, err
	}
	if info.State != lifecycle.Absent {
		if info.Resources.CPU <= 0 || info.Resources.MemoryMiB <= 0 ||
			info.Resources.CPU > e.Request.Resources.CPU || info.Resources.MemoryMiB > e.Request.Resources.MemoryMiB {
			c.j.Unsafe = true
			c.j.UnsafeNote = fmt.Sprintf("sandbox %s reports CPU=%d MiB=%d above reservation CPU=%d MiB=%d", id,
				info.Resources.CPU, info.Resources.MemoryMiB, e.Request.Resources.CPU, e.Request.Resources.MemoryMiB)
			if err := c.persist(); err != nil {
				return false, err
			}
			if w.Desired != "deleted" {
				return false, fmt.Errorf("%w: %s", ErrUnsafeCapacity, c.j.UnsafeNote)
			}
			// Keep admission blocked, but still permit explicit VM destruction.
		}
	}
	if info.State == lifecycle.Absent {
		if w.Desired != "deleted" {
			return false, ErrUnexpectedLoss
		}
		if err := c.sched.ConfirmDeleted(id); err != nil {
			return false, err
		}
		w.SandboxID, w.CreatePending = "", false
		c.j.Workloads[id] = w
		return true, c.persist()
	}
	if w.Desired == "deleted" {
		// A successful DELETE ACK never releases resources: wait for observed absence.
		return true, c.backend.Delete(op, w.SandboxID)
	}
	if info.State == lifecycle.Running {
		if e.State == admission.Reserved || e.State == admission.ResumeHeld || e.State == admission.Uncertain {
			if err := c.sched.ConfirmRunning(id); err != nil {
				return false, err
			}
			return true, c.persist()
		}
		if e.State == admission.Running && w.Desired == "paused" {
			return true, c.backend.Pause(op, w.SandboxID)
		}
		return false, nil
	}
	if info.State == lifecycle.Paused {
		if e.State == admission.Running && w.Desired == "paused" {
			if err := c.sched.ConfirmPaused(id); err != nil {
				return false, err
			}
			return true, c.persist()
		}
		if e.State == admission.ResumeHeld || (e.State == admission.Uncertain && w.Desired == "running") {
			return true, c.backend.Resume(op, w.SandboxID)
		}
		return false, errors.New("unexpected PAUSED state; reservation retained until operator investigation")
	}
	return false, errors.New("provider returned unrecognized VM state")
}
