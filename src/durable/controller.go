package durable

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"agent-runtime-lab/src/lifecycle"
)

var (
	ErrCreatePending   = errors.New("create outcome unresolved: recover existing sandbox by metadata before retrying")
	ErrMultipleMatches = errors.New("multiple sandboxes match logical key; manual investigation required")
	ErrNoMatch         = errors.New("no matching sandbox found; cannot safely retry create without operator verification")
	ErrUnexpectedLoss  = errors.New("sandbox vanished; automatic recreation disabled")
	ErrStoreUncertain  = errors.New("persistence outcome uncertain; restart controller and inspect state")
)

// Finder lists matching sandboxes for an ambiguous Create. A match is only
// admissible if the remote provider verified the exact logical key metadata.
type Finder interface {
	FindByLogicalKey(ctx context.Context, key string) ([]string, error)
}

// Controller holds the FileStore lock for its entire lifetime. The mutex
// protects concurrent calls inside this process; flock protects other ones.
type Controller struct {
	mu      sync.Mutex
	store   Store
	backend lifecycle.Backend
	record  Record
	timeout time.Duration
	poison  bool
}

// New restores an existing record, or writes a new initial record. The caller
// must provide a globally distinct, stable key for a new logical sandbox.
// For existing state, pass "" to reuse the recorded key.
func New(store Store, backend lifecycle.Backend, key string, timeout time.Duration) (*Controller, error) {
	if store == nil || backend == nil || timeout <= 0 {
		return nil, errors.New("store, backend and positive timeout required")
	}
	r, exists, err := store.Load()
	if err != nil {
		return nil, err
	}
	if exists {
		if key != "" && key != r.LogicalKey {
			return nil, fmt.Errorf("logical key differs from persisted key %q", r.LogicalKey)
		}
	} else {
		if strings.TrimSpace(key) == "" {
			return nil, errors.New("new controller requires logical key")
		}
		r = Record{Version: stateVersion, LogicalKey: key, Desired: "running", Observed: "unknown"}
		if err := store.Save(r); err != nil {
			return nil, fmt.Errorf("persist initial state: %w", err)
		}
	}
	return &Controller{store: store, backend: backend, record: r, timeout: timeout}, nil
}

func (c *Controller) Record() Record {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.record
}

func (c *Controller) persist(next Record) error {
	if c.poison {
		return ErrStoreUncertain
	}
	if err := c.store.Save(next); err != nil {
		// A failed parent-dir fsync can occur *after* rename; continuing in
		// this process risks acting on a stale state. Fail closed.
		c.poison = true
		return fmt.Errorf("%w: %v", ErrStoreUncertain, err)
	}
	c.record = next
	return nil
}

func (c *Controller) SetDesired(desired lifecycle.DesiredState) error {
	if desired != lifecycle.WantRunning && desired != lifecycle.WantPaused && desired != lifecycle.WantDeleted {
		return fmt.Errorf("invalid desired state %q", desired)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.poison {
		return ErrStoreUncertain
	}
	if c.record.Terminated && desired != lifecycle.WantDeleted {
		return lifecycle.ErrTerminal
	}
	next := c.record
	next.Desired = string(desired)
	return c.persist(next) // write desired state BEFORE remote mutations
}

func (c *Controller) Converged() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	r := c.record
	return !c.poison && !r.CreatePending && ((r.Terminated && r.Desired == "deleted") ||
		(r.SandboxID != "" && r.Observed == r.Desired))
}

// Reconcile performs at most one remote mutation and re-observes on a later
// pass. It cannot recreate a sandbox after an unexpected remote deletion.
func (c *Controller) Reconcile(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.poison {
		return ErrStoreUncertain
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	r := c.record
	if r.CreatePending {
		return ErrCreatePending
	}
	if r.Terminated {
		return nil
	}
	opCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	if r.SandboxID == "" {
		if r.Desired == "deleted" {
			r.Terminated, r.Observed = true, "absent"
			return c.persist(r)
		}
		// Persist the ambiguous-create fence before issuing an external POST.
		// A kill -9/power loss leaves the fence on disk for recovery.
		r.CreatePending = true
		if err := c.persist(r); err != nil {
			return err
		}
		id, err := c.backend.Create(opCtx, r.LogicalKey)
		if err != nil || id == "" {
			return fmt.Errorf("%w: %v", ErrCreatePending, err)
		}
		r.SandboxID, r.CreatePending, r.Observed = id, false, "unknown"
		return c.persist(r)
	}

	observed, err := c.backend.Observe(opCtx, r.SandboxID)
	if err != nil {
		return fmt.Errorf("observe sandbox %s: %w", r.SandboxID, err)
	}
	if observed != lifecycle.Absent && observed != lifecycle.Running && observed != lifecycle.Paused {
		return fmt.Errorf("unsupported observed state %q", observed)
	}
	r.Observed = string(observed)
	if observed == lifecycle.Absent {
		if r.Desired != "deleted" {
			if err := c.persist(r); err != nil {
				return err
			}
			return ErrUnexpectedLoss
		}
		r.Terminated, r.SandboxID = true, ""
		return c.persist(r)
	}
	if err := c.persist(r); err != nil {
		return err
	}
	switch r.Desired {
	case "running":
		if observed == lifecycle.Paused {
			return c.backend.Resume(opCtx, r.SandboxID)
		}
	case "paused":
		if observed == lifecycle.Running {
			return c.backend.Pause(opCtx, r.SandboxID)
		}
	case "deleted":
		return c.backend.Delete(opCtx, r.SandboxID)
	}
	return nil
}

// RecoverCreate is safe only if exactly one provider-side sandbox has the
// expected metadata. Zero or multiple matches remain fenced. An operator must
// never "reset" an ambiguous Create based only on a momentary empty listing.
func (c *Controller) RecoverCreate(ctx context.Context, finder Finder) error {
	if finder == nil {
		return errors.New("finder required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.poison {
		return ErrStoreUncertain
	}
	if !c.record.CreatePending {
		return errors.New("no ambiguous creation to recover")
	}
	opCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	ids, err := finder.FindByLogicalKey(opCtx, c.record.LogicalKey)
	if err != nil {
		return err
	}
	switch len(ids) {
	case 0:
		return ErrNoMatch
	case 1:
		if ids[0] == "" {
			return errors.New("provider returned empty sandbox ID")
		}
		r := c.record
		r.SandboxID, r.CreatePending, r.Observed = ids[0], false, "unknown"
		return c.persist(r)
	default:
		return fmt.Errorf("%w: %d matches", ErrMultipleMatches, len(ids))
	}
}
