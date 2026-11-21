// Package lifecycle is a small, backend-neutral sandbox reconciliation lab.
// Unlike sandbox.Sandbox, it models long-lived, pausable execution environments.
package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// DesiredState describes what the owner wants the sandbox to become.
type DesiredState string

const (
	WantRunning DesiredState = "running"
	WantPaused  DesiredState = "paused"
	WantDeleted DesiredState = "deleted"
)

// ObservedState is a state actually reported by the remote runtime.
type ObservedState string

const (
	Unknown ObservedState = "unknown"
	Absent  ObservedState = "absent"
	Running ObservedState = "running"
	Paused  ObservedState = "paused"
)

var (
	// ErrCreateUncertain prevents blindly retrying a create whose response was lost.
	ErrCreateUncertain = errors.New("creation outcome uncertain; reconcile external ID before retry")
	ErrUnexpectedLoss  = errors.New("remote sandbox disappeared; automatic recreation disabled")
	ErrTerminal        = errors.New("sandbox lifecycle already terminated")
)

// Backend is deliberately independent of a particular SDK or cloud provider.
// Create's logicalKey is a correlation identifier, NOT an assumed E2B
// idempotency key. A failed Create is treated as ambiguous, including timeouts.
// Implementations must ensure Observe distinguishes "absent" from errors.
type Backend interface {
	Create(ctx context.Context, logicalKey string) (sandboxID string, err error)
	Observe(ctx context.Context, sandboxID string) (ObservedState, error)
	Pause(ctx context.Context, sandboxID string) error
	Resume(ctx context.Context, sandboxID string) error
	Delete(ctx context.Context, sandboxID string) error
}

type Status struct {
	LogicalKey      string        `json:"logical_key"`
	SandboxID       string        `json:"sandbox_id,omitempty"`
	Desired         DesiredState  `json:"desired"`
	Observed        ObservedState `json:"observed"`
	CreateUncertain bool          `json:"create_uncertain"`
	Terminated      bool          `json:"terminated"`
	Converged       bool          `json:"converged"`
}

// Controller serializes operations for one logical sandbox in one process.
// It is an educational component, NOT a durable/distributed controller: a
// restart loses the ID, desired state and creation-uncertain fence.
type Controller struct {
	mu               sync.Mutex
	backend          Backend
	logicalKey       string
	operationTimeout time.Duration
	sandboxID        string
	desired          DesiredState
	observed         ObservedState
	createUncertain  bool
	terminated       bool
}

func New(backend Backend, logicalKey string, operationTimeout time.Duration) (*Controller, error) {
	if backend == nil || strings.TrimSpace(logicalKey) == "" || operationTimeout <= 0 {
		return nil, errors.New("backend, nonempty logical key and positive operation timeout required")
	}
	return &Controller{
		backend:          backend,
		logicalKey:       logicalKey,
		operationTimeout: operationTimeout,
		desired:          WantRunning,
		observed:         Unknown,
	}, nil
}

func (c *Controller) SetDesired(desired DesiredState) error {
	if desired != WantRunning && desired != WantPaused && desired != WantDeleted {
		return fmt.Errorf("invalid desired state %q", desired)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.terminated && desired != WantDeleted {
		return ErrTerminal
	}
	c.desired = desired
	return nil
}

func (c *Controller) Status() Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.statusLocked()
}

func (c *Controller) statusLocked() Status {
	converged := !c.createUncertain && ((c.desired == WantRunning && c.observed == Running && c.sandboxID != "" ||
		c.desired == WantPaused && c.observed == Paused && c.sandboxID != "") ||
		(c.desired == WantDeleted && c.terminated && c.sandboxID == ""))
	return Status{
		LogicalKey: c.logicalKey, SandboxID: c.sandboxID,
		Desired: c.desired, Observed: c.observed,
		CreateUncertain: c.createUncertain, Terminated: c.terminated,
		Converged: converged,
	}
}

// AttachAfterAmbiguousCreate clears the creation fence only when an operator
// has recovered the remote sandbox ID from the provider using the logical key.
// It must not be used to manufacture an ID for a sandbox that does not exist.
func (c *Controller) AttachAfterAmbiguousCreate(sandboxID string) error {
	if strings.TrimSpace(sandboxID) == "" {
		return errors.New("recovered sandbox ID required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.createUncertain || c.sandboxID != "" || c.terminated {
		return errors.New("no unresolved create to attach")
	}
	c.sandboxID = sandboxID
	c.observed = Unknown
	c.createUncertain = false
	return nil
}

// Reconcile performs at most ONE state-changing backend operation per call.
// The next pass re-observes remote state instead of assuming an API ack proves
// the transition. Retry this method only for repeatable operations; a failed
// Create is fenced to prevent duplicate sandboxes after ambiguous failures.
func (c *Controller) Reconcile(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}
	if c.createUncertain {
		return ErrCreateUncertain
	}
	if c.terminated {
		if c.desired != WantDeleted {
			return ErrTerminal
		}
		return nil
	}

	opCtx, cancel := context.WithTimeout(ctx, c.operationTimeout)
	defer cancel()

	if c.sandboxID == "" {
		if c.desired == WantDeleted {
			c.terminated = true
			c.observed = Absent
			return nil
		}
		id, err := c.backend.Create(opCtx, c.logicalKey)
		if err != nil || id == "" {
			c.createUncertain = true
			return fmt.Errorf("%w: %v", ErrCreateUncertain, err)
		}
		c.sandboxID = id
		c.observed = Unknown
		return nil
	}

	state, err := c.backend.Observe(opCtx, c.sandboxID)
	if err != nil {
		return fmt.Errorf("observe sandbox %s: %w", c.sandboxID, err)
	}
	if state != Absent && state != Running && state != Paused {
		return fmt.Errorf("provider returned unsupported observed state %q", state)
	}
	c.observed = state
	if state == Absent {
		if c.desired != WantDeleted {
			return ErrUnexpectedLoss
		}
		c.sandboxID = ""
		c.terminated = true
		return nil
	}

	switch c.desired {
	case WantRunning:
		if state == Paused {
			return c.backend.Resume(opCtx, c.sandboxID)
		}
	case WantPaused:
		if state == Running {
			return c.backend.Pause(opCtx, c.sandboxID)
		}
	case WantDeleted:
		return c.backend.Delete(opCtx, c.sandboxID)
	}
	return nil
}
