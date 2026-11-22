package fleet

import (
	"agent-runtime-lab/src/admission"
)

// Draining is a durable, irreversible fleet-level admission fence. A drained
// test journal cannot be reused for future sandboxes; start a new journal.
func (c *Controller) Draining() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.j.Draining
}

// BeginDrain writes the admission fence and deletion intent for EVERY job in
// a single fsynced journal transaction. On restart the original Tick loop
// sees these intents before it can create any queued VM. Remote deletions
// remain asynchronous and release resources only after observed absence.
func (c *Controller) BeginDrain() (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.poison {
		return 0, ErrStoreUncertain
	}
	c.j.Draining = true
	requested := 0
	for id, w := range c.j.Workloads {
		e, ok := c.sched.Get(id)
		if !ok {
			c.poison = true
			return 0, ErrStoreUncertain
		}
		if e.State == admission.Deleted {
			continue
		}
		w.Desired = "deleted"
		if e.State == admission.Queued && w.SandboxID == "" && !w.CreatePending {
			if err := c.sched.ConfirmDeleted(id); err != nil {
				c.poison = true
				return 0, err
			}
		}
		c.j.Workloads[id] = w
		requested++
	}
	if err := c.persist(); err != nil {
		return requested, err
	}
	return requested, nil
}
