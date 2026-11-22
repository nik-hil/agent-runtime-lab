package fleet

import (
	"errors"
	"reflect"
)

// SnapshotJournal returns an isolated read-only checkpoint for provider audits.
// The caller must not mutate it. Provider I/O occurs *after* we drop c.mu,
// so a slow audit cannot block health reconciliation or deletion.
func (c *Controller) SnapshotJournal() Journal {
	c.mu.Lock()
	defer c.mu.Unlock()
	j := c.j
	j.Scheduler = c.sched.Checkpoint()
	j.Workloads = make(map[string]Workload, len(c.j.Workloads))
	for id, w := range c.j.Workloads {
		j.Workloads[id] = w
	}
	return j
}

// FenceIfSnapshotCurrent persists a conservative admission block only if no
// workload or admission state changed while a critical provider discrepancy
// was audited. It never changes sandbox ownership or frees reservations.
// There is intentionally no automatic Unfence operation in this lab.
func (c *Controller) FenceIfSnapshotCurrent(snapshot Journal, note string) (bool, error) {
	if note == "" || len(note) > 512 {
		return false, errors.New("non-empty bounded unsafe note required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.poison {
		return false, ErrStoreUncertain
	}
	if c.j.Draining != snapshot.Draining ||
		!reflect.DeepEqual(c.j.Workloads, snapshot.Workloads) ||
		!reflect.DeepEqual(c.sched.Checkpoint(), snapshot.Scheduler) {
		return false, nil
	}
	if c.j.Unsafe {
		return true, nil
	}
	c.j.Unsafe = true
	c.j.UnsafeNote = note
	return true, c.persist()
}
