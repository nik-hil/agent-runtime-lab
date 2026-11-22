package admission

import (
	"errors"
	"fmt"
	"reflect"
)

// Checkpoint serializes the full scheduling state, including FIFO order and
// reservations. Snapshot is intentionally insufficient for crash recovery.
// Callers must commit Checkpoint durably BEFORE invoking a remote operation.
type Checkpoint struct {
	Config    Config              `json:"config"`
	Jobs      map[string]Entry    `json:"jobs"`
	Used      Resources           `json:"used"`
	Usage     map[string]Usage    `json:"usage"`
	Queues    map[string][]string `json:"queues"`
	Order     []string            `json:"order"`
	NextIndex int                 `json:"next_index"`
	Pending   int                 `json:"pending"`
}

func (s *Scheduler) Checkpoint() Checkpoint {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := Checkpoint{Config: Config{Capacity: s.config.Capacity, MaxPending: s.config.MaxPending,
		Tenants: make(map[string]Policy, len(s.config.Tenants))},
		Jobs: make(map[string]Entry, len(s.jobs)), Usage: make(map[string]Usage, len(s.usage)),
		Queues: make(map[string][]string, len(s.queues)), Order: append([]string(nil), s.order...),
		NextIndex: s.nextIndex, Pending: s.pending, Used: s.used}
	for k, v := range s.config.Tenants {
		p.Config.Tenants[k] = v
	}
	for k, v := range s.jobs {
		p.Jobs[k] = v
	}
	for k, v := range s.usage {
		p.Usage[k] = v
	}
	for k, v := range s.queues {
		p.Queues[k] = append([]string(nil), v...)
	}
	return p
}

// Restore rejects corrupt or internally inconsistent admission state rather
// than silently dropping capacity reservations or inventing runnable work.
func Restore(p Checkpoint) (*Scheduler, error) {
	s, err := New(p.Config)
	if err != nil {
		return nil, fmt.Errorf("invalid scheduler policy: %w", err)
	}
	if p.Pending < 0 || p.Pending > p.Config.MaxPending || p.NextIndex < 0 ||
		(len(p.Order) == 0 && p.NextIndex != 0) ||
		(len(p.Order) > 0 && p.NextIndex >= len(p.Order)) {
		return nil, errors.New("invalid scheduler cursor or pending count")
	}
	seen := make(map[string]bool)
	for _, tenant := range p.Order {
		if seen[tenant] || len(p.Queues[tenant]) == 0 || p.Config.Tenants[tenant].MaxLogical == 0 {
			return nil, errors.New("invalid or duplicate queued tenant")
		}
		seen[tenant] = true
	}
	queuedIDs := make(map[string]bool)
	var pending int
	for tenant, queue := range p.Queues {
		if len(queue) > 0 && !seen[tenant] {
			return nil, errors.New("nonempty queue missing from rotation")
		}
		for _, id := range queue {
			if queuedIDs[id] {
				return nil, errors.New("duplicate queued request")
			}
			queuedIDs[id] = true
			e, ok := p.Jobs[id]
			if !ok || e.Request.Tenant != tenant || (e.State != Queued && e.State != ResumeQueued) {
				return nil, errors.New("queue references invalid request")
			}
			pending++
		}
	}
	if pending != p.Pending {
		return nil, errors.New("pending count mismatch")
	}
	computedUsage := make(map[string]Usage)
	var used Resources
	for id, e := range p.Jobs {
		if id == "" || e.Request.ID != id || e.Request.Tenant == "" || !e.Request.Resources.valid() {
			return nil, errors.New("invalid checkpoint request")
		}
		policy, ok := p.Config.Tenants[e.Request.Tenant]
		if !ok || !e.Request.Resources.fits(p.Config.Capacity) || !e.Request.Resources.fits(policy.MaxActive) {
			return nil, errors.New("checkpoint request violates host or tenant capacity")
		}
		inQueue := e.State == Queued || e.State == ResumeQueued
		if queuedIDs[id] != inQueue {
			return nil, errors.New("checkpoint queue state mismatch")
		}
		if e.State != Queued && e.State != Reserved && e.State != Running && e.State != Paused &&
			e.State != ResumeQueued && e.State != ResumeHeld && e.State != Uncertain && e.State != Deleted {
			return nil, errors.New("unsupported checkpoint state")
		}
		u := computedUsage[e.Request.Tenant]
		if e.State != Deleted {
			u.Logical++
		}
		if e.State == Reserved || e.State == Running || e.State == ResumeHeld || e.State == Uncertain {
			u.Active = u.Active.add(e.Request.Resources)
			used = used.add(e.Request.Resources)
		}
		computedUsage[e.Request.Tenant] = u
	}
	if !used.fits(p.Config.Capacity) {
		return nil, errors.New("host overcommit in checkpoint")
	}
	for t, u := range computedUsage {
		policy := p.Config.Tenants[t]
		if u.Logical > policy.MaxLogical || !u.Active.fits(policy.MaxActive) {
			return nil, errors.New("tenant overcommit in checkpoint")
		}
	}
	for t, u := range p.Usage {
		if !reflect.DeepEqual(u, computedUsage[t]) {
			return nil, fmt.Errorf("tenant usage mismatch %q", t)
		}
	}
	for t, u := range computedUsage {
		if !reflect.DeepEqual(u, p.Usage[t]) {
			return nil, fmt.Errorf("missing tenant usage %q", t)
		}
	}
	if !reflect.DeepEqual(used, p.Used) {
		return nil, errors.New("host usage mismatch")
	}
	s.jobs, s.usage, s.queues, s.order, s.nextIndex, s.pending, s.used = p.Jobs, p.Usage, p.Queues, p.Order, p.NextIndex, p.Pending, p.Used
	// Clone input to prevent callers mutating restored state without locks.
	// We cloned policy in New; clone all other collections for ownership too.
	checkpoint := s.Checkpoint()
	s.jobs, s.usage, s.queues, s.order = checkpoint.Jobs, checkpoint.Usage, checkpoint.Queues, checkpoint.Order
	return s, nil
}
