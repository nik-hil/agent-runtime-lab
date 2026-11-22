// Package admission provides in-memory, single-node resource reservations for
// sandbox runtimes. It makes NO E2B calls: a caller must observe the remote
// result before confirming transitions or releasing reserved resources.
package admission

import (
	"errors"
	"fmt"
	"sync"
)

var (
	ErrInvalidRequest = errors.New("invalid sandbox admission request")
	ErrConflict       = errors.New("request ID already belongs to a different sandbox")
	ErrQuota          = errors.New("tenant logical sandbox quota reached")
	ErrQueueFull      = errors.New("pending queue full")
	ErrNotFound       = errors.New("sandbox request not found")
	ErrTransition     = errors.New("invalid sandbox state transition")
)

// Resources are reserved while a remote create or resume operation is in
// flight; a timeout is not evidence that the allocation can be released.
type Resources struct {
	CPU       int64 `json:"cpu"`
	MemoryMiB int64 `json:"memory_mib"`
}

func (r Resources) valid() bool { return r.CPU > 0 && r.MemoryMiB > 0 }
func (r Resources) fits(avail Resources) bool {
	return r.CPU <= avail.CPU && r.MemoryMiB <= avail.MemoryMiB
}
func (r Resources) add(other Resources) Resources {
	return Resources{r.CPU + other.CPU, r.MemoryMiB + other.MemoryMiB}
}
func (r Resources) sub(other Resources) Resources {
	return Resources{r.CPU - other.CPU, r.MemoryMiB - other.MemoryMiB}
}

// Policy caps simultaneously running/creating VMs plus logical sandboxes
// (running, reserved, paused and queued). Logical quota is retained on pause.
type Policy struct {
	MaxLogical int       `json:"max_logical"`
	MaxActive  Resources `json:"max_active"`
}

type Config struct {
	Capacity   Resources         `json:"capacity"`
	Tenants    map[string]Policy `json:"tenants"`
	MaxPending int               `json:"max_pending"`
}

type Request struct {
	ID        string    `json:"id"`
	Tenant    string    `json:"tenant"`
	Resources Resources `json:"resources"`
}

type State string

const (
	Queued       State = "queued"
	Reserved     State = "reserved"      // host capacity held before remote Create
	Running      State = "running"       // remote Running confirmed
	Paused       State = "paused"        // remote Paused confirmed; host capacity freed
	ResumeQueued State = "resume_queued" // waiting to reserve capacity before Resume
	ResumeHeld   State = "resume_held"   // capacity held; remote Resume in flight
	Uncertain    State = "uncertain"     // create/resume outcome unknown; keep capacity
	Deleted      State = "deleted"       // remote absence confirmed
)

type Entry struct {
	Request Request `json:"request"`
	State   State   `json:"state"`
}

type Usage struct {
	Logical int       `json:"logical"`
	Active  Resources `json:"active"`
}

type Snapshot struct {
	Capacity Resources        `json:"capacity"`
	Used     Resources        `json:"used"`
	Pending  int              `json:"pending"`
	Tenant   map[string]Usage `json:"tenant"`
}

// Scheduler serializes all admission decisions under one mutex. A reservation
// is a local promise; there is no durable storage, distributed lease, or VM
// lifecycle ownership until a later integration milestone.
type Scheduler struct {
	mu        sync.Mutex
	config    Config
	jobs      map[string]Entry
	used      Resources
	usage     map[string]Usage
	queues    map[string][]string
	order     []string
	nextIndex int
	pending   int
}

func New(config Config) (*Scheduler, error) {
	if !config.Capacity.valid() || config.MaxPending <= 0 || len(config.Tenants) == 0 {
		return nil, errors.New("capacity, tenants and positive queue bound required")
	}
	for name, p := range config.Tenants {
		if name == "" || p.MaxLogical <= 0 || !p.MaxActive.valid() {
			return nil, fmt.Errorf("invalid policy for tenant %q", name)
		}
	}
	// Isolate policy data from the caller. Mutating the input map after New
	// must never change quota decisions or introduce a data race.
	policies := make(map[string]Policy, len(config.Tenants))
	for name, p := range config.Tenants {
		policies[name] = p
	}
	config.Tenants = policies
	return &Scheduler{config: config, jobs: make(map[string]Entry),
		usage: make(map[string]Usage), queues: make(map[string][]string)}, nil
}

func (s *Scheduler) Submit(req Request) (Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if req.ID == "" || req.Tenant == "" || !req.Resources.valid() {
		return Entry{}, ErrInvalidRequest
	}
	if old, ok := s.jobs[req.ID]; ok {
		if old.Request == req { // idempotent request, including after deletion
			return old, nil
		}
		return Entry{}, ErrConflict
	}
	p, ok := s.config.Tenants[req.Tenant]
	if !ok || !req.Resources.fits(s.config.Capacity) || !req.Resources.fits(p.MaxActive) {
		return Entry{}, fmt.Errorf("%w: tenant missing or request exceeds host/tenant capacity", ErrInvalidRequest)
	}
	u := s.usage[req.Tenant]
	if u.Logical >= p.MaxLogical {
		return Entry{}, ErrQuota
	}
	if s.pending >= s.config.MaxPending {
		return Entry{}, ErrQueueFull
	}
	e := Entry{Request: req, State: Queued}
	s.jobs[req.ID] = e
	u.Logical++
	s.usage[req.Tenant] = u
	s.enqueue(req.Tenant, req.ID)
	return e, nil
}

func (s *Scheduler) enqueue(tenant, id string) {
	if len(s.queues[tenant]) == 0 {
		s.order = append(s.order, tenant)
	}
	s.queues[tenant] = append(s.queues[tenant], id)
	s.pending++
}

func (s *Scheduler) available(t string) Resources {
	p := s.config.Tenants[t]
	u := s.usage[t]
	host := s.config.Capacity.sub(s.used)
	tenant := p.MaxActive.sub(u.Active)
	if host.CPU > tenant.CPU {
		host.CPU = tenant.CPU
	}
	if host.MemoryMiB > tenant.MemoryMiB {
		host.MemoryMiB = tenant.MemoryMiB
	}
	return host
}

// Dispatch reserves one eligible head request, rotating the candidate tenant
// after each successful dispatch (round-robin). A blocked tenant does not stop
// another tenant that fits, avoiding global head-of-line blocking.
func (s *Scheduler) Dispatch() (Entry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.order) == 0 {
		return Entry{}, false
	}
	for checked := 0; checked < len(s.order); checked++ {
		i := (s.nextIndex + checked) % len(s.order)
		t := s.order[i]
		id := s.queues[t][0]
		e := s.jobs[id]
		if !e.Request.Resources.fits(s.available(t)) {
			continue
		}
		s.queues[t] = s.queues[t][1:]
		s.pending--
		s.used = s.used.add(e.Request.Resources)
		u := s.usage[t]
		u.Active = u.Active.add(e.Request.Resources)
		s.usage[t] = u
		if e.State == ResumeQueued {
			e.State = ResumeHeld
		} else {
			e.State = Reserved
		}
		s.jobs[id] = e
		if len(s.queues[t]) == 0 {
			s.order = append(s.order[:i], s.order[i+1:]...)
			if len(s.order) == 0 {
				s.nextIndex = 0
			} else {
				s.nextIndex = i % len(s.order)
			}
		} else {
			s.nextIndex = (i + 1) % len(s.order)
		}
		return e, true
	}
	return Entry{}, false
}

// ConfirmRunning must only be called after observing the VM RUNNING. A failed
// remote Create/Resume of uncertain outcome must retain its reservation.
func (s *Scheduler) ConfirmRunning(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.jobs[id]
	if !ok {
		return ErrNotFound
	}
	if e.State != Reserved && e.State != ResumeHeld && e.State != Uncertain {
		return ErrTransition
	}
	e.State = Running
	s.jobs[id] = e
	return nil
}

func (s *Scheduler) MarkUncertain(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.jobs[id]
	if !ok {
		return ErrNotFound
	}
	if e.State != Reserved && e.State != ResumeHeld {
		return ErrTransition
	}
	e.State = Uncertain
	s.jobs[id] = e
	return nil
}

// ConfirmPaused releases active CPU/memory only after observing remote PAUSED.
func (s *Scheduler) ConfirmPaused(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.jobs[id]
	if !ok {
		return ErrNotFound
	}
	if e.State != Running {
		return ErrTransition
	}
	s.releaseActive(e)
	e.State = Paused
	s.jobs[id] = e
	return nil
}

// QueueResume does not call the VM. Resume must wait until CPU/memory can be
// reserved again. Logical sandbox quota is not charged for a second time.
func (s *Scheduler) QueueResume(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.jobs[id]
	if !ok {
		return ErrNotFound
	}
	if e.State != Paused {
		return ErrTransition
	}
	if s.pending >= s.config.MaxPending {
		return ErrQueueFull
	}
	e.State = ResumeQueued
	s.jobs[id] = e
	s.enqueue(e.Request.Tenant, id)
	return nil
}

// ConfirmDeleted requires proof of remote absence. It safely frees the held
// reservation even when a previous Create/Resume result was uncertain.
// Never call it merely because an API request timed out.
func (s *Scheduler) ConfirmDeleted(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.jobs[id]
	if !ok {
		return ErrNotFound
	}
	if e.State == Deleted {
		return nil
	}
	if e.State == Queued || e.State == ResumeQueued {
		s.removeQueued(e.Request.Tenant, id)
	} else if e.State != Paused {
		s.releaseActive(e)
	}
	u := s.usage[e.Request.Tenant]
	u.Logical--
	s.usage[e.Request.Tenant] = u
	e.State = Deleted
	s.jobs[id] = e
	return nil
}

func (s *Scheduler) removeQueued(t, id string) {
	q := s.queues[t]
	for k, v := range q {
		if v == id {
			s.queues[t] = append(q[:k], q[k+1:]...)
			s.pending--
			break
		}
	}
	if len(s.queues[t]) == 0 {
		for i, name := range s.order {
			if name == t {
				s.order = append(s.order[:i], s.order[i+1:]...)
				if len(s.order) == 0 {
					s.nextIndex = 0
				} else {
					if i < s.nextIndex {
						s.nextIndex--
					}
					s.nextIndex %= len(s.order)
				}
				break
			}
		}
	}
}

func (s *Scheduler) releaseActive(e Entry) {
	s.used = s.used.sub(e.Request.Resources)
	u := s.usage[e.Request.Tenant]
	u.Active = u.Active.sub(e.Request.Resources)
	s.usage[e.Request.Tenant] = u
}

func (s *Scheduler) Get(id string) (Entry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.jobs[id]
	return e, ok
}

func (s *Scheduler) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	usage := make(map[string]Usage, len(s.usage))
	for k, v := range s.usage {
		usage[k] = v
	}
	return Snapshot{Capacity: s.config.Capacity, Used: s.used,
		Pending: s.pending, Tenant: usage}
}
