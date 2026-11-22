// Package fleetdaemon provides a local control socket and periodic reconciliation
// for the single-host durable E2B fleet. The daemon is the only state-file owner;
// clients request desired-state changes over a private Unix socket.
package fleetdaemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/fleet"
	"agent-runtime-lab/src/lifecycle"
)

// Controller is the subset of fleet.Controller needed by the worker and API.
// The live implementation serializes state changes and journals each mutation.
type Controller interface {
	Status() fleet.Status
	Submit(tenant, id string) error
	SetDesired(id string, desired lifecycle.DesiredState) error
	Recover(ctx context.Context, id string) error
	Tick(ctx context.Context) (bool, error)
}

type ReconcilerStatus struct {
	Cycles              uint64     `json:"cycles"`
	Failures            uint64     `json:"failures"`
	Transitions         uint64     `json:"transitions"`
	ConsecutiveFailures uint64     `json:"consecutive_failures"`
	LastAttempt         time.Time  `json:"last_attempt,omitempty"`
	LastSuccess         time.Time  `json:"last_success,omitempty"`
	LastError           string     `json:"last_error,omitempty"`
	LastErrorAt         time.Time  `json:"last_error_at,omitempty"`
	BackoffUntil        time.Time  `json:"backoff_until,omitempty"`
	StuckJobs           []StuckJob `json:"stuck_jobs,omitempty"`
}

// StuckJob is a locally observed lack of convergence, not proof of a dead VM.
// Queued jobs can be waiting legitimately for resources. Times reset on daemon
// restart because this monitoring state is deliberately not persisted.
type StuckJob struct {
	ID         string `json:"id"`
	State      string `json:"state"`
	Desired    string `json:"desired"`
	AgeSeconds int64  `json:"age_seconds"`
}

type Status struct {
	Fleet      fleet.Status     `json:"fleet"`
	Reconciler ReconcilerStatus `json:"reconciler"`
}

// Daemon runs a bounded number of reconciliation steps per timer tick and on
// demand. It never retries an ambiguous Create itself; the fleet controller
// retains the creation fence until an explicit Recover request is successful.
type Daemon struct {
	controller Controller
	interval   time.Duration
	maxSteps   int
	maxBackoff time.Duration
	stuckAfter time.Duration
	logger     *slog.Logger
	wake       chan struct{}
	mu         sync.Mutex
	status     ReconcilerStatus
	tracked    map[string]trackedJob
}

func New(controller Controller, interval time.Duration, maxSteps int) (*Daemon, error) {
	return NewWithOptions(controller, Options{Interval: interval, MaxSteps: maxSteps})
}

// Options leaves existing callers on safe defaults; the daemon CLI exposes
// explicit bounds for retries and detection. Logger may be nil to disable logs
// (useful in unit tests). No workload content or provider credentials are logged.
type Options struct {
	Interval   time.Duration
	MaxSteps   int
	MaxBackoff time.Duration
	StuckAfter time.Duration
	Logger     *slog.Logger
}

func NewWithOptions(controller Controller, opts Options) (*Daemon, error) {
	if controller == nil || opts.Interval < 100*time.Millisecond || opts.Interval > time.Hour || opts.MaxSteps < 1 || opts.MaxSteps > 100 {
		return nil, errors.New("controller, interval [100ms..1h] and maxSteps [1..100] required")
	}
	if opts.MaxBackoff == 0 {
		opts.MaxBackoff = 30 * time.Second
		if opts.Interval > opts.MaxBackoff {
			opts.MaxBackoff = opts.Interval
		}
	}
	if opts.StuckAfter == 0 {
		opts.StuckAfter = 2 * time.Minute
		if opts.Interval > opts.StuckAfter {
			opts.StuckAfter = opts.Interval
		}
	}
	if opts.MaxBackoff < opts.Interval || opts.MaxBackoff > time.Hour || opts.StuckAfter < opts.Interval || opts.StuckAfter > 24*time.Hour {
		return nil, errors.New("max backoff must be [interval..1h], stuck-after [interval..24h]")
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Daemon{controller: controller, interval: opts.Interval, maxSteps: opts.MaxSteps,
		maxBackoff: opts.MaxBackoff, stuckAfter: opts.StuckAfter, logger: opts.Logger,
		wake: make(chan struct{}, 1), tracked: make(map[string]trackedJob)}, nil
}

func (d *Daemon) signal() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

// Run blocks until ctx cancels. It does not release the fleet store lock;
// callers must shut down the HTTP listener before closing the store.
func (d *Daemon) Run(ctx context.Context) {
	timer := time.NewTicker(d.interval)
	defer timer.Stop()
	d.reconcile(ctx) // resume interrupted work immediately on daemon startup
	for {
		var forced bool
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		case <-d.wake:
			forced = true // an explicit operator action may bypass a retry delay
		}
		d.mu.Lock()
		waiting := !forced && time.Now().Before(d.status.BackoffUntil)
		d.mu.Unlock()
		if waiting {
			d.observeJobs(time.Now().UTC()) // still detect stalls during backoff
			continue
		}
		d.reconcile(ctx)
	}
}

func (d *Daemon) reconcile(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	begin := time.Now()
	var err error
	var transitions uint64
	for i := 0; i < d.maxSteps; i++ {
		var changed bool
		changed, err = d.controller.Tick(ctx)
		if changed {
			transitions++
		}
		if err != nil || !changed {
			break
		}
	}
	now := time.Now().UTC()
	d.mu.Lock()
	d.status.Cycles++
	d.status.Transitions += transitions
	d.status.LastAttempt = now
	if err != nil {
		d.status.LastError = err.Error()
		d.status.LastErrorAt = now
		d.status.Failures++
		d.status.ConsecutiveFailures++
		d.status.BackoffUntil = now.Add(d.retryDelay(d.status.ConsecutiveFailures))
	} else {
		d.status.ConsecutiveFailures = 0
		d.status.BackoffUntil = time.Time{}
		d.status.LastSuccess = now
	}
	cycle := d.status.Cycles
	backoff := d.status.BackoffUntil
	d.mu.Unlock()
	d.observeJobs(now)
	if err != nil {
		d.logger.Error("fleet_reconcile_failed", "cycle", cycle, "error", err.Error(),
			"backoff_until", backoff, "transitions", transitions, "elapsed_ms", time.Since(begin).Milliseconds())
	} else if transitions > 0 {
		d.logger.Info("fleet_reconcile_progress", "cycle", cycle, "transitions", transitions,
			"elapsed_ms", time.Since(begin).Milliseconds())
	}
}

// Handler is deliberately HTTP over a filesystem-permission-restricted Unix
// socket, never a TCP listener. There is no browser CORS or public API auth.
func (d *Daemon) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, r *http.Request) {
		d.mu.Lock()
		s := d.status
		d.mu.Unlock()
		respond(w, http.StatusOK, Status{Fleet: d.controller.Status(), Reconciler: s})
	})
	mux.HandleFunc("GET /v1/metrics", func(w http.ResponseWriter, r *http.Request) {
		d.mu.Lock()
		s := d.status
		d.mu.Unlock()
		writeMetrics(w, s, d.controller.Status())
	})
	mux.HandleFunc("POST /v1/submit", func(w http.ResponseWriter, r *http.Request) {
		var req struct{ ID, Tenant string }
		if !parse(w, r, &req) {
			return
		}
		if invalidID(req.ID) || invalidID(req.Tenant) {
			fail(w, http.StatusBadRequest, "id and tenant must be 1-128 safe characters")
			return
		}
		if err := d.controller.Submit(req.Tenant, req.ID); err != nil {
			fail(w, classify(err), err.Error())
			return
		}
		d.signal()
		respond(w, http.StatusAccepted, d.controller.Status())
	})
	mux.HandleFunc("POST /v1/intent", func(w http.ResponseWriter, r *http.Request) {
		var req struct{ ID, Desired string }
		if !parse(w, r, &req) {
			return
		}
		if invalidID(req.ID) {
			fail(w, http.StatusBadRequest, "invalid id")
			return
		}
		if req.Desired != string(lifecycle.WantRunning) && req.Desired != string(lifecycle.WantPaused) && req.Desired != string(lifecycle.WantDeleted) {
			fail(w, http.StatusBadRequest, "desired must be running, paused, or deleted")
			return
		}
		if err := d.controller.SetDesired(req.ID, lifecycle.DesiredState(req.Desired)); err != nil {
			fail(w, classify(err), err.Error())
			return
		}
		d.signal()
		respond(w, http.StatusAccepted, d.controller.Status())
	})
	mux.HandleFunc("POST /v1/recover", func(w http.ResponseWriter, r *http.Request) {
		var req struct{ ID string }
		if !parse(w, r, &req) {
			return
		}
		if invalidID(req.ID) {
			fail(w, http.StatusBadRequest, "invalid id")
			return
		}
		// The HTTP request context is bounded independently of the worker loop.
		ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
		defer cancel()
		if err := d.controller.Recover(ctx, req.ID); err != nil {
			fail(w, classify(err), err.Error())
			return
		}
		d.signal()
		respond(w, http.StatusAccepted, d.controller.Status())
	})
	return mux
}

func invalidID(id string) bool {
	if id == "" || len(id) > 128 {
		return true
	}
	for _, c := range id {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
			return true
		}
	}
	return false
}

func parse(w http.ResponseWriter, r *http.Request, value any) bool {
	if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
		fail(w, http.StatusUnsupportedMediaType, "JSON content type required")
		return false
	}
	decoder := json.NewDecoder(io.LimitReader(r.Body, 4096))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		fail(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON request: %v", err))
		return false
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		fail(w, http.StatusBadRequest, "exactly one JSON object required")
		return false
	}
	return true
}

func classify(err error) int {
	switch {
	case errors.Is(err, admission.ErrInvalidRequest), errors.Is(err, admission.ErrNotFound):
		return http.StatusBadRequest
	case errors.Is(err, admission.ErrQuota), errors.Is(err, admission.ErrConflict), errors.Is(err, admission.ErrQueueFull), errors.Is(err, admission.ErrTransition),
		errors.Is(err, lifecycle.ErrTerminal):
		return http.StatusConflict
	default:
		return http.StatusServiceUnavailable
	}
}

func respond(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func fail(w http.ResponseWriter, status int, message string) {
	respond(w, status, map[string]string{"error": message})
}
