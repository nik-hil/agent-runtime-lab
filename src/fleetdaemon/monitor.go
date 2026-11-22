package fleetdaemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/fleet"
	"agent-runtime-lab/src/fleetaudit"
)

type monitorProvider interface {
	fleetaudit.Provider
	SetTimeout(context.Context, string, int) error
}

// MonitorOptions control read-only inventory polling and optional TTL renewals.
// All reads/mutations use the existing authenticated loopback-only E2B adapter.
type MonitorOptions struct {
	AuditInterval time.Duration
	AuditTimeout  time.Duration
	MaxWorkloads  int
	GlobalScan    bool
	RenewTTL      bool
	TTLSeconds    int
	RenewInterval time.Duration
	Logger        *slog.Logger
}

type MonitorStatus struct {
	Runs             uint64            `json:"runs"`
	Failures         uint64            `json:"failures"`
	LastAttempt      time.Time         `json:"last_attempt,omitempty"`
	LastSuccess      time.Time         `json:"last_success,omitempty"`
	LastError        string            `json:"last_error,omitempty"`
	HasReport        bool              `json:"has_report"`
	Report           fleetaudit.Report `json:"report"`
	CriticalConfirm  int               `json:"critical_confirmations"`
	Fenced           bool              `json:"fenced"`
	RenewAttempts    uint64            `json:"renew_attempts"`
	RenewFailures    uint64            `json:"renew_failures"`
	LastRenewAttempt time.Time         `json:"last_renew_attempt,omitempty"`
}

// Monitor is an optional supervisor layer around fleetd. The journal owner is
// still the fleet Controller. Audits work on an isolated journal snapshot;
// provider I/O never holds its mutex or the on-disk journal lock.
type Monitor struct {
	controller *fleet.Controller
	provider   monitorProvider
	opts       MonitorOptions
	mu         sync.Mutex
	status     MonitorStatus
	previous   string
}

func NewMonitor(c *fleet.Controller, p monitorProvider, opts MonitorOptions) (*Monitor, error) {
	if c == nil || p == nil || opts.AuditInterval < time.Second || opts.AuditInterval > 24*time.Hour ||
		opts.AuditTimeout < time.Second || opts.AuditTimeout > opts.AuditInterval ||
		opts.MaxWorkloads < 1 || opts.MaxWorkloads > 1000 {
		return nil, errors.New("audit requires controller, provider, interval [1s..24h], timeout [1s..interval], and max-workloads [1..1000]")
	}
	if opts.RenewTTL && (opts.TTLSeconds < 30 || opts.TTLSeconds > 3600 || opts.RenewInterval < time.Second ||
		opts.RenewInterval > time.Duration(opts.TTLSeconds)*time.Second/2) {
		return nil, errors.New("renew interval must be [1s..TTL/2] when TTL renewal is enabled")
	}
	if opts.GlobalScan {
		if _, ok := p.(fleetaudit.GlobalProvider); !ok {
			return nil, errors.New("provider-wide audit requires a full-inventory provider")
		}
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Monitor{controller: c, provider: p, opts: opts,
		status: MonitorStatus{Fenced: c.Status().Unsafe}}, nil
}

func (m *Monitor) Status() MonitorStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.status
}

func (m *Monitor) Run(ctx context.Context) {
	auditTicker := time.NewTicker(m.opts.AuditInterval)
	defer auditTicker.Stop()
	var renewDone chan struct{}
	if m.opts.RenewTTL {
		// Renewals must run independently: a slow metadata audit must NOT
		// postpone TTL extensions long enough for an otherwise healthy VM
		// to expire. Both loops obey the same shutdown context.
		renewDone = make(chan struct{})
		go func() {
			defer close(renewDone)
			ticker := time.NewTicker(m.opts.RenewInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					m.renew(ctx)
				}
			}
		}()
	}
	defer func() {
		if renewDone != nil {
			<-renewDone
		}
	}()
	// Do not issue a full inventory scan during the controller's initial
	// recovery cycle. The periodic audit begins after one complete interval.
	for {
		select {
		case <-ctx.Done():
			return
		case <-auditTicker.C:
			m.Audit(ctx)
		}
	}
}

// Audit is exported so deterministic tests and explicit local operators can
// run one complete pass without timers. It never deletes or creates a VM.
func (m *Monitor) Audit(ctx context.Context) {
	now := time.Now().UTC()
	snapshot := m.controller.SnapshotJournal()
	var report fleetaudit.Report
	var err error
	if len(snapshot.Workloads) > m.opts.MaxWorkloads {
		err = fmt.Errorf("audit has %d workloads, over configured max %d; incomplete", len(snapshot.Workloads), m.opts.MaxWorkloads)
	} else {
		bounded, cancel := context.WithTimeout(ctx, m.opts.AuditTimeout)
		if m.opts.GlobalScan {
			report, err = fleetaudit.RunGlobal(bounded, snapshot, m.provider.(fleetaudit.GlobalProvider))
		} else {
			report, err = fleetaudit.Run(bounded, snapshot, m.provider)
		}
		cancel()
	}
	critical := make([]string, 0)
	if err == nil {
		for _, f := range report.Findings {
			if f.Severity == "critical" {
				critical = append(critical, f.JobID+":"+f.Code)
			}
		}
		sort.Strings(critical)
	}
	// Requiring two consecutive complete matching critical audits reduces
	// false positives from eventual consistency during rapid transitions.
	fingerprint := strings.Join(critical, "|")
	m.mu.Lock()
	m.status.Runs++
	m.status.LastAttempt = now
	if err != nil {
		m.status.Failures++
		m.status.LastError = err.Error()
		m.previous = ""
		m.status.CriticalConfirm = 0
	} else {
		m.status.LastSuccess = now
		m.status.LastError = ""
		m.status.HasReport = true
		m.status.Report = report
		if fingerprint != "" && fingerprint == m.previous {
			m.status.CriticalConfirm++
		} else if fingerprint != "" {
			m.status.CriticalConfirm = 1
		} else {
			m.status.CriticalConfirm = 0
		}
		m.previous = fingerprint
	}
	confirm := m.status.CriticalConfirm >= 2
	m.mu.Unlock()
	if err != nil {
		m.opts.Logger.Error("fleet_inventory_audit_failed", "error", err.Error())
		return
	}
	if len(report.Findings) > 0 {
		m.opts.Logger.Warn("fleet_inventory_drift", "findings", len(report.Findings),
			"critical", len(critical), "scope", report.Scope)
	}
	if confirm {
		// The controller verifies that NONE of the live journal state changed
		// since the audited snapshot, before durably fencing admission.
		fenced, fenceErr := m.controller.FenceIfSnapshotCurrent(snapshot,
			"confirmed critical E2B drift: "+fingerprint)
		if fenceErr != nil {
			m.opts.Logger.Error("fleet_inventory_fence_failed", "error", fenceErr.Error())
		} else if fenced {
			m.mu.Lock()
			m.status.Fenced = true
			m.mu.Unlock()
			m.opts.Logger.Error("fleet_inventory_admission_fenced", "findings", len(critical))
		}
	}
}

// Renew applies the opt-in provider timeout update only to known, stable
// RUNNING workloads. It cannot create a VM, revive a deleted VM, or change the
// durable admission journal. Paused, deleting and uncertain work is skipped.
func (m *Monitor) renew(ctx context.Context) {
	if !m.opts.RenewTTL || ctx.Err() != nil {
		return
	}
	snapshot := m.controller.SnapshotJournal()
	if snapshot.Draining || snapshot.Unsafe || len(snapshot.Workloads) > m.opts.MaxWorkloads {
		return
	}
	ids := make([]string, 0, len(snapshot.Workloads))
	for id := range snapshot.Workloads {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if ctx.Err() != nil {
			return
		}
		w, e := snapshot.Workloads[id], snapshot.Scheduler.Jobs[id]
		if e.State != admission.Running || w.Desired != "running" || w.SandboxID == "" || w.CreatePending {
			continue
		}
		// Prevent renewals of workloads whose intent changed since snapshot.
		current := m.controller.SnapshotJournal()
		if current.Draining || current.Unsafe || current.Workloads[id] != w || current.Scheduler.Jobs[id] != e {
			continue
		}
		bounded, cancel := context.WithTimeout(ctx, 15*time.Second)
		err := m.provider.SetTimeout(bounded, w.SandboxID, m.opts.TTLSeconds)
		cancel()
		m.mu.Lock()
		m.status.RenewAttempts++
		m.status.LastRenewAttempt = time.Now().UTC()
		if err != nil {
			m.status.RenewFailures++
		}
		m.mu.Unlock()
		if err != nil {
			m.opts.Logger.Warn("fleet_ttl_renew_failed", "job_id", id, "error", err.Error())
		}
	}
}

// Handler composes with the existing private DrainHandler without altering
// public TCP exposure. /v1/metrics retains the original metrics and appends
// bounded-cardinality monitor counters; /v1/audit shows the last full report.
func (m *Monitor) Handler(base http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/audit":
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Cache-Control", "no-store")
			_ = json.NewEncoder(w).Encode(m.Status())
		case r.Method == http.MethodGet && r.URL.Path == "/v1/metrics":
			// Stream existing metrics to the client rather than buffering an
			// httptest recorder in production. Append only on HTTP 200.
			stream := &metricsWriter{ResponseWriter: w, status: http.StatusOK}
			base.ServeHTTP(stream, r)
			if stream.status != http.StatusOK {
				return
			}
			s := m.Status()
			fmt.Fprintf(w, "# HELP fleetd_inventory_audits_total Complete inventory audit attempts.\n# TYPE fleetd_inventory_audits_total counter\nfleetd_inventory_audits_total %d\n", s.Runs)
			fmt.Fprintf(w, "# HELP fleetd_inventory_failures_total Incomplete inventory audits.\n# TYPE fleetd_inventory_failures_total counter\nfleetd_inventory_failures_total %d\n", s.Failures)
			fmt.Fprintf(w, "# HELP fleetd_inventory_findings Current inventory discrepancy count.\n# TYPE fleetd_inventory_findings gauge\nfleetd_inventory_findings %d\n", len(s.Report.Findings))
			fmt.Fprintf(w, "# HELP fleetd_ttl_renew_attempts_total Sandbox TTL renew attempts.\n# TYPE fleetd_ttl_renew_attempts_total counter\nfleetd_ttl_renew_attempts_total %d\n", s.RenewAttempts)
			fmt.Fprintf(w, "# HELP fleetd_ttl_renew_failures_total Sandbox TTL renew failures.\n# TYPE fleetd_ttl_renew_failures_total counter\nfleetd_ttl_renew_failures_total %d\n", s.RenewFailures)
		default:
			base.ServeHTTP(w, r)
		}
	})
}

type metricsWriter struct {
	http.ResponseWriter
	status int
}

func (w *metricsWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
