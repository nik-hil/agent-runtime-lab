package fleetdaemon

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/fleet"
	"agent-runtime-lab/src/lifecycle"
)

type monitorFake struct {
	*runtime
	renewAttempts atomic.Int64
	listFailures  atomic.Bool
}

func (f *monitorFake) FindByLogicalKey(ctx context.Context, key string) ([]string, error) {
	if f.listFailures.Load() {
		return nil, errors.New("simulated HTTP 503")
	}
	return f.runtime.FindByLogicalKey(ctx, key)
}

func (f *monitorFake) SetTimeout(_ context.Context, _ string, seconds int) error {
	if seconds != 300 {
		return errors.New("unexpected TTL")
	}
	f.renewAttempts.Add(1)
	return nil
}

func monitorFixture(t *testing.T) (*fleet.Controller, *monitorFake, *Monitor) {
	t.Helper()
	store, err := fleet.Open(filepath.Join(t.TempDir(), "monitor.json"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	backend := &monitorFake{runtime: newRuntime()}
	c, err := fleet.New(store, backend, testConfig(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	m, err := NewMonitor(c, backend, MonitorOptions{
		AuditInterval: time.Second, AuditTimeout: time.Second, MaxWorkloads: 10,
		RenewTTL: true, TTLSeconds: 300, RenewInterval: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return c, backend, m
}

func TestMonitorAuditsHealthyVMAndPrivateMetrics(t *testing.T) {
	c, _, m := monitorFixture(t)
	if err := c.Submit("alpha", "alive"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := c.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if c.Status().Jobs["alive"].Entry.State != admission.Running {
		t.Fatal("not running")
	}
	m.Audit(context.Background())
	s := m.Status()
	if s.Runs != 1 || s.Failures != 0 || !s.HasReport || s.Report.RequiresReview || s.Fenced {
		t.Fatalf("healthy monitor audit: %+v", s)
	}
	base := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("fleetd_reconcile_cycles_total 1\n"))
	})
	resp := httptest.NewRecorder()
	m.Handler(base).ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/v1/metrics", nil))
	if resp.Code != http.StatusOK || !strings.Contains(resp.Body.String(), "fleetd_inventory_audits_total 1") ||
		!strings.Contains(resp.Body.String(), "fleetd_reconcile_cycles_total 1") ||
		!strings.Contains(resp.Body.String(), "fleetd_ttl_renew_attempts_total 0") {
		t.Fatalf("metrics response: %s", resp.Body.String())
	}
	resp = httptest.NewRecorder()
	m.Handler(base).ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/v1/audit", nil))
	if !strings.Contains(resp.Body.String(), `"has_report":true`) {
		t.Fatal("audit status not exposed")
	}
}

func TestMonitorTwoCriticalAuditsFenceAdmissionAndKeepReservations(t *testing.T) {
	c, backend, m := monitorFixture(t)
	if err := c.Submit("alpha", "lost"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := c.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	vmID := c.Status().Jobs["lost"].Workload.SandboxID
	backend.mu.Lock()
	delete(backend.states, vmID) // E2B expired/deleted VM without controller intent.
	backend.mu.Unlock()
	m.Audit(context.Background())
	if c.Status().Unsafe || m.Status().CriticalConfirm != 1 {
		t.Fatal("fenced after only one sample")
	}
	m.Audit(context.Background())
	if !c.Status().Unsafe || !m.Status().Fenced || c.Status().Admission.Used.CPU != 1 ||
		c.Status().Jobs["lost"].Workload.SandboxID != vmID {
		t.Fatalf("critical audit released reservation or forgot ownership: %+v", c.Status())
	}
	if err := c.Submit("alpha", "unsafe-admission"); !errors.Is(err, fleet.ErrUnsafeCapacity) {
		t.Fatalf("unsafe journal admitted new work: %v", err)
	}
	if _, err := c.BeginDrain(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.Status().Jobs["lost"].Entry.State != admission.Deleted || c.Status().Admission.Used.CPU != 0 {
		t.Fatalf("explicit drain could not release after provider absence: %+v", c.Status())
	}
}

func TestMonitorProviderOutageNeverInfersAbsenceOrFences(t *testing.T) {
	c, backend, m := monitorFixture(t)
	if err := c.Submit("alpha", "healthy"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := c.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	backend.listFailures.Store(true)
	m.Audit(context.Background())
	m.Audit(context.Background())
	if s := m.Status(); s.Failures != 2 || s.HasReport || s.Fenced || c.Status().Unsafe {
		t.Fatalf("provider outage treated as absence: %+v", s)
	}
	backend.listFailures.Store(false)
	m.Audit(context.Background())
	if s := m.Status(); s.Failures != 2 || !s.HasReport || s.Report.RequiresReview {
		t.Fatalf("provider recovery did not reset errors: %+v", s)
	}
}

func TestMonitorTTLRenewIsExplicitAndSkipsDeletedOrPaused(t *testing.T) {
	c, backend, m := monitorFixture(t)
	if err := c.Submit("alpha", "renew"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := c.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	m.renew(context.Background())
	if backend.renewAttempts.Load() != 1 {
		t.Fatal("expected one renewal")
	}
	if err := c.SetDesired("renew", lifecycle.WantPaused); err != nil {
		t.Fatal(err)
	}
	m.renew(context.Background())
	if backend.renewAttempts.Load() != 1 {
		t.Fatal("renewed workload with pause intent")
	}
	if _, err := c.BeginDrain(); err != nil {
		t.Fatal(err)
	}
	m.renew(context.Background())
	if backend.renewAttempts.Load() != 1 {
		t.Fatal("renewed a draining workload")
	}
	if m.Status().RenewAttempts != 1 {
		t.Fatal("renew counter mismatch")
	}
}

func TestMonitorRejectsUnsafeOptions(t *testing.T) {
	c, b, _ := monitorFixture(t)
	for _, opts := range []MonitorOptions{
		{AuditInterval: time.Second, AuditTimeout: 2 * time.Second, MaxWorkloads: 10},
		{AuditInterval: time.Second, AuditTimeout: time.Second, MaxWorkloads: 0},
		{AuditInterval: time.Second, AuditTimeout: time.Second, MaxWorkloads: 10, RenewTTL: true, TTLSeconds: 300, RenewInterval: 200 * time.Second},
		{AuditInterval: time.Second, AuditTimeout: time.Second, MaxWorkloads: 10, GlobalScan: true},
	} {
		if _, err := NewMonitor(c, b, opts); err == nil {
			t.Fatalf("unsafe options accepted: %+v", opts)
		}
	}
}
