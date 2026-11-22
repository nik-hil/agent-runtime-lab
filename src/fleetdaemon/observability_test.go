package fleetdaemon

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/fleet"
	"agent-runtime-lab/src/lifecycle"
)

type failingController struct {
	mu       sync.Mutex
	attempts int
	err      error
	state    fleet.Status
}

func (c *failingController) Status() fleet.Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}
func (*failingController) Submit(string, string) error                     { return nil }
func (*failingController) SetDesired(string, lifecycle.DesiredState) error { return nil }
func (*failingController) Recover(context.Context, string) error           { return nil }
func (c *failingController) Tick(context.Context) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.attempts++
	return false, c.err
}
func (c *failingController) Attempts() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.attempts
}

func TestBackoffIsExponentialBoundedAndClearsAfterSuccess(t *testing.T) {
	c := &failingController{err: errors.New("provider unavailable")}
	var logs bytes.Buffer
	d, err := NewWithOptions(c, Options{Interval: 100 * time.Millisecond, MaxSteps: 2,
		MaxBackoff: 350 * time.Millisecond, StuckAfter: time.Second,
		Logger: slog.New(slog.NewJSONHandler(&logs, nil))})
	if err != nil {
		t.Fatal(err)
	}
	for failures, want := range []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 350 * time.Millisecond, 350 * time.Millisecond} {
		if got := d.retryDelay(uint64(failures + 1)); got != want {
			t.Fatalf("retry %d got %s want %s", failures+1, got, want)
		}
	}
	d.reconcile(context.Background())
	d.mu.Lock()
	r := d.status
	d.mu.Unlock()
	if r.Failures != 1 || r.ConsecutiveFailures != 1 || r.LastError == "" || r.BackoffUntil.IsZero() {
		t.Fatalf("failure not observable: %+v", r)
	}
	if !strings.Contains(logs.String(), `"msg":"fleet_reconcile_failed"`) {
		t.Fatalf("missing structured error event: %s", logs.String())
	}
	c.mu.Lock()
	c.err = nil
	c.mu.Unlock()
	d.reconcile(context.Background())
	d.mu.Lock()
	r = d.status
	d.mu.Unlock()
	if r.ConsecutiveFailures != 0 || !r.BackoffUntil.IsZero() || r.LastSuccess.IsZero() || r.Failures != 1 || r.LastError == "" {
		t.Fatalf("successful pass must reset backoff but retain failure history: %+v", r)
	}
}

func TestRunBackoffSkipsRepeatedTimerRetries(t *testing.T) {
	c := &failingController{err: errors.New("temporary outage")}
	d, err := NewWithOptions(c, Options{
		Interval: 100 * time.Millisecond, MaxSteps: 1,
		MaxBackoff: 800 * time.Millisecond, StuckAfter: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); d.Run(ctx) }()
	time.Sleep(470 * time.Millisecond)
	cancel()
	<-done
	if n := c.Attempts(); n < 2 || n > 4 {
		t.Fatalf("expected bounded exponential retries, got %d attempts in 470ms", n)
	}
}

func TestStallDetectionAndPrivatePrometheusMetrics(t *testing.T) {
	const id = "client-controlled-job-xyz"
	c := &failingController{state: fleet.Status{
		Admission: admission.Snapshot{Used: admission.Resources{CPU: 2, MemoryMiB: 256}, Pending: 1},
		Jobs: map[string]fleet.JobStatus{id: {
			Entry:    admission.Entry{State: admission.Queued},
			Workload: fleet.Workload{Desired: "running"},
		}},
	}}
	var logs bytes.Buffer
	d, err := NewWithOptions(c, Options{Interval: 100 * time.Millisecond, MaxSteps: 1,
		MaxBackoff: time.Second, StuckAfter: time.Second,
		Logger: slog.New(slog.NewJSONHandler(&logs, nil))})
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC()
	d.observeJobs(base)
	d.observeJobs(base.Add(1200 * time.Millisecond))
	d.observeJobs(base.Add(1500 * time.Millisecond))
	d.mu.Lock()
	r := d.status
	d.mu.Unlock()
	if len(r.StuckJobs) != 1 || r.StuckJobs[0].ID != id {
		t.Fatalf("queued job should be reported stalled: %+v", r)
	}
	if n := strings.Count(logs.String(), "fleet_workload_stalled"); n != 1 {
		t.Fatalf("stalled job should warn only once per unchanged state: %d", n)
	}
	resp := httptest.NewRecorder()
	d.Handler().ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/v1/metrics", nil))
	text := resp.Body.String()
	if resp.Code != http.StatusOK || !strings.Contains(resp.Header().Get("Content-Type"), "text/plain") ||
		!strings.Contains(text, "fleetd_stuck_jobs 1") ||
		!strings.Contains(text, "fleetd_admission_used_cpu 2") ||
		!strings.Contains(text, `fleetd_workloads{state="queued"} 1`) || strings.Contains(text, id) {
		t.Fatalf("invalid or sensitive metrics response: %s", text)
	}
	c.mu.Lock()
	c.state.Jobs[id] = fleet.JobStatus{
		Entry:    admission.Entry{State: admission.Running},
		Workload: fleet.Workload{Desired: "running"},
	}
	c.mu.Unlock()
	d.observeJobs(base.Add(2 * time.Second))
	d.mu.Lock()
	r = d.status
	d.mu.Unlock()
	if len(r.StuckJobs) != 0 {
		t.Fatalf("converged job still stalled: %+v", r.StuckJobs)
	}
}

func TestOperationalOptionsRejectInvalidBounds(t *testing.T) {
	c := &failingController{}
	for _, options := range []Options{
		{Interval: 100 * time.Millisecond, MaxSteps: 1, MaxBackoff: 50 * time.Millisecond},
		{Interval: 100 * time.Millisecond, MaxSteps: 1, StuckAfter: 50 * time.Millisecond},
		{Interval: 100 * time.Millisecond, MaxSteps: 1, MaxBackoff: 2 * time.Hour},
	} {
		if _, err := NewWithOptions(c, options); err == nil {
			t.Fatalf("accepted invalid options %+v", options)
		}
	}
}
