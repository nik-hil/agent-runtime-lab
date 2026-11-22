package fleetdaemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/fleet"
)

func drainFixture(t *testing.T, b fleet.Runtime) (*fleet.Controller, *Daemon, *httptest.Server, context.CancelFunc, <-chan struct{}) {
	t.Helper()
	store, err := fleet.Open(filepath.Join(t.TempDir(), "fleet.json"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	c, err := fleet.New(store, b, testConfig(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	d, err := New(c, 100*time.Millisecond, 12)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(DrainHandler(d))
	t.Cleanup(srv.Close)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); d.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	return c, d, srv, cancel, done
}

func postDrainRequest(t *testing.T, url string, v any, want int) {
	t.Helper()
	var body []byte
	var err error
	if v != nil {
		body, err = json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if v != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != want {
		t.Fatalf("POST %s: got HTTP %d, expected %d", url, resp.StatusCode, want)
	}
}

func awaitDrainState(t *testing.T, c *fleet.Controller, id string, target admission.State) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if c.Status().Jobs[id].Entry.State == target {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("%s never reached %s: %+v", id, target, c.Status())
}

func TestDrainDeletesRunningAndQueuedAndBlocksNewWork(t *testing.T) {
	b := newRuntime()
	c, _, server, _, _ := drainFixture(t, b)
	for _, job := range []struct{ id, tenant string }{{"a1", "alpha"}, {"b1", "beta"}, {"a2", "alpha"}} {
		postDrainRequest(t, server.URL+"/v1/submit", map[string]string{"id": job.id, "tenant": job.tenant}, http.StatusAccepted)
	}
	awaitDrainState(t, c, "a1", admission.Running)
	awaitDrainState(t, c, "b1", admission.Running)
	if c.Status().Jobs["a2"].Entry.State != admission.Queued {
		t.Fatal("third VM exceeded capacity")
	}
	postDrainRequest(t, server.URL+"/v1/drain", nil, http.StatusAccepted)
	postDrainRequest(t, server.URL+"/v1/submit", map[string]string{"id": "new", "tenant": "alpha"}, http.StatusConflict)
	postDrainRequest(t, server.URL+"/v1/intent", map[string]string{"id": "a1", "desired": "running"}, http.StatusConflict)
	for _, id := range []string{"a1", "a2", "b1"} {
		awaitDrainState(t, c, id, admission.Deleted)
	}
	postDrainRequest(t, server.URL+"/v1/drain", nil, http.StatusAccepted) // retry is safe
	s := c.Status()
	if s.Admission.Used.CPU != 0 || s.Admission.Used.MemoryMiB != 0 || s.Admission.Pending != 0 {
		t.Fatalf("reservations leaked after drain: %+v", s.Admission)
	}
	b.mu.Lock()
	creates := b.creates
	b.mu.Unlock()
	if creates != 2 {
		t.Fatalf("drain launched queued VM instead of cancelling it; creates=%d", creates)
	}
}

func TestDrainRetainsUncertainCreateUntilExplicitRecovery(t *testing.T) {
	b := newRuntime()
	b.loseReply = true
	c, _, server, _, _ := drainFixture(t, b)
	postDrainRequest(t, server.URL+"/v1/submit", map[string]string{"id": "ambiguous", "tenant": "alpha"}, http.StatusAccepted)
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) && !c.Status().Jobs["ambiguous"].Workload.CreatePending {
		time.Sleep(15 * time.Millisecond)
	}
	if !c.Status().Jobs["ambiguous"].Workload.CreatePending {
		t.Fatal("did not enter uncertain Create state")
	}
	postDrainRequest(t, server.URL+"/v1/drain", nil, http.StatusAccepted)
	job := c.Status().Jobs["ambiguous"]
	if job.Workload.Desired != "deleted" || job.Entry.State == admission.Deleted || c.Status().Admission.Used.CPU == 0 {
		t.Fatalf("drain released ambiguous create: %+v", job)
	}
	postDrainRequest(t, server.URL+"/v1/recover", map[string]string{"id": "ambiguous"}, http.StatusAccepted)
	awaitDrainState(t, c, "ambiguous", admission.Deleted)
	b.mu.Lock()
	creates := b.creates
	b.mu.Unlock()
	if creates != 1 {
		t.Fatalf("duplicate creation during drain: %d", creates)
	}
}

type deleteFailRuntime struct {
	*runtime
	blocked atomic.Bool
}

func (b *deleteFailRuntime) Delete(ctx context.Context, id string) error {
	if b.blocked.Load() {
		return errors.New("simulated E2B 503")
	}
	return b.runtime.Delete(ctx, id)
}

func TestDrainKeepsReservationAcrossProviderDeleteFailure(t *testing.T) {
	b := &deleteFailRuntime{runtime: newRuntime()}
	b.blocked.Store(true)
	c, _, server, _, _ := drainFixture(t, b)
	postDrainRequest(t, server.URL+"/v1/submit", map[string]string{"id": "one", "tenant": "alpha"}, http.StatusAccepted)
	awaitDrainState(t, c, "one", admission.Running)
	postDrainRequest(t, server.URL+"/v1/drain", nil, http.StatusAccepted)
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) && c.Status().Jobs["one"].Workload.Desired != "deleted" {
		time.Sleep(15 * time.Millisecond)
	}
	st := c.Status()
	if st.Jobs["one"].Entry.State == admission.Deleted || st.Admission.Used.CPU != 1 {
		t.Fatal("released reserved CPU on failed E2B DELETE")
	}
	b.blocked.Store(false)
	awaitDrainState(t, c, "one", admission.Deleted)
	if c.Status().Admission.Used.CPU != 0 {
		t.Fatal("reservation not reclaimed after observed absence")
	}
}

func TestDrainFenceAndDeleteIntentsSurviveDaemonRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fleet.json")
	b := newRuntime()
	store, err := fleet.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	c, err := fleet.New(store, b, testConfig(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Submit("alpha", "job"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := c.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if c.Status().Jobs["job"].Entry.State != admission.Running {
		t.Fatal("test sandbox was not started")
	}
	d, err := New(c, 100*time.Millisecond, 12)
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	DrainHandler(d).ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/drain", nil))
	if recorder.Code != http.StatusAccepted || !c.Draining() {
		t.Fatalf("drain was not recorded: HTTP %d", recorder.Code)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = fleet.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	c, err = fleet.New(store, b, testConfig(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !c.Draining() || c.Status().Jobs["job"].Workload.Desired != "deleted" {
		t.Fatal("restart lost drain fence or deletion intent")
	}
	d, err = New(c, 100*time.Millisecond, 12)
	if err != nil {
		t.Fatal(err)
	}
	recorder = httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/submit", bytes.NewBufferString(`{"id":"other","tenant":"alpha"}`))
	request.Header.Set("Content-Type", "application/json")
	DrainHandler(d).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusConflict {
		t.Fatalf("admission reopened after restart: HTTP %d", recorder.Code)
	}
	for i := 0; i < 4 && c.Status().Jobs["job"].Entry.State != admission.Deleted; i++ {
		if _, err := c.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if c.Status().Jobs["job"].Entry.State != admission.Deleted || c.Status().Admission.Used.CPU != 0 {
		t.Fatal("pending deletion did not recover after restart")
	}
}
