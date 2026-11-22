package fleet_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/durable"
	"agent-runtime-lab/src/e2bapi"
	"agent-runtime-lab/src/fleet"
	"agent-runtime-lab/src/fleetdaemon"
)

// faultProvider speaks the actual HTTP contract used by e2bapi.Backend. The
// server is loopback-only and is never connected to a live E2B installation.
// Crucially, a new POST always allocates a NEW VM: this test provider does not
// silently provide idempotency that the real E2B Create API does not promise.
type faultProvider struct {
	mu              sync.Mutex
	vms             map[string]string // sandbox ID -> exact metadata correlation key
	creates         int
	deletes         int
	getFaults       []int
	listFaults      []int
	deleteFaults    []int
	createFault     int // 0, 429-before-create, 503-after-create
	deleteLostReply bool
	created         chan struct{}
	createGate      chan struct{}
}

func newFaultProvider() *faultProvider {
	return &faultProvider{vms: make(map[string]string)}
}

func takeFault(queue *[]int) int {
	if len(*queue) == 0 {
		return 0
	}
	status := (*queue)[0]
	*queue = (*queue)[1:]
	return status
}

func (p *faultProvider) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("X-API-Key") != "fault-test-key" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/sandboxes":
		var request struct {
			Metadata map[string]string `json:"metadata"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		key := request.Metadata["agent_runtime_lab_key"]
		if key == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		p.mu.Lock()
		if p.createFault == http.StatusTooManyRequests {
			p.creates++
			p.mu.Unlock()
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		p.creates++
		id := fmt.Sprintf("sbx_%d", p.creates)
		p.vms[id] = key
		status := p.createFault
		gate, created := p.createGate, p.created
		p.mu.Unlock()
		if created != nil {
			select {
			case created <- struct{}{}:
			default:
			}
		}
		if gate != nil {
			select {
			case <-gate:
			case <-r.Context().Done():
				return
			}
		}
		if status == http.StatusServiceUnavailable {
			w.WriteHeader(http.StatusServiceUnavailable) // remote VM exists despite failed reply
			return
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{"sandboxID": id})

	case r.Method == http.MethodGet && r.URL.Path == "/v2/sandboxes":
		p.mu.Lock()
		status := takeFault(&p.listFaults)
		out := make([]map[string]any, 0, len(p.vms))
		for id, key := range p.vms {
			out = append(out, map[string]any{"sandboxID": id,
				"metadata": map[string]string{"agent_runtime_lab_key": key}})
		}
		p.mu.Unlock()
		if status != 0 {
			w.WriteHeader(status)
			return
		}
		_ = json.NewEncoder(w).Encode(out)

	case strings.HasPrefix(r.URL.Path, "/sandboxes/") && r.Method == http.MethodGet:
		id := strings.TrimPrefix(r.URL.Path, "/sandboxes/")
		p.mu.Lock()
		status := takeFault(&p.getFaults)
		_, exists := p.vms[id]
		p.mu.Unlock()
		if status != 0 {
			w.WriteHeader(status)
			return
		}
		if !exists {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"sandboxID": id, "state": "running", "cpuCount": 1, "memoryMB": 256,
		})

	case strings.HasPrefix(r.URL.Path, "/sandboxes/") && r.Method == http.MethodDelete:
		id := strings.TrimPrefix(r.URL.Path, "/sandboxes/")
		p.mu.Lock()
		p.deletes++
		status := takeFault(&p.deleteFaults)
		lost := p.deleteLostReply
		if status == 0 || lost {
			delete(p.vms, id)
		}
		p.mu.Unlock()
		if status != 0 {
			w.WriteHeader(status)
			return
		}
		if lost {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func faultConfig() fleet.Config {
	size := admission.Resources{CPU: 1, MemoryMiB: 256}
	cap := admission.Resources{CPU: 2, MemoryMiB: 512}
	return fleet.Config{TemplateSize: size, Admission: admission.Config{
		Capacity: cap, MaxPending: 4,
		Tenants: map[string]admission.Policy{"alpha": {MaxLogical: 2, MaxActive: cap}},
	}}
}

func faultBackend(t *testing.T, serverURL string) *e2bapi.Backend {
	t.Helper()
	b, err := e2bapi.New(e2bapi.Config{BaseURL: serverURL, APIKey: "fault-test-key", TTLSeconds: 300})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func openFaultController(t *testing.T, state string, b *e2bapi.Backend) (*fleet.Controller, *fleet.Store) {
	t.Helper()
	s, err := fleet.Open(state)
	if err != nil {
		t.Fatal(err)
	}
	c, err := fleet.New(s, b, faultConfig(), 150*time.Millisecond)
	if err != nil {
		_ = s.Close()
		t.Fatal(err)
	}
	return c, s
}

func mustTick(t *testing.T, c *fleet.Controller) bool {
	t.Helper()
	changed, err := c.Tick(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return changed
}

func assertHeld(t *testing.T, c *fleet.Controller, id string, state admission.State) {
	t.Helper()
	s := c.Status()
	if s.Jobs[id].Entry.State != state || s.Admission.Used.CPU != 1 ||
		s.Admission.Used.MemoryMiB != 256 {
		t.Fatalf("reservation released or state changed prematurely: %+v", s)
	}
}

func assertDeleted(t *testing.T, c *fleet.Controller, id string) {
	t.Helper()
	s := c.Status()
	if s.Jobs[id].Entry.State != admission.Deleted || s.Jobs[id].Workload.SandboxID != "" ||
		s.Admission.Used.CPU != 0 || s.Admission.Used.MemoryMiB != 0 || s.Admission.Pending != 0 {
		t.Fatalf("cleanup incomplete: %+v", s)
	}
}

func TestHTTPFaultsLostCreateReply429503AndRecovery(t *testing.T) {
	provider := newFaultProvider()
	provider.createFault = http.StatusServiceUnavailable // commit remote VM, lose successful reply
	server := httptest.NewServer(provider)
	defer server.Close()
	b := faultBackend(t, server.URL)
	path := filepath.Join(t.TempDir(), "fleet.json")
	c, store := openFaultController(t, path, b)
	if err := c.Submit("alpha", "job"); err != nil {
		t.Fatal(err)
	}
	changed, err := c.Tick(context.Background())
	if !changed || !errors.Is(err, fleet.ErrCreateUncertain) {
		t.Fatalf("lost reply not fenced: changed=%v err=%v", changed, err)
	}
	assertHeld(t, c, "job", admission.Uncertain)
	if !c.Status().Jobs["job"].Workload.CreatePending {
		t.Fatal("ambiguous Create lost its durable create_pending fence")
	}
	if changed := mustTick(t, c); changed {
		t.Fatal("controller retried ambiguous Create")
	}
	provider.mu.Lock()
	provider.listFaults = []int{http.StatusServiceUnavailable}
	provider.mu.Unlock()
	if err := c.Recover(context.Background(), "job"); err == nil {
		t.Fatal("accepted E2B metadata list outage as no sandbox")
	}
	assertHeld(t, c, "job", admission.Uncertain)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	c, store = openFaultController(t, path, b)
	defer store.Close()
	if changed := mustTick(t, c); changed {
		t.Fatal("restarted controller repeated an ambiguous Create")
	}
	if err := c.Recover(context.Background(), "job"); err != nil {
		t.Fatalf("metadata recovery failed: %v", err)
	}
	provider.mu.Lock()
	provider.getFaults = []int{http.StatusServiceUnavailable}
	provider.mu.Unlock()
	if _, err := c.Tick(context.Background()); err == nil {
		t.Fatal("observed running state during E2B GET outage")
	}
	assertHeld(t, c, "job", admission.Uncertain)
	mustTick(t, c) // GET is healthy; Running is now observed
	assertHeld(t, c, "job", admission.Running)
	if n, err := c.BeginDrain(); err != nil || n != 1 {
		t.Fatalf("durable drain: n=%d err=%v", n, err)
	}
	provider.mu.Lock()
	provider.deleteFaults = []int{http.StatusTooManyRequests, http.StatusServiceUnavailable}
	provider.mu.Unlock()
	for _, code := range []int{http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		if changed, err := c.Tick(context.Background()); !changed || err == nil ||
			!strings.Contains(err.Error(), fmt.Sprintf("HTTP %d", code)) {
			t.Fatalf("failed E2B delete %d not observed: changed=%v err=%v", code, changed, err)
		}
		assertHeld(t, c, "job", admission.Running)
	}
	mustTick(t, c) // E2B DELETE 204 alone is NOT proof of absence
	assertHeld(t, c, "job", admission.Running)
	mustTick(t, c) // E2B GET 404 confirms absence
	assertDeleted(t, c, "job")
	provider.mu.Lock()
	creates, deletes, remaining := provider.creates, provider.deletes, len(provider.vms)
	provider.mu.Unlock()
	if creates != 1 || deletes != 3 || remaining != 0 {
		t.Fatalf("duplicate Create or leaked VM: creates=%d deletes=%d remaining=%d", creates, deletes, remaining)
	}
}

func TestRejectedCreateWithoutRemoteMatchRemainsFenced(t *testing.T) {
	provider := newFaultProvider()
	provider.createFault = http.StatusTooManyRequests // no remote VM created
	server := httptest.NewServer(provider)
	defer server.Close()
	b := faultBackend(t, server.URL)
	path := filepath.Join(t.TempDir(), "fleet.json")
	c, store := openFaultController(t, path, b)
	if err := c.Submit("alpha", "rate-limited"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Tick(context.Background()); !errors.Is(err, fleet.ErrCreateUncertain) {
		t.Fatalf("HTTP 429 Create wasn't treated as ambiguous: %v", err)
	}
	assertHeld(t, c, "rate-limited", admission.Uncertain)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	c, store = openFaultController(t, path, b)
	defer store.Close()
	if err := c.Recover(context.Background(), "rate-limited"); !errors.Is(err, durable.ErrNoMatch) {
		t.Fatalf("missing remote Create should remain unresolved: %v", err)
	}
	if n, err := c.BeginDrain(); n != 1 || err != nil {
		t.Fatalf("drain after ambiguous Create: n=%d err=%v", n, err)
	}
	for i := 0; i < 5; i++ {
		if changed := mustTick(t, c); changed {
			t.Fatal("uncertain Create was silently recreated or forgotten")
		}
	}
	assertHeld(t, c, "rate-limited", admission.Uncertain)
	provider.mu.Lock()
	creates := provider.creates
	provider.mu.Unlock()
	if creates != 1 {
		t.Fatalf("unsafe automatic retry after missing metadata match: creates=%d", creates)
	}
}

func TestCreateTimeoutAfterRemoteCommitCanRecoverExactlyOnce(t *testing.T) {
	provider := newFaultProvider()
	provider.created = make(chan struct{}, 1)
	provider.createGate = make(chan struct{}) // withhold the HTTP response
	server := httptest.NewServer(provider)
	defer server.Close()
	b := faultBackend(t, server.URL)
	path := filepath.Join(t.TempDir(), "fleet.json")
	c, store := openFaultController(t, path, b)
	defer store.Close()
	if err := c.Submit("alpha", "timeout"); err != nil {
		t.Fatal(err)
	}
	changed, err := c.Tick(context.Background()) // controller HTTP deadline is 150ms
	if !changed || !errors.Is(err, fleet.ErrCreateUncertain) {
		t.Fatalf("Create timeout must retain ambiguity: changed=%v err=%v", changed, err)
	}
	select {
	case <-provider.created:
	default:
		t.Fatal("timeout occurred before provider allocated the VM")
	}
	close(provider.createGate)
	assertHeld(t, c, "timeout", admission.Uncertain)
	if err := c.Recover(context.Background(), "timeout"); err != nil {
		t.Fatal(err)
	}
	mustTick(t, c)
	assertHeld(t, c, "timeout", admission.Running)
	if _, err := c.BeginDrain(); err != nil {
		t.Fatal(err)
	}
	mustTick(t, c)
	mustTick(t, c)
	assertDeleted(t, c, "timeout")
	provider.mu.Lock()
	creates := provider.creates
	provider.mu.Unlock()
	if creates != 1 {
		t.Fatalf("Create was repeated after a timeout: %d", creates)
	}
}

func TestDeleteCommittedButReplyLostAcrossRestart(t *testing.T) {
	provider := newFaultProvider()
	provider.deleteLostReply = true
	server := httptest.NewServer(provider)
	defer server.Close()
	b := faultBackend(t, server.URL)
	path := filepath.Join(t.TempDir(), "fleet.json")
	c, store := openFaultController(t, path, b)
	if err := c.Submit("alpha", "cleanup"); err != nil {
		t.Fatal(err)
	}
	mustTick(t, c) // create
	mustTick(t, c) // observe running
	assertHeld(t, c, "cleanup", admission.Running)
	if _, err := c.BeginDrain(); err != nil {
		t.Fatal(err)
	}
	if changed, err := c.Tick(context.Background()); !changed || err == nil {
		t.Fatalf("lost DELETE acknowledgement should report an error: %v %v", changed, err)
	}
	assertHeld(t, c, "cleanup", admission.Running)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	c, store = openFaultController(t, path, b)
	defer store.Close()
	if !c.Draining() || c.Status().Jobs["cleanup"].Workload.Desired != "deleted" {
		t.Fatal("restart lost durable drain intent")
	}
	mustTick(t, c) // provider returns absent; no repeat DELETE needed
	assertDeleted(t, c, "cleanup")
	provider.mu.Lock()
	creates, deletes := provider.creates, provider.deletes
	provider.mu.Unlock()
	if creates != 1 || deletes != 1 {
		t.Fatalf("restarted controller repeated remote operation: creates=%d deletes=%d", creates, deletes)
	}
}

// This test kills a real Go child process while the E2B HTTP server is
// withholding its Create response AFTER allocating the sandbox. It exercises
// the pre-Create fsynced fence through a genuine SIGKILL, not a fake panic.
func TestDaemonSIGKILLDuringRemoteCreate(t *testing.T) {
	if os.Getenv("FLEET_FAULT_CHILD") == "create" {
		path := os.Getenv("FLEET_FAULT_JOURNAL")
		b := faultBackend(t, os.Getenv("FLEET_FAULT_URL"))
		c, store := openFaultController(t, path, b)
		defer store.Close()
		if err := c.Submit("alpha", "crash"); err != nil {
			t.Fatal(err)
		}
		d, err := fleetdaemon.New(c, 100*time.Millisecond, 4)
		if err != nil {
			t.Fatal(err)
		}
		d.Run(context.Background()) // parent terminates process mid-Create
		return
	}
	provider := newFaultProvider()
	provider.created = make(chan struct{}, 1)
	provider.createGate = make(chan struct{})
	server := httptest.NewServer(provider)
	defer server.Close()
	path := filepath.Join(t.TempDir(), "fleet.json")
	child := exec.Command(os.Args[0], "-test.run=^TestDaemonSIGKILLDuringRemoteCreate$")
	child.Env = append(os.Environ(), "FLEET_FAULT_CHILD=create", "FLEET_FAULT_JOURNAL="+path,
		"FLEET_FAULT_URL="+server.URL)
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = child.Process.Kill(); _ = child.Wait() }()
	select {
	case <-provider.created:
	case <-time.After(5 * time.Second):
		t.Fatal("child never reached the provider Create boundary")
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := child.Wait(); err == nil {
		t.Fatal("expected SIGKILL child exit")
	}
	close(provider.createGate)
	b := faultBackend(t, server.URL)
	c, store := openFaultController(t, path, b)
	defer store.Close()
	before := c.Status()
	if !before.Jobs["crash"].Workload.CreatePending || before.Admission.Used.CPU != 1 {
		t.Fatalf("SIGKILL erased durable pre-Create intent: %+v", before)
	}
	for i := 0; i < 5; i++ {
		if changed := mustTick(t, c); changed {
			t.Fatal("recovered process retried uncertain Create")
		}
	}
	if err := c.Recover(context.Background(), "crash"); err != nil {
		t.Fatal(err)
	}
	mustTick(t, c)
	assertHeld(t, c, "crash", admission.Running)
	if _, err := c.BeginDrain(); err != nil {
		t.Fatal(err)
	}
	mustTick(t, c) // DELETE
	mustTick(t, c) // GET 404
	assertDeleted(t, c, "crash")
	provider.mu.Lock()
	creates, remaining := provider.creates, len(provider.vms)
	provider.mu.Unlock()
	if creates != 1 || remaining != 0 {
		t.Fatalf("SIGKILL recovery leaked or duplicated sandbox: creates=%d remaining=%d", creates, remaining)
	}
}
