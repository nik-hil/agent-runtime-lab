package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/e2bapi"
	"agent-runtime-lab/src/fleetcleanup"
	"agent-runtime-lab/src/fleetdaemon"
	"agent-runtime-lab/src/lifecycle"
	"agent-runtime-lab/src/m4exec"
	"agent-runtime-lab/src/taskjournal"
)

type liveProcess struct {
	cmd  *exec.Cmd
	done chan struct{}
	err  error // read only after done closes
}

type liveFleet struct {
	root    string
	dir     string
	state   string
	socket  string
	process *liveProcess
	client  *http.Client
	backend *e2bapi.Backend
	ids     map[string]bool
}

func newLiveFleet(t *testing.T) *liveFleet {
	t.Helper()
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatal("run this integration test through go test ./tests from the repository")
	}
	directory := filepath.Join(root, ".state")
	if err := os.MkdirAll(directory, 0700); err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(directory, "verify-")
	if err != nil {
		t.Fatal(err)
	}
	apiURL := os.Getenv("E2B_API_URL")
	if apiURL == "" {
		apiURL = "http://127.0.0.1:3000"
	}
	backend, err := e2bapi.New(e2bapi.Config{
		BaseURL: apiURL, APIKey: os.Getenv("E2B_API_KEY"), TTLSeconds: 1800,
	})
	if err != nil {
		t.Fatal(err)
	}
	v := &liveFleet{
		root: root, dir: dir, state: filepath.Join(dir, "fleet.json"),
		backend: backend, ids: make(map[string]bool),
	}
	// This is a persistent private directory: even a failed run keeps its
	// journals and exact cleanup report for the operator.
	t.Logf("live verification journals and logs: %s", dir)
	t.Cleanup(func() { v.cleanup(t) })
	v.build(t, "fleetd", "./cmd/e2b-fleetd", false)
	v.build(t, "task-agent", "./cmd/e2b-task-agent", true)
	return v
}

func (v *liveFleet) build(t *testing.T, name, source string, static bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "build", "-buildvcs=false", "-trimpath", "-o", filepath.Join(v.dir, name), source)
	cmd.Dir = v.root
	if static {
		for _, value := range os.Environ() {
			if !strings.HasPrefix(value, "CGO_ENABLED=") && !strings.HasPrefix(value, "GOOS=") && !strings.HasPrefix(value, "GOARCH=") {
				cmd.Env = append(cmd.Env, value)
			}
		}
		cmd.Env = append(cmd.Env, "CGO_ENABLED=0", "GOOS=linux", "GOARCH=amd64")
	}
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v: %s", source, err, output)
	}
}

func (v *liveFleet) start(t *testing.T, name string, crash bool, extra ...string) {
	t.Helper()
	v.socket = filepath.Join(v.dir, name+".sock")
	if len(v.socket) > 100 {
		t.Fatal("repository path is too long for a Unix socket; use a shorter checkout path")
	}
	proxy := os.Getenv("E2B_EXEC_PROXY")
	if proxy == "" {
		proxy = "http://127.0.0.1:3002"
	}
	args := []string{
		"--execute", "--state-file", v.state, "--socket", v.socket,
		"--task-helper", filepath.Join(v.dir, "task-agent"),
		"--host-cpu", "2", "--host-mib", "2048",
		"--vm-cpu", "2", "--vm-mib", "2048",
		"--interval", "200ms", "--ttl", "1800",
		"--renew-ttl", "--renew-interval", "2s",
		"--audit-interval", "5s", "--audit-timeout", "5s",
		"--exec-proxy", proxy,
	}
	if crash {
		args = append(args, "--test-crash-after-evidence-task", "verify-crash")
	}
	args = append(args, extra...)
	logFile, err := os.OpenFile(filepath.Join(v.dir, name+".log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(filepath.Join(v.dir, "fleetd"), args...)
	cmd.Dir, cmd.Stdout, cmd.Stderr = v.root, logFile, logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		t.Fatal(err)
	}
	p := &liveProcess{cmd: cmd, done: make(chan struct{})}
	v.process = p
	go func() {
		p.err = cmd.Wait()
		_ = logFile.Close()
		close(p.done)
	}()
	socket := v.socket
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		},
	}
	v.client = &http.Client{
		Transport: transport, Timeout: 45 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	v.wait(t, "daemon ready", func() bool {
		select {
		case <-p.done:
			t.Fatalf("daemon exited before readiness: %v; inspect %s.log", p.err, filepath.Join(v.dir, name))
		default:
		}
		code, _, err := v.request(http.MethodGet, "/v1/status", nil)
		return err == nil && code == http.StatusOK
	})
}

func (v *liveFleet) stop() error {
	if v.client != nil {
		v.client.CloseIdleConnections()
	}
	if v.process == nil {
		return nil
	}
	p := v.process
	select {
	case <-p.done:
		return nil
	default:
	}
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	select {
	case <-p.done:
		return p.err
	case <-time.After(20 * time.Second):
		if err := p.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			return err
		}
		<-p.done
		return errors.New("daemon required a forced stop; inspect its log")
	}
}

// request sends mutations exactly once. Only wait's read-only probes repeat.
func (v *liveFleet) request(method, path string, payload any) (int, []byte, error) {
	var body io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return 0, nil, err
		}
		body = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, "http://fleetd"+path, body)
	if err != nil {
		return 0, nil, err
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	response, err := v.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, (2<<20)+1))
	if len(data) > 2<<20 {
		return response.StatusCode, nil, errors.New("fleet response exceeded 2 MiB")
	}
	return response.StatusCode, data, err
}

func (v *liveFleet) call(t *testing.T, method, path string, payload any, want int, out any) {
	t.Helper()
	code, body, err := v.request(method, path, payload)
	if err != nil || code != want {
		t.Fatalf("%s %s: status=%d want=%d error=%v body=%s", method, path, code, want, err, body)
	}
	if out != nil {
		if err := json.Unmarshal(body, out); err != nil {
			t.Fatalf("decode %s: %v", path, err)
		}
	}
}

func (v *liveFleet) wait(t *testing.T, description string, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for {
		if ready() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s; inspect %s", description, v.dir)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (v *liveFleet) status(t *testing.T) fleetdaemon.Status {
	t.Helper()
	var status fleetdaemon.Status
	v.call(t, http.MethodGet, "/v1/status", nil, http.StatusOK, &status)
	if status.Fleet.Unsafe || status.Fleet.Admission.Used.CPU > 2 || status.Fleet.Admission.Used.MemoryMiB > 2048 {
		t.Fatalf("unsafe fleet or exceeded configured budget: %+v", status.Fleet)
	}
	for _, job := range status.Fleet.Jobs {
		if job.Workload.SandboxID != "" {
			v.ids[job.Workload.SandboxID] = true
		}
	}
	return status
}

func (v *liveFleet) jobs(t *testing.T, states map[string]admission.State) fleetdaemon.Status {
	t.Helper()
	var status fleetdaemon.Status
	v.wait(t, fmt.Sprint(states), func() bool {
		status = v.status(t)
		for id, state := range states {
			job, ok := status.Fleet.Jobs[id]
			if !ok || job.Entry.State != state {
				return false
			}
		}
		return true
	})
	return status
}

func (v *liveFleet) intent(t *testing.T, id, desired string) {
	t.Helper()
	v.call(t, http.MethodPost, "/v1/intent", map[string]string{"ID": id, "Desired": desired}, http.StatusAccepted, nil)
}

func (v *liveFleet) command(t *testing.T, command string, exit int) m4exec.Result {
	t.Helper()
	var result m4exec.Result
	v.call(t, http.MethodPost, "/v1/execute", map[string]any{
		"id": "one", "command": command, "timeout_seconds": 30,
	}, http.StatusOK, &result)
	if result.ExitCode != exit || result.Truncated || !v.ids[result.SandboxID] {
		t.Fatalf("unexpected command result: %+v", result)
	}
	return result
}

func (v *liveFleet) task(t *testing.T, path string, request any) taskjournal.Entry {
	t.Helper()
	var entry taskjournal.Entry
	method := http.MethodPost
	if request == nil {
		method = http.MethodGet
	}
	v.call(t, method, path, request, http.StatusOK, &entry)
	return entry
}

func (v *liveFleet) cleanup(t *testing.T) {
	t.Helper()
	if err := v.stop(); err != nil {
		t.Error("stop verification daemon:", err)
	}
	if _, err := os.Stat(v.state); errors.Is(err, os.ErrNotExist) {
		return // build/readiness failed before a journal could allocate anything
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	report, err := fleetcleanup.Run(ctx, v.state, v.backend, fleetcleanup.Options{Timeout: 120 * time.Second})
	data, encodeErr := json.MarshalIndent(report, "", "  ")
	if encodeErr != nil {
		t.Error("encode cleanup report:", encodeErr)
	} else if writeErr := os.WriteFile(filepath.Join(v.dir, "cleanup.json"), append(data, '\n'), 0600); writeErr != nil {
		t.Error("save cleanup report:", writeErr)
	}
	if err != nil || !report.Complete {
		t.Errorf("cleanup incomplete: %v; inspect %s/cleanup.json; reservations retained", err, v.dir)
		return
	}
	for id := range v.ids {
		state, observeErr := v.backend.Observe(ctx, id)
		if observeErr != nil || state != lifecycle.Absent {
			t.Errorf("cleanup sandbox %s: state=%s error=%v", id, state, observeErr)
		}
	}
	t.Logf("cleanup complete: CPU=0 MiB=0 pending=0; report=%s/cleanup.json", v.dir)
}
