package fleetdaemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/artifactstore"
	"agent-runtime-lab/src/fleet"
	"agent-runtime-lab/src/lifecycle"
	"agent-runtime-lab/src/taskevidence"
	"agent-runtime-lab/src/taskjournal"
)

type jobFake struct {
	mu        sync.Mutex
	calls     map[string]int
	proofs    map[string]taskevidence.Envelope
	reads     int
	block     chan struct{}
	started   chan string
	lost      bool
	copyError bool
	exit      int
}

func (f *jobFake) EvidenceHelperHash() string { return taskevidence.Hash([]byte("helper")) }
func (f *jobFake) RunEvidence(ctx context.Context, id taskevidence.Identity, _ string) (taskevidence.Envelope, error) {
	f.mu.Lock()
	f.calls[id.TaskID]++
	f.mu.Unlock()
	if f.started != nil {
		f.started <- id.TaskID
	}
	if f.block != nil {
		select {
		case <-ctx.Done():
			return taskevidence.Envelope{}, ctx.Err()
		case <-f.block:
		}
	}
	artifacts := make([]taskevidence.Artifact, 0)
	for _, path := range id.Intent.Artifacts {
		artifacts = append(artifacts, taskevidence.Artifact{Path: path, Size: 7, SHA256: taskevidence.Hash([]byte("payload"))})
	}
	proof, _ := taskevidence.Seal(taskevidence.Manifest{Identity: id, CompletedAt: time.Now().UTC(), ExitCode: f.exit,
		Stdout: "completed\n", StdoutSHA256: taskevidence.Hash([]byte("completed\n")), StderrSHA256: taskevidence.Hash(nil), Artifacts: artifacts})
	f.mu.Lock()
	f.proofs[id.TaskID] = proof
	f.mu.Unlock()
	if f.lost {
		return taskevidence.Envelope{}, errors.New("response lost after completion")
	}
	return proof, nil
}
func (f *jobFake) ReadEvidence(_ context.Context, id taskevidence.Identity) (taskevidence.Envelope, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reads++
	proof, ok := f.proofs[id.TaskID]
	if !ok {
		return proof, errors.New("completion marker missing")
	}
	return proof, nil
}
func (f *jobFake) ReadArtifact(context.Context, taskevidence.Identity, taskevidence.Artifact) ([]byte, error) {
	if f.copyError {
		return nil, errors.New("artifact copy failed")
	}
	return []byte("payload"), nil
}

type jobsFixture struct {
	c         *fleet.Controller
	tasks     *taskjournal.Store
	artifacts *artifactstore.Store
	backend   *jobFake
	jobs      *Jobs
	path      string
}

func newJobsFixture(t *testing.T) *jobsFixture {
	t.Helper()
	c, _ := runnable(t)
	dir := filepath.Join(t.TempDir(), "private")
	tasks, err := taskjournal.Open(filepath.Join(dir, "tasks.json"))
	if err != nil {
		t.Fatal(err)
	}
	artifacts, err := artifactstore.Open(filepath.Join(dir, "artifacts"), 16<<20)
	if err != nil {
		t.Fatal(err)
	}
	f := &jobsFixture{c: c, tasks: tasks, artifacts: artifacts, path: filepath.Join(dir, "tasks.json"),
		backend: &jobFake{calls: make(map[string]int), proofs: make(map[string]taskevidence.Envelope)}}
	f.jobs, err = NewJobs(c, tasks, artifacts, f.backend, f.backend, JobOptions{Workers: 2, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.tasks.Close(); f.artifacts.Close() })
	return f
}

func (f *jobsFixture) start(t *testing.T) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.jobs.Run(ctx) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			if err := <-done; err != nil {
				t.Error(err)
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func jobHTTP(t *testing.T, handler http.Handler, method, endpoint string, body any, want int) *httptest.ResponseRecorder {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(method, endpoint, bytes.NewReader(data))
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	r = r.WithContext(ctx)
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != want {
		t.Fatalf("%s %s: HTTP %d want %d: %s", method, endpoint, w.Code, want, w.Body.String())
	}
	return w
}

func (f *jobsFixture) submit(t *testing.T, id string) taskjournal.Entry {
	t.Helper()
	w := jobHTTP(t, f.jobs.Handler(http.NewServeMux()), http.MethodPost, "/v1/jobs",
		JobRequest{ID: id, WorkloadID: "work", Command: "echo private-job", Timeout: 30, Artifacts: []string{"/tmp/out"}}, http.StatusAccepted)
	var e taskjournal.Entry
	if err := json.Unmarshal(w.Body.Bytes(), &e); err != nil {
		t.Fatal(err)
	}
	return e
}

func jobState(t *testing.T, store *taskjournal.Store, id string, state taskjournal.State) taskjournal.Entry {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		e, err := store.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if e.State == state {
			return e
		}
		time.Sleep(5 * time.Millisecond)
	}
	e, _ := store.Get(id)
	t.Fatalf("job %s state=%s want=%s", id, e.State, state)
	return e
}

func TestJobsSubmitReturnsBeforeExecutionAndDisconnectDoesNotCancel(t *testing.T) {
	f := newJobsFixture(t)
	e := f.submit(t, "one")
	if e.State != taskjournal.Queued || f.backend.calls["one"] != 0 {
		t.Fatal("submit performed execution")
	}
	f.start(t)
	done := jobState(t, f.tasks, "one", taskjournal.Succeeded)
	if !done.ArtifactsStored || done.Evidence == nil {
		t.Fatal("completion lacks durable artifact receipt")
	}
	h := f.jobs.Handler(http.NewServeMux())
	jobHTTP(t, h, http.MethodPost, "/v1/jobs", JobRequest{ID: "one", WorkloadID: "work", Command: "echo private-job", Timeout: 30, Artifacts: []string{"/tmp/out"}}, http.StatusOK)
	jobHTTP(t, h, http.MethodPost, "/v1/jobs", JobRequest{ID: "one", WorkloadID: "work", Command: "different", Timeout: 30, Artifacts: []string{"/tmp/out"}}, http.StatusConflict)
	w := jobHTTP(t, h, http.MethodGet, "/v1/jobs/one/artifacts/0", nil, http.StatusOK)
	if w.Body.String() != "payload" || w.Header().Get("X-Artifact-SHA256") != taskevidence.Hash([]byte("payload")) {
		t.Fatal("download omitted integrity-checked bytes")
	}
	jobHTTP(t, h, http.MethodGet, "/v1/jobs/one/artifacts/1", nil, http.StatusNotFound)
	list := jobHTTP(t, h, http.MethodGet, "/v1/jobs", nil, http.StatusOK)
	if strings.Contains(list.Body.String(), "private-job") {
		t.Fatal("API exposed private queued command bytes")
	}
}

func TestJobsCancellationSerializesVMAndNeverClaimsRollback(t *testing.T) {
	f := newJobsFixture(t)
	f.backend.block = make(chan struct{})
	f.backend.started = make(chan string, 2)
	f.submit(t, "one")
	f.submit(t, "two")
	f.start(t)
	select {
	case <-f.backend.started:
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not start")
	}
	jobState(t, f.tasks, "one", taskjournal.Running)
	if e, _ := f.tasks.Get("two"); e.State != taskjournal.Queued {
		t.Fatal("same VM ran concurrently")
	}
	cancelled, err := f.jobs.Cancel("two")
	if err != nil || cancelled.State != taskjournal.Cancelled {
		t.Fatal("queued cancellation failed", err)
	}
	if _, err := f.jobs.Cancel("one"); err != nil {
		t.Fatal(err)
	}
	one := jobState(t, f.tasks, "one", taskjournal.Uncertain)
	if !one.CancelRequested || one.Result != nil {
		t.Fatal("running cancellation invented completion or rollback")
	}
	f.backend.mu.Lock()
	defer f.backend.mu.Unlock()
	if f.backend.calls["one"] != 1 || f.backend.calls["two"] != 0 {
		t.Fatal("cancelled or fenced job was executed")
	}
}

func TestJobLostResponseRequiresExplicitRecovery(t *testing.T) {
	f := newJobsFixture(t)
	f.backend.lost = true
	f.submit(t, "one")
	f.start(t)
	jobState(t, f.tasks, "one", taskjournal.Uncertain)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var recovered taskjournal.Entry
	deadline := time.Now().Add(time.Second)
	for {
		var err error
		recovered, err = f.jobs.Recover(ctx, "one")
		if err == nil {
			break
		}
		if !errors.Is(err, taskjournal.ErrBusy) || time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}
	if recovered.State != taskjournal.Succeeded || !recovered.ArtifactsStored {
		t.Fatal("recovery lost durable receipt")
	}
	f.backend.mu.Lock()
	defer f.backend.mu.Unlock()
	if f.backend.calls["one"] != 1 || f.backend.reads != 1 {
		t.Fatal("recovery reran command or did not read evidence")
	}
}

func TestDurableBundleRecoversAfterCrashAndVMDeletion(t *testing.T) {
	f := newJobsFixture(t)
	f.submit(t, "one")
	e, command, run, err := f.tasks.ClaimNext(f.backend.EvidenceHelperHash())
	if err != nil || !run {
		t.Fatal(err)
	}
	f.jobs.opts.AfterStored = func(string) { f.tasks.Close() }
	if err := f.jobs.execute(context.Background(), e, command); !errors.Is(err, taskjournal.ErrPoisoned) {
		t.Fatal("did not simulate lost host finish", err)
	}
	f.tasks, err = taskjournal.Open(f.path)
	if err != nil {
		t.Fatal(err)
	}
	jobState(t, f.tasks, "one", taskjournal.Uncertain)
	if err := f.c.SetDesired("work", lifecycle.WantDeleted); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5 && f.c.Status().Jobs["work"].Entry.State != admission.Deleted; i++ {
		if _, err := f.c.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if f.c.Status().Jobs["work"].Entry.State != admission.Deleted {
		t.Fatal("fixture VM was not deleted")
	}
	j, err := NewJobs(f.c, f.tasks, f.artifacts, f.backend, f.backend, JobOptions{})
	if err != nil {
		t.Fatal(err)
	}
	done, err := j.Recover(context.Background(), "one")
	if err != nil || done.State != taskjournal.Succeeded || !done.ArtifactsStored {
		t.Fatal("local bundle did not resolve deleted VM job", err)
	}
	jobHTTP(t, j.Handler(http.NewServeMux()), http.MethodGet, "/v1/jobs/one/artifacts/0", nil, http.StatusOK)
	if f.backend.calls["one"] != 1 || f.backend.reads != 0 {
		t.Fatal("durable recovery contacted or reexecuted original VM")
	}
}

func TestJobCopyFailureAndNonzeroExit(t *testing.T) {
	for _, copyFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "nonzero_exit", true: "copy_failure"}[copyFailure], func(t *testing.T) {
			f := newJobsFixture(t)
			f.backend.copyError, f.backend.exit = copyFailure, 17
			f.submit(t, "one")
			f.start(t)
			want := taskjournal.Failed
			if copyFailure {
				want = taskjournal.Uncertain
			}
			e := jobState(t, f.tasks, "one", want)
			if copyFailure && (e.Result != nil || e.ArtifactsStored) || !copyFailure && (e.Result.ExitCode != 17 || !e.ArtifactsStored) {
				t.Fatal("wrong completion evidence after copy failure or exit 17")
			}
		})
	}
}

func TestDrainCancelsQueuedJobsWithWorkersPaused(t *testing.T) {
	f := newJobsFixture(t)
	f.submit(t, "one")
	d, err := New(f.c, time.Second, 12)
	if err != nil {
		t.Fatal(err)
	}
	h := f.jobs.Handler(DrainHandler(d))
	jobHTTP(t, h, http.MethodPost, "/v1/drain", struct{}{}, http.StatusAccepted)
	jobState(t, f.tasks, "one", taskjournal.Cancelled)
	if err := f.c.Submit("alpha", "direct-after-drain"); !errors.Is(err, fleet.ErrUnsafeCapacity) {
		t.Fatal("controller-level admission bypassed durable drain fence", err)
	}
	jobHTTP(t, h, http.MethodPost, "/v1/jobs", JobRequest{ID: "new", WorkloadID: "work", Command: "echo hi", Timeout: 30}, http.StatusConflict)
}

func TestJobTimeoutAndShutdownPersistUnknownOutcome(t *testing.T) {
	for _, timedOut := range []bool{false, true} {
		t.Run(map[bool]string{false: "shutdown", true: "timeout"}[timedOut], func(t *testing.T) {
			f := newJobsFixture(t)
			f.backend.block = make(chan struct{})
			f.backend.started = make(chan string, 1)
			vm := f.c.Status().Jobs["work"].Workload.SandboxID
			if _, _, err := f.tasks.Enqueue("one", "work", vm, "echo private-job", 1, f.backend.EvidenceHelperHash(), nil); err != nil {
				t.Fatal(err)
			}
			stop := f.start(t)
			select {
			case <-f.backend.started:
			case <-time.After(3 * time.Second):
				t.Fatal("worker did not start")
			}
			if !timedOut {
				stop()
			}
			e := jobState(t, f.tasks, "one", taskjournal.Uncertain)
			if e.Result != nil || e.ArtifactsStored {
				t.Fatal("timeout/shutdown invented durable completion")
			}
		})
	}
}
