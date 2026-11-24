// e2b-fleetd is a manually started single-host E2B reconciliation service.
// It owns one fleet journal and exposes only a private Unix-domain socket.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/artifactstore"
	"agent-runtime-lab/src/e2bapi"
	"agent-runtime-lab/src/fleet"
	"agent-runtime-lab/src/fleetdaemon"
	"agent-runtime-lab/src/m4exec"
	"agent-runtime-lab/src/taskjournal"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
}

func run() error {
	stateFile := flag.String("state-file", ".state/m3-1.json", "single-host journal path; do not share with the old CLI")
	taskFile := flag.String("task-state-file", "", "private task journal path (default: <state-file>.tasks.json)")
	socket := flag.String("socket", ".state/m3-1.sock", "private Unix control socket path")
	interval := flag.Duration("interval", 2*time.Second, "reconciliation interval")
	steps := flag.Int("steps", 12, "maximum state transitions per reconciliation cycle")
	maxBackoff := flag.Duration("max-backoff", 30*time.Second, "maximum delay for repeated transient reconciliation errors")
	stuckAfter := flag.Duration("stuck-after", 2*time.Minute, "time before a non-converged workload is reported stalled")
	auditInterval := flag.Duration("audit-interval", 60*time.Second, "periodic provider inventory reconciliation interval")
	auditTimeout := flag.Duration("audit-timeout", 30*time.Second, "provider inventory request budget")
	auditMaxJobs := flag.Int("audit-max-jobs", 100, "maximum workload records in one complete inventory audit")
	auditGlobal := flag.Bool("audit-global", false, "also scan provider-wide lab-tagged active sandboxes")
	renewTTL := flag.Bool("renew-ttl", false, "opt in to extending RUNNING sandbox TTL")
	renewInterval := flag.Duration("renew-interval", 2*time.Minute, "TTL renewal interval when --renew-ttl is set")
	execProxy := flag.String("exec-proxy", "http://127.0.0.1:3002", "E2B envd client proxy loopback HTTP origin")
	taskHelper := flag.String("task-helper", "", "optional static Linux amd64 cmd/e2b-task-agent binary for M4.3 evidence tasks")
	crashTask := flag.String("test-crash-after-evidence-task", "", "TEST ONLY: exit 86 after this task's VM evidence is verified, before saving its host result")
	artifactDir := flag.String("artifact-dir", "", "private durable artifact directory (default: <task-state-file>.artifacts)")
	artifactMiB := flag.Int64("artifact-max-mib", 128, "host artifact storage quota MiB (1..1024)")
	jobWorkers := flag.Int("job-workers", 2, "asynchronous job workers (0..8; zero pauses dispatch)")
	crashStored := flag.String("test-crash-after-artifacts-job", "", "TEST ONLY: exit 87 after this job's artifacts are durable, before its result is saved")
	execute := flag.Bool("execute", false, "required to enable live E2B operations")
	template := flag.String("template", "base", "E2B sandbox template")
	ttl := flag.Int("ttl", 900, "E2B sandbox TTL (seconds)")
	hostCPU := flag.Int64("host-cpu", 4, "scheduler host CPU reservation budget")
	hostMiB := flag.Int64("host-mib", 4096, "scheduler host memory reservation MiB")
	vmCPU := flag.Int64("vm-cpu", 2, "reservation per E2B template")
	vmMiB := flag.Int64("vm-mib", 2048, "memory reservation per E2B template MiB")
	flag.Parse()
	if !*execute || flag.NArg() != 0 {
		return errors.New("run with --execute and no positional arguments to enable the local E2B daemon")
	}
	if *jobWorkers < 0 || *jobWorkers > 8 || *artifactMiB < 1 || *artifactMiB > 1024 {
		return errors.New("job-workers must be 0..8 and artifact-max-mib 1..1024")
	}
	if *crashStored != "" {
		if *taskHelper == "" || len(*crashStored) > 128 {
			return errors.New("artifact crash requires --task-helper and safe job ID <=128 bytes")
		}
		for _, c := range *crashStored {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
				return errors.New("artifact crash job ID must use letters, digits, hyphen, underscore or dot")
			}
		}
	}
	if *crashTask != "" {
		if *taskHelper == "" || len(*crashTask) > 128 {
			return errors.New("test crash requires --task-helper and a safe task ID of at most 128 characters")
		}
		for _, c := range *crashTask {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
				return errors.New("test crash task ID must use letters, digits, hyphen, underscore or dot")
			}
		}
	}
	if *hostCPU <= 0 || *hostMiB <= 0 || *vmCPU <= 0 || *vmMiB <= 0 {
		return errors.New("resource reservations must be positive")
	}
	if *taskFile == "" {
		*taskFile = *stateFile + ".tasks.json"
	}
	if *taskFile == *stateFile || *taskFile == *socket {
		return errors.New("task state file must not be fleet journal or control socket")
	}
	store, err := fleet.Open(*stateFile)
	if err != nil {
		return err
	}
	defer store.Close()
	apiURL := os.Getenv("E2B_API_URL")
	if apiURL == "" {
		apiURL = "http://127.0.0.1:3000"
	}
	backend, err := e2bapi.New(e2bapi.Config{
		BaseURL: apiURL, APIKey: os.Getenv("E2B_API_KEY"), Template: *template, TTLSeconds: *ttl,
	})
	if err != nil {
		return err
	}
	cap := admission.Resources{CPU: *hostCPU, MemoryMiB: *hostMiB}
	controller, err := fleet.New(store, backend, fleet.Config{
		TemplateSize: admission.Resources{CPU: *vmCPU, MemoryMiB: *vmMiB},
		Admission: admission.Config{Capacity: cap, MaxPending: 8, Tenants: map[string]admission.Policy{
			"alpha": {MaxLogical: 2, MaxActive: cap},
			"beta":  {MaxLogical: 2, MaxActive: cap},
		}},
	}, 45*time.Second)
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	d, err := fleetdaemon.NewWithOptions(controller, fleetdaemon.Options{
		Interval: *interval, MaxSteps: *steps, MaxBackoff: *maxBackoff,
		StuckAfter: *stuckAfter, Logger: logger,
	})
	if err != nil {
		return err
	}
	monitor, err := fleetdaemon.NewMonitor(controller, backend, fleetdaemon.MonitorOptions{
		AuditInterval: *auditInterval, AuditTimeout: *auditTimeout,
		MaxWorkloads: *auditMaxJobs, GlobalScan: *auditGlobal,
		RenewTTL: *renewTTL, TTLSeconds: *ttl, RenewInterval: *renewInterval,
		Logger: logger,
	})
	if err != nil {
		return err
	}
	commandRunner, err := m4exec.New(m4exec.Config{
		APIURL: apiURL, ProxyURL: *execProxy, APIKey: os.Getenv("E2B_API_KEY"), TTL: *ttl,
		HelperPath: *taskHelper,
	})
	if err != nil {
		return fmt.Errorf("configure E2B command executor: %w", err)
	}
	var taskExecutor fleetdaemon.Executor = commandRunner
	if *crashTask != "" {
		logger.Warn("fleet_test_crash_armed", "task_id", *crashTask, "exit_code", 86)
		taskExecutor = fleetdaemon.EvidenceHookExecutor{
			Executor: commandRunner, EvidenceExecutor: commandRunner,
			AfterVerified: func(taskID string) {
				if taskID == *crashTask {
					logger.Error("fleet_test_crash_after_evidence", "task_id", taskID, "exit_code", 86)
					os.Exit(86)
				}
			},
		}
	}
	// Open after fleet validation but before the control socket. Recovery
	// durably marks any interrupted in-flight task UNCERTAIN before admission.
	tasks, err := taskjournal.Open(*taskFile)
	if err != nil {
		return fmt.Errorf("open task journal: %w", err)
	}
	defer tasks.Close()
	if *artifactDir == "" {
		*artifactDir = *taskFile + ".artifacts"
	}
	artifacts, err := artifactstore.Open(*artifactDir, *artifactMiB<<20)
	if err != nil {
		return fmt.Errorf("open durable artifacts: %w", err)
	}
	defer artifacts.Close()
	jobOpts := fleetdaemon.JobOptions{Workers: *jobWorkers, Logger: logger}
	if *crashStored != "" {
		logger.Warn("job_test_crash_armed", "job_id", *crashStored, "exit_code", 87)
		jobOpts.AfterStored = func(id string) {
			if id == *crashStored {
				logger.Error("job_test_crash_after_artifacts", "job_id", id, "exit_code", 87)
				os.Exit(87)
			}
		}
	}
	jobs, err := fleetdaemon.NewJobs(controller, tasks, artifacts, taskExecutor.(fleetdaemon.EvidenceExecutor), commandRunner, jobOpts)
	if err != nil {
		return err
	}
	listener, cleanup, err := fleetdaemon.ListenUnix(*socket)
	if err != nil {
		return err
	}
	defer func() {
		if err := cleanup(); err != nil {
			fmt.Fprintln(os.Stderr, "socket cleanup:", err)
		}
	}()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	base := fleetdaemon.DrainHandler(d)
	base = fleetdaemon.ExecuteHandler(base, controller, commandRunner)
	base = fleetdaemon.TaskHandler(base, controller, taskExecutor, tasks)
	base = jobs.Handler(base)
	server := &http.Server{
		Handler: monitor.Handler(base), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 10 * time.Second, WriteTimeout: 90 * time.Second,
	}
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		d.Run(ctx)
	}()
	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		monitor.Run(ctx)
	}()
	jobsDone := make(chan struct{})
	var jobsErr error
	go func() {
		defer close(jobsDone)
		jobsErr = jobs.Run(ctx)
	}()
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()
	fmt.Printf("Fleet reconciler running; socket=%s journal=%s tasks=%s (Ctrl+C stops controller only; use fleetctl drain for cleanup)\n", *socket, *stateFile, *taskFile)
	select {
	case <-ctx.Done():
	case <-jobsDone:
		stop()
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			stop()
			<-workerDone
			<-monitorDone
			<-jobsDone
			return fmt.Errorf("Unix API server: %w", err)
		}
	}
	stop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		_ = server.Close()
		<-workerDone
		<-monitorDone
		<-jobsDone
		return fmt.Errorf("HTTP shutdown: %w", err)
	}
	<-workerDone
	<-monitorDone
	<-jobsDone
	if jobsErr != nil {
		return fmt.Errorf("async job worker stopped: %w", jobsErr)
	}
	state := controller.Status()
	remaining := 0
	for _, job := range state.Jobs {
		if job.Entry.State != admission.Deleted {
			remaining++
		}
	}
	if remaining == 0 && state.Admission.Pending == 0 && state.Admission.Used.CPU == 0 && state.Admission.Used.MemoryMiB == 0 {
		fmt.Println("Fleet controller stopped; all journal workloads confirmed deleted; shutdown performed no VM mutations")
	} else {
		fmt.Printf("Fleet controller stopped; %d unresolved workload(s), CPU=%d and RAM=%d MiB still reserved; shutdown performed no VM mutations. Restart with same journal to continue.\n",
			remaining, state.Admission.Used.CPU, state.Admission.Used.MemoryMiB)
	}
	return nil
}
