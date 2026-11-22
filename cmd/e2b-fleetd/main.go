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
	"agent-runtime-lab/src/e2bapi"
	"agent-runtime-lab/src/fleet"
	"agent-runtime-lab/src/fleetdaemon"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
}

func run() error {
	stateFile := flag.String("state-file", ".state/m3-1.json", "single-host journal path; do not share with the old CLI")
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
	if *hostCPU <= 0 || *hostMiB <= 0 || *vmCPU <= 0 || *vmMiB <= 0 {
		return errors.New("resource reservations must be positive")
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
	server := &http.Server{
		Handler: monitor.Handler(fleetdaemon.DrainHandler(d)), ReadHeaderTimeout: 5 * time.Second,
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
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()
	fmt.Printf("Fleet reconciler running; socket=%s journal=%s (Ctrl+C stops controller only; use fleetctl drain for cleanup)\n", *socket, *stateFile)
	select {
	case <-ctx.Done():
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			stop()
			<-workerDone
			<-monitorDone
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
		return fmt.Errorf("HTTP shutdown: %w", err)
	}
	<-workerDone
	<-monitorDone
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
