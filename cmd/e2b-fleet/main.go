// e2b-fleet runs a single-host durable admission controller against E2B Embed.
// Each CLI invocation acquires the same exclusive state-file lock. It is
// intentionally NOT a service, multi-node scheduler, or replacement for E2B.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/e2bapi"
	"agent-runtime-lab/src/fleet"
	"agent-runtime-lab/src/lifecycle"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
}

func emit(value any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(value)
}

func run() error {
	statePath := flag.String("state-file", ".state/m2-9.json", "journal on a local Linux filesystem")
	execute := flag.Bool("execute", false, "required for E2B requests and journal mutations")
	template := flag.String("template", "base", "E2B template ID")
	ttl := flag.Int("ttl", 900, "sandbox time-to-live in seconds")
	id := flag.String("id", "", "stable request ID (e.g. alpha-1)")
	tenant := flag.String("tenant", "alpha", "tenant for submit (alpha or beta)")
	steps := flag.Int("steps", 12, "maximum reconcile steps for tick")
	hostCPU := flag.Int64("host-cpu", 4, "host CPU reservation budget (not measured VM usage)")
	hostMiB := flag.Int64("host-mib", 4096, "host RAM reservation budget")
	vmCPU := flag.Int64("vm-cpu", 2, "conservative CPU reservation per E2B template VM")
	vmMiB := flag.Int64("vm-mib", 2048, "conservative memory reservation per E2B template VM")
	flag.Parse()
	if flag.NArg() != 1 {
		return errors.New("usage: e2b-fleet [flags] submit|tick|pause|resume|delete|recover|status")
	}
	action := flag.Arg(0)
	if action != "status" && action != "submit" && action != "tick" && action != "pause" && action != "resume" && action != "delete" && action != "recover" {
		return fmt.Errorf("unknown action %q", action)
	}
	if action != "status" && !*execute {
		return errors.New("real E2B and journal actions require --execute")
	}
	if *steps < 1 || *steps > 100 {
		return errors.New("steps must be between 1 and 100")
	}
	store, err := fleet.Open(*statePath)
	if err != nil {
		return err
	}
	defer store.Close()
	if action == "status" {
		j, exists, err := store.Load()
		if err != nil {
			return err
		}
		if !exists {
			return errors.New("no fleet state yet; submit first")
		}
		s, err := admission.Restore(j.Scheduler)
		if err != nil {
			return err
		}
		jobs := make(map[string]fleet.JobStatus, len(j.Workloads))
		for id, work := range j.Workloads {
			e, ok := s.Get(id)
			if !ok {
				return errors.New("inconsistent fleet journal")
			}
			jobs[id] = fleet.JobStatus{Entry: e, Workload: work}
		}
		return emit(fleet.Status{Unsafe: j.Unsafe, UnsafeNote: j.UnsafeNote, Admission: s.Snapshot(), Jobs: jobs})
	}
	if *vmCPU <= 0 || *vmMiB <= 0 || *hostCPU <= 0 || *hostMiB <= 0 {
		return errors.New("resource budgets must be positive")
	}
	cap := admission.Resources{CPU: *hostCPU, MemoryMiB: *hostMiB}
	vmSize := admission.Resources{CPU: *vmCPU, MemoryMiB: *vmMiB}
	cfg := fleet.Config{TemplateSize: vmSize, Admission: admission.Config{
		Capacity:   cap,
		MaxPending: 8,
		Tenants: map[string]admission.Policy{
			"alpha": {MaxLogical: 2, MaxActive: cap},
			"beta":  {MaxLogical: 2, MaxActive: cap},
		},
	}}
	apiURL := os.Getenv("E2B_API_URL")
	if apiURL == "" {
		apiURL = "http://127.0.0.1:3000"
	}
	b, err := e2bapi.New(e2bapi.Config{
		BaseURL: apiURL, APIKey: os.Getenv("E2B_API_KEY"), Template: *template, TTLSeconds: *ttl,
	})
	if err != nil {
		return err
	}
	c, err := fleet.New(store, b, cfg, 45*time.Second)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	switch action {
	case "submit":
		if *id == "" {
			return errors.New("submit requires --id")
		}
		err = c.Submit(*tenant, *id)
	case "pause":
		err = c.SetDesired(*id, lifecycle.WantPaused)
	case "resume":
		err = c.SetDesired(*id, lifecycle.WantRunning)
	case "delete":
		err = c.SetDesired(*id, lifecycle.WantDeleted)
	case "recover":
		err = c.Recover(ctx, *id)
	case "tick":
		for i := 0; i < *steps; i++ {
			var changed bool
			changed, err = c.Tick(ctx)
			if err != nil {
				break
			}
			if !changed {
				break
			}
		}
	}
	if err != nil {
		return fmt.Errorf("%s: %w (do not remove %s until all sandboxes are confirmed deleted)", action, err, *statePath)
	}
	return emit(c.Status())
}
