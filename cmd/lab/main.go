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

	"agent-runtime-lab/src/experiments"
	"agent-runtime-lab/src/sandbox"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: lab run|experiments|bench [options] [-- command args...]")
	}
	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
	backend := flags.String("backend", "docker", "docker, gvisor; all is accepted by experiments and bench")
	image := flags.String("image", "agent-runtime-lab:local", "trusted local image; resolved to immutable ID")
	timeout := flags.Duration("timeout", 5*time.Second, "start and execution deadline (at most 10m)")
	memory := flags.Int64("memory-mib", 128, "memory cap in MiB, swap disabled")
	pids := flags.Int("pids", 64, "cgroup host-task limit")
	nproc := flags.Int("nproc", 0, "RLIMIT_NPROC; zero follows --pids for compatibility")
	processAccounting := flags.Bool("process-accounting", false, "sample local host cgroup and OS threads (read-only)")
	network := flags.String("network", "none", "none or bridge (explicit egress opt-in)")
	output := flags.String("output", "", "JSON output file; defaults to stdout")
	n := flags.Int("n", 20, "measured benchmark iterations per backend")
	warmup := flags.Int("warmup", 3, "unmeasured warmups per backend")
	experiment := flags.String("experiment", "all", "one experiment or all")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if *memory < 16 || *memory > 2048 {
		return errors.New("memory-mib must be between 16 and 2048")
	}
	cfg := sandbox.DefaultConfig()
	cfg.Image = *image
	cfg.Timeout = *timeout
	cfg.MemoryBytes = *memory << 20
	cfg.PidsLimit = *pids
	cfg.NprocLimit = *nproc
	cfg.CollectProcessAccounting = *processAccounting
	cfg.Network = sandbox.NetworkPolicy(*network)
	emit := func(value any) error {
		if *output != "" {
			return experiments.WriteJSON(*output, value)
		}
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(value)
	}
	switch args[0] {
	case "run":
		if len(flags.Args()) == 0 {
			return errors.New("provide a command after -- (for example /lab-probe noop)")
		}
		cfg.Command = flags.Args()
		s, err := sandbox.New(*backend)
		if err != nil {
			return err
		}
		result, runErr := sandbox.Run(ctx, s, cfg)
		return errors.Join(runErr, emit(result))
	case "experiments":
		backends, err := backendList(*backend)
		if err != nil {
			return err
		}
		names := experiments.Names
		if *experiment != "all" {
			names = []string{*experiment}
		}
		observations := []experiments.Observation{}
		failed := false
		for _, b := range backends {
			for _, name := range names {
				if ctx.Err() != nil {
					return errors.Join(ctx.Err(), emit(observations))
				}
				ob := experiments.Run(ctx, b, *image, name)
				observations = append(observations, ob)
				fmt.Fprintf(os.Stderr, "%s/%s: %s\n", b, name, ob.Status)
				failed = failed || ob.Status != "passed"
			}
		}
		if err := emit(observations); err != nil {
			return err
		}
		if failed {
			return errors.New("one or more experiments failed; see JSON evidence")
		}
		return nil
	case "bench":
		backends, err := backendList(*backend)
		if err != nil {
			return err
		}
		if *n < 1 || *n > 1000 || *warmup < 0 || *warmup > 100 {
			return errors.New("n must be 1..1000 and warmup 0..100")
		}
		cfg.Command = []string{"/lab-probe", "noop"}
		type sample struct {
			Iteration int            `json:"iteration"`
			Warmup    bool           `json:"warmup"`
			Result    sandbox.Result `json:"result"`
			Error     string         `json:"error,omitempty"`
		}
		samples := []sample{}
		var failure error
		for i := -*warmup; i < *n; i++ {
			// Alternate first runtime each round to reduce fixed-order bias.
			for j := range backends {
				if ctx.Err() != nil {
					return errors.Join(ctx.Err(), emit(samples))
				}
				b := backends[(j+(i+*warmup)%len(backends))%len(backends)]
				s, _ := sandbox.New(b)
				result, runErr := sandbox.Run(ctx, s, cfg)
				entry := sample{Iteration: i, Warmup: i < 0, Result: result}
				if runErr != nil {
					entry.Error = runErr.Error()
					failure = errors.Join(failure, runErr)
				}
				if result.ExitCode == nil || *result.ExitCode != 0 || result.TimedOut || result.Cancelled {
					entry.Error += " workload did not complete successfully"
					failure = errors.Join(failure, errors.New(entry.Error))
				}
				samples = append(samples, entry)
			}
		}
		return errors.Join(failure, emit(samples))
	default:
		return fmt.Errorf("unknown operation %q", args[0])
	}
}

func backendList(value string) ([]string, error) {
	if value == "all" {
		return []string{"docker", "gvisor"}, nil
	}
	if value == "docker" || value == "gvisor" {
		return []string{value}, nil
	}
	return nil, fmt.Errorf("unknown backend %q", value)
}
