// e2b-fleet-cleanup explicitly drains an existing fleet when its daemon is
// stopped. It takes the same journal lock, uses E2B directly and needs no socket.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"agent-runtime-lab/src/e2bapi"
	"agent-runtime-lab/src/fleetcleanup"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("e2b-fleet-cleanup", flag.ContinueOnError)
	flags.SetOutput(stderr)
	state := flags.String("state-file", "", "existing fleet journal whose workloads will be deleted; required")
	execute := flags.Bool("execute", false, "required to delete workloads in the selected journal")
	timeout := flags.Duration("timeout", 120*time.Second, "cleanup verification deadline (1s..10m)")
	poll := flags.Duration("poll", 500*time.Millisecond, "per-workload polling interval (100ms..5s)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if !*execute || *state == "" || flags.NArg() != 0 {
		return errors.New("usage: e2b-fleet-cleanup --execute --state-file EXISTING_JOURNAL [--timeout 120s]; no daemon or socket required")
	}
	if *timeout < time.Second || *timeout > 10*time.Minute || *poll < 100*time.Millisecond || *poll > 5*time.Second {
		return errors.New("timeout must be 1s..10m and poll 100ms..5s")
	}
	apiURL := os.Getenv("E2B_API_URL")
	if apiURL == "" {
		apiURL = "http://127.0.0.1:3000"
	}
	backend, err := e2bapi.New(e2bapi.Config{BaseURL: apiURL, APIKey: os.Getenv("E2B_API_KEY")})
	if err != nil {
		return err
	}
	report, cleanupErr := fleetcleanup.Run(ctx, *state, backend, fleetcleanup.Options{Timeout: *timeout, Poll: *poll})
	if report.StateFile != "" {
		encoder := json.NewEncoder(stdout)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(report); err != nil {
			return err
		}
	}
	if cleanupErr != nil {
		return cleanupErr
	}
	_, err = fmt.Fprintln(stdout, "PASS: selected journal workloads confirmed deleted; CPU=0 MiB=0 pending=0. Journals preserved.")
	return err
}
