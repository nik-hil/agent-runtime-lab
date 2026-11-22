// e2b-audit compares an existing fleet journal to E2B without mutating either.
// Stop the fleet daemon first: the audit obtains its exclusive journal lock.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"agent-runtime-lab/src/e2bapi"
	"agent-runtime-lab/src/fleet"
	"agent-runtime-lab/src/fleetaudit"
)

func main() { os.Exit(run()) }

func run() int {
	stateFile := flag.String("state-file", "", "existing journal path; stop fleetd first")
	timeout := flag.Duration("timeout", 90*time.Second, "whole-audit timeout")
	global := flag.Bool("global", false, "also scan provider-wide lab-tagged active sandboxes (read-only)")
	flag.Parse()
	if *stateFile == "" || flag.NArg() != 0 || *timeout <= 0 || *timeout > 5*time.Minute {
		fmt.Fprintln(os.Stderr, "usage: e2b-audit --state-file PATH [--timeout 90s]; read-only, daemon must be stopped")
		return 1
	}
	store, err := fleet.Open(*stateFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "acquire journal lock:", err)
		return 1
	}
	defer store.Close()
	j, exists, err := store.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "load journal:", err)
		return 1
	}
	if !exists {
		fmt.Fprintln(os.Stderr, "journal does not exist")
		return 1
	}
	apiURL := os.Getenv("E2B_API_URL")
	if apiURL == "" {
		apiURL = "http://127.0.0.1:3000"
	}
	api, err := e2bapi.New(e2bapi.Config{
		BaseURL: apiURL, APIKey: os.Getenv("E2B_API_KEY"),
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "configure E2B:", err)
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	var report fleetaudit.Report
	if *global {
		report, err = fleetaudit.RunGlobal(ctx, j, api)
	} else {
		report, err = fleetaudit.Run(ctx, j, api)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "audit incomplete; do not infer provider absence:", err)
		return 1
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(report); err != nil {
		fmt.Fprintln(os.Stderr, "emit report:", err)
		return 1
	}
	if report.RequiresReview {
		fmt.Fprintln(os.Stderr, "audit found discrepancies; no journal state, reservations or remote VMs were changed")
		return 2
	}
	return 0
}
