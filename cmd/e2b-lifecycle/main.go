// Command e2b-lifecycle runs the educational lifecycle controller against E2B
// Embed's loopback API. It does not replace or modify E2B components.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"agent-runtime-lab/src/e2bapi"
	"agent-runtime-lab/src/lifecycle"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
}

func run() error {
	execute := flag.Bool("execute", false, "required to create a real E2B sandbox")
	template := flag.String("template", "base", "E2B template ID")
	flag.Parse()
	if !*execute {
		return errors.New("live sandbox operations require --execute (run simulated demo with go run ./cmd/lifecycle-demo)")
	}
	apiURL := os.Getenv("E2B_API_URL")
	if apiURL == "" {
		apiURL = "http://127.0.0.1:3000"
	}
	b, err := e2bapi.New(e2bapi.Config{
		BaseURL: apiURL, APIKey: os.Getenv("E2B_API_KEY"), Template: *template, TTLSeconds: 300,
	})
	if err != nil {
		return err
	}
	// An interrupted CLI run still receives a short independent cleanup period.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	c, err := lifecycle.New(b, "agent-runtime-lab/m2-6", 45*time.Second)
	if err != nil {
		return err
	}
	var createdID string
	defer func() {
		if createdID == "" {
			return
		}
		cleanup, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		if err := b.Delete(cleanup, createdID); err != nil {
			fmt.Fprintf(os.Stderr, "CLEANUP FAILED for sandbox %s: %v\n", createdID, err)
		} else {
			fmt.Printf("Cleanup confirmed for sandbox %s\n", createdID)
		}
	}()
	for _, desired := range []lifecycle.DesiredState{lifecycle.WantRunning, lifecycle.WantPaused, lifecycle.WantRunning, lifecycle.WantDeleted} {
		if err := c.SetDesired(desired); err != nil {
			return err
		}
		fmt.Println("DESIRED:", desired)
		if err := converge(ctx, c, &createdID); err != nil {
			if errors.Is(err, lifecycle.ErrCreateUncertain) {
				fmt.Fprintln(os.Stderr, "CREATE UNCERTAIN: find sandbox by metadata agent_runtime_lab_key=agent-runtime-lab/m2-6; do not run a second create blindly")
			}
			return err
		}
	}
	fmt.Println("PASS: live E2B sandbox lifecycle converged")
	return nil
}

func converge(ctx context.Context, c *lifecycle.Controller, createdID *string) error {
	const maxPasses = 15
	for i := 0; i < maxPasses; i++ {
		if err := c.Reconcile(ctx); err != nil {
			return err
		}
		status := c.Status()
		if status.SandboxID != "" && *createdID == "" {
			*createdID = status.SandboxID
		}
		fmt.Printf("  observed=%s converged=%t sandbox=%s\n", status.Observed, status.Converged, status.SandboxID)
		if status.Converged {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
	return errors.New("controller did not converge after 15 passes")
}
