// e2b-durable demonstrates crash-safe desired-state reconciliation with the
// running E2B Embed API. One local state file owns one logical sandbox.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"agent-runtime-lab/src/durable"
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
	statePath := flag.String("state-file", ".state/m2-7.json", "durable state JSON file (local filesystem only)")
	template := flag.String("template", "base", "E2B template")
	execute := flag.Bool("execute", false, "acknowledge real E2B mutation")
	ttl := flag.Int("ttl", 900, "sandbox TTL in seconds (30..3600)")
	flag.Parse()
	if len(flag.Args()) != 1 {
		return errors.New("usage: e2b-durable [--state-file PATH] [--execute] start|pause|resume|delete|recover|status")
	}
	action := flag.Arg(0)
	if action != "status" && action != "start" && action != "pause" && action != "resume" && action != "delete" && action != "recover" {
		return fmt.Errorf("unknown action %q", action)
	}
	if action != "status" && !*execute {
		return errors.New("live actions require --execute")
	}
	store, err := durable.OpenFileStore(*statePath)
	if err != nil {
		return err
	}
	defer store.Close()
	r, exists, err := store.Load()
	if err != nil {
		return err
	}
	if action == "status" {
		if !exists {
			return errors.New("no state found; first run start --execute")
		}
		return printJSON(r)
	}
	if !exists && action != "start" {
		return errors.New("no sandbox state; use start first")
	}
	apiURL := os.Getenv("E2B_API_URL")
	if apiURL == "" {
		apiURL = "http://127.0.0.1:3000"
	}
	backend, err := e2bapi.New(e2bapi.Config{
		BaseURL: apiURL, APIKey: os.Getenv("E2B_API_KEY"),
		Template: *template, TTLSeconds: *ttl,
	})
	if err != nil {
		return err
	}
	key := ""
	if !exists {
		var nonce [16]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return err
		}
		key = "agent-runtime-lab/m2-7/" + hex.EncodeToString(nonce[:])
	}
	controller, err := durable.New(store, backend, key, 45*time.Second)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if action == "recover" {
		if err := controller.RecoverCreate(ctx, backend); err != nil {
			return err
		}
		fmt.Println("Recovered original sandbox ID without calling Create")
		return printJSON(controller.Record())
	}
	var desired lifecycle.DesiredState
	switch action {
	case "start", "resume":
		desired = lifecycle.WantRunning
	case "pause":
		desired = lifecycle.WantPaused
	case "delete":
		desired = lifecycle.WantDeleted
	}
	if err := controller.SetDesired(desired); err != nil {
		return err
	}
	for i := 0; i < 15; i++ {
		if err := controller.Reconcile(ctx); err != nil {
			return fmt.Errorf("%w (state file %s; inspect status and recover if create pending)", err, *statePath)
		}
		if controller.Converged() {
			fmt.Println("PASS: desired state converged")
			return printJSON(controller.Record())
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
	return fmt.Errorf("did not converge after 15 passes; state retained in %s", *statePath)
}

func printJSON(r durable.Record) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}
