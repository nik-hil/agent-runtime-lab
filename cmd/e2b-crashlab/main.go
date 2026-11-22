// Command e2b-crashlab deliberately SIGKILLs itself after a successful E2B
// Create response but before durable.Controller can persist the sandbox ID.
// It exists solely to verify the create-pending fence and metadata recovery.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"syscall"
	"time"

	"agent-runtime-lab/src/durable"
	"agent-runtime-lab/src/e2bapi"
	"agent-runtime-lab/src/lifecycle"
)

// crashBackend delegates to the real provider before injecting the failure.
// If signal delivery unexpectedly returns, Create reports an ambiguous result
// so the durable controller never writes a guessed sandbox ID.
type crashBackend struct {
	lifecycle.Backend
	afterCreate func(string) error
}

func (b crashBackend) Create(ctx context.Context, key string) (string, error) {
	id, err := b.Backend.Create(ctx, key)
	if err != nil || id == "" {
		return id, err
	}
	if err := b.afterCreate(id); err != nil {
		return "", fmt.Errorf("crash injection failed after remote create: %w", err)
	}
	return "", errors.New("crash injection unexpectedly returned after remote create")
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	flags := flag.NewFlagSet("e2b-crashlab", flag.ContinueOnError)
	execute := flags.Bool("execute", false, "allow real E2B Create")
	confirm := flags.Bool("confirm-sigkill", false, "explicitly permit SIGKILL after remote Create succeeds")
	statePath := flags.String("state-file", "", "required unused local path; do not reuse M2.7's normal state file")
	ttl := flags.Int("ttl", 300, "sandbox TTL in seconds (30..3600)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if !*execute || !*confirm || *statePath == "" {
		return errors.New("requires --execute --confirm-sigkill --state-file NEW_PATH")
	}
	store, err := durable.OpenFileStore(*statePath)
	if err != nil {
		return err
	}
	defer store.Close()
	if _, exists, err := store.Load(); err != nil {
		return err
	} else if exists {
		return errors.New("refusing to inject crash: state file already exists")
	}

	apiURL := os.Getenv("E2B_API_URL")
	if apiURL == "" {
		apiURL = "http://127.0.0.1:3000"
	}
	backend, err := e2bapi.New(e2bapi.Config{
		BaseURL:    apiURL,
		APIKey:     os.Getenv("E2B_API_KEY"),
		Template:   "base",
		TTLSeconds: *ttl,
	})
	if err != nil {
		return err
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	logicalKey := "agent-runtime-lab/m2-7-crash/" + hex.EncodeToString(nonce[:])
	wrapped := crashBackend{Backend: backend, afterCreate: func(id string) error {
		fmt.Fprintf(os.Stderr, "EXPECTED CRASH: E2B created sandbox %s; terminating before saving ID.\n", id)
		fmt.Fprintf(os.Stderr, "Recover using --state-file %s with e2b-durable.\n", *statePath)
		return syscall.Kill(os.Getpid(), syscall.SIGKILL)
	}}
	controller, err := durable.New(store, wrapped, logicalKey, 45*time.Second)
	if err != nil {
		return err
	}
	// durable.Reconcile writes create_pending=true *before* the remote Create.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	if err := controller.Reconcile(ctx); err != nil {
		return err
	}
	return errors.New("fault injection failed to terminate the controller")
}
