// admission-demo illustrates single-node quota admission with no cloud calls.
package main

import (
	"encoding/json"
	"fmt"
	"os"

	"agent-runtime-lab/src/admission"
)

func run() error {
	s, err := admission.New(admission.Config{
		Capacity:   admission.Resources{CPU: 4, MemoryMiB: 512},
		MaxPending: 10,
		Tenants: map[string]admission.Policy{
			"alpha": {MaxLogical: 2, MaxActive: admission.Resources{CPU: 4, MemoryMiB: 512}},
			"beta":  {MaxLogical: 2, MaxActive: admission.Resources{CPU: 4, MemoryMiB: 512}},
		},
	})
	if err != nil {
		return err
	}
	for _, r := range []admission.Request{
		{ID: "alpha-1", Tenant: "alpha", Resources: admission.Resources{CPU: 2, MemoryMiB: 256}},
		{ID: "alpha-2", Tenant: "alpha", Resources: admission.Resources{CPU: 2, MemoryMiB: 256}},
		{ID: "beta-1", Tenant: "beta", Resources: admission.Resources{CPU: 2, MemoryMiB: 256}},
	} {
		if _, err := s.Submit(r); err != nil {
			return err
		}
	}
	for _, id := range []string{"alpha-1", "beta-1"} {
		e, ok := s.Dispatch()
		if !ok || e.Request.ID != id {
			return fmt.Errorf("expected fair admission of %s, got %+v", id, e)
		}
		fmt.Printf("RESERVE %s (%d CPU, %d MiB)\n", id, e.Request.Resources.CPU, e.Request.Resources.MemoryMiB)
		if err := s.ConfirmRunning(id); err != nil {
			return err
		}
	}
	if _, ok := s.Dispatch(); ok {
		return fmt.Errorf("overcommitted host capacity")
	}
	fmt.Println("HOLD alpha-2: host is fully reserved")
	if err := s.ConfirmPaused("alpha-1"); err != nil {
		return err
	}
	fmt.Println("PAUSE alpha-1: release active CPU/RAM, retain tenant logical quota")
	e, ok := s.Dispatch()
	if !ok || e.Request.ID != "alpha-2" {
		return fmt.Errorf("alpha-2 not admitted after pause")
	}
	fmt.Println("RESERVE alpha-2: capacity available after pause")
	if err := s.ConfirmRunning("alpha-2"); err != nil {
		return err
	}
	if err := s.QueueResume("alpha-1"); err != nil {
		return err
	}
	if _, ok := s.Dispatch(); ok {
		return fmt.Errorf("resume exceeded capacity")
	}
	fmt.Println("HOLD resume alpha-1: must wait for CPU/RAM")
	if err := s.ConfirmDeleted("beta-1"); err != nil {
		return err
	}
	e, ok = s.Dispatch()
	if !ok || e.Request.ID != "alpha-1" || e.State != admission.ResumeHeld {
		return fmt.Errorf("paused VM was not re-admitted")
	}
	fmt.Println("RESERVE resume alpha-1: capacity released by deletion")
	if err := s.ConfirmRunning("alpha-1"); err != nil {
		return err
	}
	if _, err := s.Submit(admission.Request{ID: "alpha-3", Tenant: "alpha", Resources: admission.Resources{CPU: 1, MemoryMiB: 128}}); err != admission.ErrQuota {
		return fmt.Errorf("expected alpha logical quota, got %v", err)
	}
	fmt.Println("REJECT alpha-3: tenant logical quota reached")
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(s.Snapshot()); err != nil {
		return err
	}
	fmt.Println("PASS: host resource and tenant admission policies respected")
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
}
