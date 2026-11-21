// lifecycle-demo exercises the in-memory reconciler without contacting E2B.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"agent-runtime-lab/src/lifecycle"
)

// demoBackend models a runtime for teaching only. It creates no VM.
type demoBackend struct {
	states map[string]lifecycle.ObservedState
}

func (b *demoBackend) Create(_ context.Context, key string) (string, error) {
	id := "example-sandbox-1"
	b.states[id] = lifecycle.Running
	fmt.Printf("  backend.Create(%q) -> %s\n", key, id)
	return id, nil
}

func (b *demoBackend) Observe(_ context.Context, id string) (lifecycle.ObservedState, error) {
	if state, found := b.states[id]; found {
		return state, nil
	}
	return lifecycle.Absent, nil
}

func (b *demoBackend) Pause(_ context.Context, id string) error {
	fmt.Printf("  backend.Pause(%s)\n", id)
	b.states[id] = lifecycle.Paused
	return nil
}

func (b *demoBackend) Resume(_ context.Context, id string) error {
	fmt.Printf("  backend.Resume(%s)\n", id)
	b.states[id] = lifecycle.Running
	return nil
}

func (b *demoBackend) Delete(_ context.Context, id string) error {
	fmt.Printf("  backend.Delete(%s)\n", id)
	delete(b.states, id)
	return nil
}

func run() error {
	b := &demoBackend{states: make(map[string]lifecycle.ObservedState)}
	c, err := lifecycle.New(b, "tenant-a/task-42", 2*time.Second)
	if err != nil {
		return err
	}
	ctx := context.Background()
	for _, desired := range []lifecycle.DesiredState{
		lifecycle.WantRunning, lifecycle.WantPaused,
		lifecycle.WantRunning, lifecycle.WantDeleted,
	} {
		fmt.Printf("DESIRED=%s\n", desired)
		if err := c.SetDesired(desired); err != nil {
			return err
		}
		for i := 0; i < 4; i++ {
			if err := c.Reconcile(ctx); err != nil {
				return err
			}
			state := c.Status()
			fmt.Printf("  observed=%s converged=%t\n", state.Observed, state.Converged)
			if state.Converged {
				break
			}
		}
		if !c.Status().Converged {
			return fmt.Errorf("could not reach desired state %s", desired)
		}
	}
	fmt.Println("PASS: simulated sandbox lifecycle converged")
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
