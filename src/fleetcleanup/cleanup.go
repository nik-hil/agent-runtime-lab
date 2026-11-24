// Package fleetcleanup drains one existing journal without a daemon or socket.
// It owns the same exclusive fleet lock and never creates or resumes a VM.
package fleetcleanup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/durable"
	"agent-runtime-lab/src/fleet"
)

type Options struct {
	Timeout time.Duration
	Poll    time.Duration
}

type Report struct {
	StateFile  string            `json:"state_file"`
	Complete   bool              `json:"complete"`
	Fleet      fleet.Status      `json:"fleet"`
	Unresolved []string          `json:"unresolved"`
	Errors     map[string]string `json:"errors,omitempty"`
}

func unresolved(state fleet.Status) []string {
	remaining := []string{}
	for id, job := range state.Jobs {
		if job.Entry.State != admission.Deleted || job.Workload.SandboxID != "" || job.Workload.CreatePending {
			remaining = append(remaining, id)
		}
	}
	sort.Strings(remaining)
	return remaining
}

func snapshot(path string, c *fleet.Controller, failures map[string]string) Report {
	state := c.Status()
	remaining := unresolved(state)
	return Report{StateFile: path, Fleet: state, Unresolved: remaining, Errors: failures,
		Complete: len(remaining) == 0 && state.Admission.Used.CPU == 0 &&
			state.Admission.Used.MemoryMiB == 0 && state.Admission.Pending == 0}
}

func failedSnapshot(path string, c *fleet.Controller, failures map[string]string) Report {
	report := snapshot(path, c, failures)
	// A failed fsync/rename can leave in-memory counters ahead of the durable
	// checkpoint. Never print complete=true when persistence is uncertain.
	report.Complete = false
	return report
}

// Run requires an existing regular journal and exclusive ownership. It leaves
// fleet/task journals, lock files and socket paths intact. Completion means all
// workloads in THIS journal are confirmed deleted and reservations are zero.
func Run(ctx context.Context, path string, backend fleet.Runtime, opts Options) (Report, error) {
	if opts.Timeout == 0 {
		opts.Timeout = 120 * time.Second
	}
	if opts.Poll == 0 {
		opts.Poll = 500 * time.Millisecond
	}
	if path == "" || backend == nil || opts.Timeout < time.Second || opts.Timeout > 10*time.Minute ||
		opts.Poll < 100*time.Millisecond || opts.Poll > 5*time.Second {
		return Report{}, errors.New("existing state-file, runtime, timeout 1s..10m and poll 100ms..5s required")
	}
	if err := ctx.Err(); err != nil {
		return Report{}, err
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return Report{}, err
	}
	info, err := os.Lstat(abs)
	if err != nil || !info.Mode().IsRegular() {
		return Report{}, errors.New("cleanup requires an existing regular fleet journal; no new journal was created")
	}
	store, err := fleet.Open(abs)
	if err != nil {
		return Report{}, fmt.Errorf("cleanup cannot own fleet journal; stop the daemon or other journal owner first: %w", err)
	}
	defer store.Close()
	journal, exists, err := store.Load()
	if err != nil {
		return Report{}, err
	}
	if !exists {
		return Report{}, errors.New("fleet journal disappeared; refusing to initialize a new fleet")
	}
	// Restore the exact recorded policies/shape, not CLI resource defaults.
	c, err := fleet.New(store, backend, fleet.Config{Admission: journal.Scheduler.Config, TemplateSize: journal.TemplateSize}, 30*time.Second)
	if err != nil {
		return Report{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()
	if _, err := c.BeginDrain(); err != nil {
		return failedSnapshot(abs, c, nil), err
	}
	failures := make(map[string]string)
	blocked := make(map[string]bool)
	for {
		report := snapshot(abs, c, failures)
		if report.Complete {
			return report, nil
		}
		for _, id := range report.Unresolved {
			if ctx.Err() != nil {
				break
			}
			if blocked[id] {
				continue
			}
			job := c.Status().Jobs[id]
			if job.Workload.CreatePending {
				// Metadata lookup can recover an ID, never authorize a new Create.
				// A complete zero/multiple-match result stays fenced and is not
				// polled again automatically during this cleanup invocation.
				err = c.Recover(ctx, id)
				if err != nil {
					failures[id] = err.Error()
					blocked[id] = errors.Is(err, durable.ErrNoMatch) || errors.Is(err, durable.ErrMultipleMatches)
					if errors.Is(err, fleet.ErrStoreUncertain) {
						return failedSnapshot(abs, c, failures), err
					}
					continue
				}
			}
			_, err = c.DrainStep(ctx, id)
			if err != nil {
				failures[id] = err.Error()
				if errors.Is(err, fleet.ErrStoreUncertain) {
					return failedSnapshot(abs, c, failures), err
				}
			} else {
				delete(failures, id)
			}
		}
		report = snapshot(abs, c, failures)
		if report.Complete {
			return report, nil
		}
		allBlocked := len(report.Unresolved) > 0
		for _, id := range report.Unresolved {
			allBlocked = allBlocked && blocked[id]
		}
		if allBlocked {
			return report, errors.New("known workloads drained, but ambiguous Create outcomes remain; inspect metadata before retrying cleanup; reservations retained")
		}
		timer := time.NewTimer(opts.Poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return snapshot(abs, c, failures), fmt.Errorf("cleanup incomplete; deletion intent persisted and unresolved reservations retained: %w", ctx.Err())
		case <-timer.C:
		}
	}
}
