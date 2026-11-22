package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/fleetdaemon"
)

// pendingDrain checks the *observed* states, not merely desired deletion or
// E2B DELETE ACKs. A create-pending workload is never considered deleted.
func pendingDrain(state fleetdaemon.Status) []string {
	remaining := make([]string, 0)
	for id, job := range state.Fleet.Jobs {
		if job.Entry.State != admission.Deleted || job.Workload.SandboxID != "" || job.Workload.CreatePending {
			remaining = append(remaining, fmt.Sprintf("%s(%s,desired=%s,pending_create=%t)", id,
				job.Entry.State, job.Workload.Desired, job.Workload.CreatePending))
		}
	}
	if state.Fleet.Admission.Used.CPU != 0 || state.Fleet.Admission.Used.MemoryMiB != 0 || state.Fleet.Admission.Pending != 0 {
		remaining = append(remaining, fmt.Sprintf("reservations(cpu=%d,mib=%d,pending=%d)",
			state.Fleet.Admission.Used.CPU, state.Fleet.Admission.Used.MemoryMiB, state.Fleet.Admission.Pending))
	}
	sort.Strings(remaining)
	return remaining
}

func readDrainStatus(ctx context.Context, client *http.Client) (fleetdaemon.Status, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://fleetd/v1/status", nil)
	if err != nil {
		return fleetdaemon.Status{}, err
	}
	res, err := client.Do(req)
	if err != nil {
		return fleetdaemon.Status{}, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fleetdaemon.Status{}, fmt.Errorf("status HTTP %d", res.StatusCode)
	}
	var raw struct {
		Fleet json.RawMessage `json:"fleet"`
	}
	limited := io.LimitReader(res.Body, 2<<20)
	if err := json.NewDecoder(limited).Decode(&raw); err != nil {
		return fleetdaemon.Status{}, err
	}
	if len(raw.Fleet) == 0 {
		return fleetdaemon.Status{}, errors.New("missing fleet status")
	}
	var required struct {
		Admission json.RawMessage `json:"admission"`
		Jobs      json.RawMessage `json:"jobs"`
	}
	if err := json.Unmarshal(raw.Fleet, &required); err != nil || len(required.Admission) == 0 || len(required.Jobs) == 0 {
		return fleetdaemon.Status{}, errors.New("incomplete fleet status")
	}
	var state fleetdaemon.Status
	if err := json.Unmarshal(raw.Fleet, &state.Fleet); err != nil {
		return fleetdaemon.Status{}, err
	}
	return state, nil
}

func waitForDrain(client *http.Client, timeout, poll time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	var remaining []string
	for {
		state, err := readDrainStatus(ctx, client)
		if err != nil {
			return fmt.Errorf("cannot verify drain status; do not assume cleanup: %w", err)
		}
		remaining = pendingDrain(state)
		if len(remaining) == 0 {
			fmt.Println("PASS: all journal workloads confirmed deleted; CPU=0 MiB=0 pending=0. You may now stop the daemon with Ctrl+C.")
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("drain incomplete after %s; unresolved: %s. Daemon continues reconciling; inspect status/recover and retry drain",
				timeout, strings.Join(remaining, ", "))
		case <-ticker.C:
		}
	}
}
