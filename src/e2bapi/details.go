package e2bapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/fleet"
	"agent-runtime-lab/src/lifecycle"
)

var _ fleet.Runtime = (*Backend)(nil)

// Describe verifies the actual E2B shape before a VM can be counted RUNNING
// in the admission controller. E2B GET /sandboxes/{id} exposes cpuCount and
// memoryMB at pinned Embed runtime 7278c2a38. Never trust caller-submitted size.
func (b *Backend) Describe(ctx context.Context, id string) (fleet.Details, error) {
	path, err := sandboxPath(id)
	if err != nil {
		return fleet.Details{}, err
	}
	data, status, err := b.do(ctx, http.MethodGet, path, nil, http.StatusOK, http.StatusNotFound)
	if err != nil {
		return fleet.Details{}, err
	}
	if status == http.StatusNotFound {
		return fleet.Details{State: lifecycle.Absent}, nil
	}
	var out struct {
		SandboxID string `json:"sandboxID"`
		State     string `json:"state"`
		CPU       int64  `json:"cpuCount"`
		Memory    int64  `json:"memoryMB"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return fleet.Details{}, fmt.Errorf("invalid E2B sandbox detail: %w", err)
	}
	if out.SandboxID != id {
		return fleet.Details{}, errors.New("E2B sandbox detail ID mismatch")
	}
	var state lifecycle.ObservedState
	switch out.State {
	case "running":
		state = lifecycle.Running
	case "paused":
		state = lifecycle.Paused
	default:
		return fleet.Details{}, fmt.Errorf("unsupported E2B state %q", out.State)
	}
	return fleet.Details{State: state, Resources: admission.Resources{CPU: out.CPU, MemoryMiB: out.Memory}}, nil
}
