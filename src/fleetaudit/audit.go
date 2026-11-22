// Package fleetaudit compares a durable fleet journal with E2B without
// changing the journal or making any remote mutations.
package fleetaudit

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/fleet"
	"agent-runtime-lab/src/lifecycle"
)

type Provider interface {
	Describe(context.Context, string) (fleet.Details, error)
	FindByLogicalKey(context.Context, string) ([]string, error)
}

type Finding struct {
	JobID     string `json:"job_id"`
	Code      string `json:"code"`
	Severity  string `json:"severity"`
	Detail    string `json:"detail"`
	SandboxID string `json:"sandbox_id,omitempty"`
}

type Report struct {
	AuditedAt      time.Time `json:"audited_at"`
	Scope          string    `json:"scope"`
	Workloads      int       `json:"workloads"`
	Findings       []Finding `json:"findings"`
	RequiresReview bool      `json:"requires_review"`
}

// Run checks exact metadata keys already stored in the journal. It cannot
// discover VMs created without a known metadata key. GET 404 and empty list
// results must NEVER be treated as authorization to release reservations.
func Run(ctx context.Context, j fleet.Journal, p Provider) (Report, error) {
	r := Report{AuditedAt: time.Now().UTC(), Scope: "known_journal_metadata_keys_only",
		Workloads: len(j.Workloads), Findings: []Finding{}}
	if p == nil {
		return Report{}, errors.New("provider required")
	}
	if _, err := admission.Restore(j.Scheduler); err != nil {
		return Report{}, fmt.Errorf("invalid scheduler checkpoint: %w", err)
	}
	if len(j.Workloads) != len(j.Scheduler.Jobs) {
		return Report{}, errors.New("workload and admission records disagree")
	}
	add := func(jobID, code, severity, detail, sandboxID string) {
		r.Findings = append(r.Findings, Finding{JobID: jobID, Code: code,
			Severity: severity, Detail: detail, SandboxID: sandboxID})
		if severity != "info" {
			r.RequiresReview = true
		}
	}
	if j.Unsafe {
		add("", "journal_unsafe", "critical", "journal already blocks admission; investigate the saved unsafe reason", "")
	}
	ids := make([]string, 0, len(j.Workloads))
	for id := range j.Workloads {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	knownKeys := make(map[string]bool)
	for _, id := range ids {
		w, e := j.Workloads[id], j.Scheduler.Jobs[id]
		if w.LogicalKey == "" || knownKeys[w.LogicalKey] || e.Request.ID != id || e.Request.Resources != j.TemplateSize {
			return Report{}, fmt.Errorf("invalid or duplicate journal workload %q", id)
		}
		knownKeys[w.LogicalKey] = true
		if err := ctx.Err(); err != nil {
			return Report{}, err
		}
		matches, err := p.FindByLogicalKey(ctx, w.LogicalKey)
		if err != nil {
			return Report{}, fmt.Errorf("metadata inventory for %q failed; do not infer absence: %w", id, err)
		}
		sort.Strings(matches)
		for i, match := range matches {
			if match == "" || i > 0 && matches[i-1] == match {
				return Report{}, fmt.Errorf("invalid provider inventory for %q", id)
			}
		}
		if len(matches) > 1 {
			add(id, "duplicate_metadata_key", "critical", "multiple active VMs match one known key; never retry Create", "")
		}
		if e.State == admission.Deleted {
			if w.SandboxID != "" || w.CreatePending {
				return Report{}, fmt.Errorf("deleted workload %q has a live identity", id)
			}
			if len(matches) > 0 {
				add(id, "orphan_after_deletion", "critical", "active VM matches a deleted journal record; inspect manually", "")
			}
			continue
		}
		if w.CreatePending {
			if w.SandboxID != "" {
				return Report{}, fmt.Errorf("pending Create for %q also has sandbox ID", id)
			}
			if len(matches) == 1 {
				add(id, "ambiguous_create_match", "warning", "one candidate; use explicit recovery before deletion", matches[0])
			} else if len(matches) == 0 {
				add(id, "ambiguous_create_no_match", "warning", "empty list does not prove remote Create never happened", "")
			}
			continue
		}
		if w.SandboxID == "" {
			if len(matches) > 0 {
				add(id, "remote_vm_without_journal_id", "critical", "active VM has matching metadata but journal lacks remote ID", "")
			}
			continue
		}
		observed, err := p.Describe(ctx, w.SandboxID)
		if err != nil {
			return Report{}, fmt.Errorf("describe %q failed; reservation retained: %w", id, err)
		}
		matched := false
		for _, match := range matches {
			matched = matched || match == w.SandboxID
		}
		if len(matches) > 0 && !matched {
			add(id, "identity_mismatch", "critical", "metadata lists VM different from journal ID", w.SandboxID)
		}
		if observed.State == lifecycle.Absent {
			if w.Desired == "deleted" {
				add(id, "deletion_not_yet_reconciled", "info", "GET observed absence; deletion not yet journaled", w.SandboxID)
			} else {
				add(id, "unexpected_absence", "critical", "GET 404 without delete intent; possible TTL expiry; do not recreate", w.SandboxID)
			}
			continue
		}
		if len(matches) == 0 {
			add(id, "missing_from_metadata_inventory", "warning", "GET works but active metadata list is empty; may be eventually consistent", w.SandboxID)
		}
		if observed.Resources.CPU <= 0 || observed.Resources.MemoryMiB <= 0 ||
			observed.Resources.CPU > e.Request.Resources.CPU || observed.Resources.MemoryMiB > e.Request.Resources.MemoryMiB {
			add(id, "unexpected_resource_shape", "critical", "observed VM resources exceed or invalidate reservation", w.SandboxID)
		}
		if w.Desired == "running" && e.State == admission.Running && observed.State != lifecycle.Running ||
			w.Desired == "paused" && e.State == admission.Paused && observed.State != lifecycle.Paused {
			add(id, "observed_state_mismatch", "warning", "provider state differs from converged journal state", w.SandboxID)
		}
	}
	return r, nil
}
