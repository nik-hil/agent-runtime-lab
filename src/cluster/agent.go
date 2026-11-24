package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"reflect"
	"sync"
	"time"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/artifactstore"
	"agent-runtime-lab/src/fleetdaemon"
	"agent-runtime-lab/src/taskjournal"
)

type Agent struct {
	Repo      *Repository
	State     *LocalState
	Fleet     Fleet
	Archive   *artifactstore.Store
	Node      Node
	TTL       int64
	Logger    *slog.Logger
	HoldClaim string // explicit test-only pause, no provider calls
	mu        sync.Mutex
	live      Live
	ready     bool
}

func ValidURL(address string) bool {
	u, err := url.Parse(address)
	return err == nil && u.Scheme == "https" && u.Hostname() != "" && u.Port() != "" && u.User == nil && u.Path == "" && u.RawQuery == "" && u.Fragment == ""
}

func (a *Agent) session() (Live, bool) { a.mu.Lock(); defer a.mu.Unlock(); return a.live, a.ready }
func (a *Agent) setSession(live Live, ready bool) {
	a.mu.Lock()
	a.live = live
	a.ready = ready
	a.mu.Unlock()
}

func (a *Agent) Run(ctx context.Context) error {
	if a.Repo == nil || a.State == nil || a.Fleet == nil || a.Archive == nil || !SafeID(a.Node.ID) || !ValidURL(a.Node.URL) || a.TTL < 3 || a.TTL > 120 {
		return errors.New("invalid node agent configuration")
	}
	if a.Logger == nil {
		a.Logger = slog.Default()
	}
	a.Node.Incarnation = a.State.Incarnation()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	heartbeatDone := make(chan struct{})
	go func() { defer close(heartbeatDone); a.heartbeat(ctx) }()
	defer func() { cancel(); <-heartbeatDone }()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return nil
		}
		if err := a.Step(ctx); err != nil {
			if errors.Is(err, ErrLocalState) {
				return err
			}
			a.Logger.Warn("cluster_agent_step_failed", "node_id", a.Node.ID)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (a *Agent) heartbeat(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(a.TTL) * time.Second / 3)
	defer ticker.Stop()
	var lease int64
	var live Live
	defer func() {
		if lease != 0 {
			end, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = a.Repo.KV.Revoke(end, lease)
		}
	}()
	for {
		if ctx.Err() != nil {
			return
		}
		step, cancel := context.WithTimeout(ctx, 5*time.Second)
		caps, err := a.Fleet.Capabilities(step)
		if err == nil && (!positive(caps.Capacity) || !positive(caps.Template) || !fits(caps.Template, caps.Capacity)) {
			err = errors.New("invalid local fleet shape")
		}
		var view Snapshot
		if err == nil {
			view, err = a.Repo.Read(step)
		}
		var status fleetdaemon.Status
		if err == nil {
			status, err = a.Fleet.Status(step)
		}
		if err == nil {
			n := a.Node
			n.Capacity = caps.Capacity
			n.Template = caps.Template
			n.ForeignWork = status.Fleet.Unsafe || caps.Workers == 0 || caps.Helper == ""
			known := map[string]bool{}
			for _, job := range view.Jobs {
				if job.Node == n.ID && job.Incarnation == n.Incarnation {
					known[job.LocalID] = true
				}
			}
			for id, vm := range status.Fleet.Jobs {
				if vm.Entry.State != admission.Deleted && !known[id] {
					n.ForeignWork = true
				}
			}
			if lease == 0 {
				lease, err = a.Repo.KV.Grant(step, a.TTL)
				live = Live{Node: n.ID, Incarnation: n.Incarnation, Token: Token()}
			} else {
				err = a.Repo.KV.KeepAlive(step, lease)
				if err != nil {
					lease = 0
				}
			}
			if err == nil {
				err = a.Repo.Register(step, n, live, lease)
			}
		}
		a.setSession(live, err == nil)
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (a *Agent) Step(ctx context.Context) error {
	live, ready := a.session()
	if !ready {
		return nil
	}
	step, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	s, err := a.Repo.Read(step)
	if err != nil {
		return err
	}
	current, exists := s.Live[a.Node.ID]
	if !exists || !equalData(current.Data, encoded(live)) {
		return ErrFenced
	}
	for _, job := range ordered(s) {
		if job.Node != a.Node.ID || job.Incarnation != a.Node.Incarnation {
			continue
		}
		if job.State == "assigned" {
			if a.HoldClaim == job.Request.ID {
				continue
			}
			if err := a.State.Prepare(job); err != nil {
				return err
			}
			_, err := a.Repo.Claim(step, job.Request.ID, job.Epoch, live)
			return err
		}
		if job.State == "queued" || job.Released && (job.State == "succeeded" || job.State == "failed" || job.State == "cancelled") {
			continue
		}
		if err := a.advance(step, s, job, live); err != nil {
			return err
		}
	}
	return nil
}

func (a *Agent) unknown(ctx context.Context, job Job, live Live, note string) error {
	if job.State == "succeeded" || job.State == "failed" || job.State == "collecting" {
		return nil
	}
	job.State = "uncertain"
	job.Note = note
	_, err := a.Repo.UpdateOwned(ctx, job, live)
	return err
}

func (a *Agent) advance(ctx context.Context, s Snapshot, job Job, live Live) error {
	intent, exists, err := a.State.Get(job.Request.ID)
	if err != nil {
		return err
	}
	if !exists || intent.Epoch != job.Epoch {
		return a.unknown(ctx, job, live, "node execution journal missing; no action or replay authorized")
	}
	status, err := a.Fleet.Status(ctx)
	if err != nil {
		return a.unknown(ctx, job, live, "local fleet unavailable; pinned to original node")
	}
	vm, hasVM := status.Fleet.Jobs[job.LocalID]
	terminal := job.State == "succeeded" || job.State == "failed" || job.State == "cancelled"
	if terminal {
		if hasVM && vm.Entry.State == admission.Deleted || !hasVM && !intent.VMRequestSent {
			job.Released = true
			_, err = a.Repo.UpdateOwned(ctx, job, live)
			return err
		}
		if hasVM && vm.Workload.Desired != "deleted" {
			return a.Fleet.DeleteVM(ctx, job.LocalID)
		}
		return nil // a missing record after a sent VM request is not absence proof
	}
	if intent.JobRequestSent {
		local, err := a.Fleet.Job(ctx, job.LocalID)
		if err != nil {
			return a.unknown(ctx, job, live, "command request sent but local record unavailable; never resend")
		}
		if hasVM && vm.Entry.State == admission.Deleted {
			job.Released = true
		}
		sandbox := ""
		if hasVM {
			sandbox = vm.Workload.SandboxID
		}
		return a.observe(ctx, job, live, local, sandbox)
	}
	if job.Cancel && !intent.JobRequestSent {
		job.State = "cancelled"
		job.Request.Command = ""
		job.Note = "cancelled before local command intent"
		_, err = a.Repo.UpdateOwned(ctx, job, live)
		return err
	}
	if !hasVM {
		if intent.VMRequestSent {
			return a.unknown(ctx, job, live, "VM request sent but local record unavailable; do not recreate")
		}
		send, err := a.State.Mark(job.Request.ID, job.Epoch, false)
		if err != nil || !send {
			return err
		}
		if err := a.Fleet.SubmitVM(ctx, job.LocalID, job.Request.Tenant); err != nil {
			return a.unknown(ctx, job, live, "VM admission response unconfirmed; inspect original node")
		}
		return nil
	}
	if vm.Entry.Request.Resources != job.Request.Resources {
		return a.unknown(ctx, job, live, "local VM shape differs from global reservation; node configuration requires review")
	}
	if vm.Entry.State != admission.Running {
		if vm.Entry.State == admission.Uncertain || vm.Entry.State == admission.Deleted {
			return a.unknown(ctx, job, live, "VM execution state unavailable; no relocation")
		}
		return nil
	}
	local, err := a.Fleet.Job(ctx, job.LocalID)
	if errors.Is(err, ErrNotFound) {
		if intent.JobRequestSent {
			return a.unknown(ctx, job, live, "command intent sent but local task record missing; never resend")
		}
		if job.Request.Command == "" {
			return a.unknown(ctx, job, live, "missing undispatched command bytes; no replay authorized")
		}
		send, err := a.State.Mark(job.Request.ID, job.Epoch, true)
		if err != nil || !send {
			return err
		}
		if err := a.Fleet.SubmitJob(ctx, fleetdaemon.JobRequest{ID: job.LocalID, WorkloadID: job.LocalID, Command: job.Request.Command, Timeout: job.Request.Timeout, Artifacts: job.Request.Artifacts}); err != nil {
			return a.unknown(ctx, job, live, "local command acceptance unconfirmed; inspect same local ID")
		}
		return nil
	}
	if err != nil {
		return a.unknown(ctx, job, live, "local job response unavailable; no command retry")
	}
	return a.observe(ctx, job, live, local, vm.Workload.SandboxID)
}

func (a *Agent) observe(ctx context.Context, job Job, live Live, local taskjournal.Entry, sandbox string) error {
	if !local.Async || local.ID != job.LocalID || local.WorkloadID != job.LocalID || sandbox != "" && local.SandboxID != sandbox || local.CommandHash != job.CommandHash || local.Timeout != job.Request.Timeout || local.EvidenceIntent == nil {
		return a.unknown(ctx, job, live, "local job binding mismatch")
	}
	if !reflect.DeepEqual(local.EvidenceIntent.Artifacts, job.Request.Artifacts) {
		return a.unknown(ctx, job, live, "local artifact intent differs from cluster request")
	}
	job.Accepted = true
	job.Request.Command = ""
	identity := local.EvidenceIdentity()
	job.Identity = &identity
	if job.Cancel && local.State != taskjournal.Succeeded && local.State != taskjournal.Failed && local.State != taskjournal.Cancelled {
		if err := a.Fleet.Cancel(ctx, job.LocalID); err != nil {
			return err
		}
	}
	switch local.State {
	case taskjournal.Succeeded, taskjournal.Failed:
		if !local.ArtifactsStored || local.Evidence == nil || local.Result == nil || local.Evidence.Validate(identity) != nil {
			return a.unknown(ctx, job, live, "local job lacks verified durable completion")
		}
		job.State = "collecting"
		job.ManifestHash = local.Evidence.SHA256
		job.ExitCode = local.Result.ExitCode
		job.Note = "awaiting independent artifact replicas"
	case taskjournal.Uncertain:
		job.State = "uncertain"
		job.Note = "local task outcome uncertain; explicit evidence recovery required"
	case taskjournal.Cancelled:
		job.State = "cancelled"
		job.Note = "local queued task cancelled before execution"
	case taskjournal.Queued, taskjournal.Running:
		if job.State != "uncertain" {
			job.State = "dispatched"
		}
		job.Note = "observing original local job"
	default:
		return fmt.Errorf("unknown local job state %q", local.State)
	}
	_, err := a.Repo.UpdateOwned(ctx, job, live)
	return err
}
