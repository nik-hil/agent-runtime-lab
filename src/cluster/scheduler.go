package cluster

import (
	"context"
	"errors"
	"sort"

	"agent-runtime-lab/src/coordkv"
)

// Campaign grants a server-managed lease, then claims the empty leader key.
// A lost transaction response is resolved by reading the exact token back.
func (r *Repository) Campaign(ctx context.Context, id, address string, ttl int64) (Leader, int64, bool, error) {
	if !SafeID(id) {
		return Leader{}, 0, false, ErrConflict
	}
	s, err := r.Read(ctx)
	if err != nil {
		return Leader{}, 0, false, err
	}
	if s.Config.Version != 1 || !ValidURL(address) {
		return Leader{}, 0, false, ErrConflict
	}
	if s.Leader.ID != "" {
		return Leader{}, 0, false, nil
	}
	lease, err := r.KV.Grant(ctx, ttl)
	if err != nil {
		return Leader{}, 0, false, err
	}
	leader := Leader{ID: id, Token: Token(), URL: address}
	ok, err := r.KV.Txn(ctx, []coordkv.Compare{compareMod(r.Prefix+"leader", 0)}, []coordkv.Put{{Key: r.Prefix + "leader", Data: encoded(leader), Lease: lease}})
	if err != nil {
		view, readErr := r.Read(ctx)
		if readErr == nil && view.Leader == leader {
			return leader, lease, true, nil
		}
		return Leader{}, lease, false, err
	}
	if !ok {
		_ = r.KV.Revoke(ctx, lease)
		return Leader{}, 0, false, nil
	}
	return leader, lease, ok, nil
}

// Schedule makes at most one placement/health transition. Every placement is
// guarded by both the current leader token and selected node's live session.
// The snapshot gate serializes capacity changes with node/worker reports.
func (r *Repository) Schedule(ctx context.Context, leader Leader) (bool, error) {
	s, err := r.Read(ctx)
	if err != nil {
		return false, err
	}
	if s.Leader != leader {
		return false, ErrFenced
	}
	for _, job := range ordered(s) {
		node, exists := s.Nodes[job.Node]
		_, live := liveFor(s, node)
		if job.State == "assigned" && (!exists || !live || node.Draining || node.ForeignWork) {
			job.State = "queued"
			job.Epoch++
			job.Node = ""
			job.Incarnation = ""
			job.LocalID = ""
			job.Note = "never-dispatched assignment returned to queue after node became unavailable"
			return r.Commit(ctx, s, &leader, nil, coordkv.Put{Key: "jobs/" + job.Request.ID, Data: encoded(job)})
		}
		if job.State == "dispatched" && (!exists || !live) {
			job.State = "uncertain"
			job.Note = "worker lease absent; dispatched work stays pinned; command was not relocated"
			return r.Commit(ctx, s, &leader, nil, coordkv.Put{Key: "jobs/" + job.Request.ID, Data: encoded(job)})
		}
	}
	if s.Draining {
		return false, nil
	}
	names := make([]string, 0, len(s.Nodes))
	for name := range s.Nodes {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, job := range ordered(s) {
		if job.State != "queued" || job.Cancel {
			continue
		}
		policy, exists := s.Config.Tenants[job.Request.Tenant]
		if !exists {
			return false, errors.New("job references missing tenant policy")
		}
		if !fits(job.Request.Resources, available(policy.MaxActive, usage(s, "", job.Request.Tenant))) {
			continue
		}
		selected := ""
		score := int64(1 << 62)
		for _, name := range names {
			n := s.Nodes[name]
			_, live := liveFor(s, n)
			if !live || n.Draining || n.ForeignWork || n.Template != job.Request.Resources || !matches(job.Request.Labels, n.Labels) {
				continue
			}
			used := usage(s, name, "")
			if !fits(job.Request.Resources, available(n.Capacity, used)) {
				continue
			}
			load := used.CPU*10000/n.Capacity.CPU + used.MemoryMiB*10000/n.Capacity.MemoryMiB
			if load < score {
				selected, score = name, load
			}
		}
		if selected == "" {
			continue
		}
		n := s.Nodes[selected]
		live, _ := liveFor(s, n)
		job.Epoch++
		job.Node = n.ID
		job.Incarnation = n.Incarnation
		job.LocalID = localID(s.Config.ID, job.Request.ID, job.Epoch)
		job.State = "assigned"
		job.Note = ""
		return r.Commit(ctx, s, &leader, []coordkv.Compare{{Key: r.Prefix + "live/" + n.ID, Data: live.Data}}, coordkv.Put{Key: "jobs/" + job.Request.ID, Data: encoded(job)})
	}
	return false, nil
}

func (r *Repository) Register(ctx context.Context, n Node, live Live, lease int64) error {
	if !SafeID(n.ID) || n.Incarnation != live.Incarnation || n.ID != live.Node || lease == 0 || !positive(n.Capacity) || !positive(n.Template) || !fits(n.Template, n.Capacity) || !labelsOK(n.Labels) {
		return ErrConflict
	}
	for attempt := 0; attempt < 8; attempt++ {
		s, err := r.Read(ctx)
		if err != nil {
			return err
		}
		if old, exists := s.Nodes[n.ID]; exists {
			if old.Incarnation != n.Incarnation {
				return errors.New("node ID is bound to another durable incarnation; use a new node ID")
			}
			n.Draining = old.Draining
		} else if len(s.Nodes) >= MaxNodes {
			return ErrFull
		}
		if current, exists := s.Live[n.ID]; exists && !equalData(current.Data, encoded(live)) {
			return ErrFenced
		}
		if current, exists := s.Live[n.ID]; exists && current.Lease == lease && equalData(current.Data, encoded(live)) && equalData(encoded(s.Nodes[n.ID]), encoded(n)) {
			return nil
		}
		cmp := compareMod(r.Prefix+"live/"+n.ID, 0)
		if current, exists := s.Live[n.ID]; exists {
			cmp = coordkv.Compare{Key: r.Prefix + "live/" + n.ID, Data: current.Data}
		}
		ok, err := r.Commit(ctx, s, nil, []coordkv.Compare{cmp}, coordkv.Put{Key: "nodes/" + n.ID, Data: encoded(n)}, coordkv.Put{Key: "live/" + n.ID, Data: encoded(live), Lease: lease})
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
	}
	return ErrConflict
}

func equalData(a, b []byte) bool { return string(a) == string(b) }

// Claim is the irreversible distributed dispatch point. Losing a node lease
// after this commit cannot authorize another node to execute this job.
func (r *Repository) Claim(ctx context.Context, id string, epoch uint64, live Live) (bool, error) {
	s, err := r.Read(ctx)
	if err != nil {
		return false, err
	}
	job, exists := s.Jobs[id]
	if !exists {
		return false, ErrNotFound
	}
	if s.Draining || job.State != "assigned" || job.Cancel || job.Epoch != epoch || job.Node != live.Node || job.Incarnation != live.Incarnation || s.Leader.ID == "" {
		return false, ErrFenced
	}
	node, exists := s.Nodes[live.Node]
	if !exists || node.Draining || node.ForeignWork {
		return false, ErrFenced
	}
	job.State = "dispatched"
	return r.Commit(ctx, s, &s.Leader, []coordkv.Compare{{Key: r.Prefix + "live/" + live.Node, Data: encoded(live)}}, coordkv.Put{Key: "jobs/" + id, Data: encoded(job)})
}

func (r *Repository) UpdateOwned(ctx context.Context, job Job, live Live) (bool, error) {
	s, err := r.Read(ctx)
	if err != nil {
		return false, err
	}
	old, exists := s.Jobs[job.Request.ID]
	if !exists {
		return false, ErrNotFound
	}
	if old.Revision != job.Revision {
		return false, nil
	}
	if old.Node != live.Node || old.Incarnation != live.Incarnation || old.Epoch != job.Epoch || old.LocalID != job.LocalID {
		return false, ErrFenced
	}
	if old.State == "queued" || old.State == "assigned" || old.State == "cancelled" && old.Released {
		return false, ErrFenced
	}
	// A worker report cannot undo cancellation, published proof or release.
	job.Cancel = job.Cancel || old.Cancel
	if old.State == "succeeded" || old.State == "failed" {
		job.State = old.State
		job.ManifestHash = old.ManifestHash
		job.Replicas = old.Replicas
		job.Identity = old.Identity
		job.ExitCode = old.ExitCode
	}
	job.Released = old.Released || job.Released
	if equalData(encoded(old), encoded(job)) {
		return true, nil
	}
	return r.Commit(ctx, s, nil, []coordkv.Compare{{Key: r.Prefix + "live/" + live.Node, Data: encoded(live)}}, coordkv.Put{Key: "jobs/" + job.Request.ID, Data: encoded(job)})
}
