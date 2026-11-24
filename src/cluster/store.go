package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/coordkv"
	"agent-runtime-lab/src/taskevidence"
)

type Repository struct {
	KV     coordkv.Store
	Prefix string
}
type Snapshot struct {
	Config       Config
	Gate         uint64
	GateRevision int64
	Leader       Leader
	LeaderValue  coordkv.Value
	Draining     bool
	Jobs         map[string]Job
	Nodes        map[string]Node
	Live         map[string]coordkv.Value
	Values       map[string]coordkv.Value
}

func NewRepository(kv coordkv.Store, id string) (*Repository, error) {
	if kv == nil || !SafeID(id) {
		return nil, errors.New("coordination backend and safe cluster ID required")
	}
	return &Repository{KV: kv, Prefix: "/agent-runtime-lab/m5/" + id + "/"}, nil
}

func decode(data []byte, out any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return err
	}
	if d.Decode(new(any)) != io.EOF {
		return errors.New("trailing cluster data")
	}
	return nil
}

func (r *Repository) Read(ctx context.Context) (Snapshot, error) {
	values, err := r.KV.Range(ctx, r.Prefix)
	if err != nil {
		return Snapshot{}, err
	}
	s := Snapshot{Jobs: map[string]Job{}, Nodes: map[string]Node{}, Live: map[string]coordkv.Value{}, Values: map[string]coordkv.Value{}}
	for _, v := range values {
		key := strings.TrimPrefix(string(v.Key), r.Prefix)
		if key == string(v.Key) {
			return s, errors.New("backend returned a key outside the cluster namespace")
		}
		s.Values[key] = v
		switch {
		case key == "config":
			err = decode(v.Data, &s.Config)
		case key == "gate":
			s.Gate, err = strconv.ParseUint(string(v.Data), 10, 64)
			s.GateRevision = v.Mod
		case key == "leader":
			err = decode(v.Data, &s.Leader)
			s.LeaderValue = v
			if v.Lease == 0 {
				err = errors.New("leader record lacks lease")
			}
		case key == "draining":
			err = decode(v.Data, &s.Draining)
		case strings.HasPrefix(key, "jobs/"):
			var j Job
			err = decode(v.Data, &j)
			j.Revision = v.Mod
			if err == nil && (!SafeID(j.Request.ID) || key != "jobs/"+j.Request.ID || j.Sequence == 0 || j.CommandHash == "" || j.CreatedAt.IsZero()) {
				err = errors.New("invalid job checkpoint")
			}
			s.Jobs[j.Request.ID] = j
		case strings.HasPrefix(key, "nodes/"):
			var n Node
			err = decode(v.Data, &n)
			if err == nil && (!SafeID(n.ID) || key != "nodes/"+n.ID || !taskevidence.IsHash(n.Incarnation) || !positive(n.Capacity) || !positive(n.Template) || !fits(n.Template, n.Capacity) || !labelsOK(n.Labels)) {
				err = errors.New("invalid node checkpoint")
			}
			s.Nodes[n.ID] = n
		case strings.HasPrefix(key, "live/"):
			var live Live
			err = decode(v.Data, &live)
			if err == nil && (key != "live/"+live.Node || v.Lease == 0) {
				err = errors.New("invalid node lease")
			}
			s.Live[live.Node] = v
		default:
			err = errors.New("unknown cluster checkpoint key")
		}
		if err != nil {
			return s, fmt.Errorf("invalid cluster checkpoint %q: %w", key, err)
		}
	}
	if len(s.Jobs) > MaxJobs || len(s.Nodes) > MaxNodes {
		return s, errors.New("cluster bound exceeded")
	}
	if s.Config.Version != 0 && (s.Config.Version != 1 || !SafeID(s.Config.ID) || r.Prefix != "/agent-runtime-lab/m5/"+s.Config.ID+"/" || s.Gate == 0) {
		return s, errors.New("invalid cluster configuration")
	}
	if err := validateSnapshot(s); err != nil {
		return s, err
	}
	return s, nil
}

func compareMod(key string, revision int64) coordkv.Compare {
	return coordkv.Compare{Key: key, Mod: &revision}
}

func (r *Repository) Bootstrap(ctx context.Context, c Config) error {
	if c.Version != 1 || !SafeID(c.ID) || r.Prefix != "/agent-runtime-lab/m5/"+c.ID+"/" || c.Replicas < 1 || c.Replicas > 3 || len(c.Tenants) == 0 {
		return errors.New("invalid cluster configuration")
	}
	for tenant, p := range c.Tenants {
		if !SafeID(tenant) || p.MaxLogical < 1 || p.MaxLogical > MaxJobs || !positive(p.MaxActive) {
			return errors.New("invalid global tenant policy")
		}
	}
	s, err := r.Read(ctx)
	if err != nil {
		return err
	}
	if s.Config.Version != 0 {
		if !bytes.Equal(encoded(s.Config), encoded(c)) {
			return ErrConflict
		}
		return nil
	}
	ok, err := r.KV.Txn(ctx, []coordkv.Compare{compareMod(r.Prefix+"config", 0), compareMod(r.Prefix+"gate", 0)}, []coordkv.Put{{Key: r.Prefix + "config", Data: encoded(c)}, {Key: r.Prefix + "gate", Data: []byte("1")}})
	if err != nil {
		return err
	}
	if !ok {
		view, readErr := r.Read(ctx)
		if readErr == nil && equalData(encoded(view.Config), encoded(c)) {
			return nil
		}
		return ErrConflict
	}
	return nil
}

// Commit serializes every durable cluster mutation against a snapshot gate.
// Lease deletions also require explicit value comparisons when they matter.
func (r *Repository) Commit(ctx context.Context, s Snapshot, leader *Leader, extra []coordkv.Compare, puts ...coordkv.Put) (bool, error) {
	if s.Config.Version != 1 || s.Gate == math.MaxUint64 {
		return false, errors.New("cluster not bootstrapped or mutation sequence exhausted")
	}
	cmp := []coordkv.Compare{compareMod(r.Prefix+"gate", s.GateRevision)}
	if leader != nil {
		if *leader != s.Leader || s.LeaderValue.Lease == 0 {
			return false, ErrFenced
		}
		cmp = append(cmp, coordkv.Compare{Key: r.Prefix + "leader", Data: encoded(*leader)})
	}
	cmp = append(cmp, extra...)
	for i := range puts {
		puts[i].Key = r.Prefix + puts[i].Key
	}
	puts = append(puts, coordkv.Put{Key: r.Prefix + "gate", Data: []byte(strconv.FormatUint(s.Gate+1, 10))})
	return r.KV.Txn(ctx, cmp, puts)
}

func (r *Repository) Submit(ctx context.Context, req Request) (Job, bool, error) {
	req, err := normalize(req)
	if err != nil {
		return Job{}, false, err
	}
	for attempt := 0; attempt < 8; attempt++ {
		s, err := r.Read(ctx)
		if err != nil {
			return Job{}, false, err
		}
		if old, ok := s.Jobs[req.ID]; ok {
			if !sameRequest(old, req) {
				return Job{}, false, ErrConflict
			}
			return public(old), false, nil
		}
		if s.Draining {
			return Job{}, false, ErrFenced
		}
		policy, ok := s.Config.Tenants[req.Tenant]
		if !ok || !fits(req.Resources, policy.MaxActive) {
			return Job{}, false, errors.New("unknown tenant or request exceeds tenant capacity")
		}
		count := 0
		for _, j := range s.Jobs {
			if j.Request.Tenant == req.Tenant && logical(j) {
				count++
			}
		}
		if len(s.Jobs) >= MaxJobs || count >= policy.MaxLogical {
			return Job{}, false, ErrFull
		}
		job := Job{Request: req, CommandHash: taskevidence.Hash([]byte(req.Command)), Sequence: s.Gate + 1, State: "queued", CreatedAt: time.Now().UTC()}
		ok, err = r.Commit(ctx, s, nil, nil, coordkv.Put{Key: "jobs/" + req.ID, Data: encoded(job)})
		if err != nil {
			return Job{}, false, err
		}
		if ok {
			return public(job), true, nil
		}
	}
	return Job{}, false, ErrConflict
}

// Drain durably closes new admission and atomically cancels never-dispatched
// jobs. Dispatched work retains its owner and reservations until confirmed.
func (r *Repository) Drain(ctx context.Context) error {
	for attempt := 0; attempt < 8; attempt++ {
		s, err := r.Read(ctx)
		if err != nil {
			return err
		}
		puts := []coordkv.Put{{Key: "draining", Data: []byte("true")}}
		for _, j := range s.Jobs {
			if j.State == "queued" || j.State == "assigned" {
				j.State = "cancelled"
				j.Cancel = true
				j.Released = true
				j.Request.Command = ""
				j.Note = "cluster drained before dispatch"
				puts = append(puts, coordkv.Put{Key: "jobs/" + j.Request.ID, Data: encoded(j)})
			}
		}
		// etcd's default transaction bound is 128 operations including compares.
		// Cancel at most 100 entries per transaction; the durable admission gate
		// prevents new placement while subsequent passes finish cancellation.
		if len(puts) > 101 {
			puts = puts[:101]
		}
		ok, err := r.Commit(ctx, s, nil, nil, puts...)
		if err != nil {
			return err
		}
		if ok {
			if len(puts) < 101 {
				return nil
			}
			continue
		}
	}
	return ErrConflict
}

func (r *Repository) Cancel(ctx context.Context, id string) (Job, error) {
	for attempt := 0; attempt < 8; attempt++ {
		s, err := r.Read(ctx)
		if err != nil {
			return Job{}, err
		}
		j, ok := s.Jobs[id]
		if !ok {
			return j, ErrNotFound
		}
		if j.State == "succeeded" || j.State == "failed" || j.State == "cancelled" {
			return public(j), nil
		}
		j.Cancel = true
		if j.State == "queued" || j.State == "assigned" {
			j.State = "cancelled"
			j.Released = true
			j.Request.Command = ""
			j.Note = "cancelled before distributed dispatch"
		}
		ok, err = r.Commit(ctx, s, nil, nil, coordkv.Put{Key: "jobs/" + id, Data: encoded(j)})
		if err != nil {
			return j, err
		}
		if ok {
			return public(j), nil
		}
	}
	return Job{}, ErrConflict
}

func liveFor(s Snapshot, n Node) (coordkv.Value, bool) {
	v, ok := s.Live[n.ID]
	if !ok {
		return v, false
	}
	var live Live
	if decode(v.Data, &live) != nil || live.Incarnation != n.Incarnation {
		return v, false
	}
	return v, true
}

func usage(s Snapshot, node, tenant string) admission.Resources {
	var used admission.Resources
	for _, j := range s.Jobs {
		if reserved(j) && (node == "" || j.Node == node) && (tenant == "" || j.Request.Tenant == tenant) {
			used = add(used, j.Request.Resources)
		}
	}
	return used
}

func ordered(s Snapshot) []Job {
	jobs := make([]Job, 0, len(s.Jobs))
	for _, j := range s.Jobs {
		jobs = append(jobs, j)
	}
	sort.Slice(jobs, func(a, b int) bool { return jobs[a].Sequence < jobs[b].Sequence })
	return jobs
}

func (r *Repository) DrainNode(ctx context.Context, id string) error {
	for attempt := 0; attempt < 8; attempt++ {
		s, err := r.Read(ctx)
		if err != nil {
			return err
		}
		n, ok := s.Nodes[id]
		if !ok {
			return ErrNotFound
		}
		n.Draining = true
		ok, err = r.Commit(ctx, s, nil, nil, coordkv.Put{Key: "nodes/" + id, Data: encoded(n)})
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
	}
	return ErrConflict
}
