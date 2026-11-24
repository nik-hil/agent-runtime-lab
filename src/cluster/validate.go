package cluster

import (
	"errors"
	"reflect"

	"agent-runtime-lab/src/taskevidence"
)

func validateSnapshot(s Snapshot) error {
	if s.Config.Version == 0 {
		if len(s.Values) != 0 {
			return errors.New("orphan cluster keys without configuration")
		}
		return nil
	}
	if s.Config.Replicas < 1 || s.Config.Replicas > 3 || len(s.Config.Tenants) == 0 {
		return errors.New("invalid cluster replication/policy configuration")
	}
	for tenant, p := range s.Config.Tenants {
		if !SafeID(tenant) || p.MaxLogical < 1 || p.MaxLogical > MaxJobs || !positive(p.MaxActive) {
			return errors.New("invalid tenant policy checkpoint")
		}
	}
	if s.Leader.ID != "" && (!SafeID(s.Leader.ID) || !taskevidence.IsHash(s.Leader.Token) || !ValidURL(s.Leader.URL)) {
		return errors.New("invalid leader identity")
	}
	for _, n := range s.Nodes {
		if !ValidURL(n.URL) {
			return errors.New("invalid node origin")
		}
	}
	for id, v := range s.Live {
		var live Live
		if decode(v.Data, &live) != nil || !taskevidence.IsHash(live.Token) || s.Nodes[id].Incarnation != live.Incarnation {
			return errors.New("invalid node session binding")
		}
	}
	seq := map[uint64]bool{}
	for _, j := range s.Jobs {
		if !SafeID(j.Request.Tenant) || !positive(j.Request.Resources) || !labelsOK(j.Request.Labels) || j.Request.Timeout < 1 || j.Request.Timeout > 60 || !taskevidence.IsHash(j.CommandHash) || j.Sequence > s.Gate || seq[j.Sequence] {
			return errors.New("invalid job request checkpoint")
		}
		seq[j.Sequence] = true
		paths, err := taskevidence.NormalizeArtifacts(j.Request.Artifacts)
		if err != nil || !reflect.DeepEqual(paths, j.Request.Artifacts) {
			return errors.New("invalid job artifact checkpoint")
		}
		if j.Request.Command != "" && taskevidence.Hash([]byte(j.Request.Command)) != j.CommandHash {
			return errors.New("cluster command digest mismatch")
		}
		switch j.State {
		case "queued", "assigned":
			if j.Request.Command == "" {
				return errors.New("missing undispatched command")
			}
		case "dispatched", "uncertain", "collecting", "succeeded", "failed", "cancelled":
		default:
			return errors.New("invalid global job state")
		}
		if j.Node != "" {
			n, ok := s.Nodes[j.Node]
			if !ok || j.Incarnation != n.Incarnation || j.Epoch == 0 || j.LocalID != localID(s.Config.ID, j.Request.ID, j.Epoch) {
				return errors.New("invalid placement ownership binding")
			}
		}
		if j.State != "queued" && j.State != "cancelled" && j.Node == "" {
			return errors.New("owned job lacks placement")
		}
		if j.Identity != nil {
			id := *j.Identity
			if id.Validate() != nil || id.TaskID != j.LocalID || id.WorkloadID != j.LocalID || id.CommandSHA256 != j.CommandHash || id.Timeout != j.Request.Timeout || !reflect.DeepEqual(id.Intent.Artifacts, j.Request.Artifacts) {
				return errors.New("invalid global completion binding")
			}
		}
		if j.State == "collecting" || j.State == "succeeded" || j.State == "failed" {
			if j.Identity == nil || !taskevidence.IsHash(j.ManifestHash) {
				return errors.New("missing global completion proof")
			}
		}
		seen := map[string]bool{}
		for _, id := range j.Replicas {
			if _, ok := s.Nodes[id]; !ok || seen[id] {
				return errors.New("invalid artifact replica catalog")
			}
			seen[id] = true
		}
		if (j.State == "succeeded" || j.State == "failed") && len(j.Replicas) < s.Config.Replicas {
			return errors.New("terminal job lacks durable replica receipts")
		}
	}
	return nil
}
