// Package cluster schedules bounded agent jobs across independent fleetd
// servers. Quorum state controls dispatch; a lost heartbeat never proves a
// command or VM disappeared and never authorizes replay of dispatched work.
package cluster

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/taskevidence"
)

const MaxJobs = 128
const MaxNodes = 16

var (
	ErrConflict = errors.New("cluster identity, ownership or state conflict")
	ErrFull     = errors.New("cluster or tenant admission capacity reached")
	ErrNotFound = errors.New("cluster record not found")
	ErrFenced   = errors.New("cluster lease or ownership fence rejected mutation")
)

type Policy struct {
	MaxLogical int                 `json:"max_logical"`
	MaxActive  admission.Resources `json:"max_active"`
}
type Config struct {
	Version  int               `json:"version"`
	ID       string            `json:"id"`
	Replicas int               `json:"artifact_replicas"`
	Tenants  map[string]Policy `json:"tenants"`
}

func DefaultConfig(id string) Config {
	return Config{Version: 1, ID: id, Replicas: 2, Tenants: map[string]Policy{
		"alpha": {MaxLogical: 8, MaxActive: admission.Resources{CPU: 8, MemoryMiB: 8192}},
		"beta":  {MaxLogical: 8, MaxActive: admission.Resources{CPU: 8, MemoryMiB: 8192}},
	}}
}

type Request struct {
	ID        string              `json:"id"`
	Tenant    string              `json:"tenant"`
	Command   string              `json:"command,omitempty"`
	Timeout   int                 `json:"timeout_seconds"`
	Artifacts []string            `json:"artifacts"`
	Resources admission.Resources `json:"resources"`
	Labels    map[string]string   `json:"labels,omitempty"`
}

type Job struct {
	Revision     int64                  `json:"-"`
	Request      Request                `json:"request"`
	CommandHash  string                 `json:"command_sha256"`
	Sequence     uint64                 `json:"sequence"`
	State        string                 `json:"state"`
	Epoch        uint64                 `json:"epoch"`
	Node         string                 `json:"node_id,omitempty"`
	Incarnation  string                 `json:"node_incarnation,omitempty"`
	LocalID      string                 `json:"local_id,omitempty"`
	Accepted     bool                   `json:"accepted,omitempty"`
	Cancel       bool                   `json:"cancel_requested,omitempty"`
	Released     bool                   `json:"resources_released,omitempty"`
	Identity     *taskevidence.Identity `json:"evidence_identity,omitempty"`
	ManifestHash string                 `json:"manifest_sha256,omitempty"`
	ExitCode     int                    `json:"exit_code"`
	Replicas     []string               `json:"artifact_nodes,omitempty"`
	CreatedAt    time.Time              `json:"created_at"`
	Note         string                 `json:"note,omitempty"`
}

type Node struct {
	ID          string              `json:"id"`
	Incarnation string              `json:"incarnation"`
	URL         string              `json:"url"`
	Capacity    admission.Resources `json:"capacity"`
	Template    admission.Resources `json:"template"`
	Labels      map[string]string   `json:"labels,omitempty"`
	Draining    bool                `json:"draining"`
	ForeignWork bool                `json:"foreign_work"`
}

type Leader struct {
	ID    string `json:"id"`
	Token string `json:"token"`
	URL   string `json:"url"`
}
type Live struct {
	Node        string `json:"node"`
	Incarnation string `json:"incarnation"`
	Token       string `json:"token"`
}

func SafeID(id string) bool {
	if id == "" || len(id) > 128 || id == "." || id == ".." {
		return false
	}
	for _, c := range id {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
			return false
		}
	}
	return true
}
func Token() string {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
func encoded(value any) []byte {
	b, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return b
}
func positive(r admission.Resources) bool {
	return r.CPU > 0 && r.CPU <= 256 && r.MemoryMiB > 0 && r.MemoryMiB <= 1<<20
}
func add(a, b admission.Resources) admission.Resources {
	return admission.Resources{CPU: a.CPU + b.CPU, MemoryMiB: a.MemoryMiB + b.MemoryMiB}
}
func fits(a, b admission.Resources) bool { return a.CPU <= b.CPU && a.MemoryMiB <= b.MemoryMiB }
func available(cap, used admission.Resources) admission.Resources {
	return admission.Resources{CPU: cap.CPU - used.CPU, MemoryMiB: cap.MemoryMiB - used.MemoryMiB}
}
func labelsOK(labels map[string]string) bool {
	if len(labels) > 8 {
		return false
	}
	for k, v := range labels {
		if !SafeID(k) || len(k) > 32 || !SafeID(v) || len(v) > 64 {
			return false
		}
	}
	return true
}
func matches(want, have map[string]string) bool {
	for k, v := range want {
		if have[k] != v {
			return false
		}
	}
	return true
}
func logical(j Job) bool {
	return j.State == "queued" || j.State == "assigned" || j.State == "dispatched" || j.State == "uncertain" || j.State == "collecting" || !j.Released && j.Node != ""
}
func reserved(j Job) bool { return j.Node != "" && !j.Released }
func localID(clusterID, id string, epoch uint64) string {
	return "m5-" + taskevidence.Hash(encoded([]any{clusterID, id, epoch}))[:32]
}

func normalize(req Request) (Request, error) {
	if !SafeID(req.ID) || !SafeID(req.Tenant) || req.Command == "" || len(req.Command) > 4000 || !utf8.ValidString(req.Command) || strings.IndexByte(req.Command, 0) >= 0 || req.Timeout < 1 || req.Timeout > 60 || !positive(req.Resources) || !labelsOK(req.Labels) {
		return req, errors.New("invalid bounded cluster request")
	}
	if len(req.Labels) == 0 {
		req.Labels = nil
	}
	paths, err := taskevidence.NormalizeArtifacts(req.Artifacts)
	req.Artifacts = paths
	return req, err
}
func sameRequest(a Job, b Request) bool {
	command := b.Command
	b.Command = ""
	old := a.Request
	old.Command = ""
	return a.CommandHash == taskevidence.Hash([]byte(command)) && reflect.DeepEqual(old, b)
}
func public(job Job) Job { job.Request.Command = ""; return job }
