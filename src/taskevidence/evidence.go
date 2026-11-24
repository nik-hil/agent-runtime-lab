// Package taskevidence defines the bounded, versioned completion record shared
// by fleetd and the source-built helper running inside an owned E2B sandbox.
// Hashes bind observed bytes; they do not attest an untrusted VM or external effects.
package taskevidence

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	Version          = 1
	Root             = "/tmp/agentlab-m43"
	MaxOutputBytes   = 64 << 10
	MaxManifestBytes = 512 << 10
	MaxArtifactBytes = 1 << 20
	MaxArtifacts     = 8
)

type Intent struct {
	Version      int      `json:"version"`
	Nonce        string   `json:"nonce"`
	HelperSHA256 string   `json:"helper_sha256"`
	Artifacts    []string `json:"artifacts"`
}

type Identity struct {
	TaskID        string `json:"task_id"`
	WorkloadID    string `json:"workload_id"`
	SandboxID     string `json:"sandbox_id"`
	CommandSHA256 string `json:"command_sha256"`
	Timeout       int    `json:"timeout_seconds"`
	Intent        Intent `json:"intent"`
}

// Request is sent only to the VM helper. Raw commands never enter the host journal.
type Request struct {
	Identity Identity `json:"identity"`
	Command  string   `json:"command"`
}

type Artifact struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

type Manifest struct {
	Identity     Identity   `json:"identity"`
	CompletedAt  time.Time  `json:"completed_at"`
	ExitCode     int        `json:"exit_code"`
	Stdout       string     `json:"stdout"`
	Stderr       string     `json:"stderr"`
	Truncated    bool       `json:"truncated"`
	StdoutSHA256 string     `json:"stdout_sha256"`
	StderrSHA256 string     `json:"stderr_sha256"`
	Artifacts    []Artifact `json:"artifacts"`
}

type Envelope struct {
	Manifest Manifest `json:"manifest"`
	SHA256   string   `json:"manifest_sha256"`
}

func Hash(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func IsHash(value string) bool {
	b, err := hex.DecodeString(value)
	return err == nil && len(b) == sha256.Size && value == strings.ToLower(value)
}

func validID(id string) bool {
	if id == "" || len(id) > 128 {
		return false
	}
	for _, c := range id {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
			return false
		}
	}
	return true
}

func NormalizeArtifacts(paths []string) ([]string, error) {
	if len(paths) > MaxArtifacts {
		return nil, errors.New("at most 8 artifact paths allowed")
	}
	out := append([]string{}, paths...)
	sort.Strings(out)
	for i, p := range out {
		if len(p) > 256 || !utf8.ValidString(p) || !strings.HasPrefix(p, "/") || path.Clean(p) != p || p == "/" ||
			strings.IndexByte(p, 0) >= 0 || p == Root || strings.HasPrefix(p, Root+"/") ||
			i > 0 && p == out[i-1] {
			return nil, errors.New("artifacts require distinct clean absolute file paths outside the evidence directory")
		}
	}
	return out, nil
}

func NewIntent(helperHash string, paths []string) (Intent, error) {
	paths, err := NormalizeArtifacts(paths)
	if err != nil || !IsHash(helperHash) {
		return Intent{}, errors.New("invalid helper hash or artifact paths")
	}
	var nonce [32]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return Intent{}, err
	}
	return Intent{Version: Version, Nonce: hex.EncodeToString(nonce[:]), HelperSHA256: helperHash, Artifacts: paths}, nil
}

func (i Intent) Validate() error {
	paths, err := NormalizeArtifacts(i.Artifacts)
	if err != nil || i.Version != Version || !IsHash(i.Nonce) || !IsHash(i.HelperSHA256) ||
		!reflect.DeepEqual(paths, i.Artifacts) {
		return errors.New("invalid evidence intent")
	}
	return nil
}

func (i Identity) Validate() error {
	if !validID(i.TaskID) || !validID(i.WorkloadID) || !validID(i.SandboxID) || !IsHash(i.CommandSHA256) ||
		i.Timeout < 1 || i.Timeout > 60 {
		return errors.New("invalid evidence identity")
	}
	return i.Intent.Validate()
}

func (r Request) Validate() error {
	if err := r.Identity.Validate(); err != nil {
		return err
	}
	if r.Command == "" || len(r.Command) > 4000 || !utf8.ValidString(r.Command) || strings.IndexByte(r.Command, 0) >= 0 || Hash([]byte(r.Command)) != r.Identity.CommandSHA256 {
		return errors.New("command does not match evidence identity")
	}
	return nil
}

func ManifestPath(intent Intent) string { return Root + "/" + intent.Nonce + "/completion.json" }

func Seal(manifest Manifest) (Envelope, error) {
	data, err := json.Marshal(manifest)
	if err != nil {
		return Envelope{}, err
	}
	return Envelope{Manifest: manifest, SHA256: Hash(data)}, nil
}

// Validate binds the marker to the original durable intent and to bounded
// output/artifact metadata. Artifact bytes must also be checked independently.
func (e Envelope) Validate(expected Identity) error {
	if err := expected.Validate(); err != nil {
		return err
	}
	m := e.Manifest
	sealed, err := Seal(m)
	if err != nil || !IsHash(e.SHA256) || sealed.SHA256 != e.SHA256 || !reflect.DeepEqual(m.Identity, expected) ||
		m.CompletedAt.IsZero() || m.ExitCode < -1 || m.ExitCode > 255 ||
		len(m.Stdout)+len(m.Stderr) > MaxOutputBytes || m.StdoutSHA256 != Hash([]byte(m.Stdout)) ||
		m.StderrSHA256 != Hash([]byte(m.Stderr)) || m.Artifacts == nil || len(m.Artifacts) != len(expected.Intent.Artifacts) {
		return errors.New("completion manifest identity, hash or output mismatch")
	}
	for n, artifact := range m.Artifacts {
		if artifact.Path != expected.Intent.Artifacts[n] || artifact.Size < 0 || artifact.Size > MaxArtifactBytes || !IsHash(artifact.SHA256) {
			return fmt.Errorf("invalid artifact metadata at index %d", n)
		}
	}
	return nil
}
