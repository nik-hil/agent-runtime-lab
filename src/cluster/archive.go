package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"agent-runtime-lab/src/artifactstore"
	"agent-runtime-lab/src/coordkv"
	"agent-runtime-lab/src/taskevidence"
	"agent-runtime-lab/src/taskjournal"
)

const MaxTransfer = 12 << 20

type Transfer struct {
	Leader   Leader                `json:"leader"`
	Epoch    uint64                `json:"epoch"`
	Evidence taskevidence.Envelope `json:"evidence"`
	Files    [][]byte              `json:"files"`
}

func (t Transfer) validate(job Job) error {
	if job.Identity == nil || t.Epoch != job.Epoch || t.Evidence.Validate(*job.Identity) != nil || t.Evidence.SHA256 != job.ManifestHash || t.Evidence.Manifest.ExitCode != job.ExitCode || len(t.Files) != len(t.Evidence.Manifest.Artifacts) {
		return errors.New("artifact transfer binding mismatch")
	}
	for index, meta := range t.Evidence.Manifest.Artifacts {
		if int64(len(t.Files[index])) != meta.Size || taskevidence.Hash(t.Files[index]) != meta.SHA256 {
			return errors.New("artifact transfer bytes mismatch")
		}
	}
	return nil
}

func (a *Agent) bundle(ctx context.Context, job Job) (Transfer, error) {
	transfer := Transfer{Epoch: job.Epoch, Files: [][]byte{}}
	if job.Identity == nil {
		return transfer, ErrConflict
	}
	evidence, err := a.Archive.Verify(*job.Identity)
	if err == nil {
		transfer.Evidence = evidence
		for index := range evidence.Manifest.Artifacts {
			data, _, err := a.Archive.Read(*job.Identity, index)
			if err != nil {
				return transfer, err
			}
			transfer.Files = append(transfer.Files, data)
		}
	} else if errors.Is(err, artifactstore.ErrNotFound) && job.Node == a.Node.ID && job.Incarnation == a.Node.Incarnation {
		local, err := a.Fleet.Job(ctx, job.LocalID)
		if err != nil {
			return transfer, err
		}
		if !local.ArtifactsStored || local.Evidence == nil || (local.State != taskjournal.Succeeded && local.State != taskjournal.Failed) {
			return transfer, ErrConflict
		}
		transfer.Evidence = *local.Evidence
		for index := range local.Evidence.Manifest.Artifacts {
			data, err := a.Fleet.Artifact(ctx, job.LocalID, index)
			if err != nil {
				return transfer, err
			}
			transfer.Files = append(transfer.Files, data)
		}
	} else {
		return transfer, err
	}
	return transfer, transfer.validate(job)
}

func readJSON(r *http.Request, out any, limit int64) error {
	data, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil || int64(len(data)) > limit {
		return errors.New("request exceeds bounds")
	}
	return decode(data, out)
}
func reply(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_, _ = w.Write(append(encoded(value), '\n'))
}
func apiError(w http.ResponseWriter, err error) {
	code := http.StatusServiceUnavailable
	switch {
	case errors.Is(err, ErrNotFound), errors.Is(err, artifactstore.ErrNotFound):
		code = http.StatusNotFound
	case errors.Is(err, ErrConflict), errors.Is(err, ErrFenced):
		code = http.StatusConflict
	case errors.Is(err, ErrFull), errors.Is(err, artifactstore.ErrQuota):
		code = http.StatusTooManyRequests
	}
	reply(w, code, map[string]any{"error": err.Error()})
}

func (a *Agent) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/node/status" && r.Method == http.MethodGet {
			_, ready := a.session()
			reply(w, http.StatusOK, map[string]any{"node": a.Node.ID, "incarnation": a.Node.Incarnation, "lease_ready": ready, "storage": a.Archive.Stats()})
			return
		}
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/node/jobs/"), "/")
		if !strings.HasPrefix(r.URL.Path, "/v1/node/jobs/") || len(parts) != 2 || !SafeID(parts[0]) {
			reply(w, http.StatusNotFound, map[string]string{"error": "unknown node route"})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		s, err := a.Repo.Read(ctx)
		if err != nil {
			apiError(w, err)
			return
		}
		job, exists := s.Jobs[parts[0]]
		if !exists {
			apiError(w, ErrNotFound)
			return
		}
		if parts[1] == "bundle" && r.Method == http.MethodGet {
			transfer, err := a.bundle(ctx, job)
			if err != nil {
				apiError(w, err)
				return
			}
			reply(w, http.StatusOK, transfer)
			return
		}
		if parts[1] == "bundle" && r.Method == http.MethodPost {
			var transfer Transfer
			if err := readJSON(r, &transfer, MaxTransfer); err != nil {
				reply(w, http.StatusBadRequest, map[string]string{"error": "invalid bounded artifact transfer"})
				return
			}
			if s.Leader != transfer.Leader || s.Leader.ID == "" || s.LeaderValue.Lease == 0 {
				apiError(w, ErrFenced)
				return
			}
			if transfer.validate(job) != nil {
				apiError(w, ErrConflict)
				return
			}
			if err := a.Archive.Put(ctx, *job.Identity, transfer.Evidence, func(_ context.Context, meta taskevidence.Artifact) ([]byte, error) {
				for index, a := range transfer.Evidence.Manifest.Artifacts {
					if a.Path == meta.Path {
						return transfer.Files[index], nil
					}
				}
				return nil, ErrNotFound
			}); err != nil {
				apiError(w, err)
				return
			}
			reply(w, http.StatusOK, map[string]string{"manifest_sha256": transfer.Evidence.SHA256, "node_incarnation": a.Node.Incarnation})
			return
		}
		if parts[1] == "recover" && r.Method == http.MethodPost {
			var input struct {
				Leader Leader `json:"leader"`
				Epoch  uint64 `json:"epoch"`
			}
			if readJSON(r, &input, 4096) != nil {
				reply(w, http.StatusBadRequest, map[string]string{"error": "invalid recovery request"})
				return
			}
			if input.Leader != s.Leader || s.Leader.ID == "" || input.Epoch != job.Epoch || job.Node != a.Node.ID || job.Incarnation != a.Node.Incarnation {
				apiError(w, ErrFenced)
				return
			}
			if err := a.Fleet.Recover(ctx, job.LocalID); err != nil {
				apiError(w, err)
				return
			}
			reply(w, http.StatusOK, map[string]string{"note": "original evidence inspected; command was not rerun"})
			return
		}
		reply(w, http.StatusNotFound, map[string]string{"error": "unknown node operation"})
	})
}

// Fetch chooses only already recorded replica locations plus the original
// owner. Read-only fallback is safe; it never calls execution/recovery APIs.
func Fetch(ctx context.Context, client *http.Client, s Snapshot, job Job) (Transfer, error) {
	names := append([]string{}, job.Replicas...)
	names = append(names, job.Node)
	seen := map[string]bool{}
	for _, id := range names {
		if seen[id] {
			continue
		}
		seen[id] = true
		n, exists := s.Nodes[id]
		if !exists || !ValidURL(n.URL) {
			continue
		}
		var transfer Transfer
		if err := call(ctx, client, http.MethodGet, n.URL+"/v1/node/jobs/"+job.Request.ID+"/bundle", nil, &transfer, MaxTransfer); err == nil && transfer.validate(job) == nil {
			return transfer, nil
		}
	}
	return Transfer{}, errors.New("no verified artifact replica reachable; no execution was attempted")
}

func ArtifactIndex(value string) (int, error) {
	index, err := strconv.Atoi(value)
	if err != nil || index < 0 || strconv.Itoa(index) != value {
		return 0, fmt.Errorf("invalid artifact index")
	}
	return index, nil
}

func publish(r *Repository, ctx context.Context, s Snapshot, leader Leader, job Job) (bool, error) {
	return r.Commit(ctx, s, &leader, nil, coordkv.Put{Key: "jobs/" + job.Request.ID, Data: encoded(job)})
}
