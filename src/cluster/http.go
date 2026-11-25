package cluster

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"agent-runtime-lab/src/taskevidence"
)

func (c *Coordinator) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		if r.URL.Path == "/v1/drain" && r.Method == http.MethodPost {
			var empty struct{}
			if readJSON(r, &empty, 4096) != nil {
				reply(w, 400, map[string]string{"error": "empty object required"})
				return
			}
			if err := c.Repo.Drain(ctx); err != nil {
				apiError(w, err)
				return
			}
			reply(w, 202, map[string]string{"state": "draining; dispatched work retains ownership until completion or evidence review"})
			return
		}
		if r.URL.Path == "/v1/jobs" && r.Method == http.MethodPost {
			var input Request
			if readJSON(r, &input, 64<<10) != nil {
				reply(w, http.StatusBadRequest, map[string]string{"error": "invalid bounded job request"})
				return
			}
			if _, err := normalize(input); err != nil {
				reply(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
				return
			}
			job, created, err := c.Repo.Submit(ctx, input)
			if err != nil {
				apiError(w, err)
				return
			}
			code := http.StatusOK
			if created {
				code = http.StatusAccepted
			}
			reply(w, code, job)
			return
		}
		s, err := c.Repo.Read(ctx)
		if err != nil {
			apiError(w, err)
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/v1/status" {
			nodes := make([]map[string]any, 0, len(s.Nodes))
			for _, n := range s.Nodes {
				_, live := liveFor(s, n)
				nodes = append(nodes, map[string]any{"node": n, "alive": live, "reserved": usage(s, n.ID, "")})
			}
			counts := map[string]int{}
			for _, j := range s.Jobs {
				counts[j.State]++
			}
			reply(w, http.StatusOK, map[string]any{"cluster_id": s.Config.ID, "draining": s.Draining, "leader": map[string]any{"id": s.Leader.ID, "url": s.Leader.URL, "revision": s.LeaderValue.Create}, "nodes": nodes, "job_states": counts, "artifact_replicas": s.Config.Replicas})
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/v1/jobs" {
			jobs := make([]Job, 0, len(s.Jobs))
			for _, j := range ordered(s) {
				jobs = append(jobs, public(j))
			}
			reply(w, http.StatusOK, map[string]any{"jobs": jobs})
			return
		}
		if strings.HasPrefix(r.URL.Path, "/v1/nodes/") && r.Method == http.MethodPost {
			parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/nodes/"), "/")
			if len(parts) != 2 || !SafeID(parts[0]) || parts[1] != "drain" {
				apiError(w, ErrNotFound)
				return
			}
			var empty struct{}
			if readJSON(r, &empty, 4096) != nil {
				reply(w, http.StatusBadRequest, map[string]string{"error": "empty object required"})
				return
			}
			if err := c.Repo.DrainNode(ctx, parts[0]); err != nil {
				apiError(w, err)
				return
			}
			reply(w, http.StatusAccepted, map[string]string{"node": parts[0], "state": "draining; existing dispatched work remains owned"})
			return
		}
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/jobs/"), "/")
		if !strings.HasPrefix(r.URL.Path, "/v1/jobs/") || len(parts) > 3 || !SafeID(parts[0]) {
			apiError(w, ErrNotFound)
			return
		}
		job, exists := s.Jobs[parts[0]]
		if !exists {
			apiError(w, ErrNotFound)
			return
		}
		if len(parts) == 1 && r.Method == http.MethodGet {
			reply(w, http.StatusOK, public(job))
			return
		}
		if len(parts) == 2 && parts[1] == "cancel" && r.Method == http.MethodPost {
			var empty struct{}
			if readJSON(r, &empty, 4096) != nil {
				reply(w, http.StatusBadRequest, map[string]string{"error": "empty object required"})
				return
			}
			updated, err := c.Repo.Cancel(ctx, job.Request.ID)
			if err != nil {
				apiError(w, err)
				return
			}
			reply(w, http.StatusOK, updated)
			return
		}
		if len(parts) == 2 && parts[1] == "recover" && r.Method == http.MethodPost {
			var empty struct{}
			if readJSON(r, &empty, 4096) != nil {
				reply(w, http.StatusBadRequest, map[string]string{"error": "empty object required"})
				return
			}
			if job.State == "succeeded" || job.State == "failed" {
				reply(w, http.StatusOK, public(job))
				return
			}
			if job.State != "uncertain" {
				apiError(w, ErrConflict)
				return
			}
			n, exists := s.Nodes[job.Node]
			if !exists || n.Incarnation != job.Incarnation || s.Leader.ID == "" {
				apiError(w, ErrFenced)
				return
			}
			if err := call(ctx, c.Client, http.MethodPost, n.URL+"/v1/node/jobs/"+job.Request.ID+"/recover", map[string]any{"leader": s.Leader, "epoch": job.Epoch}, nil, 64<<10); err != nil {
				apiError(w, err)
				return
			}
			reply(w, http.StatusAccepted, map[string]string{"id": job.Request.ID, "state": "evidence inspected; waiting for verified replica publication"})
			return
		}
		if (len(parts) == 2 && parts[1] == "result" || len(parts) == 3 && parts[1] == "artifacts") && r.Method == http.MethodGet {
			if job.State != "succeeded" && job.State != "failed" {
				apiError(w, ErrConflict)
				return
			}
			transfer, err := Fetch(ctx, c.Client, s, job)
			if err != nil {
				apiError(w, err)
				return
			}
			if len(parts) == 2 {
				reply(w, http.StatusOK, transfer.Evidence)
				return
			}
			index, err := ArtifactIndex(parts[2])
			if err != nil {
				reply(w, http.StatusBadRequest, map[string]string{"error": "invalid artifact index"})
				return
			}
			if index >= len(transfer.Files) {
				apiError(w, ErrNotFound)
				return
			}
			data := transfer.Files[index]
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("X-Artifact-SHA256", taskevidence.Hash(data))
			w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=artifact-%d", index))
			w.Header().Set("Cache-Control", "no-store")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data)
			return
		}
		apiError(w, ErrNotFound)
	})
}
