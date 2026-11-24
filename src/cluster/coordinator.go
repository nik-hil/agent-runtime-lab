package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"time"
)

type Coordinator struct {
	Repo           *Repository
	ID             string
	URL            string
	TTL            int64
	Client         *http.Client
	Logger         *slog.Logger
	mu             sync.Mutex
	leader         Leader
	ready          bool
	replicateAfter uint64
}

func (c *Coordinator) ownership() (Leader, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.leader, c.ready
}
func (c *Coordinator) setOwnership(leader Leader, ready bool) {
	c.mu.Lock()
	c.leader = leader
	c.ready = ready
	c.mu.Unlock()
}

func (c *Coordinator) Run(ctx context.Context) error {
	if c.Repo == nil || !SafeID(c.ID) || !ValidURL(c.URL) || c.TTL < 3 || c.TTL > 120 || c.Client == nil {
		return errors.New("invalid coordinator configuration")
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); c.elect(ctx) }()
	defer func() { cancel(); <-done }()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return nil
		}
		if leader, ready := c.ownership(); ready {
			step, stop := context.WithTimeout(ctx, 5*time.Second)
			_, err := c.Repo.Schedule(step, leader)
			stop()
			if err == nil {
				archive, stop := context.WithTimeout(ctx, 30*time.Second)
				err = c.Replicate(archive, leader)
				stop()
			}
			if err != nil {
				c.Logger.Warn("cluster_reconcile_deferred", "coordinator_id", c.ID)
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (c *Coordinator) elect(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(c.TTL) * time.Second / 3)
	defer ticker.Stop()
	var leader Leader
	var lease int64
	defer func() {
		if lease != 0 {
			end, stop := context.WithTimeout(context.Background(), 3*time.Second)
			defer stop()
			_ = c.Repo.KV.Revoke(end, lease)
		}
	}()
	for {
		if ctx.Err() != nil {
			return
		}
		step, stop := context.WithTimeout(ctx, 5*time.Second)
		ready := false
		if lease == 0 {
			var err error
			leader, lease, ready, err = c.Repo.Campaign(step, c.ID, c.URL, c.TTL)
			if err != nil {
				ready = false
			}
		} else {
			err := c.Repo.KV.KeepAlive(step, lease)
			if err == nil {
				view, readErr := c.Repo.Read(step)
				ready = readErr == nil && view.Leader == leader
				if readErr == nil && !ready {
					lease = 0
				}
			} else {
				lease = 0
			}
		}
		c.setOwnership(leader, ready)
		stop()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func call(ctx context.Context, client *http.Client, method, address string, input, output any, limit int64) error {
	var body io.Reader
	if input != nil {
		body = bytes.NewReader(encoded(input))
	}
	req, err := http.NewRequestWithContext(ctx, method, address, body)
	if err != nil {
		return err
	}
	if input != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return errors.New("cluster peer response unavailable")
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil || int64(len(data)) > limit {
		return errors.New("cluster peer response exceeded bounds")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("cluster peer HTTP %d", resp.StatusCode)
	}
	if output != nil {
		return json.Unmarshal(data, output)
	}
	return nil
}

// Replicate only writes immutable verified evidence, then publishes a quorum
// metadata receipt under the current leader fence. Artifact bytes never enter
// the consensus database. Repair keeps the requested number of live replicas.
func (c *Coordinator) Replicate(ctx context.Context, leader Leader) error {
	s, err := c.Repo.Read(ctx)
	if err != nil {
		return err
	}
	if s.Leader != leader {
		return ErrFenced
	}
	jobs := ordered(s)
	c.mu.Lock()
	after := c.replicateAfter
	c.mu.Unlock()
	start := 0
	for i, j := range jobs {
		if j.Sequence > after {
			start = i
			break
		}
	}
	var deferred error
	for offset := range jobs {
		job := jobs[(start+offset)%len(jobs)]
		if job.State != "collecting" && job.State != "succeeded" && job.State != "failed" {
			continue
		}
		available := 0
		have := map[string]bool{}
		for _, id := range job.Replicas {
			have[id] = true
			n, exists := s.Nodes[id]
			_, live := liveFor(s, n)
			if exists && live {
				available++
			}
		}
		if available >= s.Config.Replicas {
			continue
		}
		c.mu.Lock()
		c.replicateAfter = job.Sequence
		c.mu.Unlock()
		transfer, err := Fetch(ctx, c.Client, s, job)
		if err != nil {
			deferred = err
			continue
		}
		transfer.Leader = leader
		names := make([]string, 0, len(s.Nodes))
		for name := range s.Nodes {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, id := range names {
			n := s.Nodes[id]
			_, live := liveFor(s, n)
			if !live || have[id] {
				continue
			}
			var ack struct {
				Hash        string `json:"manifest_sha256"`
				Incarnation string `json:"node_incarnation"`
			}
			if err := call(ctx, c.Client, http.MethodPost, n.URL+"/v1/node/jobs/"+job.Request.ID+"/bundle", transfer, &ack, 4096); err != nil {
				continue
			}
			if ack.Hash != job.ManifestHash || ack.Incarnation != n.Incarnation {
				continue
			}
			job.Replicas = append(job.Replicas, id)
			have[id] = true
			available++
			if available >= s.Config.Replicas {
				job.State = "succeeded"
				if job.ExitCode != 0 {
					job.State = "failed"
				}
				job.Note = "completion and artifact replicas verified"
			}
			_, err = publish(c.Repo, ctx, s, leader, job)
			return err // refresh the snapshot before publishing another replica
		}
		deferred = errors.New("artifact replication pending healthy storage capacity")
	}
	return deferred
}
