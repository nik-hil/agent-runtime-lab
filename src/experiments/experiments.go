// Package experiments contains the reproducible sandbox learning experiments.
package experiments

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"agent-runtime-lab/src/processinfo"
	"agent-runtime-lab/src/sandbox"
)

var Names = []string{"filesystem", "network", "memory", "processes", "startup", "cleanup", "task-success", "task-partial", "task-retry"}

type Observation struct {
	Backend      string             `json:"backend"`
	Experiment   string             `json:"experiment"`
	Expected     string             `json:"expected"`
	Status       string             `json:"status"`
	Details      string             `json:"details"`
	Runs         []sandbox.Result   `json:"runs"`
	ProcessCases []ProcessCase      `json:"process_cases,omitempty"`
	Task         *TaskInvestigation `json:"task,omitempty"`
}

type ProcessCase struct {
	Name            string `json:"name"`
	CgroupPidsLimit int    `json:"cgroup_pids_limit"`
	RlimitNproc     int    `json:"rlimit_nproc"`
	MemoryBytes     int64  `json:"memory_bytes"`
	AttemptBound    int    `json:"attempt_bound"`
	RunIndex        int    `json:"run_index"`
}

func Run(ctx context.Context, backend, image, name string) (ob Observation) {
	if name == "task-success" || name == "task-partial" || name == "task-retry" {
		return runTaskExperiment(ctx, backend, image, name)
	}
	ob = Observation{Backend: backend, Experiment: name, Status: "failed"}
	cfg := sandbox.DefaultConfig()
	cfg.Image = image
	run := func(config sandbox.Config) (sandbox.Result, error) {
		s, err := sandbox.New(backend)
		if err != nil {
			return sandbox.Result{}, err
		}
		result, err := sandbox.Run(ctx, s, config)
		ob.Runs = append(ob.Runs, result)
		return result, err
	}
	pass := false
	var err error
	switch name {
	case "filesystem":
		ob.Expected = "Host canary and Docker socket absent; UID nonzero; capabilities empty; root write denied; /tmp writable."
		var canary *os.File
		canary, err = os.CreateTemp("", "agent-runtime-lab-canary-")
		if err != nil {
			break
		}
		defer os.Remove(canary.Name())
		_, err = canary.WriteString("lab-host-canary")
		closeErr := canary.Close()
		err = errors.Join(err, closeErr)
		if err != nil {
			break
		}
		cfg.Command = []string{"/lab-probe", "fs", canary.Name()}
		var result sandbox.Result
		result, err = run(cfg)
		pass = success(result) && strings.Contains(result.Stdout, "\"host_canary_hidden\":true")
		if data, readErr := os.ReadFile(canary.Name()); readErr != nil || string(data) != "lab-host-canary" {
			err = errors.Join(err, errors.New("host canary was changed or removed"))
		}
	case "network":
		ob.Expected = "Bridge client reaches a local lab peer; network=none cannot reach the same peer. No external internet dependency."
		peer, makeErr := sandbox.New(backend)
		if makeErr != nil {
			err = makeErr
			break
		}
		peerCfg := cfg
		peerCfg.Network = sandbox.NetworkBridge
		peerCfg.Timeout = 20 * time.Second
		peerCfg.Command = []string{"/lab-probe", "serve"}
		peerCtx, cancelPeer := context.WithCancel(ctx)
		defer cancelPeer()
		type peerExecution struct {
			result sandbox.Result
			err    error
		}
		done := make(chan peerExecution, 1)
		go func() {
			result, runErr := sandbox.Run(peerCtx, peer, peerCfg)
			done <- peerExecution{result, runErr}
		}()
		endpoint := ""
		endpointProvider := peer.(interface {
			Endpoint(context.Context) (string, error)
		})
		for i := 0; i < 50; i++ {
			endpoint, err = endpointProvider.Endpoint(ctx)
			if err == nil {
				break
			}
			if ctx.Err() != nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if err == nil {
			cfg.Command = []string{"/lab-probe", "network", endpoint, "allow"}
			cfg.Network = sandbox.NetworkBridge
			var allowed, denied sandbox.Result
			allowed, err = run(cfg)
			if err == nil {
				cfg.Network = sandbox.NetworkNone
				cfg.Command[3] = "deny"
				denied, err = run(cfg)
				pass = success(allowed) && success(denied)
				if err == nil {
					_, err = endpointProvider.Endpoint(ctx)
				}
			}
		}
		// Cancel and inspect the peer before removal; Run owns its full cleanup.
		cancelPeer()
		select {
		case execution := <-done:
			ob.Runs = append(ob.Runs, execution.result)
			err = errors.Join(err, execution.err)
		case <-time.After(20 * time.Second):
			err = errors.Join(err, errors.New("peer attach did not stop"), destroy(peer))
		}
	case "memory":
		ob.Expected = "256 MiB allocation succeeds with 512 MiB; 64 MiB configuration is OOM-killed or reports an explicit OOM error."
		cfg.Timeout = 12 * time.Second
		cfg.Command = []string{"/lab-probe", "memory"}
		cfg.MemoryBytes = 512 << 20
		var control, limited sandbox.Result
		control, err = run(cfg)
		if err != nil {
			break
		}
		cfg.MemoryBytes = 64 << 20
		limited, err = run(cfg)
		oom := limited.Resources.OOMKilled != nil && *limited.Resources.OOMKilled
		explicitOOM := strings.Contains(strings.ToLower(limited.Stderr), "out of memory")
		pass = success(control) && limited.ExitCode != nil && *limited.ExitCode != 0 && !limited.TimedOut && (oom || explicitOOM)
	case "processes":
		ob.Expected = "At 512 MiB, control 256/256 starts 24 children; independently investigate 64/256 and 256/64 without assuming refusal or its cause."
		cfg.Command = []string{"/lab-probe", "processes", "24", "observe"}
		cfg.Timeout = 12 * time.Second
		cfg.MemoryBytes = 512 << 20
		cfg.CollectProcessAccounting = true
		pass = true
		for _, test := range []struct {
			name        string
			pids, nproc int
		}{{"control", 256, 256}, {"cgroup-restricted", 64, 256}, {"rlimit-restricted", 256, 64}} {
			cfg.PidsLimit, cfg.NprocLimit = test.pids, test.nproc
			ob.ProcessCases = append(ob.ProcessCases, ProcessCase{Name: test.name, CgroupPidsLimit: test.pids, RlimitNproc: test.nproc, MemoryBytes: cfg.MemoryBytes, AttemptBound: 24, RunIndex: len(ob.Runs)})
			result, runErr := run(cfg)
			err = errors.Join(err, runErr)
			var probe processinfo.LaunchReport
			parseErr := json.Unmarshal([]byte(result.Stdout), &probe)
			if parseErr != nil {
				err = errors.Join(err, fmt.Errorf("%s probe JSON: %w", test.name, parseErr))
				pass = false
				continue
			}
			valid := success(result) && probe.AttemptBound == 24 && probe.Attempts <= 24 && probe.Attempts == len(probe.Launches) && probe.Spawned <= probe.Attempts && len(probe.Samples) > 0 && result.StdoutDropped == 0
			if test.name == "control" {
				valid = valid && probe.Spawned == 24 && !probe.CreationRejected
			}
			pass = pass && valid
		}
	case "startup":
		ob.Expected = "Source-built no-op exits 0; record create, attach-to-exit, workload, cleanup, and total wall times without a speed threshold."
		cfg.Command = []string{"/lab-probe", "noop"}
		var result sandbox.Result
		result, err = run(cfg)
		pass = success(result) && result.WorkloadMS != nil && result.TotalMS > 0
	case "cleanup":
		ob.Expected = "Timed-out parent and child are terminated, owned container disappears, and a fresh sandbox has no old /tmp marker."
		cfg.Command = []string{"/lab-probe", "tree"}
		cfg.Timeout = 2 * time.Second
		var terminated, fresh sandbox.Result
		terminated, err = run(cfg)
		if err != nil {
			break
		}
		var absent bool
		absent, err = Absent(ctx, terminated.Container)
		if err != nil {
			break
		}
		cfg.Command = []string{"/lab-probe", "clean"}
		cfg.Timeout = 5 * time.Second
		fresh, err = run(cfg)
		pass = terminated.TimedOut && terminated.ExitCode != nil &&
			strings.Contains(terminated.Stdout, "\"marker_written\":true") && absent && success(fresh)
	default:
		err = fmt.Errorf("unknown experiment %q", name)
	}
	for _, result := range ob.Runs {
		if result.Container == "" {
			continue
		}
		absent, checkErr := Absent(ctx, result.Container)
		if checkErr != nil || !absent {
			err = errors.Join(err, checkErr, fmt.Errorf("cleanup not confirmed for %s", result.Container))
		}
	}
	if err != nil {
		ob.Status = "error"
		ob.Details = err.Error()
	} else if pass {
		ob.Status = "passed"
	} else {
		ob.Details = "Observed behavior did not meet the stated expectation; inspect captured runs."
	}
	return
}

func success(result sandbox.Result) bool {
	return result.ExitCode != nil && *result.ExitCode == 0 && !result.TimedOut && !result.Cancelled
}
func destroy(s sandbox.Sandbox) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return s.Destroy(ctx)
}

// Absent checks the exact generated container name and propagates daemon errors.
func Absent(ctx context.Context, name string) (bool, error) {
	if !strings.HasPrefix(name, "agent-runtime-lab-") {
		return false, errors.New("refusing to query a non-lab name")
	}
	cmd := exec.CommandContext(ctx, "docker", "ps", "--all", "--filter", "name=^/"+name+"$", "--format", "{{.ID}}")
	data, err := cmd.CombinedOutput()
	if err != nil {
		return false, fmt.Errorf("verify cleanup: %w: %s", err, data)
	}
	return strings.TrimSpace(string(data)) == "", nil
}

func WriteJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0600)
}
