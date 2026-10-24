package sandbox

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"agent-runtime-lab/src/processinfo"
)

type HostProcessAccounting struct {
	Scope          string              `json:"scope"`
	CgroupPath     string              `json:"cgroup_path"`
	IntervalMS     int                 `json:"interval_ms"`
	Samples        []HostProcessSample `json:"samples"`
	DroppedSamples int                 `json:"dropped_samples"`
	Errors         []string            `json:"errors,omitempty"`
}

type HostProcessSample struct {
	ElapsedMS       float64              `json:"elapsed_ms"`
	PidsCurrent     *uint64              `json:"pids_current"`
	PidsMax         string               `json:"pids_max"`
	PidsEvents      map[string]uint64    `json:"pids_events"`
	PidsEventsRaw   string               `json:"pids_events_raw"`
	CgroupOSThreads *int                 `json:"cgroup_os_threads"`
	Proc            processinfo.Snapshot `json:"host_proc"`
	Errors          []string             `json:"errors,omitempty"`
}

func parseCounters(data string) (map[string]uint64, error) {
	values := map[string]uint64{}
	for _, line := range strings.Split(strings.TrimSpace(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			return nil, fmt.Errorf("invalid counter line %q", line)
		}
		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return nil, err
		}
		values[fields[0]] = value
	}
	return values, nil
}

func readHostSample(procRoot, cgroup string) HostProcessSample {
	s := HostProcessSample{}
	read := func(name string) string {
		data, err := os.ReadFile(filepath.Join(cgroup, name))
		if err != nil {
			s.Errors = append(s.Errors, err.Error())
			return ""
		}
		return strings.TrimSpace(string(data))
	}
	if data := read("pids.current"); data != "" {
		value, err := strconv.ParseUint(data, 10, 64)
		if err != nil {
			s.Errors = append(s.Errors, err.Error())
		} else {
			s.PidsCurrent = &value
		}
	}
	s.PidsMax = read("pids.max")
	s.PidsEventsRaw = read("pids.events")
	if s.PidsEventsRaw != "" {
		var err error
		s.PidsEvents, err = parseCounters(s.PidsEventsRaw)
		if err != nil {
			s.Errors = append(s.Errors, err.Error())
		}
	}
	pids := []int{}
	procSet := map[int]bool{}
	threadSet := map[string]bool{}
	threadsComplete := true
	err := filepath.WalkDir(cgroup, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(filepath.Join(path, "cgroup.procs"))
		if err != nil {
			return err
		}
		for _, field := range strings.Fields(string(data)) {
			pid, err := strconv.Atoi(field)
			if err != nil {
				return err
			}
			if !procSet[pid] {
				procSet[pid] = true
				pids = append(pids, pid)
			}
		}
		data, err = os.ReadFile(filepath.Join(path, "cgroup.threads"))
		if err != nil {
			threadsComplete = false
			s.Errors = append(s.Errors, err.Error())
		} else {
			for _, field := range strings.Fields(string(data)) {
				threadSet[field] = true
			}
		}
		return nil
	})
	if err != nil {
		s.Errors = append(s.Errors, err.Error())
		threadsComplete = false
	}
	if threadsComplete {
		count := len(threadSet)
		s.CgroupOSThreads = &count
	}
	s.Proc = processinfo.ReadProcesses(procRoot, "host OS processes and /proc/<pid>/task entries in the owned container cgroup subtree", pids)
	if err != nil {
		s.Proc.Threads = nil
	}
	return s
}

func unifiedCgroup(data, id string) (string, error) {
	for _, line := range strings.Split(data, "\n") {
		parts := strings.SplitN(line, ":", 3)
		if len(parts) != 3 || parts[0] != "0" || parts[1] != "" {
			continue
		}
		path := parts[2]
		if id == "" || !strings.Contains(path, id) || !strings.HasPrefix(path, "/") || filepath.Clean(path) != path {
			return "", fmt.Errorf("cannot tie local cgroup path %q to owned container ID", path)
		}
		return filepath.Join("/sys/fs/cgroup", strings.TrimPrefix(path, "/")), nil
	}
	return "", fmt.Errorf("host cgroup v2 path unavailable; cgroup v1 sampling is unsupported")
}

func (c *container) collectProcessAccounting(ctx context.Context) *HostProcessAccounting {
	a := &HostProcessAccounting{Scope: "host cgroup v2 and native OS threads; not gVisor virtual process accounting", IntervalMS: 50, Samples: []HostProcessSample{}}
	defer func() {
		if len(a.Samples) == 0 && len(a.Errors) == 0 {
			a.Errors = append(a.Errors, "no running owned host cgroup observed before workload termination")
		}
	}()
	// A remote Docker PID must never be interpreted as a local host PID.
	endpoint := os.Getenv("DOCKER_HOST")
	if endpoint == "" || os.Getenv("DOCKER_CONTEXT") != "" {
		data, err := c.capture(ctx, "context", "inspect", "--format", "{{json .Endpoints.docker.Host}}")
		if err != nil {
			a.Errors = append(a.Errors, err.Error())
			return a
		}
		if err := json.Unmarshal(data, &endpoint); err != nil {
			a.Errors = append(a.Errors, err.Error())
			return a
		}
	}
	if !strings.HasPrefix(endpoint, "unix://") {
		a.Errors = append(a.Errors, "host sampling requires a local Unix Docker endpoint")
		return a
	}
	start := time.Now()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return a
		}
		if a.CgroupPath == "" {
			info, err := c.inspect(ctx)
			if err == nil && info.State.Running && info.State.Pid > 0 {
				data, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(info.State.Pid), "cgroup"))
				if err != nil {
					a.Errors = append(a.Errors, err.Error())
					return a
				}
				a.CgroupPath, err = unifiedCgroup(string(data), info.ID)
				if err != nil {
					a.Errors = append(a.Errors, err.Error())
					return a
				}
			}
		}
		if a.CgroupPath != "" {
			sample := readHostSample("/proc", a.CgroupPath)
			sample.ElapsedMS = milliseconds(time.Since(start))
			if len(a.Samples) < 256 {
				a.Samples = append(a.Samples, sample)
			} else {
				a.Samples[255] = sample
				a.DroppedSamples++
			}
		}
		select {
		case <-ctx.Done():
			return a
		case <-ticker.C:
		}
	}
}
