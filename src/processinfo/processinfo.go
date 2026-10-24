// Package processinfo reads process accounting without changing limits or mounts.
// A workload-visible /proc may be implemented by gVisor; it is not host accounting.
package processinfo

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type Limit struct {
	Soft string `json:"soft"`
	Hard string `json:"hard"`
}

type Process struct {
	PID     int     `json:"pid"`
	Name    string  `json:"name"`
	State   string  `json:"state"`
	RealUID *uint64 `json:"real_uid"`
	Threads *int    `json:"threads"`
	Nproc   *Limit  `json:"effective_nproc"`
}

type Snapshot struct {
	Scope         string    `json:"scope"`
	Processes     []Process `json:"processes"`
	Threads       *int      `json:"threads"`
	SelfNproc     *Limit    `json:"self_effective_nproc,omitempty"`
	SelfLimitsRaw string    `json:"self_limits_raw,omitempty"`
	Errors        []string  `json:"errors,omitempty"`
}

type WorkloadSample struct {
	Stage      string   `json:"stage"`
	Attempt    int      `json:"attempt"`
	Accounting Snapshot `json:"accounting"`
}

type Launch struct {
	Attempt              int    `json:"attempt"`
	PID                  *int   `json:"pid"`
	StartError           string `json:"start_error"`
	Errno                *int   `json:"errno"`
	Ready                bool   `json:"ready"`
	Stdout               string `json:"stdout"`
	Stderr               string `json:"stderr"`
	StdoutDropped        int64  `json:"stdout_dropped_bytes"`
	StderrDropped        int64  `json:"stderr_dropped_bytes"`
	ExitCode             *int   `json:"exit_code"`
	WaitError            string `json:"wait_error"`
	TerminationRequested bool   `json:"termination_requested"`
}

type LaunchReport struct {
	Spawned          int              `json:"spawned"`
	AttemptBound     int              `json:"attempt_bound"`
	Attempts         int              `json:"attempts"`
	CreationRejected bool             `json:"creation_rejected"`
	Error            string           `json:"error"`
	LimitUnit        string           `json:"limit_unit"`
	Launches         []Launch         `json:"launches"`
	Samples          []WorkloadSample `json:"workload_samples"`
}

func ParseNproc(data string) (*Limit, error) {
	for _, line := range strings.Split(data, "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 4 && fields[0] == "Max" && fields[1] == "processes" {
			for _, value := range fields[2:4] {
				if value == "unlimited" {
					continue
				}
				if _, err := strconv.ParseUint(value, 10, 64); err != nil {
					return nil, fmt.Errorf("invalid NPROC limit: %w", err)
				}
			}
			return &Limit{Soft: fields[2], Hard: fields[3]}, nil
		}
	}
	return nil, fmt.Errorf("Max processes is absent from proc limits")
}

func ReadProcess(procRoot string, pid int) (Process, error) {
	p := Process{PID: pid}
	dir := filepath.Join(procRoot, strconv.Itoa(pid))
	data, err := os.ReadFile(filepath.Join(dir, "status"))
	if err != nil {
		return p, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		switch fields[0] {
		case "Name:":
			p.Name = strings.Join(fields[1:], " ")
		case "State:":
			p.State = fields[1]
		case "Uid:":
			uid, parseErr := strconv.ParseUint(fields[1], 10, 64)
			if parseErr != nil {
				return p, parseErr
			}
			p.RealUID = &uid
		}
	}
	tasks, err := os.ReadDir(filepath.Join(dir, "task"))
	if err != nil {
		return p, err
	}
	count := 0
	for _, task := range tasks {
		if _, err := strconv.Atoi(task.Name()); err == nil {
			count++
		}
	}
	p.Threads = &count
	if limits, err := os.ReadFile(filepath.Join(dir, "limits")); err == nil {
		p.Nproc, _ = ParseNproc(string(limits))
	}
	return p, nil
}

// ReadProcesses counts proc task entries, not goroutines. Completeness is explicit.
func ReadProcesses(procRoot, scope string, pids []int) Snapshot {
	s := Snapshot{Scope: scope, Processes: []Process{}}
	total := 0
	for _, pid := range pids {
		p, err := ReadProcess(procRoot, pid)
		s.Processes = append(s.Processes, p)
		if err != nil {
			s.Errors = append(s.Errors, err.Error())
			continue
		}
		total += *p.Threads
	}
	if len(s.Errors) == 0 {
		s.Threads = &total
	}
	return s
}

func WorkloadSnapshot() Snapshot {
	entries, err := os.ReadDir("/proc")
	pids := []int{}
	if err == nil {
		for _, entry := range entries {
			if pid, err := strconv.Atoi(entry.Name()); err == nil {
				pids = append(pids, pid)
			}
		}
	}
	s := ReadProcesses("/proc", "workload-visible proc task entries; virtualized under runsc, not host OS thread counts", pids)
	if err != nil {
		s.Errors = append(s.Errors, err.Error())
		s.Threads = nil
	}
	data, err := os.ReadFile("/proc/self/limits")
	if err != nil {
		s.Errors = append(s.Errors, err.Error())
		return s
	}
	s.SelfLimitsRaw = string(data)
	s.SelfNproc, err = ParseNproc(string(data))
	if err != nil {
		s.Errors = append(s.Errors, err.Error())
	}
	return s
}
