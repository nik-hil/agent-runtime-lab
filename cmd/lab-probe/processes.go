package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"time"

	"agent-runtime-lab/src/processinfo"
)

// capture drains child pipes while bounding each diagnostic independently.
type capture struct {
	data    bytes.Buffer
	limit   int
	dropped int64
}

func (c *capture) Write(p []byte) (int, error) {
	keep := min(len(p), max(0, c.limit-c.data.Len()))
	_, _ = c.data.Write(p[:keep])
	c.dropped += int64(len(p) - keep)
	return len(p), nil
}

func runProcesses(args []string) error {
	if len(args) != 2 || (args[1] != "allow" && args[1] != "reject" && args[1] != "observe") {
		return errors.New("processes requires count and allow|reject|observe")
	}
	bound, err := strconv.Atoi(args[0])
	if err != nil || bound < 1 || bound > 96 {
		return errors.New("process count must be 1..96")
	}
	report := processinfo.LaunchReport{AttemptBound: bound, LimitUnit: "cgroup counts host tasks; NPROC is runtime-visible real-UID accounting; launches are not live-child counts", Launches: []processinfo.Launch{}}
	snapshot := func(stage string, attempt int) {
		report.Samples = append(report.Samples, processinfo.WorkloadSample{Stage: stage, Attempt: attempt, Accounting: processinfo.WorkloadSnapshot()})
	}
	type child struct {
		cmd             *exec.Cmd
		out, diagnostic *capture
		index           int
	}
	children := []child{}
	// Ensure teardown and Wait happen before emitting diagnostics, even on rejection.
	cleanup := func() {
		for _, c := range children {
			report.Launches[c.index].TerminationRequested = c.cmd.Process.Kill() == nil
		}
		for _, c := range children {
			waitErr := c.cmd.Wait()
			entry := &report.Launches[c.index]
			if waitErr != nil {
				entry.WaitError = waitErr.Error()
			}
			if c.cmd.ProcessState != nil {
				code := c.cmd.ProcessState.ExitCode()
				entry.ExitCode = &code
			}
			entry.Stdout = c.out.data.String()
			entry.Stderr = c.diagnostic.data.String()
			entry.StdoutDropped = c.out.dropped
			entry.StderrDropped = c.diagnostic.dropped
			var ready struct{ Ready bool }
			if json.Unmarshal(c.out.data.Bytes(), &ready) == nil {
				entry.Ready = ready.Ready
			}
		}
		children = nil
	}
	defer cleanup()
	// Leave the workload live long enough for the opt-in host sampler to attach.
	time.Sleep(300 * time.Millisecond)
	snapshot("before", 0)
	for i := 0; i < bound; i++ {
		cmd := exec.Command("/lab-probe", "process-child")
		cmd.Env = os.Environ()
		out, diagnostic := &capture{limit: 2048}, &capture{limit: 16 << 10}
		cmd.Stdout, cmd.Stderr = out, diagnostic
		entry := processinfo.Launch{Attempt: i + 1}
		report.Attempts++
		if err := cmd.Start(); err != nil {
			entry.StartError = err.Error()
			var errno syscall.Errno
			if errors.As(err, &errno) {
				value := int(errno)
				entry.Errno = &value
			}
			report.CreationRejected = errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.ENOMEM)
			report.Error = err.Error()
			report.Launches = append(report.Launches, entry)
			snapshot("start-failed", i+1)
			break
		}
		pid := cmd.Process.Pid
		entry.PID = &pid
		report.Launches = append(report.Launches, entry)
		children = append(children, child{cmd: cmd, out: out, diagnostic: diagnostic, index: len(report.Launches) - 1})
		report.Spawned++
		time.Sleep(20 * time.Millisecond)
		snapshot("after-launch", i+1)
	}
	time.Sleep(500 * time.Millisecond)
	snapshot("before-cleanup", report.Attempts)
	cleanup()
	emit(report)
	if report.Error != "" && !report.CreationRejected {
		return fmt.Errorf("unexpected process launch error: %s", report.Error)
	}
	if args[1] != "observe" && report.CreationRejected != (args[1] == "reject") {
		return errors.New("process creation did not meet the requested expectation")
	}
	return nil
}
