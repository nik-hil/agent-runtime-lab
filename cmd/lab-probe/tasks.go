package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"time"

	"agent-runtime-lab/src/tasklab"
)

func runTaskProbe(args []string) error {
	switch args[0] {
	case "task-inspect", "task-clean":
		data, err := os.ReadFile(tasklab.ArtifactPath)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		observed := tasklab.ArtifactObservation{Exists: err == nil, Contents: string(data)}
		if string(data) == tasklab.PartialArtifact && args[0] == "task-inspect" {
			exitData, readErr := os.ReadFile(tasklab.ChildExitPath)
			if readErr != nil {
				return fmt.Errorf("child has not exited before artifact read: %w", readErr)
			}
			var code int
			if err := json.Unmarshal(exitData, &code); err != nil {
				return err
			}
			observed.ChildExitCode = &code
		}
		emit(observed)
		if args[0] == "task-inspect" {
			if err != nil {
				return errors.New("artifact not available yet")
			}
			return os.WriteFile(tasklab.ReleasePath, []byte("observed"), 0600)
		}
		if err == nil {
			return errors.New("artifact survived into fresh sandbox")
		}
		return nil
	case "task-ledger":
		records, err := (tasklab.Ledger{Directory: "/ledger"}).Snapshot()
		if err == nil {
			emit(records)
		}
		return err
	case "task-health":
		if len(args) != 2 {
			return errors.New("task-health requires endpoint")
		}
		response, err := taskClient().Get("http://" + args[1] + "/health")
		if err != nil {
			return err
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusNoContent {
			return fmt.Errorf("health status %d", response.StatusCode)
		}
		return nil
	case "task-receiver":
		if len(args) != 2 || (args[1] != "normal" && args[1] != "lose-ack") {
			return errors.New("task-receiver requires normal or lose-ack")
		}
		var crash func()
		if args[1] == "lose-ack" {
			crash = func() {
				emit(map[string]any{"phase": "durable_commit_before_ack_crash", "exit_code": 43})
				os.Exit(43)
			}
		}
		handler := tasklab.Receiver(tasklab.Ledger{Directory: "/ledger"}, crash)
		server := http.Server{Addr: ":8080", Handler: handler, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second, IdleTimeout: 3 * time.Second, MaxHeaderBytes: 4096}
		emit(map[string]string{"phase": "receiver_starting"})
		return server.ListenAndServe()
	case "task":
		return runTask(args[1:])
	default:
		return errors.New("unknown task probe")
	}
}

func taskClient() *http.Client {
	return &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true}}
}

func runTask(args []string) error {
	if len(args) != 5 {
		return errors.New("task requires mode, task ID, attempt, key, endpoint")
	}
	mode, id, key, endpoint := args[0], args[1], args[3], args[4]
	attempt, err := strconv.Atoi(args[2])
	if err != nil || attempt < 1 || attempt > 10 {
		return errors.New("invalid attempt")
	}
	op := tasklab.Operation{TaskID: id, Key: key, Payload: tasklab.Artifact}
	if err := op.Validate(); err != nil {
		return err
	}
	event := func(phase string) { emit(tasklab.Event{TaskID: id, Attempt: attempt, Phase: phase}) }
	if mode == "partial" {
		// Keep the sandbox alive after the actual child fails, so a separate reader
		// can observe the previously written file AFTER Wait reports exit 42.
		childArgs := append([]string{"task", "partial-child"}, args[1:]...)
		child := exec.Command("/lab-probe", childArgs...)
		child.Stdout, child.Stderr = os.Stdout, os.Stderr
		if err := child.Start(); err != nil {
			return err
		}
		emit(tasklab.Event{TaskID: id, Attempt: attempt, Phase: "child_started", ChildPID: child.Process.Pid})
		waitErr := child.Wait()
		code := child.ProcessState.ExitCode()
		if waitErr == nil || code != 42 {
			return fmt.Errorf("expected child exit 42, got %d: %v", code, waitErr)
		}
		if err := os.WriteFile(tasklab.ChildExitPath, []byte(strconv.Itoa(code)), 0600); err != nil {
			return err
		}
		emit(tasklab.Event{TaskID: id, Attempt: attempt, Phase: "child_exited", ChildExitCode: &code})
		if err := waitForTaskObserver(); err != nil {
			return err
		}
		os.Exit(code)
	}
	event("ready")
	switch mode {
	case "success", "partial-child":
		end := 10
		if mode == "partial-child" {
			end = 5
		}
		sum := 0
		for i := 1; i <= end; i++ {
			sum += i
		}
		contents := fmt.Sprintf("sum(1..%d)=%d\n", end, sum)
		if err := os.WriteFile(tasklab.ArtifactPath, []byte(contents), 0600); err != nil {
			return err
		}
		event("artifact_written")
		if mode == "partial-child" {
			event("deliberate_crash_after_partial_write")
			os.Exit(42)
		}
		if err := waitForTaskObserver(); err != nil {
			return err
		}
		event("work_completed")
		return nil
	case "before-commit":
		event("deliberate_crash_before_commit")
		os.Exit(44)
	case "commit":
		body, err := json.Marshal(op)
		if err != nil {
			return err
		}
		event("commit_sent")
		response, err := taskClient().Post("http://"+endpoint+"/commit", "application/json", bytes.NewReader(body))
		if err != nil {
			fmt.Fprintln(os.Stderr, "commit acknowledgement unavailable:", err)
			os.Exit(45)
		}
		data, readErr := io.ReadAll(io.LimitReader(response.Body, 4097))
		_ = response.Body.Close()
		if readErr != nil {
			return readErr
		}
		if response.StatusCode != http.StatusOK {
			fmt.Fprintf(os.Stderr, "commit rejected: HTTP %d: %s", response.StatusCode, data)
			os.Exit(46)
		}
		var receipt tasklab.Receipt
		if len(data) > 4096 || json.Unmarshal(data, &receipt) != nil || receipt.Record.Operation != op || receipt.Record.ResourceID == "" {
			return errors.New("invalid commit receipt")
		}
		emit(tasklab.Event{TaskID: id, Attempt: attempt, Phase: "work_completed", Receipt: &receipt})
		return nil
	default:
		return errors.New("unknown task mode")
	}
	return errors.New("unreachable task mode")
}

func waitForTaskObserver() error {
	deadline := time.Now().Add(6 * time.Second)
	for {
		if _, err := os.Stat(tasklab.ReleasePath); err == nil {
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if time.Now().After(deadline) {
			return errors.New("independent artifact observer did not release task")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
