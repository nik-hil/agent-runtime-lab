// e2b-taskctl submits and inspects durable task records over the existing
// private fleet daemon Unix socket. No E2B API key is needed in the client.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"agent-runtime-lab/src/taskevidence"
)

type artifactFlags []string

func (a *artifactFlags) String() string { return strings.Join(*a, ",") }
func (a *artifactFlags) Set(value string) error {
	*a = append(*a, value)
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
}

func validID(id string) bool {
	if id == "" || len(id) > 128 {
		return false
	}
	for _, c := range id {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' || c == '.') {
			return false
		}
	}
	return true
}

func run() error {
	socket := flag.String("socket", ".state/m3-1.sock", "path to private fleet daemon Unix socket")
	taskID := flag.String("task-id", "", "stable idempotent task ID")
	workload := flag.String("id", "", "fleet workload ID (submit only)")
	command := flag.String("command", "", "bounded sandbox command; avoid putting secrets in command arguments")
	timeout := flag.Int("timeout", 30, "sandbox timeout in seconds (1..60)")
	execute := flag.Bool("execute", false, "required for task submission or explicit evidence recovery")
	evidence := flag.Bool("evidence", false, "opt in to VM completion evidence and explicit recovery (submit only)")
	var artifactPaths artifactFlags
	flag.Var(&artifactPaths, "artifact", "absolute sandbox output file to hash and verify; repeat up to 8 times with --evidence")
	flag.Parse()
	if flag.NArg() != 1 {
		return errors.New("usage: e2b-taskctl [flags] submit|status|list|recover")
	}
	action := flag.Arg(0)
	method, endpoint := http.MethodGet, ""
	var payload any
	switch action {
	case "submit":
		if !*execute || !validID(*taskID) || !validID(*workload) || *command == "" ||
			len(*command) > 4000 || strings.IndexByte(*command, 0) >= 0 || *timeout < 1 || *timeout > 60 {
			return errors.New("submit requires --execute, --task-id, --id, --command <=4000 bytes, --timeout 1..60")
		}
		artifacts, err := taskevidence.NormalizeArtifacts(artifactPaths)
		if err != nil || !*evidence && len(artifacts) != 0 {
			return errors.New("artifacts require --evidence and at most 8 distinct clean absolute file paths")
		}
		method, endpoint = http.MethodPost, "/v1/tasks"
		payload = struct {
			TaskID         string   `json:"task_id"`
			WorkloadID     string   `json:"workload_id"`
			Command        string   `json:"command"`
			TimeoutSeconds int      `json:"timeout_seconds"`
			Evidence       bool     `json:"evidence,omitempty"`
			Artifacts      []string `json:"artifacts,omitempty"`
		}{*taskID, *workload, *command, *timeout, *evidence, artifacts}
	case "recover":
		if !*execute || !validID(*taskID) || *command != "" || *workload != "" || *evidence || len(artifactPaths) != 0 {
			return errors.New("recover requires --execute and --task-id only; it inspects evidence without rerunning the command")
		}
		method, endpoint = http.MethodPost, "/v1/tasks/"+*taskID+"/recover"
		payload = struct{}{}
	case "status":
		if !validID(*taskID) {
			return errors.New("status requires a valid --task-id")
		}
		endpoint = "/v1/tasks/" + *taskID
	case "list":
		endpoint = "/v1/tasks"
	default:
		return fmt.Errorf("unknown action %q", action)
	}
	var body io.Reader
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	transport := &http.Transport{Proxy: nil, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", *socket)
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 90 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	req, err := http.NewRequest(method, "http://fleetd"+endpoint, body)
	if err != nil {
		return err
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("task response unconfirmed: %w; inspect status by task ID; never blindly rerun side effects", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("fleet daemon HTTP %d: %s", resp.StatusCode, bytes.TrimSpace(data))
	}
	var formatted bytes.Buffer
	if err := json.Indent(&formatted, data, "", "  "); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(os.Stdout, formatted.String()); err != nil {
		return err
	}
	if action == "submit" || action == "recover" {
		var state struct {
			State string `json:"state"`
		}
		if err := json.Unmarshal(data, &state); err != nil {
			return err
		}
		if state.State != "succeeded" {
			return fmt.Errorf("task state %q: inspect the persisted task record; never auto-retry", state.State)
		}
	}
	return nil
}
