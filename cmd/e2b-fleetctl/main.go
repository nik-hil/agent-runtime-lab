// e2b-fleetctl sends desired-state changes to the locally running E2B fleet
// daemon. It never opens the fleet journal or calls E2B directly.
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
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
}

func run() error {
	socket := flag.String("socket", ".state/m3-1.sock", "path to local fleet daemon socket")
	id := flag.String("id", "", "stable workload ID")
	tenant := flag.String("tenant", "alpha", "tenant for submit")
	execute := flag.Bool("execute", false, "required for mutating commands")
	command := flag.String("command", "", "shell command to run inside an owned E2B sandbox")
	execTimeout := flag.Int("exec-timeout", 30, "sandbox command timeout in seconds (1..60)")
	drainTimeout := flag.Duration("drain-timeout", 120*time.Second, "time to wait for confirmed drain completion")
	drainPoll := flag.Duration("drain-poll", 500*time.Millisecond, "status polling interval during drain")
	flag.Parse()
	if flag.NArg() != 1 {
		return errors.New("usage: e2b-fleetctl [flags] status|audit|metrics|submit|pause|resume|delete|recover|drain|exec")
	}
	action := flag.Arg(0)
	method, endpoint := http.MethodPost, ""
	var payload any
	switch action {
	case "status":
		method, endpoint = http.MethodGet, "/v1/status"
	case "audit":
		method, endpoint = http.MethodGet, "/v1/audit"
	case "metrics":
		method, endpoint = http.MethodGet, "/v1/metrics"
	case "submit":
		endpoint = "/v1/submit"
		payload = struct{ ID, Tenant string }{*id, *tenant}
	case "pause", "resume", "delete":
		endpoint = "/v1/intent"
		desired := action
		if action == "resume" {
			desired = "running"
		} else if action == "pause" {
			desired = "paused"
		} else {
			desired = "deleted"
		}
		payload = struct{ ID, Desired string }{*id, desired}
	case "recover":
		endpoint = "/v1/recover"
		payload = struct{ ID string }{*id}
	case "drain":
		endpoint = "/v1/drain"
	case "exec":
		if *command == "" || len(*command) > 4096 || *execTimeout < 1 || *execTimeout > 60 {
			return errors.New("exec requires --command <=4096 bytes and --exec-timeout 1..60 seconds")
		}
		endpoint = "/v1/execute"
		payload = struct {
			ID string `json:"id"`
			Command string `json:"command"`
			TimeoutSeconds int `json:"timeout_seconds"`
		}{*id, *command, *execTimeout}
	default:
		return fmt.Errorf("unknown action %q", action)
	}
	if method != http.MethodGet && (!*execute || (*id == "" && action != "drain")) {
		return errors.New("mutating commands require --execute, and --id except for drain")
	}
	if action == "drain" && (*drainTimeout < time.Second || *drainTimeout > 10*time.Minute || *drainPoll < 100*time.Millisecond || *drainPoll > 5*time.Second) {
		return errors.New("drain-timeout must be 1s..10m and drain-poll 100ms..5s")
	}
	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		body = bytes.NewReader(encoded)
	}
	transport := &http.Transport{Proxy: nil, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", *socket)
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 90 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	req, err := http.NewRequest(method, "http://fleetd"+endpoint, body)
	if err != nil {
		return err
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("connect to fleet daemon socket: %w", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("fleet daemon HTTP %d: %s", resp.StatusCode, bytes.TrimSpace(data))
	}
	if action == "metrics" {
		_, err := os.Stdout.Write(data)
		return err
	}
	if action == "drain" {
		fmt.Fprintln(os.Stdout, "Drain requested; awaiting observed sandbox deletion (Ctrl+C on the daemon remains non-destructive)")
		return waitForDrain(client, *drainTimeout, *drainPoll)
	}
	var formatted bytes.Buffer
	if err := json.Indent(&formatted, data, "", "  "); err != nil {
		return err
	}
	_, err = fmt.Fprintln(os.Stdout, formatted.String())
	if err != nil {
		return err
	}
	if action == "exec" {
		var result struct { ExitCode int `json:"exit_code"` }
		if err := json.Unmarshal(data, &result); err != nil { return err }
		if result.ExitCode != 0 { return fmt.Errorf("sandbox command exited with code %d", result.ExitCode) }
	}
	return nil
}
