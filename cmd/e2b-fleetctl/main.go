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
	flag.Parse()
	if flag.NArg() != 1 {
		return errors.New("usage: e2b-fleetctl [flags] status|submit|pause|resume|delete|recover")
	}
	action := flag.Arg(0)
	method, endpoint := http.MethodPost, ""
	var payload any
	switch action {
	case "status":
		method, endpoint = http.MethodGet, "/v1/status"
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
	default:
		return fmt.Errorf("unknown action %q", action)
	}
	if method != http.MethodGet && (!*execute || *id == "") {
		return errors.New("mutating commands require --execute and --id")
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
	var formatted bytes.Buffer
	if err := json.Indent(&formatted, data, "", "  "); err != nil {
		return err
	}
	_, err = fmt.Fprintln(os.Stdout, formatted.String())
	return err
}
