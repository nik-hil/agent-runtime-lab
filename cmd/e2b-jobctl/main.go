// e2b-jobctl manages asynchronous jobs and downloads verified host artifacts
// through fleetd's private Unix socket. It needs no provider credentials.
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
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"agent-runtime-lab/src/fleetdaemon"
	"agent-runtime-lab/src/taskevidence"
	"agent-runtime-lab/src/taskjournal"
)

type paths []string

func (p *paths) String() string     { return strings.Join(*p, ",") }
func (p *paths) Set(s string) error { *p = append(*p, s); return nil }

func main() {
	if err := run(os.Args[1:], os.Stdout, nil); err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
}

func safeID(s string) bool {
	if s == "" || len(s) > 128 || s == "." || s == ".." {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
			return false
		}
	}
	return true
}

func run(args []string, stdout io.Writer, send func(*http.Request) (*http.Response, error)) error {
	f := flag.NewFlagSet("e2b-jobctl", flag.ContinueOnError)
	socket := f.String("socket", ".state/m4-4-fleet.sock", "private fleet daemon socket")
	id := f.String("job-id", "", "stable idempotent job ID")
	vm := f.String("id", "", "existing workload ID (submit only)")
	command := f.String("command", "", "sandbox command, persisted privately until dispatch")
	timeout := f.Int("timeout", 30, "execution budget in seconds (1..60)")
	execute := f.Bool("execute", false, "required for submit, cancel and recover")
	index := f.Int("index", -1, "artifact index in the sorted artifact list (download only)")
	out := f.String("out", "", "new host file for download; existing files are never overwritten")
	waitTimeout := f.Duration("wait-timeout", 2*time.Minute, "wait command deadline (1s..10m)")
	var artifacts paths
	f.Var(&artifacts, "artifact", "declared absolute sandbox file to copy durably; repeat up to 8")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 1 {
		return errors.New("usage: e2b-jobctl [flags] submit|status|list|wait|cancel|recover|artifacts|download")
	}
	action := f.Arg(0)
	if action != "list" && !safeID(*id) {
		return errors.New("a valid --job-id is required")
	}
	method, endpoint := http.MethodGet, "/v1/jobs/"+*id
	var payload any
	switch action {
	case "submit":
		if !*execute || !safeID(*vm) || *command == "" || len(*command) > 4000 || strings.IndexByte(*command, 0) >= 0 || *timeout < 1 || *timeout > 60 {
			return errors.New("submit requires --execute, --job-id, --id, --command <=4000 bytes and --timeout 1..60")
		}
		normalized, err := taskevidence.NormalizeArtifacts(artifacts)
		if err != nil {
			return err
		}
		method, endpoint = http.MethodPost, "/v1/jobs"
		payload = fleetdaemon.JobRequest{ID: *id, WorkloadID: *vm, Command: *command, Timeout: *timeout, Artifacts: normalized}
	case "cancel", "recover":
		if !*execute {
			return errors.New("cancel and recover require --execute")
		}
		method, endpoint, payload = http.MethodPost, endpoint+"/"+action, struct{}{}
	case "list":
		endpoint = "/v1/jobs"
	case "status", "wait":
		if *waitTimeout < time.Second || *waitTimeout > 10*time.Minute {
			return errors.New("wait-timeout must be 1s..10m")
		}
	case "artifacts":
		endpoint += "/artifacts"
	case "download":
		if *index < 0 || *out == "" {
			return errors.New("download requires --index >=0 and --out NEW_FILE")
		}
		endpoint += "/artifacts/" + strconv.Itoa(*index)
	default:
		return fmt.Errorf("unknown action %q", action)
	}
	if action != "submit" && (*vm != "" || *command != "" || len(artifacts) != 0) {
		return errors.New("--id, --command and --artifact apply only to submit")
	}
	if action != "download" && (*out != "" || *index != -1) {
		return errors.New("--index and --out apply only to download")
	}
	transport := &http.Transport{Proxy: nil, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", *socket)
	}}
	defer transport.CloseIdleConnections()
	if send == nil {
		client := &http.Client{Transport: transport, Timeout: 45 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		send = client.Do
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	if action == "wait" {
		cancel()
		ctx, cancel = context.WithTimeout(context.Background(), *waitTimeout)
	}
	defer cancel()
	for {
		var body io.Reader
		if payload != nil {
			data, err := json.Marshal(payload)
			if err != nil {
				return err
			}
			body = bytes.NewReader(data)
		}
		req, err := http.NewRequestWithContext(ctx, method, "http://fleetd"+endpoint, body)
		if err != nil {
			return err
		}
		if payload != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		response, err := send(req)
		if err != nil {
			return fmt.Errorf("job response unconfirmed: %w; inspect status using the same job ID", err)
		}
		data, readErr := io.ReadAll(io.LimitReader(response.Body, (2<<20)+1))
		response.Body.Close()
		if readErr != nil || len(data) > 2<<20 {
			return errors.New("job response unreadable or exceeds 2 MiB")
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return fmt.Errorf("fleet daemon HTTP %d: %s", response.StatusCode, bytes.TrimSpace(data))
		}
		if action == "download" {
			if len(data) > taskevidence.MaxArtifactBytes || !taskevidence.IsHash(response.Header.Get("X-Artifact-SHA256")) || taskevidence.Hash(data) != response.Header.Get("X-Artifact-SHA256") {
				return errors.New("artifact download failed size or SHA-256 verification")
			}
			if err := saveNew(*out, data); err != nil {
				return err
			}
			_, err = fmt.Fprintf(stdout, "Downloaded %d verified bytes to %s\n", len(data), *out)
			return err
		}
		var entry taskjournal.Entry
		if action == "wait" {
			if err := json.Unmarshal(data, &entry); err != nil {
				return err
			}
			if entry.State == taskjournal.Queued || entry.State == taskjournal.Running {
				select {
				case <-ctx.Done():
					return errors.New("wait timed out; job was not cancelled or resubmitted")
				case <-time.After(200 * time.Millisecond):
					continue
				}
			}
		}
		var formatted bytes.Buffer
		if err := json.Indent(&formatted, data, "", "  "); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(stdout, formatted.String()); err != nil {
			return err
		}
		if action == "wait" && entry.State != taskjournal.Succeeded {
			return fmt.Errorf("job finished in state %q; inspect evidence; never auto-retry", entry.State)
		}
		return nil
	}
}

// Link publishes a synced temporary file without overwriting an existing path.
func saveNew(path string, data []byte) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, ".job-download-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Link(f.Name(), path); err != nil {
		return err
	}
	parent, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer parent.Close()
	return parent.Sync()
}
