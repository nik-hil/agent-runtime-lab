// Package m4exec implements bounded synchronous command execution in an
// existing E2B microVM. Commands never execute on the host. A lost response
// is ambiguous; callers must not retry without an idempotent task protocol.
package m4exec

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
)

const (
	maxCommandBytes = 4096
	maxOutputBytes  = 64 << 10
	maxFrameBytes   = 1 << 20
	maxEvents       = 4096
)

type Config struct {
	APIURL     string
	ProxyURL   string
	APIKey     string
	TTL        int
	HelperPath string
}

type Result struct {
	SandboxID string `json:"sandbox_id"`
	ExitCode  int    `json:"exit_code"`
	Stdout    string `json:"stdout"`
	Stderr    string `json:"stderr"`
	Truncated bool   `json:"truncated"`
}

type Runner struct {
	controlURL string
	proxyURL   string
	apiKey     string
	ttl        int
	client     *http.Client
	helper     []byte
	helperHash string
}

func loopbackURL(raw string) (string, error) {
	u, err := url.Parse(strings.TrimRight(raw, "/"))
	if err != nil || u == nil || u.Scheme != "http" || u.User != nil || u.RawQuery != "" ||
		u.Fragment != "" || u.Path != "" || u.Port() == "" {
		return "", errors.New("E2B endpoint must be a loopback HTTP origin with explicit port")
	}
	host := u.Hostname()
	ip := net.ParseIP(host)
	if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return "", errors.New("E2B endpoint must be loopback")
	}
	return u.String(), nil
}

func New(cfg Config) (*Runner, error) {
	control, err := loopbackURL(cfg.APIURL)
	if err != nil {
		return nil, fmt.Errorf("control URL: %w", err)
	}
	proxy, err := loopbackURL(cfg.ProxyURL)
	if err != nil {
		return nil, fmt.Errorf("proxy URL: %w", err)
	}
	if cfg.APIKey == "" || cfg.TTL < 30 || cfg.TTL > 3600 {
		return nil, errors.New("E2B key and TTL in [30,3600] required")
	}
	runner := &Runner{controlURL: control, proxyURL: proxy, apiKey: cfg.APIKey, ttl: cfg.TTL,
		client: &http.Client{Transport: &http.Transport{Proxy: nil},
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	if cfg.HelperPath != "" {
		if err := runner.loadHelper(cfg.HelperPath); err != nil {
			return nil, err
		}
	}
	return runner, nil
}

func validID(id string) bool {
	if id == "" || len(id) > 128 {
		return false
	}
	for _, c := range id {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return false
		}
	}
	return true
}

type connectResponse struct {
	SandboxID       string `json:"sandboxID"`
	EnvdAccessToken string `json:"envdAccessToken"`
}

func (r *Runner) connect(ctx context.Context, id string) (connectResponse, error) {
	payload, _ := json.Marshal(struct {
		Timeout int `json:"timeout"`
	}{Timeout: r.ttl})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.controlURL+"/sandboxes/"+id+"/connect", bytes.NewReader(payload))
	if err != nil {
		return connectResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", r.apiKey)
	resp, err := r.client.Do(req)
	if err != nil {
		return connectResponse{}, fmt.Errorf("E2B connect response unknown: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return connectResponse{}, fmt.Errorf("E2B connect returned HTTP %d", resp.StatusCode)
	}
	var out connectResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&out); err != nil {
		return connectResponse{}, fmt.Errorf("parse E2B connect: %w", err)
	}
	if out.SandboxID != id || out.EnvdAccessToken == "" {
		return connectResponse{}, errors.New("E2B connect missing matching identity or envd credential")
	}
	return out, nil
}

type startEvent struct {
	PID uint32 `json:"pid"`
}
type dataEvent struct {
	Stdout []byte `json:"stdout"`
	Stderr []byte `json:"stderr"`
}
type endEvent struct {
	ExitCode *int   `json:"exitCode"`
	Exited   bool   `json:"exited"`
	Error    string `json:"error"`
}
type processEventParts struct {
	Start *startEvent `json:"start"`
	Data  *dataEvent  `json:"data"`
	End   *endEvent   `json:"end"`
}
type processEvent struct {
	Event *processEventParts `json:"event"`
	processEventParts
}

func appendBounded(dst *bytes.Buffer, data []byte, remaining *int, truncated *bool) {
	if *remaining <= 0 {
		if len(data) > 0 {
			*truncated = true
		}
		return
	}
	if len(data) > *remaining {
		*truncated = true
		data = data[:*remaining]
	}
	dst.Write(data)
	*remaining -= len(data)
}

func (r *Runner) Run(ctx context.Context, id, command string) (Result, error) {
	return r.run(ctx, id, command, maxCommandBytes)
}

func (r *Runner) run(ctx context.Context, id, command string, limit int) (Result, error) {
	if !validID(id) || command == "" || len(command) > limit || strings.IndexByte(command, 0) >= 0 {
		return Result{}, fmt.Errorf("invalid sandbox ID or command (maximum %d bytes)", limit)
	}
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		return Result{}, errors.New("execution requires an explicit timeout")
	}
	connected, err := r.connect(ctx, id)
	if err != nil {
		return Result{}, err
	}
	payload, err := json.Marshal(map[string]any{
		"process": map[string]any{"cmd": "/bin/bash", "args": []string{"-l", "-c", command}},
		"stdin":   false,
	})
	if err != nil {
		return Result{}, err
	}
	request := make([]byte, len(payload)+5)
	binary.BigEndian.PutUint32(request[1:5], uint32(len(payload)))
	copy(request[5:], payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.proxyURL+"/process.Process/Start", bytes.NewReader(request))
	if err != nil {
		return Result{}, err
	}
	req.Header.Set("Content-Type", "application/connect+json")
	req.Header.Set("E2b-Sandbox-Id", id)
	req.Header.Set("E2b-Sandbox-Port", "49983")
	req.Header.Set("X-Access-Token", connected.EnvdAccessToken)
	resp, err := r.client.Do(req)
	if err != nil {
		return Result{}, fmt.Errorf("command outcome uncertain: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Result{}, fmt.Errorf("envd command returned HTTP %d (do not retry blindly)", resp.StatusCode)
	}
	result := Result{SandboxID: id}
	var out, errs bytes.Buffer
	remaining := maxOutputBytes
	started := false
	for count := 0; count < maxEvents; count++ {
		header := [5]byte{}
		if _, err := io.ReadFull(resp.Body, header[:]); err != nil {
			return Result{}, fmt.Errorf("command outcome uncertain (no terminal event): %w", err)
		}
		size := binary.BigEndian.Uint32(header[1:])
		if size > maxFrameBytes {
			return Result{}, errors.New("envd frame exceeds 1 MiB; command outcome uncertain")
		}
		data := make([]byte, size)
		if _, err := io.ReadFull(resp.Body, data); err != nil {
			return Result{}, fmt.Errorf("command outcome uncertain (truncated event): %w", err)
		}
		if header[0]&2 != 0 {
			return Result{}, errors.New("envd stream ended without a terminal command event")
		}
		if header[0] != 0 {
			return Result{}, errors.New("unsupported envd stream encoding")
		}
		var event processEvent
		if err := json.Unmarshal(data, &event); err != nil {
			return Result{}, fmt.Errorf("invalid envd event: %w", err)
		}
		var start bool
		var stdout, stderr []byte
		var end *endEvent
		if event.Event != nil {
			start = event.Event.Start != nil
			if event.Event.Data != nil {
				stdout, stderr = event.Event.Data.Stdout, event.Event.Data.Stderr
			}
			end = event.Event.End
		} else {
			start = event.Start != nil
			if event.Data != nil {
				stdout, stderr = event.Data.Stdout, event.Data.Stderr
			}
			end = event.End
		}
		if start {
			started = true
		}
		if !started && (len(stdout) > 0 || len(stderr) > 0 || end != nil) {
			return Result{}, errors.New("envd returned output before process start")
		}
		appendBounded(&out, stdout, &remaining, &result.Truncated)
		appendBounded(&errs, stderr, &remaining, &result.Truncated)
		if end != nil {
			// envd may supply an error message *alongside* a real, nonzero
			// process exit status. That is a completed failed command, not a
			// transport failure. The pinned E2B SDK makes the same distinction:
			// it treats nonzero exit codes as CommandExitError with the
			// provider's error message attached.
			//
			// exit_code is a non-optional proto3 scalar: ProtoJSON normally
			// omits it when zero. Accept that default only with an explicit
			// exited=true marker and no terminal error. An empty end event
			// still provides insufficient evidence of process completion.
			exitCode := 0
			if end.ExitCode != nil {
				exitCode = *end.ExitCode
			}
			if !started || (end.ExitCode == nil && !end.Exited) || (exitCode == 0 && end.Error != "") {
				return Result{}, errors.New("command completion ambiguous (missing exit evidence or terminal error); do not retry blindly")
			}
			result.ExitCode = exitCode
			result.Stdout = strings.ToValidUTF8(out.String(), "\ufffd")
			result.Stderr = strings.ToValidUTF8(errs.String(), "\ufffd")
			return result, nil
		}
	}
	return Result{}, errors.New("envd produced too many events; command outcome uncertain")
}
