// Package e2bapi adapts the E2B Embed HTTP API to lifecycle.Backend.
// The request shapes match e2b-dev/runtime commit 7278c2a380767c9989da73cf4c04b1af1b32da18.
// It requires no Go SDK and does not change the installed Python SDK.
package e2bapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"agent-runtime-lab/src/lifecycle"
)

const metadataKey = "agent_runtime_lab_key"

// Config restricts the lab to the local Embed API. Do not forward API keys to
// arbitrary URLs or allow HTTP redirects to send credentials elsewhere.
type Config struct {
	BaseURL    string
	APIKey     string
	Template   string
	TTLSeconds int
}

type Backend struct {
	baseURL  string
	apiKey   string
	template string
	ttl      int
	client   *http.Client
}

var _ lifecycle.Backend = (*Backend)(nil)

// New limits requests to a loopback HTTP API. Use an SSH tunnel for remote hosts.
func New(cfg Config) (*Backend, error) {
	u, err := url.Parse(strings.TrimRight(cfg.BaseURL, "/"))
	if err != nil || u == nil || u.Scheme != "http" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" {
		return nil, errors.New("E2B API URL must be an HTTP loopback origin such as http://127.0.0.1:3000")
	}
	host := u.Hostname()
	ip := net.ParseIP(host)
	if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return nil, errors.New("E2B API URL must resolve to loopback (use a local SSH tunnel)")
	}
	if u.Port() == "" {
		return nil, errors.New("E2B API URL requires an explicit port")
	}
	if strings.TrimSpace(cfg.APIKey) == "" {
		return nil, errors.New("E2B_API_KEY is required")
	}
	if cfg.Template == "" {
		cfg.Template = "base"
	}
	if cfg.TTLSeconds == 0 {
		cfg.TTLSeconds = 300
	}
	if cfg.TTLSeconds < 30 || cfg.TTLSeconds > 3600 {
		return nil, errors.New("TTL must be in range 30..3600 seconds")
	}
	return &Backend{
		baseURL: strings.TrimRight(u.String(), "/"), apiKey: cfg.APIKey,
		template: cfg.Template, ttl: cfg.TTLSeconds,
		client: &http.Client{
			Transport: &http.Transport{Proxy: nil},
			Timeout:   30 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

func (b *Backend) do(ctx context.Context, method, path string, payload any, want ...int) ([]byte, int, error) {
	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return nil, 0, fmt.Errorf("encode E2B payload: %w", err)
		}
		body = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, b.baseURL+path, body)
	if err != nil {
		return nil, 0, fmt.Errorf("prepare E2B request: %w", err)
	}
	req.Header.Set("X-API-Key", b.apiKey)
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := b.client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("E2B %s %s: %w", method, path, err)
	}
	defer res.Body.Close()
	// Do not print response bodies; some E2B responses contain envd tokens.
	for _, status := range want {
		if res.StatusCode == status {
			data, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
			return data, res.StatusCode, err
		}
	}
	return nil, res.StatusCode, fmt.Errorf("E2B %s %s returned HTTP %d", method, path, res.StatusCode)
}

func sandboxPath(id string) (string, error) {
	if id == "" || len(id) > 128 {
		return "", errors.New("invalid sandbox ID")
	}
	for _, ch := range id {
		if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '-' || ch == '_') {
			return "", errors.New("sandbox ID must be alphanumeric, hyphen or underscore")
		}
	}
	return "/sandboxes/" + id, nil
}

// Create's metadata is a correlation marker, not an E2B idempotency key.
// The lifecycle controller fences an ambiguous create to prevent duplicates.
func (b *Backend) Create(ctx context.Context, logicalKey string) (string, error) {
	data, _, err := b.do(ctx, http.MethodPost, "/sandboxes", map[string]any{
		"templateID": b.template,
		"timeout":    b.ttl,
		"autoPause":  false,
		"metadata":   map[string]string{metadataKey: logicalKey},
	}, http.StatusCreated)
	if err != nil {
		return "", err
	}
	var out struct {
		SandboxID string `json:"sandboxID"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return "", fmt.Errorf("parse E2B create response: %w", err)
	}
	if _, err := sandboxPath(out.SandboxID); err != nil {
		return "", fmt.Errorf("E2B returned unusable sandbox ID: %w", err)
	}
	return out.SandboxID, nil
}

func (b *Backend) Observe(ctx context.Context, id string) (lifecycle.ObservedState, error) {
	path, err := sandboxPath(id)
	if err != nil {
		return lifecycle.Unknown, err
	}
	data, status, err := b.do(ctx, http.MethodGet, path, nil, http.StatusOK, http.StatusNotFound)
	if err != nil {
		return lifecycle.Unknown, err
	}
	if status == http.StatusNotFound {
		return lifecycle.Absent, nil
	}
	var out struct {
		State     string `json:"state"`
		SandboxID string `json:"sandboxID"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return lifecycle.Unknown, fmt.Errorf("parse E2B status: %w", err)
	}
	if out.SandboxID != id {
		return lifecycle.Unknown, errors.New("E2B status ID mismatch")
	}
	switch out.State {
	case "running":
		return lifecycle.Running, nil
	case "paused":
		return lifecycle.Paused, nil
	default:
		return lifecycle.Unknown, fmt.Errorf("unrecognized E2B sandbox state %q", out.State)
	}
}

// Pause saves memory by default to match the SDK's keep_memory=True semantics.
func (b *Backend) Pause(ctx context.Context, id string) error {
	path, err := sandboxPath(id)
	if err != nil {
		return err
	}
	_, _, err = b.do(ctx, http.MethodPost, path+"/pause", map[string]bool{"memory": true}, http.StatusNoContent)
	return err
}

// Resume uses the pinned runtime's /connect endpoint, not the deprecated /resume.
func (b *Backend) Resume(ctx context.Context, id string) error {
	path, err := sandboxPath(id)
	if err != nil {
		return err
	}
	_, _, err = b.do(ctx, http.MethodPost, path+"/connect", map[string]int{"timeout": b.ttl}, http.StatusOK, http.StatusCreated)
	return err
}

func (b *Backend) Delete(ctx context.Context, id string) error {
	path, err := sandboxPath(id)
	if err != nil {
		return err
	}
	_, _, err = b.do(ctx, http.MethodDelete, path, nil, http.StatusNoContent, http.StatusNotFound)
	return err
}
