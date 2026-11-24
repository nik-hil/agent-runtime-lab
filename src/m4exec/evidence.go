package m4exec

import (
	"bytes"
	"context"
	"debug/elf"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"

	"agent-runtime-lab/src/taskevidence"
)

const maxHelperBytes = 16 << 20

func (r *Runner) loadHelper(path string) error {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() > maxHelperBytes {
		return errors.New("task helper must be a regular, nonsymlink file of at most 16 MiB")
	}
	helper, err := os.ReadFile(path)
	if err != nil || len(helper) > maxHelperBytes {
		return errors.New("could not load bounded task helper")
	}
	file, err := elf.NewFile(bytes.NewReader(helper))
	if err != nil {
		return errors.New("task helper must be a static Linux amd64 ELF binary built from cmd/e2b-task-agent")
	}
	defer file.Close()
	if file.Machine != elf.EM_X86_64 || file.Class != elf.ELFCLASS64 || file.Type != elf.ET_EXEC {
		return errors.New("task helper must be a static Linux amd64 executable")
	}
	for _, program := range file.Progs {
		if program.Type == elf.PT_INTERP {
			return errors.New("task helper must be statically linked; build with CGO_ENABLED=0")
		}
	}
	r.helper, r.helperHash = helper, taskevidence.Hash(helper)
	return nil
}

func (r *Runner) EvidenceHelperHash() string { return r.helperHash }

func (r *Runner) fileRequest(ctx context.Context, method, id, token, path string, body io.Reader) (*http.Request, error) {
	query := url.Values{"path": {path}, "username": {"user"}}
	req, err := http.NewRequestWithContext(ctx, method, r.proxyURL+"/files?"+query.Encode(), body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("E2b-Sandbox-Id", id)
	req.Header.Set("E2b-Sandbox-Port", "49983")
	req.Header.Set("X-Access-Token", token)
	return req, nil
}

func (r *Runner) uploadHelper(ctx context.Context, identity taskevidence.Identity) (string, error) {
	connected, err := r.connect(ctx, identity.SandboxID)
	if err != nil {
		return "", err
	}
	path := "/tmp/agentlab-m43-helper-" + identity.Intent.Nonce
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", filepath.Base(path))
	if err != nil {
		return "", err
	}
	if _, err := part.Write(r.helper); err != nil {
		return "", err
	}
	if err := writer.Close(); err != nil {
		return "", err
	}
	req, err := r.fileRequest(ctx, http.MethodPost, identity.SandboxID, connected.EnvdAccessToken, path, &body)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	resp, err := r.client.Do(req)
	if err != nil {
		return "", errors.New("task helper upload unconfirmed")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		return "", fmt.Errorf("task helper upload returned HTTP %d", resp.StatusCode)
	}
	return path, nil
}

// RunEvidence uploads the source-built helper and executes it only through the
// existing sandbox proxy. A lost helper response is not retried, even if a
// completion marker might already exist; recovery is a separate operator action.
func (r *Runner) RunEvidence(ctx context.Context, identity taskevidence.Identity, command string) (taskevidence.Envelope, error) {
	request := taskevidence.Request{Identity: identity, Command: command}
	if err := request.Validate(); err != nil {
		return taskevidence.Envelope{}, err
	}
	if r.helperHash == "" || r.helperHash != identity.Intent.HelperSHA256 {
		return taskevidence.Envelope{}, errors.New("matching task helper is not configured")
	}
	if _, ok := ctx.Deadline(); !ok {
		return taskevidence.Envelope{}, errors.New("evidence execution requires an explicit deadline")
	}
	path, err := r.uploadHelper(ctx, identity)
	if err != nil {
		return taskevidence.Envelope{}, err
	}
	data, err := json.Marshal(request)
	if err != nil {
		return taskevidence.Envelope{}, err
	}
	// All shell-interpolated values are fixed prefixes, validated hexadecimal
	// strings or URL-safe base64. User command bytes occur only inside JSON.
	encoded := base64.RawURLEncoding.EncodeToString(data)
	invocation := "test \"$(sha256sum '" + path + "' | cut -d ' ' -f1)\" = '" + r.helperHash +
		"' && chmod 0700 '" + path + "' && exec '" + path + "' --request-base64 '" + encoded + "'"
	result, err := r.run(ctx, identity.SandboxID, invocation, 64<<10)
	if err != nil || result.ExitCode != 0 {
		return taskevidence.Envelope{}, errors.New("helper completion unconfirmed; inspect evidence explicitly before any new work")
	}
	return r.ReadEvidence(ctx, identity)
}

func (r *Runner) readFile(ctx context.Context, identity taskevidence.Identity, token, path string, limit int64) ([]byte, error) {
	req, err := r.fileRequest(ctx, http.MethodGet, identity.SandboxID, token, path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, errors.New("sandbox evidence read unconfirmed")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("sandbox evidence read returned HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil || int64(len(data)) > limit {
		return nil, errors.New("sandbox evidence read failed or exceeded bounds")
	}
	return data, nil
}

// ReadEvidence never uploads a helper or starts a process. It connects to the
// original VM to obtain an envd token (which can extend TTL), then GETs the
// marker and every declared artifact, verifying their identity, size and hash.
// Missing or invalid evidence never authorizes command replay.
func (r *Runner) ReadEvidence(ctx context.Context, identity taskevidence.Identity) (taskevidence.Envelope, error) {
	if err := identity.Validate(); err != nil {
		return taskevidence.Envelope{}, err
	}
	if _, ok := ctx.Deadline(); !ok {
		return taskevidence.Envelope{}, errors.New("evidence inspection requires an explicit deadline")
	}
	connected, err := r.connect(ctx, identity.SandboxID)
	if err != nil {
		return taskevidence.Envelope{}, err
	}
	data, err := r.readFile(ctx, identity, connected.EnvdAccessToken, taskevidence.ManifestPath(identity.Intent), taskevidence.MaxManifestBytes)
	if err != nil {
		return taskevidence.Envelope{}, err
	}
	var envelope taskevidence.Envelope
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return taskevidence.Envelope{}, errors.New("invalid sandbox completion manifest")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return taskevidence.Envelope{}, errors.New("trailing completion manifest data")
	}
	if err := envelope.Validate(identity); err != nil {
		return taskevidence.Envelope{}, err
	}
	for _, artifact := range envelope.Manifest.Artifacts {
		data, err := r.readFile(ctx, identity, connected.EnvdAccessToken, artifact.Path, taskevidence.MaxArtifactBytes)
		if err != nil || int64(len(data)) != artifact.Size || taskevidence.Hash(data) != artifact.SHA256 {
			return taskevidence.Envelope{}, errors.New("sandbox artifact is missing, changed or exceeds bounds")
		}
	}
	return envelope, nil
}
