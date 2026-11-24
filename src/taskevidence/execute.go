package taskevidence

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

type capture struct {
	mu        sync.Mutex
	stdout    bytes.Buffer
	stderr    bytes.Buffer
	remaining int
	truncated bool
}

type outputWriter struct {
	buffer *capture
	stderr bool
}

func (w outputWriter) Write(data []byte) (int, error) {
	w.buffer.mu.Lock()
	defer w.buffer.mu.Unlock()
	n := len(data)
	if len(data) > w.buffer.remaining {
		w.buffer.truncated = true
		data = data[:w.buffer.remaining]
	}
	w.buffer.remaining -= len(data)
	if w.stderr {
		w.buffer.stderr.Write(data)
	} else {
		w.buffer.stdout.Write(data)
	}
	return n, nil
}

func privateDirectory(dir string) error {
	if err := os.Mkdir(dir, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return errors.New("evidence root must be a private regular directory")
	}
	return nil
}

func syncDirectory(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// ReadArtifact refuses symlinks, devices and FIFOs, and bounds allocation.
// A VM workload can still mutate its own files; this is not a security attestation.
func ReadArtifact(path string) ([]byte, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	before, err := f.Stat()
	if err != nil || !before.Mode().IsRegular() || before.Size() > MaxArtifactBytes {
		return nil, errors.New("artifact must be a regular file of at most 1 MiB")
	}
	data, err := io.ReadAll(io.LimitReader(f, MaxArtifactBytes+1))
	if err != nil || len(data) > MaxArtifactBytes {
		return nil, errors.New("artifact read failed or exceeded 1 MiB")
	}
	after, err := f.Stat()
	if err != nil || int64(len(data)) != after.Size() || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		return nil, errors.New("artifact changed while being read")
	}
	return data, nil
}

// Execute is called by e2b-task-agent INSIDE the sandbox. fleetd never calls it
// on the host. It reserves a nonce directory before running, then publishes an
// atomic completion marker only after process exit and artifact observation.
func Execute(ctx context.Context, root string, req Request) (Envelope, error) {
	if err := req.Validate(); err != nil {
		return Envelope{}, err
	}
	if err := privateDirectory(root); err != nil {
		return Envelope{}, err
	}
	dir := filepath.Join(root, req.Identity.Intent.Nonce)
	// An existing directory is deliberately never reused, even if empty:
	// absence of a completion marker does not authorize replay.
	if err := os.Mkdir(dir, 0700); err != nil {
		return Envelope{}, fmt.Errorf("task nonce already reserved or unavailable: %w", err)
	}
	if err := syncDirectory(root); err != nil {
		return Envelope{}, err
	}
	commandCtx, cancel := context.WithTimeout(ctx, time.Duration(req.Identity.Timeout)*time.Second)
	defer cancel()
	output := &capture{remaining: MaxOutputBytes}
	cmd := exec.CommandContext(commandCtx, "/bin/bash", "-l", "-c", req.Command)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	cmd.WaitDelay = time.Second
	cmd.Stdout = outputWriter{buffer: output}
	cmd.Stderr = outputWriter{buffer: output, stderr: true}
	err := cmd.Run()
	if commandCtx.Err() != nil {
		return Envelope{}, errors.New("task deadline or helper cancellation; completion remains unknown")
	}
	var exitError *exec.ExitError
	if err != nil && !errors.As(err, &exitError) {
		return Envelope{}, errors.New("process completion unavailable")
	}
	manifest := Manifest{
		Identity: req.Identity, CompletedAt: time.Now().UTC(), ExitCode: cmd.ProcessState.ExitCode(),
		Stdout: strings.ToValidUTF8(output.stdout.String(), "?"), Stderr: strings.ToValidUTF8(output.stderr.String(), "?"),
		Truncated: output.truncated, Artifacts: []Artifact{},
	}
	manifest.StdoutSHA256, manifest.StderrSHA256 = Hash([]byte(manifest.Stdout)), Hash([]byte(manifest.Stderr))
	for _, path := range req.Identity.Intent.Artifacts {
		data, err := ReadArtifact(path)
		if err != nil {
			return Envelope{}, fmt.Errorf("artifact evidence unavailable for %q: %w", path, err)
		}
		manifest.Artifacts = append(manifest.Artifacts, Artifact{Path: path, Size: int64(len(data)), SHA256: Hash(data)})
	}
	envelope, err := Seal(manifest)
	if err != nil {
		return Envelope{}, err
	}
	if err := envelope.Validate(req.Identity); err != nil {
		return Envelope{}, err
	}
	data, err := json.Marshal(envelope)
	if err != nil || len(data) > MaxManifestBytes {
		return Envelope{}, errors.New("completion manifest exceeds bounds")
	}
	f, err := os.CreateTemp(dir, ".completion-*")
	if err != nil {
		return Envelope{}, err
	}
	defer os.Remove(f.Name())
	if _, err := f.Write(data); err != nil {
		f.Close()
		return Envelope{}, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return Envelope{}, err
	}
	if err := f.Close(); err != nil {
		return Envelope{}, err
	}
	if err := os.Rename(f.Name(), filepath.Join(dir, "completion.json")); err != nil {
		return Envelope{}, err
	}
	if err := syncDirectory(dir); err != nil {
		return Envelope{}, err
	}
	return envelope, nil
}
