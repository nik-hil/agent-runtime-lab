package experiments

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"agent-runtime-lab/src/tasklab"
)

// This trusted test fixture alone mounts the dedicated Docker volume. Workload
// containers can only use its bounded HTTP protocol on the internal bridge.
type taskReceiver struct {
	token, network, volume, imageID, endpoint string
	containers                                []string
	current                                   string
	Evidence                                  []ReceiverEvidence
}

type ReceiverEvidence struct {
	Container       string `json:"container"`
	ExitCode        int    `json:"exit_code"`
	Logs            string `json:"logs"`
	LogError        string `json:"log_error,omitempty"`
	Runtime         string `json:"runtime"`
	InternalNetwork bool   `json:"internal_network"`
	PublishedPorts  int    `json:"published_ports"`
	Volume          string `json:"volume"`
}

// Bound metadata and diagnostics without blocking the Docker CLI's writes.
type taskOutput struct {
	bytes.Buffer
	dropped bool
}

func (b *taskOutput) Write(p []byte) (int, error) {
	n := len(p)
	keep := min(n, 1<<20-b.Len())
	_, _ = b.Buffer.Write(p[:keep])
	if keep < n {
		b.dropped = true
	}
	return n, nil
}

func taskDocker(ctx context.Context, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	var out, diagnostic taskOutput
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stdout, cmd.Stderr = &out, &diagnostic
	cmd.WaitDelay = time.Second
	if err := cmd.Run(); err != nil {
		return out.Bytes(), fmt.Errorf("docker %s: %w: %s", args[0], err, diagnostic.String())
	}
	if out.dropped || diagnostic.dropped {
		return nil, errors.New("task fixture metadata truncated")
	}
	return out.Bytes(), nil
}

func newTaskReceiver(ctx context.Context, image string) (fixture *taskReceiver, err error) {
	var nonce [16]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	token := hex.EncodeToString(nonce[:])
	f := &taskReceiver{token: token, network: "agent-runtime-lab-task-" + token, volume: "agent-runtime-lab-task-" + token}
	// Return the fixture on error as well, so its caller owns partial cleanup.
	fixture = f
	data, err := taskDocker(ctx, "image", "inspect", "--format", "{{json .}}", image)
	if err != nil {
		return fixture, err
	}
	var meta struct {
		ID     string
		Config struct{ Volumes map[string]json.RawMessage }
	}
	if err = json.Unmarshal(data, &meta); err != nil {
		return fixture, err
	}
	if meta.ID == "" || len(meta.Config.Volumes) != 0 {
		return fixture, errors.New("receiver requires a trusted local image without declared volumes")
	}
	f.imageID = meta.ID
	labels := []string{"--label", "agent-runtime-lab.managed=true", "--label", "agent-runtime-lab.session=" + token}
	args := append([]string{"network", "create", "--internal", "--driver", "bridge"}, labels...)
	if _, err = taskDocker(ctx, append(args, f.network)...); err != nil {
		return fixture, err
	}
	data, err = taskDocker(ctx, "network", "inspect", "--format", "{{json .}}", f.network)
	if err != nil {
		return fixture, err
	}
	var netInfo struct {
		Internal bool
		Driver   string
	}
	if err = json.Unmarshal(data, &netInfo); err != nil {
		return fixture, err
	}
	if !netInfo.Internal || netInfo.Driver != "bridge" {
		return fixture, errors.New("receiver network must be an internal bridge")
	}
	args = append([]string{"volume", "create"}, labels...)
	_, err = taskDocker(ctx, append(args, f.volume)...)
	return fixture, err
}

func (f *taskReceiver) start(ctx context.Context, mode string) error {
	f.current = fmt.Sprintf("%s-receiver-%d", f.network, len(f.containers)+1)
	f.containers = append(f.containers, f.current)
	args := []string{"create", "--pull=never", "--name", f.current,
		"--label", "agent-runtime-lab.managed=true", "--label", "agent-runtime-lab.session=" + f.token,
		"--runtime", "runc", "--network", f.network, "--user", "65532:65532", "--cap-drop=ALL",
		"--security-opt", "no-new-privileges=true", "--read-only", "--tmpfs", "/tmp:rw,noexec,nosuid,nodev,size=4m",
		"--mount", "type=volume,src=" + f.volume + ",dst=/ledger", "--memory", "268435456", "--memory-swap", "268435456",
		"--pids-limit", "128", "--cpus", "1", "--ulimit", "nproc=128:128", "--ulimit", "nofile=256:256", "--ulimit", "core=0:0",
		"--shm-size", "1m", "--ipc", "private", "--cgroupns", "private", "--restart=no",
		"--log-driver=local", "--log-opt", "max-size=64k", "--log-opt", "max-file=1", "--log-opt", "compress=false",
		"--env", "GOMAXPROCS=1", "--entrypoint", "/lab-probe", f.imageID, "task-receiver", mode}
	if _, err := taskDocker(ctx, args...); err != nil {
		return err
	}
	if _, err := taskDocker(ctx, "start", f.current); err != nil {
		return err
	}
	deadline := time.Now().Add(5 * time.Second)
	var last error
	for time.Now().Before(deadline) && ctx.Err() == nil {
		if _, last = taskDocker(ctx, "exec", f.current, "/lab-probe", "task-health", "127.0.0.1:8080"); last == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if last != nil {
		return fmt.Errorf("receiver readiness: %w", last)
	}
	data, err := taskDocker(ctx, "inspect", "--format", "{{json .}}", f.current)
	if err != nil {
		return err
	}
	var info struct {
		HostConfig struct {
			Runtime      string
			PortBindings map[string]json.RawMessage
			Privileged   bool
		}
		Mounts          []struct{ Type, Name, Destination string }
		NetworkSettings struct {
			Networks map[string]struct{ IPAddress string }
		}
	}
	if err := json.Unmarshal(data, &info); err != nil {
		return err
	}
	if info.HostConfig.Runtime != "runc" || info.HostConfig.Privileged || len(info.HostConfig.PortBindings) != 0 || len(info.Mounts) != 1 || info.Mounts[0].Type != "volume" || info.Mounts[0].Name != f.volume || info.Mounts[0].Destination != "/ledger" || len(info.NetworkSettings.Networks) != 1 {
		return errors.New("receiver isolation configuration mismatch")
	}
	ip := info.NetworkSettings.Networks[f.network].IPAddress
	if ip == "" {
		return errors.New("missing internal receiver IP")
	}
	f.endpoint = ip + ":8080"
	return nil
}

func (f *taskReceiver) snapshot(ctx context.Context) (records []tasklab.Record, err error) {
	data, err := taskDocker(ctx, "exec", f.current, "/lab-probe", "task-ledger")
	if err == nil {
		err = json.Unmarshal(data, &records)
	}
	return
}

func (f *taskReceiver) restartAfterCrash(ctx context.Context) error {
	data, err := taskDocker(ctx, "wait", f.current)
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(data)) != "43" {
		return fmt.Errorf("expected receiver crash 43, got %s", data)
	}
	if err := f.remove(ctx, "container", f.current); err != nil {
		return err
	}
	return f.start(ctx, "normal")
}

func missingTaskResource(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "no such container") || strings.Contains(s, "no such network") || strings.Contains(s, "no such volume") || strings.Contains(s, "no such object") || (strings.Contains(s, "network ") && strings.Contains(s, "not found"))
}

func (f *taskReceiver) remove(ctx context.Context, kind, name string) error {
	data, err := taskDocker(ctx, kind, "inspect", "--format", "{{json .}}", name)
	if missingTaskResource(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var info struct {
		Labels map[string]string
		Config struct{ Labels map[string]string }
		State  struct {
			ExitCode int
			Running  bool
		}
	}
	if err := json.Unmarshal(data, &info); err != nil {
		return err
	}
	labels := info.Labels
	if kind == "container" {
		labels = info.Config.Labels
	}
	if labels["agent-runtime-lab.managed"] != "true" || labels["agent-runtime-lab.session"] != f.token {
		return fmt.Errorf("refusing to remove unowned %s", kind)
	}
	if kind == "container" {
		if info.State.Running {
			if _, err := taskDocker(ctx, "stop", "--time", "1", name); err != nil {
				return err
			}
			data, err = taskDocker(ctx, "inspect", "--format", "{{json .State}}", name)
			if err != nil {
				return err
			}
			if err := json.Unmarshal(data, &info.State); err != nil {
				return err
			}
		}
		logs, err := taskDocker(ctx, "logs", name)
		logError := ""
		if err != nil {
			logError = err.Error()
		}
		// Diagnostic collection must never prevent removal after a failed start.
		f.Evidence = append(f.Evidence, ReceiverEvidence{Container: name, ExitCode: info.State.ExitCode, Logs: string(logs), LogError: logError, Runtime: "runc", InternalNetwork: true, PublishedPorts: 0, Volume: f.volume})
		_, err = taskDocker(ctx, "rm", "--volumes", name)
	} else {
		_, err = taskDocker(ctx, kind, "rm", name)
	}
	if err != nil {
		return err
	}
	_, err = taskDocker(ctx, kind, "inspect", "--format", "{{json .}}", name)
	if !missingTaskResource(err) {
		return fmt.Errorf("%s removal not confirmed: %v", kind, err)
	}
	return nil
}

func (f *taskReceiver) close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var err error
	for _, name := range f.containers {
		err = errors.Join(err, f.remove(ctx, "container", name))
	}
	err = errors.Join(err, f.remove(ctx, "network", f.network))
	err = errors.Join(err, f.remove(ctx, "volume", f.volume))
	return err
}
