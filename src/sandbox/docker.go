package sandbox

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

const labelKey = "agent-runtime-lab.session"
const OutputLimit = 1 << 20

type commandFunc func(context.Context, io.Writer, io.Writer, ...string) error

// Docker and GVisor share Docker's lifecycle and differ only in explicit OCI runtime.
type Docker struct{ *container }
type GVisor struct{ *container }

func NewDocker() *Docker { return &Docker{newContainer("docker", "runc")} }
func NewGVisor() *GVisor { return &GVisor{newContainer("gvisor", "runsc")} }

func New(backend string) (Sandbox, error) {
	switch backend {
	case "docker":
		return NewDocker(), nil
	case "gvisor":
		return NewGVisor(), nil
	default:
		return nil, fmt.Errorf("unknown backend %q", backend)
	}
}

type container struct {
	mu                                     sync.Mutex
	backend, runtime, name, token, imageID string
	config                                 Config
	attempted, created, executed           bool
	run                                    commandFunc
}

func newContainer(backend, runtime string) *container {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	token := hex.EncodeToString(b)
	return &container{backend: backend, runtime: runtime, name: "agent-runtime-lab-" + token,
		token: token, run: dockerCommand}
}

func dockerCommand(ctx context.Context, stdout, stderr io.Writer, args ...string) error {
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stdout, cmd.Stderr = stdout, stderr
	// Bound waiting if a CLI child keeps an inherited pipe open after cancellation.
	cmd.WaitDelay = time.Second
	return cmd.Run()
}

func (c *container) capture(ctx context.Context, args ...string) ([]byte, error) {
	var out, diagnostic limitedBuffer
	out.limit, diagnostic.limit = OutputLimit, OutputLimit
	err := c.run(ctx, &out, &diagnostic, args...)
	if err != nil {
		return out.Bytes(), fmt.Errorf("docker %s: %w: %s", args[0], err, diagnostic.String())
	}
	if out.dropped > 0 {
		return nil, errors.New("Docker metadata exceeds output limit")
	}
	return out.Bytes(), nil
}

func (c *container) createArgs(cfg Config) []string {
	network := string(cfg.Network)
	if cfg.Network == NetworkLabInternal {
		network = cfg.LabNetwork
	}
	return append([]string{"create", "--pull=never", "--name", c.name,
		"--label", "agent-runtime-lab.managed=true", "--label", labelKey + "=" + c.token,
		"--runtime", c.runtime, "--network", network,
		"--user", "65532:65532", "--cap-drop=ALL",
		"--security-opt", "no-new-privileges=true", "--read-only",
		"--tmpfs", "/tmp:rw,noexec,nosuid,nodev,size=16m,mode=1777",
		"--memory", strconv.FormatInt(cfg.MemoryBytes, 10),
		"--memory-swap", strconv.FormatInt(cfg.MemoryBytes, 10),
		"--pids-limit", strconv.Itoa(cfg.PidsLimit), "--cpus", "1",
		"--ulimit", "nofile=256:256", "--ulimit", "core=0:0", "--ulimit", fmt.Sprintf("nproc=%d:%d", cfg.EffectiveNprocLimit(), cfg.EffectiveNprocLimit()),
		"--shm-size", "1m", "--ipc", "private", "--cgroupns", "private",
		"--restart=no", "--log-driver=none", "--workdir", "/tmp",
		"--env", "HOME=/tmp", "--env", "GOMAXPROCS=1",
		"--entrypoint", cfg.Command[0], c.imageID}, cfg.Command[1:]...)
}

func (c *container) Create(ctx context.Context, cfg Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.attempted {
		return errors.New("sandbox is single-use")
	}
	c.config = cfg
	if cfg.Network == NetworkLabInternal {
		if err := c.validateLabNetwork(ctx, cfg.LabNetwork); err != nil {
			return err
		}
	}
	// Resolve the local image to immutable ID; never pull or accept image volumes.
	out, err := c.capture(ctx, "image", "inspect", "--format", "{{json .}}", cfg.Image)
	if err != nil {
		return err
	}
	var image struct {
		ID     string
		Config struct{ Volumes map[string]json.RawMessage }
	}
	if err := json.Unmarshal(out, &image); err != nil {
		return fmt.Errorf("image metadata: %w", err)
	}
	if image.ID == "" {
		return errors.New("missing immutable image ID")
	}
	if len(image.Config.Volumes) != 0 {
		return errors.New("images declaring volumes are unsupported")
	}
	c.imageID, c.attempted = image.ID, true
	_, err = c.capture(ctx, c.createArgs(cfg)...)
	if err != nil {
		return err
	}
	c.created = true
	return nil
}

type inspection struct {
	ID    string
	State struct {
		Pid        int
		Status     string
		Running    bool
		ExitCode   int
		OOMKilled  bool
		Error      string
		StartedAt  time.Time
		FinishedAt time.Time
	}
	Config          struct{ Labels map[string]string }
	NetworkSettings struct {
		IPAddress string
		Networks  map[string]struct{ IPAddress string }
	}
}

func (c *container) inspect(ctx context.Context) (inspection, error) {
	out, err := c.capture(ctx, "inspect", "--format", "{{json .}}", c.name)
	var info inspection
	if err != nil {
		return info, err
	}
	if err := json.Unmarshal(out, &info); err != nil {
		return info, err
	}
	if info.Config.Labels[labelKey] != c.token {
		return info, errors.New("sandbox ownership label mismatch")
	}
	return info, nil
}

// Endpoint returns the bridge IP of this lab-owned container, for local experiments.
func (c *container) Endpoint(ctx context.Context) (string, error) {
	info, err := c.inspect(ctx)
	if err != nil {
		return "", err
	}
	if !info.State.Running {
		return "", errors.New("sandbox is not running yet")
	}
	address := info.NetworkSettings.Networks["bridge"].IPAddress
	if address == "" {
		address = info.NetworkSettings.IPAddress
	}
	if address == "" {
		return "", errors.New("no bridge address")
	}
	return address + ":8080", nil
}

func (c *container) Exec(ctx context.Context) (Result, error) {
	c.mu.Lock()
	if !c.created || c.executed {
		c.mu.Unlock()
		return Result{}, errors.New("Create must succeed; Exec is single-use")
	}
	c.executed = true
	cfg := c.config
	c.mu.Unlock()
	result := Result{Backend: c.backend, Container: c.name, ImageID: c.imageID}
	var stdout, stderr limitedBuffer
	stdout.limit, stderr.limit = OutputLimit, OutputLimit
	runCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	// CLI statistics are optional, sampled concurrently, and never used as exact peaks.
	statsCtx, stopStats := context.WithCancel(runCtx)
	var stats limitedBuffer
	stats.limit = OutputLimit
	statsDone := make(chan struct{})
	go func() {
		defer close(statsDone)
		_ = c.run(statsCtx, &stats, io.Discard, "stats", "--no-trunc", "--format", "{{json .}}", c.name)
	}()
	var accounting *HostProcessAccounting
	accountingDone := make(chan struct{})
	go func() {
		defer close(accountingDone)
		if cfg.CollectProcessAccounting {
			accounting = c.collectProcessAccounting(statsCtx)
		}
	}()
	start := time.Now()
	runErr := c.run(runCtx, &stdout, &stderr, "start", "--attach", c.name)
	result.ExecMS = milliseconds(time.Since(start))
	result.TimedOut = errors.Is(runCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil
	result.Cancelled = ctx.Err() != nil
	stopStats()
	<-statsDone
	<-accountingDone
	result.Stdout, result.Stderr = stdout.String(), stderr.String()
	result.StdoutDropped, result.StderrDropped = stdout.dropped, stderr.dropped
	result.Resources = parseStats(stats.Bytes())
	result.Resources.HostProcessAccounting = accounting
	// Killing a Docker CLI does not kill its workload. Explicitly kill the container.
	if runCtx.Err() != nil {
		killCtx, killCancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, _ = c.capture(killCtx, "kill", "--signal=KILL", c.name)
		killCancel()
	}
	inspectCtx, inspectCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer inspectCancel()
	info, err := c.inspect(inspectCtx)
	if err != nil {
		return result, errors.Join(runErr, err)
	}
	if info.State.Running || info.State.Status != "exited" || info.State.Error != "" {
		return result, fmt.Errorf("workload did not exit normally: status=%s running=%v error=%s: %w",
			info.State.Status, info.State.Running, info.State.Error, errors.Join(runErr, errors.New("runtime failure")))
	}
	result.ExitCode = &info.State.ExitCode
	result.Resources.OOMKilled = &info.State.OOMKilled
	if !info.State.StartedAt.IsZero() && !info.State.FinishedAt.IsZero() {
		duration := milliseconds(info.State.FinishedAt.Sub(info.State.StartedAt))
		if duration >= 0 {
			result.WorkloadMS = &duration
		}
	}
	// Nonzero workload exit is data, not an infrastructure error.
	return result, nil
}

func (c *container) Destroy(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.attempted {
		return nil
	}
	_, err := c.inspect(ctx)
	if err != nil {
		message := strings.ToLower(err.Error())
		if strings.Contains(message, "no such object") || strings.Contains(message, "no such container") {
			return nil
		}
		return err
	}
	_, err = c.capture(ctx, "rm", "--force", "--volumes", c.name)
	if err == nil {
		c.created = false
	}
	return err
}

type limitedBuffer struct {
	data    bytes.Buffer
	limit   int
	dropped int64
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	n := len(p)
	keep := min(n, max(0, b.limit-b.data.Len()))
	_, _ = b.data.Write(p[:keep])
	b.dropped += int64(n - keep)
	return n, nil
}
func (b *limitedBuffer) String() string { return b.data.String() }
func (b *limitedBuffer) Bytes() []byte  { return b.data.Bytes() }

func parseStats(data []byte) Resources {
	r := Resources{Note: "Docker CLI samples; observed maxima are not peaks. CPU is a sampled percentage, not CPU time. Short jobs may have no samples; null means unavailable."}
	for _, line := range bytes.Split(data, []byte("\n")) {
		// Docker stats can prepend terminal-control characters in streaming output.
		if i := bytes.IndexByte(line, '{'); i >= 0 {
			line = line[i:]
		}
		var s struct{ MemUsage, PIDs, CPUPerc string }
		if json.Unmarshal(line, &s) != nil {
			continue
		}
		mem, err := parseBytes(strings.TrimSpace(strings.Split(s.MemUsage, "/")[0]))
		pids, pErr := strconv.ParseUint(s.PIDs, 10, 64)
		cpu, cErr := strconv.ParseFloat(strings.TrimSuffix(s.CPUPerc, "%"), 64)
		if err != nil || pErr != nil || cErr != nil {
			continue
		}
		r.Samples++
		if r.MaxObservedMemoryBytes == nil || mem > *r.MaxObservedMemoryBytes {
			value := mem
			r.MaxObservedMemoryBytes = &value
		}
		if r.MaxObservedPIDs == nil || pids > *r.MaxObservedPIDs {
			value := pids
			r.MaxObservedPIDs = &value
		}
		if r.MaxObservedCPUPercent == nil || cpu > *r.MaxObservedCPUPercent {
			value := cpu
			r.MaxObservedCPUPercent = &value
		}
	}
	return r
}

func parseBytes(s string) (uint64, error) {
	units := []struct {
		suffix     string
		multiplier float64
	}{
		{"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}, {"GB", 1e9}, {"MB", 1e6}, {"kB", 1e3}, {"B", 1},
	}
	for _, u := range units {
		if strings.HasSuffix(s, u.suffix) {
			n, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimSuffix(s, u.suffix)), 64)
			if err != nil || n < 0 {
				return 0, errors.New("invalid byte quantity")
			}
			return uint64(n * u.multiplier), nil
		}
	}
	return 0, errors.New("unrecognized byte quantity")
}
