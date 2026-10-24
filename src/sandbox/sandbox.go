// Package sandbox provides one-shot, least-privilege container workloads.
package sandbox

import (
	"context"
	"errors"
	"time"
)

type NetworkPolicy string

const (
	NetworkNone   NetworkPolicy = "none"
	NetworkBridge NetworkPolicy = "bridge"
)

type Config struct {
	Image       string
	Command     []string
	Timeout     time.Duration
	MemoryBytes int64
	PidsLimit   int
	// NprocLimit zero preserves the previous behavior: follow PidsLimit.
	NprocLimit               int
	CollectProcessAccounting bool
	Network                  NetworkPolicy
}

func DefaultConfig() Config {
	return Config{Image: "agent-runtime-lab:local", Command: []string{"/lab-probe", "noop"},
		Timeout: 5 * time.Second, MemoryBytes: 128 << 20, PidsLimit: 64, Network: NetworkNone}
}

func (c Config) Validate() error {
	if c.Image == "" || c.Image[0] == '-' {
		return errors.New("a local image reference is required")
	}
	if len(c.Command) == 0 || c.Command[0] == "" {
		return errors.New("command is required")
	}
	if c.Timeout <= 0 || c.Timeout > 10*time.Minute {
		return errors.New("timeout must be positive and at most 10m")
	}
	if c.MemoryBytes < 16<<20 || c.MemoryBytes > 2<<30 {
		return errors.New("memory must be between 16 MiB and 2 GiB")
	}
	if c.PidsLimit < 1 || c.PidsLimit > 512 {
		return errors.New("pids limit must be between 1 and 512")
	}
	if c.NprocLimit < 0 || c.NprocLimit > 512 {
		return errors.New("nproc limit must be zero (follow pids) or between 1 and 512")
	}
	if c.Network != NetworkNone && c.Network != NetworkBridge {
		return errors.New("network must be none or bridge")
	}
	return nil
}

func (c Config) EffectiveNprocLimit() int {
	if c.NprocLimit == 0 {
		return c.PidsLimit
	}
	return c.NprocLimit
}

// Sandbox is a single-use lifecycle: Create, Exec, Destroy. Destroy is idempotent.
// Exec starts the command configured by Create; it is not docker exec in a keeper.
type Sandbox interface {
	Create(context.Context, Config) error
	Exec(context.Context) (Result, error)
	Destroy(context.Context) error
}

type Resources struct {
	OOMKilled              *bool                  `json:"oom_killed"`
	Samples                int                    `json:"samples"`
	MaxObservedMemoryBytes *uint64                `json:"max_observed_memory_bytes"`
	MaxObservedPIDs        *uint64                `json:"max_observed_pids"`
	MaxObservedCPUPercent  *float64               `json:"max_observed_cpu_percent"`
	Note                   string                 `json:"note"`
	HostProcessAccounting  *HostProcessAccounting `json:"host_process_accounting,omitempty"`
}

type Result struct {
	Backend       string    `json:"backend"`
	Container     string    `json:"container"`
	ImageID       string    `json:"image_id"`
	Stdout        string    `json:"stdout"`
	Stderr        string    `json:"stderr"`
	StdoutDropped int64     `json:"stdout_dropped_bytes"`
	StderrDropped int64     `json:"stderr_dropped_bytes"`
	ExitCode      *int      `json:"exit_code"`
	TimedOut      bool      `json:"timed_out"`
	Cancelled     bool      `json:"cancelled"`
	CreateMS      float64   `json:"create_ms"`
	ExecMS        float64   `json:"exec_ms"`
	WorkloadMS    *float64  `json:"workload_ms"`
	CleanupMS     float64   `json:"cleanup_ms"`
	TotalMS       float64   `json:"total_ms"`
	Resources     Resources `json:"resources"`
}

// Run always attempts cleanup, even when Create or Exec fails or ctx is cancelled.
func Run(ctx context.Context, s Sandbox, c Config) (result Result, err error) {
	begin := time.Now()
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		start := time.Now()
		err = errors.Join(err, s.Destroy(cleanup))
		result.CleanupMS = milliseconds(time.Since(start))
		result.TotalMS = milliseconds(time.Since(begin))
	}()
	start := time.Now()
	err = s.Create(ctx, c)
	createMS := milliseconds(time.Since(start))
	if err != nil {
		result.CreateMS = createMS
		return
	}
	result, err = s.Exec(ctx)
	result.CreateMS = createMS
	return
}

func milliseconds(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
