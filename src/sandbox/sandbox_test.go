package sandbox

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestConfigRejectsUnsafeInputs(t *testing.T) {
	mutations := []func(*Config){
		func(c *Config) { c.Network = "host" }, func(c *Config) { c.Network = "container:other" },
		func(c *Config) { c.Image = "--privileged" }, func(c *Config) { c.Command = nil },
		func(c *Config) { c.Timeout = 0 }, func(c *Config) { c.Timeout = 11 * time.Minute },
		func(c *Config) { c.MemoryBytes = 1 }, func(c *Config) { c.PidsLimit = -1 },
	}
	for i, mutate := range mutations {
		c := DefaultConfig()
		mutate(&c)
		if c.Validate() == nil {
			t.Errorf("case %d accepted", i)
		}
	}
	if err := DefaultConfig().Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestBackendSafetyAndExplicitRuntime(t *testing.T) {
	for _, runtime := range []string{"runc", "runsc"} {
		c := newContainer("test", runtime)
		c.imageID = "sha256:local"
		cfg := DefaultConfig()
		cfg.Command = []string{"/tool", "literal; $(no-shell)"}
		args := c.createArgs(cfg)
		joined := strings.Join(args, " ")
		for _, required := range []string{"--runtime " + runtime, "--network none", "--user 65532:65532", "--cap-drop=ALL", "no-new-privileges=true", "--read-only", "--memory-swap 134217728", "--pids-limit 64", "--pull=never", "--log-driver=none", "nproc=64:64"} {
			if !strings.Contains(joined, required) {
				t.Errorf("missing %s", required)
			}
		}
		for _, arg := range args {
			if arg == "--privileged" || arg == "--volume" || arg == "--mount" || arg == "--pid=host" || strings.Contains(arg, "docker.sock") {
				t.Errorf("unsafe argument %s", arg)
			}
		}
		if args[len(args)-1] != "literal; $(no-shell)" {
			t.Fatal("command argument altered")
		}
	}
}

func TestBoundedCaptureDrainsOutput(t *testing.T) {
	b := limitedBuffer{limit: 4}
	n, err := b.Write([]byte("123456789"))
	if err != nil || n != 9 || b.String() != "1234" || b.dropped != 5 {
		t.Fatalf("unexpected capture: %+v %d %v", b, n, err)
	}
	n, _ = b.Write([]byte("abc"))
	if n != 3 || b.dropped != 8 {
		t.Fatal("failed to drain excess output")
	}
}

func TestResourceSamplesAndUnavailable(t *testing.T) {
	r := parseStats([]byte("{\"MemUsage\":\"2MiB / 64MiB\",\"PIDs\":\"4\",\"CPUPerc\":\"3.25%\"}\n{\"MemUsage\":\"3MiB / 64MiB\",\"PIDs\":\"5\",\"CPUPerc\":\"2%\"}\n"))
	if r.Samples != 2 || *r.MaxObservedMemoryBytes != 3<<20 || *r.MaxObservedPIDs != 5 || *r.MaxObservedCPUPercent != 3.25 {
		t.Fatalf("%+v", r)
	}
	r = parseStats(nil)
	if r.Samples != 0 || r.MaxObservedMemoryBytes != nil || r.MaxObservedPIDs != nil {
		t.Fatal("fabricated unavailable metrics")
	}
}

func fakeContainer(t *testing.T, start func(context.Context, io.Writer, io.Writer) error) *container {
	t.Helper()
	c := newContainer("docker", "runc")
	c.run = func(ctx context.Context, out, stderr io.Writer, args ...string) error {
		switch args[0] {
		case "image":
			fmt.Fprint(out, `{"Id":"sha256:test","Config":{"Volumes":{}}}`)
		case "create", "rm", "kill":
			fmt.Fprintln(out, c.name)
		case "stats":
			<-ctx.Done()
			return ctx.Err()
		case "start":
			return start(ctx, out, stderr)
		case "inspect":
			fmt.Fprintf(out, `{"Config":{"Labels":{"%s":"%s"}},"State":{"Status":"exited","ExitCode":7,"OOMKilled":false,"StartedAt":"2026-01-01T00:00:00Z","FinishedAt":"2026-01-01T00:00:00.001Z"}}`, labelKey, c.token)
		default:
			return fmt.Errorf("unexpected command: %v", args)
		}
		return nil
	}
	return c
}

func TestNonzeroExitIsCapturedAndSingleUse(t *testing.T) {
	c := fakeContainer(t, func(_ context.Context, out, stderr io.Writer) error {
		fmt.Fprint(out, "out")
		fmt.Fprint(stderr, "err")
		return errors.New("exit 7")
	})
	if err := c.Create(context.Background(), DefaultConfig()); err != nil {
		t.Fatal(err)
	}
	r, err := c.Exec(context.Background())
	if err != nil || r.ExitCode == nil || *r.ExitCode != 7 || r.Stdout != "out" || r.Stderr != "err" || r.WorkloadMS == nil {
		t.Fatalf("%+v %v", r, err)
	}
	if _, err = c.Exec(context.Background()); err == nil {
		t.Fatal("allowed repeated Exec")
	}
	if err = c.Create(context.Background(), DefaultConfig()); err == nil {
		t.Fatal("allowed repeated Create")
	}
}

func TestDeadlineAndParentCancellationKillWorkload(t *testing.T) {
	for _, parentCancel := range []bool{false, true} {
		t.Run(fmt.Sprint(parentCancel), func(t *testing.T) {
			c := fakeContainer(t, func(ctx context.Context, _ io.Writer, _ io.Writer) error { <-ctx.Done(); return ctx.Err() })
			base := c.run
			var mu sync.Mutex
			killed := false
			c.run = func(ctx context.Context, out, stderr io.Writer, args ...string) error {
				if args[0] == "kill" {
					mu.Lock()
					killed = true
					mu.Unlock()
					if ctx.Err() != nil {
						t.Error("kill inherited cancelled context")
					}
				}
				return base(ctx, out, stderr, args...)
			}
			cfg := DefaultConfig()
			cfg.Timeout = 10 * time.Millisecond
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if err := c.Create(ctx, cfg); err != nil {
				t.Fatal(err)
			}
			if parentCancel {
				cancel()
			}
			r, err := c.Exec(ctx)
			mu.Lock()
			wasKilled := killed
			mu.Unlock()
			if err != nil || !wasKilled || r.TimedOut == parentCancel || r.Cancelled != parentCancel {
				t.Fatalf("%+v %v killed=%v", r, err, wasKilled)
			}
		})
	}
}

type failingSandbox struct {
	destroyCtxErr error
	destroyed     bool
}

func (s *failingSandbox) Create(context.Context, Config) error { return errors.New("partial create") }
func (s *failingSandbox) Exec(context.Context) (Result, error) { panic("must not execute") }
func (s *failingSandbox) Destroy(ctx context.Context) error {
	s.destroyed = true
	s.destroyCtxErr = ctx.Err()
	return errors.New("cleanup failure")
}

func TestCleanupAfterCreateFailureUsesIndependentContext(t *testing.T) {
	s := &failingSandbox{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := Run(ctx, s, DefaultConfig())
	if !s.destroyed || s.destroyCtxErr != nil || !strings.Contains(err.Error(), "partial create") || !strings.Contains(err.Error(), "cleanup failure") {
		t.Fatalf("%+v %v", s, err)
	}
}

func TestImageVolumesRejectedBeforeCreate(t *testing.T) {
	c := newContainer("docker", "runc")
	c.run = func(_ context.Context, out, stderr io.Writer, args ...string) error {
		if args[0] != "image" {
			t.Fatalf("unexpected Docker operation: %v", args)
		}
		fmt.Fprint(out, `{"Id":"sha256:test","Config":{"Volumes":{"/data":{}}}}`)
		return nil
	}
	if err := c.Create(context.Background(), DefaultConfig()); err == nil {
		t.Fatal("accepted image volumes")
	}
}

func TestDestroyRefusesOwnershipMismatch(t *testing.T) {
	c := newContainer("docker", "runc")
	c.attempted = true
	c.run = func(_ context.Context, out, stderr io.Writer, args ...string) error {
		if args[0] != "inspect" {
			t.Fatal("must not remove foreign container")
		}
		fmt.Fprint(out, `{"Config":{"Labels":{"agent-runtime-lab.session":"foreign"}}}`)
		return nil
	}
	if err := c.Destroy(context.Background()); err == nil {
		t.Fatal("ownership check missing")
	}
}

func TestDestroyAcceptsMissingContainerAndPropagatesDaemonFailure(t *testing.T) {
	for _, message := range []string{"No such object", "error: no such object", "No such container", "Cannot connect to the Docker daemon"} {
		t.Run(message, func(t *testing.T) {
			c := newContainer("docker", "runc")
			c.attempted = true
			c.run = func(_ context.Context, out, stderr io.Writer, args ...string) error {
				if args[0] != "inspect" {
					t.Fatal("unexpected mutation")
				}
				fmt.Fprint(stderr, message)
				return errors.New("exit status 1")
			}
			err := c.Destroy(context.Background())
			if (err != nil) != strings.Contains(message, "daemon") {
				t.Fatalf("message=%q err=%v", message, err)
			}
		})
	}
}

func TestEndpointReadsCurrentAndLegacyDockerMetadata(t *testing.T) {
	for _, settings := range []string{`{"Networks":{"bridge":{"IPAddress":"172.17.0.2"}}}`, `{"IPAddress":"172.17.0.2"}`} {
		c := newContainer("docker", "runc")
		c.run = func(_ context.Context, out, stderr io.Writer, args ...string) error {
			fmt.Fprintf(out, `{"Config":{"Labels":{"%s":"%s"}},"State":{"Running":true},"NetworkSettings":%s}`, labelKey, c.token, settings)
			return nil
		}
		endpoint, err := c.Endpoint(context.Background())
		if err != nil || endpoint != "172.17.0.2:8080" {
			t.Fatalf("endpoint=%q err=%v", endpoint, err)
		}
	}
}
