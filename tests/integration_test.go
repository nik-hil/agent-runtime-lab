package tests

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"agent-runtime-lab/src/experiments"
	"agent-runtime-lab/src/sandbox"
)

func integration(t *testing.T) {
	t.Helper()
	if os.Getenv("LAB_INTEGRATION") != "1" {
		t.Skip("set LAB_INTEGRATION=1 to require real Docker and gVisor")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", "info", "--format", "{{json .Runtimes}}").CombinedOutput()
	if err != nil {
		t.Fatalf("Docker unavailable: %v: %s", err, out)
	}
	for _, runtime := range []string{"runc", "runsc"} {
		if !strings.Contains(string(out), "\""+runtime+"\"") {
			t.Fatalf("required runtime %s is not registered", runtime)
		}
	}
}
func image() string {
	if value := os.Getenv("LAB_IMAGE"); value != "" {
		return value
	}
	return "agent-runtime-lab:local"
}

func TestRuntimeContract(t *testing.T) {
	integration(t)
	for _, backend := range []string{"docker", "gvisor"} {
		t.Run(backend, func(t *testing.T) {
			cfg := sandbox.DefaultConfig()
			cfg.Image = image()
			cfg.Command = []string{"/lab-probe", "echo", "7"}
			s, _ := sandbox.New(backend)
			r, err := sandbox.Run(context.Background(), s, cfg)
			if err != nil || r.ExitCode == nil || *r.ExitCode != 7 || !strings.Contains(r.Stdout, "stdout-marker") || !strings.Contains(r.Stderr, "stderr-marker") {
				t.Fatalf("%+v err=%v", r, err)
			}
			absent, err := experiments.Absent(context.Background(), r.Container)
			if err != nil || !absent {
				t.Fatalf("leaked container: %v", err)
			}
		})
	}
}
func TestExperiments(t *testing.T) {
	integration(t)
	for _, backend := range []string{"docker", "gvisor"} {
		t.Run(backend, func(t *testing.T) {
			observations := []experiments.Observation{}
			for _, name := range experiments.Names {
				t.Run(name, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
					defer cancel()
					ob := experiments.Run(ctx, backend, image(), name)
					observations = append(observations, ob)
					if ob.Status != "passed" {
						t.Errorf("%s: %s; runs=%+v", ob.Status, ob.Details, ob.Runs)
					}
					if name == "processes" {
						checkProcessInvestigation(t, ob)
					}
					if strings.HasPrefix(name, "task-") {
						checkTaskCompletion(t, ob)
					}
				})
			}
			if directory := os.Getenv("LAB_RESULTS"); directory != "" {
				if err := os.MkdirAll(directory, 0700); err != nil {
					t.Fatal(err)
				}
				if err := experiments.WriteJSON(filepath.Join(directory, "integration-"+backend+".json"), observations); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
func TestCancellationAndOutputBound(t *testing.T) {
	integration(t)
	for _, backend := range []string{"docker", "gvisor"} {
		t.Run(backend, func(t *testing.T) {
			cfg := sandbox.DefaultConfig()
			cfg.Image = image()
			cfg.Command = []string{"/lab-probe", "tree"}
			cfg.Timeout = 10 * time.Second
			ctx, cancel := context.WithCancel(context.Background())
			timer := time.AfterFunc(2*time.Second, cancel)
			defer timer.Stop()
			s, _ := sandbox.New(backend)
			r, err := sandbox.Run(ctx, s, cfg)
			cancel()
			if err != nil || !r.Cancelled {
				t.Fatalf("cancel: %+v err=%v", r, err)
			}
			absent, err := experiments.Absent(context.Background(), r.Container)
			if err != nil || !absent {
				t.Fatal("cancel leaked", err)
			}
			cfg.Command = []string{"/lab-probe", "flood"}
			s, _ = sandbox.New(backend)
			r, err = sandbox.Run(context.Background(), s, cfg)
			if err != nil || r.ExitCode == nil || *r.ExitCode != 0 || len(r.Stdout) != sandbox.OutputLimit || len(r.Stderr) != sandbox.OutputLimit || r.StdoutDropped != 180224 || r.StderrDropped != 180224 {
				t.Fatalf("output cap: exit=%v lengths=%d/%d dropped=%d/%d err=%v", r.ExitCode, len(r.Stdout), len(r.Stderr), r.StdoutDropped, r.StderrDropped, err)
			}
		})
	}
}
func TestLifecycle(t *testing.T) {
	integration(t)
	for _, backend := range []string{"docker", "gvisor"} {
		t.Run(backend, func(t *testing.T) {
			cfg := sandbox.DefaultConfig()
			cfg.Image = image()
			s, _ := sandbox.New(backend)
			if err := s.Create(context.Background(), cfg); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				if err := s.Destroy(ctx); err != nil {
					t.Error(err)
				}
			})
			r, err := s.Exec(context.Background())
			if err != nil || r.ExitCode == nil || *r.ExitCode != 0 {
				t.Fatal(fmt.Sprint(r), err)
			}
			if _, err := s.Exec(context.Background()); err == nil {
				t.Fatal("repeated Exec accepted")
			}
			if err := s.Destroy(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := s.Destroy(context.Background()); err != nil {
				t.Fatal("Destroy not idempotent", err)
			}
		})
	}
}
