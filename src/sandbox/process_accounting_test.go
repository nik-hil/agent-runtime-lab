package sandbox

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestIndependentProcessControlsAndLegacyDefault(t *testing.T) {
	for _, limits := range [][3]int{{256, 0, 256}, {256, 256, 256}, {64, 256, 256}, {256, 64, 64}} {
		cfg := DefaultConfig()
		cfg.PidsLimit = limits[0]
		cfg.NprocLimit = limits[1]
		if err := cfg.Validate(); err != nil {
			t.Fatal(err)
		}
		if cfg.EffectiveNprocLimit() != limits[2] {
			t.Fatal("unexpected effective nproc", cfg)
		}
		args := newContainer("docker", "runc").createArgs(cfg)
		pairs := map[string]string{}
		for i := 0; i < len(args)-1; i++ {
			if args[i] == "--pids-limit" {
				pairs["pids"] = args[i+1]
			}
			if args[i] == "--ulimit" && strings.HasPrefix(args[i+1], "nproc=") {
				pairs["nproc"] = args[i+1]
			}
		}
		if pairs["pids"] != strconv.Itoa(limits[0]) || pairs["nproc"] != "nproc="+strconv.Itoa(limits[2])+":"+strconv.Itoa(limits[2]) {
			t.Fatalf("%+v", pairs)
		}
	}
	for _, limit := range []int{-1, 513} {
		cfg := DefaultConfig()
		cfg.NprocLimit = limit
		if cfg.Validate() == nil {
			t.Fatal("accepted invalid nproc", limit)
		}
	}
}

func TestUnifiedCgroupRequiresOwnedContainerIdentity(t *testing.T) {
	path, err := unifiedCgroup("0::/system.slice/docker-owned.scope\n", "owned")
	if err != nil || path != "/sys/fs/cgroup/system.slice/docker-owned.scope" {
		t.Fatal(path, err)
	}
	for _, data := range []string{"0::/system.slice/docker-other.scope", "0::/x/../docker-owned.scope", "2:pids:/docker-owned"} {
		if _, err := unifiedCgroup(data, "owned"); err == nil {
			t.Fatal("accepted untrusted/unsupported path", data)
		}
	}
}

func TestHostCountersAndNativeThreadCounts(t *testing.T) {
	root := t.TempDir()
	cg := filepath.Join(root, "cgroup")
	proc := filepath.Join(root, "proc")
	if err := os.MkdirAll(cg, 0700); err != nil {
		t.Fatal(err)
	}
	for _, task := range []string{"11", "12"} {
		if err := os.MkdirAll(filepath.Join(proc, "11", "task", task), 0700); err != nil {
			t.Fatal(err)
		}
	}
	files := map[string]string{
		filepath.Join(cg, "pids.current"): "2\n", filepath.Join(cg, "pids.max"): "64\n", filepath.Join(cg, "pids.events"): "max 7\n", filepath.Join(cg, "cgroup.procs"): "11\n", filepath.Join(cg, "cgroup.threads"): "11\n12\n",
		filepath.Join(proc, "11", "status"): "Name:\tprobe\nState:\tS\nUid:\t65532 65532 65532 65532\n", filepath.Join(proc, "11", "limits"): "Max processes 256 256 processes\n",
	}
	for path, data := range files {
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	s := readHostSample(proc, cg)
	if s.PidsCurrent == nil || *s.PidsCurrent != 2 || s.PidsEvents["max"] != 7 || s.CgroupOSThreads == nil || *s.CgroupOSThreads != 2 || s.Proc.Threads == nil || *s.Proc.Threads != 2 {
		t.Fatalf("%+v", s)
	}
	if err := os.Remove(filepath.Join(cg, "pids.current")); err != nil {
		t.Fatal(err)
	}
	s = readHostSample(proc, cg)
	if s.PidsCurrent != nil || len(s.Errors) == 0 {
		t.Fatalf("unavailable accounting must remain null: %+v", s)
	}
	if _, err := parseCounters("max bad"); err == nil {
		t.Fatal("accepted malformed event counter")
	}
}

func TestRemoteDaemonNeverUsesLocalHostPID(t *testing.T) {
	t.Setenv("DOCKER_HOST", "tcp://remote:2376")
	t.Setenv("DOCKER_CONTEXT", "")
	c := newContainer("docker", "runc")
	c.run = func(context.Context, io.Writer, io.Writer, ...string) error {
		t.Fatal("must not inspect remote PID locally")
		return nil
	}
	a := c.collectProcessAccounting(context.Background())
	if len(a.Samples) != 0 || len(a.Errors) == 0 || a.CgroupPath != "" {
		t.Fatalf("%+v", a)
	}
}
