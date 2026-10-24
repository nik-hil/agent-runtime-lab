package tests

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"agent-runtime-lab/src/experiments"
	"agent-runtime-lab/src/processinfo"
)

func checkProcessInvestigation(t *testing.T, observation experiments.Observation) {
	t.Helper()
	want := [][2]int{{256, 256}, {64, 256}, {256, 64}}
	if len(observation.ProcessCases) != 3 || len(observation.Runs) != 3 {
		t.Fatalf("expected three independent process cases, got %d/%d", len(observation.ProcessCases), len(observation.Runs))
	}
	for i, c := range observation.ProcessCases {
		if c.CgroupPidsLimit != want[i][0] || c.RlimitNproc != want[i][1] || c.MemoryBytes != 512<<20 || c.AttemptBound != 24 || c.RunIndex != i {
			t.Errorf("incorrect controlled configuration: %+v", c)
		}
		result := observation.Runs[c.RunIndex]
		var probe processinfo.LaunchReport
		if err := json.Unmarshal([]byte(result.Stdout), &probe); err != nil {
			t.Errorf("%s probe: %v", c.Name, err)
			continue
		}
		if probe.AttemptBound != 24 || probe.Attempts > 24 || probe.Spawned > probe.Attempts || probe.Attempts != len(probe.Launches) {
			t.Errorf("unbounded or inconsistent launches: %+v", probe)
		}
		if len(probe.Samples) == 0 {
			t.Error("workload accounting absent")
		}
		for _, sample := range probe.Samples {
			if !strings.HasPrefix(sample.Accounting.Scope, "workload-visible") {
				t.Error("guest accounting mislabeled as host")
			}
			if limit := sample.Accounting.SelfNproc; limit != nil {
				if limit.Soft != strconv.Itoa(c.RlimitNproc) || limit.Hard != strconv.Itoa(c.RlimitNproc) {
					t.Errorf("%s effective NPROC=%+v", c.Name, limit)
				}
			} else if len(sample.Accounting.Errors) == 0 {
				t.Error("unavailable NPROC has no explanation")
			}
		}
		for _, launch := range probe.Launches {
			if launch.StartError != "" && launch.Errno == nil {
				t.Error("launch failure lost errno")
			}
			if c.Name == "control" && !launch.Ready {
				t.Errorf("control child %d did not initialize; stderr=%s", launch.Attempt, launch.Stderr)
			}
		}
		host := result.Resources.HostProcessAccounting
		if host == nil {
			t.Error("opt-in host accounting absent")
			continue
		}
		if !strings.HasPrefix(host.Scope, "host cgroup v2") {
			t.Error("host scope missing")
		}
		if len(host.Samples) == 0 && len(host.Errors) == 0 {
			t.Error("unavailable host accounting has no explanation")
		}
		for _, sample := range host.Samples {
			if sample.PidsMax != "" && sample.PidsMax != strconv.Itoa(c.CgroupPidsLimit) {
				t.Errorf("%s host pids.max=%s", c.Name, sample.PidsMax)
			}
		}
	}
}
