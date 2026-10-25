package tests

import (
	"testing"

	"agent-runtime-lab/src/experiments"
	"agent-runtime-lab/src/tasklab"
)

func checkTaskCompletion(t *testing.T, ob experiments.Observation) {
	t.Helper()
	if ob.Task == nil {
		t.Fatal("missing task evidence")
	}
	e := ob.Task
	if !e.CleanupConfirmed || e.DuplicateSideEffects || e.FinalState == "" {
		t.Fatalf("task verification or cleanup failed: %+v", e)
	}
	for _, a := range e.Attempts {
		if !a.ProcessStarted || !a.Ready || a.ExitCode == nil || a.TaskID == "" || a.Key == "" || a.ExternalCommitExists == nil {
			t.Errorf("missing start/readiness/identity: %+v", a)
		}
		if a.RunIndex < 0 || a.RunIndex >= len(ob.Runs) {
			t.Fatal("invalid run index")
		}
	}
	switch ob.Experiment {
	case "task-success":
		if len(e.Attempts) != 1 {
			t.Fatal("expected one successful attempt")
		}
		a := e.Attempts[0]
		if !a.WorkCompleted || *a.ExitCode != 0 || a.Artifact == nil || !a.Artifact.Exists || a.Artifact.Contents != tasklab.Artifact || !e.FreshSandboxArtifactAbsent || *a.ExternalCommitExists {
			t.Fatalf("artifact task: %+v", a)
		}
	case "task-partial":
		if len(e.Attempts) != 1 {
			t.Fatal("expected one partial attempt")
		}
		a := e.Attempts[0]
		if a.WorkCompleted || *a.ExitCode != 42 || !a.PartialArtifactCreated || a.Artifact == nil || a.Artifact.Contents != tasklab.PartialArtifact || a.Artifact.ChildExitCode == nil || *a.Artifact.ChildExitCode != 42 || !e.FreshSandboxArtifactAbsent || *a.ExternalCommitExists {
			t.Fatalf("partial task: %+v", a)
		}
	case "task-retry":
		if len(e.Attempts) != 8 || len(e.Receivers) != 2 || len(e.LedgerChecks) != 7 || !e.RetryOccurred {
			t.Fatalf("incomplete recovery evidence: %+v", e)
		}
		if e.Receivers[0].ExitCode != 43 || e.Receivers[0].Volume != e.Receivers[1].Volume || e.Receivers[0].Container == e.Receivers[1].Container {
			t.Fatal("receiver restart/persistent volume not demonstrated")
		}
		for _, r := range e.Receivers {
			if !r.InternalNetwork || r.PublishedPorts != 0 {
				t.Fatal("receiver network was exposed")
			}
		}
		codes := []int{45, 0, 0, 0, 46, 0, 44, 0}
		commits := []bool{true, true, true, true, false, true, false, true}
		complete := []bool{false, true, true, true, false, true, false, true}
		for i, a := range e.Attempts {
			if *a.ExitCode != codes[i] || *a.ExternalCommitExists != commits[i] || a.WorkCompleted != complete[i] || a.DuplicateSideEffects {
				t.Errorf("attempt %d: %+v", i, a)
			}
		}
		counts := []int{1, 1, 1, 1, 2, 2, 3}
		for i, check := range e.LedgerChecks {
			if len(check.Records) != counts[i] {
				t.Errorf("stage %s: %+v", check.Stage, check.Records)
			}
		}
	}
}
