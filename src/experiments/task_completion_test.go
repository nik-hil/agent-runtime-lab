package experiments

import (
	"testing"

	"agent-runtime-lab/src/sandbox"
	"agent-runtime-lab/src/tasklab"
)

func TestZeroExitDoesNotProveArtifactCompletion(t *testing.T) {
	zero := 0
	r := sandbox.Result{ExitCode: &zero}
	a := TaskAttempt{Ready: true, ChildReportedCompletion: true}
	for _, artifact := range []*tasklab.ArtifactObservation{nil, {}, {Exists: true, Contents: "incorrect"}, {Exists: true, Contents: tasklab.PartialArtifact}} {
		a.Artifact = artifact
		if artifactCompleted(r, a) {
			t.Fatal("accepted missing or incorrect artifact despite zero exit")
		}
	}
	a.Artifact = &tasklab.ArtifactObservation{Exists: true, Contents: tasklab.Artifact}
	if !artifactCompleted(r, a) {
		t.Fatal("correct artifact rejected")
	}
}

func TestTaskEvidenceRejectsWrongIdentityAndMissingReadiness(t *testing.T) {
	zero := 0
	for _, stdout := range []string{"", `{"task_id":"wrong","attempt":1,"phase":"ready"}`} {
		_, err := taskAttempt(sandbox.Result{ExitCode: &zero, Stdout: stdout}, "task", "key", "unit", 1, 0)
		if err == nil {
			t.Fatal("accepted incomplete process evidence")
		}
	}
}
