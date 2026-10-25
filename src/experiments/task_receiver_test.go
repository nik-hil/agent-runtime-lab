package experiments

import (
	"errors"
	"testing"
)

func TestMissingFixtureResourceDoesNotHideDaemonFailure(t *testing.T) {
	for _, message := range []string{"No such container: example", "No such volume: example", "network example not found"} {
		if !missingTaskResource(errors.New(message)) {
			t.Fatalf("missing resource not recognized: %s", message)
		}
	}
	if missingTaskResource(errors.New("Cannot connect to the Docker daemon")) {
		t.Fatal("daemon failure treated as successful cleanup")
	}
}
