package sandbox

import (
	"context"
	"fmt"
	"io"
	"testing"
)

func TestInternalNetworkRequiresExplicitOwnedConfiguration(t *testing.T) {
	name := "agent-runtime-lab-task-0123456789abcdef0123456789abcdef"
	cfg := DefaultConfig()
	cfg.LabNetwork = name
	if cfg.Validate() == nil {
		t.Fatal("accepted lab network without explicit policy")
	}
	cfg.Network = NetworkLabInternal
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"bridge", "host", "agent-runtime-lab-task-nothex"} {
		cfg.LabNetwork = bad
		if cfg.Validate() == nil {
			t.Fatalf("accepted %q", bad)
		}
	}
	for _, meta := range []string{
		`{"Name":"` + name + `","Driver":"bridge","Internal":false}`,
		`{"Name":"` + name + `","Driver":"bridge","Internal":true,"Labels":{"agent-runtime-lab.managed":"true","agent-runtime-lab.session":"other"}}`,
	} {
		c := newContainer("docker", "runc")
		c.run = func(_ context.Context, out, stderr io.Writer, args ...string) error {
			fmt.Fprint(out, meta)
			return nil
		}
		if c.validateLabNetwork(context.Background(), name) == nil {
			t.Fatal("accepted unowned or noninternal network")
		}
	}
}
