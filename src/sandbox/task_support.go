package sandbox

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
)

const labNetworkPrefix = "agent-runtime-lab-task-"

func validLabNetworkName(name string) bool {
	if !strings.HasPrefix(name, labNetworkPrefix) {
		return false
	}
	token := strings.TrimPrefix(name, labNetworkPrefix)
	_, err := hex.DecodeString(token)
	return err == nil && len(token) == 32
}

func (c *container) validateLabNetwork(ctx context.Context, name string) error {
	data, err := c.capture(ctx, "network", "inspect", "--format", "{{json .}}", name)
	if err != nil {
		return err
	}
	var info struct {
		Name, Driver string
		Internal     bool
		Labels       map[string]string
	}
	if err := json.Unmarshal(data, &info); err != nil {
		return err
	}
	if info.Name != name || !info.Internal || info.Driver != "bridge" || info.Labels["agent-runtime-lab.managed"] != "true" || info.Labels[labelKey] != strings.TrimPrefix(name, labNetworkPrefix) {
		return errors.New("network is not an owned internal lab bridge")
	}
	return nil
}

// InspectTaskArtifact runs a second source-built reader inside the owned sandbox.
// It releases the task's bounded observation barrier only after reading the file.
// No filesystem is mounted into the host, and Sandbox's lifecycle stays unchanged.
func (c *container) InspectTaskArtifact(ctx context.Context) ([]byte, error) {
	info, err := c.inspect(ctx)
	if err != nil {
		return nil, err
	}
	if !info.State.Running {
		return nil, errors.New("task is not running")
	}
	return c.capture(ctx, "exec", "--user", "65532:65532", c.name, "/lab-probe", "task-inspect")
}
