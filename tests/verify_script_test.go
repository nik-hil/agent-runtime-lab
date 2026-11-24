package tests

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Exercise the real entrypoint with fake host CLIs. These tests validate
// privilege routing, not Docker or E2B behavior, and never invoke real sudo.
func TestVerificationDockerAccess(t *testing.T) {
	script, err := os.ReadFile("../scripts/verify.sh")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name       string
		mode       string
		direct     string
		sudo       string
		wantError  bool
		wantSudo   bool
		wantDocker bool
	}{
		{"direct", "all", "1", "0", false, false, true},
		{"passwordless_sudo", "all", "0", "1", false, true, true},
		{"no_access", "all", "0", "0", true, true, false},
		{"unit_without_docker", "unit", "0", "0", false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			bin := filepath.Join(root, "bin")
			scripts := filepath.Join(root, "scripts")
			for _, dir := range []string{bin, scripts} {
				if err := os.Mkdir(dir, 0700); err != nil {
					t.Fatal(err)
				}
			}
			write := func(path, content string) {
				t.Helper()
				if err := os.WriteFile(path, []byte(content), 0700); err != nil {
					t.Fatal(err)
				}
			}
			write(filepath.Join(scripts, "verify.sh"), string(script))
			write(filepath.Join(scripts, "env.sh"), "LAB_ROOT=\"$VERIFY_FIXTURE_ROOT\"\ncd \"$LAB_ROOT\"\n")
			write(filepath.Join(scripts, "build.sh"), "#!/bin/sh\nset -eu\ndocker info\n")
			write(filepath.Join(bin, "gofmt"), "#!/bin/sh\nexit 0\n")
			write(filepath.Join(bin, "go"), `#!/bin/sh
set -eu
echo "go:$*" >> "$VERIFY_FIXTURE_LOG"
if [ "${LAB_INTEGRATION:-0}" = 1 ]; then
  env docker info
fi
`)
			write(filepath.Join(bin, "docker"), `#!/bin/sh
set -eu
if [ "$VERIFY_FIXTURE_DIRECT" != 1 ] && [ "${VERIFY_FIXTURE_PRIVILEGED:-0}" != 1 ]; then
  exit 42
fi
echo "docker:$*" >> "$VERIFY_FIXTURE_LOG"
`)
			write(filepath.Join(bin, "sudo"), `#!/bin/sh
set -eu
echo "sudo:$*" >> "$VERIFY_FIXTURE_LOG"
[ "$1" = -n ] || exit 92
shift
[ "$1" = -- ] || exit 93
shift
[ "$VERIFY_FIXTURE_SUDO" = 1 ] || exit 43
export VERIFY_FIXTURE_PRIVILEGED=1
exec "$@"
`)
			log := filepath.Join(root, "commands.log")
			cmd := exec.Command("bash", filepath.Join(scripts, "verify.sh"), tc.mode)
			cmd.Env = append(os.Environ(),
				"PATH="+bin+":/usr/bin:/bin", "E2B_API_KEY=test-only",
				"VERIFY_FIXTURE_ROOT="+root, "VERIFY_FIXTURE_LOG="+log,
				"VERIFY_FIXTURE_DIRECT="+tc.direct, "VERIFY_FIXTURE_SUDO="+tc.sudo,
				"VERIFY_FIXTURE_PRIVILEGED=0", "LAB_INTEGRATION=0",
			)
			output, err := cmd.CombinedOutput()
			if (err != nil) != tc.wantError {
				t.Fatalf("error=%v wantError=%t output=%s", err, tc.wantError, output)
			}
			data, readErr := os.ReadFile(log)
			if readErr != nil {
				t.Fatal(readErr)
			}
			commands := string(data)
			if strings.Contains(commands, "sudo:") != tc.wantSudo || strings.Contains(commands, "docker:") != tc.wantDocker {
				t.Fatalf("unexpected privilege route: %s", commands)
			}
			if tc.wantError && strings.Contains(commands, "go:") {
				t.Fatal("Docker preflight failure did not stop verification before Go checks")
			}
			if tc.mode == "all" && !tc.wantError && strings.Count(commands, "docker:info") < 3 {
				t.Fatalf("Docker route was not inherited by build and test subprocesses: %s", commands)
			}
		})
	}
}
