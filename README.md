# agent-runtime-lab

An educational Go sandbox runner comparing Docker with explicit `runc` and gVisor with explicit `runsc`. Both use the same Docker lifecycle, image, workload and security settings. This milestone contains no server API, orchestration platform, AI agent or additional runtime.

## Setup

Use a Linux Docker host with cgroups, a working Docker CLI, and both runtimes registered. Go 1.23+ and a C compiler are required for race-enabled tests. The Go application uses only the standard library. See [exact setup and cleanup](docs/setup.md); no script installs or reconfigures system services.

```sh
docker info --format '{{json .Runtimes}}'
./scripts/build.sh
./scripts/test.sh unit
./scripts/test.sh integration
./bin/lab experiments --backend all --output results/experiments.json
./scripts/benchmark.sh 20 3 results/benchmark
```

The build compiles a static Go probe into a local `FROM scratch` image with no image downloads. Scripts detect a project-local compiler under `.tools/go/` and keep caches inside `.tools/`. Plain `go test ./...` skips integration tests unless `LAB_INTEGRATION=1`; the integration script requires both runtimes and treats missing infrastructure as failure.

## Run a command

```sh
./bin/lab run --backend docker --timeout 3s --memory-mib 64 --network none -- /lab-probe echo 7
./bin/lab run --backend gvisor --timeout 3s --memory-mib 64 --pids 64 -- /lab-probe noop
# Set the two process controls independently and collect host accounting:
./bin/lab run --backend gvisor --timeout 12s --memory-mib 512 --pids 64 --nproc 256 --process-accounting -- /lab-probe processes 24 observe
```

The command is an argument vector, with no implicit shell. The scratch image contains only `/lab-probe`; select a trusted local image with `--image` for other executables. Tags resolve to immutable local image IDs before creation; no automatic pulls occur. Images declaring volumes are rejected.

JSON includes separate stdout/stderr, dropped-output counts, nullable workload exit code, deadline/cancellation flags, lifecycle timings, Docker OOM state and optional sampled usage. A workload's nonzero exit is reported in JSON; the CLI returns nonzero for infrastructure errors. Experiments and benchmarks return nonzero when expectations or workloads fail.

`--pids` sets the host cgroup task limit. `--nproc` independently sets soft/hard RLIMIT_NPROC; its default of zero follows `--pids` for compatibility. `--process-accounting` enables read-only local host cgroup v2 and OS-thread sampling. Workload-visible `/proc` data is labeled separately because runsc virtualizes it. See the [process-limit investigation](docs/process-limit-investigation.md) for the three controlled cases and measured results.

## Layout and safety

- `src/sandbox/`: common interface, lifecycle and runtime adapters.
- `src/experiments/`: six bounded experiments and assertions.
- `cmd/`: runner, source-built probe and benchmark summary.
- `tests/`: real-runtime integration tests.
- `scripts/`, `images/`, `docs/`, `results/`: tooling, probe image, design and raw evidence.

Defaults: UID/GID 65532, no capabilities, no new privileges, read-only root, 16 MiB temporary filesystem, no network, one CPU, 128 MiB RAM with swap disabled, and 64 host tasks plus a per-user process limit. There are no host bind mounts, published ports or Docker sockets inside workloads. Host control of Docker remains privileged. `--network bridge` explicitly permits ordinary bridge networking; it is not an egress allowlist.

Read [architecture and limitations](docs/architecture.md), [experiment definitions](docs/experiments.md), and the [first milestone report](docs/milestone-1-report.md). Passing probes establish observed behavior, not proof against container escapes. Contributor and Cursor rules live in `AGENTS.md` and `.cursor/rules/`.

The initial commit includes the report's recorded milestone evidence under `results/`. Toolchains, caches, compiled binaries and subsequent generated results are ignored. The project includes its own Go rules so contributor instructions work in a standalone clone.

## Cleanup

Stop lab commands, then run `./scripts/cleanup.sh --image`. It removes lab-labeled containers and the labeled local probe image. Evidence and the local toolchain are retained; see [setup](docs/setup.md) for file cleanup.
