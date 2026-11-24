# agent-runtime-lab

An educational Go sandbox runner comparing Docker with explicit `runc` and gVisor with explicit `runsc`. Both use the same Docker lifecycle, image, workload and security settings. A small private test receiver demonstrates durable commits and safe retries; no real external business operations, orchestration platforms, AI agents or additional runtimes are included.

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
- `src/experiments/`: the six original experiments plus three task-completion experiments.
- `src/tasklab/`: bounded task protocol, filesystem ledger and test-only receiver.
- `cmd/`: runner, source-built probe and benchmark summary.
- `tests/`: real-runtime integration tests.
- `scripts/`, `images/`, `docs/`, `results/`: tooling, probe image, design and raw evidence.

Defaults: UID/GID 65532, no capabilities, no new privileges, read-only root, 16 MiB temporary filesystem, no network, one CPU, 128 MiB RAM with swap disabled, and 64 host tasks plus a per-user process limit. There are no host bind mounts, published ports or Docker sockets inside workloads. Host control of Docker remains privileged. `--network bridge` explicitly permits ordinary bridge networking; it is not an egress allowlist.

Read [architecture and limitations](docs/architecture.md), [experiment definitions](docs/experiments.md), and the [first milestone report](docs/milestone-1-report.md). Passing probes establish observed behavior, not proof against container escapes. Contributor and Cursor rules live in `AGENTS.md` and `.cursor/rules/`.

## Task completion and recovery (Milestone 1B)

```sh
mkdir -p results/task-completion
./bin/lab experiments --backend all --experiment task-success --output results/task-completion/task-success.json
./bin/lab experiments --backend all --experiment task-partial --output results/task-completion/task-partial.json
./bin/lab experiments --backend all --experiment task-retry --output results/task-completion/task-retry.json
```

These distinguish process readiness/exit, independently verified work, and committed side effects. The retry experiment owns an internal Docker network and a dedicated receiver volume; workloads never mount that volume. It verifies recovery after both sandbox destruction and receiver restart. Read the [investigation and reproduction guide](docs/task-completion-investigation.md) for the timeline, assertions, actual observations and limitations.

Experiment evidence under `results/` is tracked in Git, including the original milestone, process-limit investigation and task-completion runs. See [the results index](results/README.md) for final evidence versus development runs. Toolchains, caches and compiled binaries remain ignored. The project includes its own Go rules so contributor instructions work in a standalone clone.

## Cleanup

Stop lab commands, then run `./scripts/cleanup.sh --image`. It removes lab-labeled containers, dedicated networks/volumes and the labeled local probe image. Evidence and the local toolchain are retained; see [setup](docs/setup.md) for file cleanup.

## Durable E2B control loop (M2.7)

The `src/durable` package records sandbox ownership and desired state before issuing remote operations. On restart, an ambiguous create stays fenced until an existing E2B sandbox is identified by its exact metadata. The state file uses atomic rename, fsync and an exclusive local lock; no new Go dependencies or E2B upgrades are required. See [M2.7 reproduction and limitations](docs/durable-controller-m2-7.md).

```sh
cd ~/e2b
eval "$(sudo docker compose exec -T ready cat /run/e2b/sdk.env)"
cd ~/src/agent-runtime-lab
go run ./cmd/e2b-durable --execute start
go run ./cmd/e2b-durable --execute pause
go run ./cmd/e2b-durable --execute resume
go run ./cmd/e2b-durable --execute delete
```

## M2.8 — CPU/RAM admission control and tenant quotas

The standalone `src/admission` lab implements bounded queueing, fair
round-robin dispatch between tenants, host/tenant CPU and memory reservations,
and cautious handling of ambiguous VM operations. Pausing releases **active**
resources only after E2B has confirmed PAUSED; resuming requires a new
reservation. It does **not** make live E2B calls.

```sh
go test -race -count=1 ./src/admission
go run ./cmd/admission-demo
```

See [M2.8 experiments and known limitations](docs/admission-scheduling-m2-8.md).

## M2.9 — Durable E2B fleet admission and scheduling

`src/fleet` integrates the M2.8 admission scheduler with E2B lifecycle
operations and persists the complete reservation checkpoint together with
sandbox identity/intent before contacting E2B. CPU/RAM reservations are
conservative upper bounds for one E2B template and are checked against the
actual E2B-reported `cpuCount` and `memoryMB` after creation.

The experimental CLI can queue, tick, pause, resume, recover and delete
multiple sandboxes for two tenants across independent invocations. It is
**single-host only** and requires explicit `--execute` for changes.

```sh
CGO_ENABLED=1 go test -race ./src/admission ./src/fleet ./src/e2bapi
go vet ./...
# Read the live E2B runbook before creating any VM:
# docs/fleet-admission-m2-9.md
```

See [M2.9 design, live commands and limits](docs/fleet-admission-m2-9.md).

## M3.1 — Continuous local E2B fleet reconciliation

The M2.9 controller can now run as a manually started daemon. The daemon
holds the exclusive journal lock, periodically reconciles desired state and
accepts commands over a private Unix-domain socket (mode 0600). Clients no
longer compete for the journal lock or need the E2B API key.

- Server: `go run ./cmd/e2b-fleetd --execute`
- Client: `go run ./cmd/e2b-fleetctl status`
- Full two-terminal live experiment: [M3.1 runbook](docs/continuous-reconciler-m3-1.md).

This is a single-host experiment; it does not install/start a system service,
extend E2B TTL automatically, or deploy a multi-host control plane.

## M3.2 — Operational reliability

The local `e2b-fleetd` now emits structured JSON reconciliation progress/error
events, bounds timer-driven retry traffic with exponential backoff, detects
non-converged workloads that have made no progress, and exposes Prometheus
metrics **only over its existing private Unix socket**.

Use `go run ./cmd/e2b-fleetctl metrics` while the daemon is running.
Monitoring counters and stall ages reset when the daemon restarts; the
M2.9 fleet journal still preserves VM IDs, desired states and resource budgets.
This stage does **not** install an observability stack or change E2B itself.

See [M3.2 behavior and live experiment](docs/operational-reliability-m3-2.md).

## M3.3a — Explicit fleet draining

The daemon now supports a separate `fleetctl --execute drain` operation.
It requests deletion for all workloads in the active single-host journal,
refuses new submissions during drain, and polls the read-only status until
all workloads are confirmed deleted with zero active reservations. A timeout
reports unresolved work and **does not** assume sandbox cleanup succeeded.
`Ctrl+C` is still non-destructive and now prints an accurate summary of
remaining journal workloads. The admission fence and deletion intents are persisted together in
the journal; use a fresh journal after draining to admit new work. See [M3.3a runbook](docs/graceful-drain-m3-3.md).

## M3.4 — Continuous E2B inventory reconciliation

The E2B fleet daemon runs a bounded, read-only provider audit periodically.
Critical drift (e.g. confirmed unexpected VM disappearance or duplicate
ownership) is only allowed to **durably fence new admissions** after two
matching complete audits and a strict current-journal snapshot check. It never
blindly recreates, deletes unknown VMs, or releases uncertain reservations.
New `e2b-fleetctl audit` and expanded `metrics` expose the monitor.

Optional `--renew-ttl` (off by default) extends known, healthy RUNNING
sandboxes via E2B's timeout API; `--audit-global` reports lab-tagged sandboxes
missing from this journal without deleting them. For a consolidated
end-to-end run, flags, limitations and cleanup, see
[the M3.4 runbook](docs/control-plane-reliability-m3-4.md).

## M4.1 — Execute commands inside a managed microVM

`e2b-fleetctl --execute --id JOB --command 'echo hello' exec` now runs a bounded
synchronous shell command inside an existing, confirmed RUNNING E2B sandbox
through the daemon's private Unix socket. It uses the local envd client proxy,
never the host shell. The result carries stdout, stderr, exit code and output
truncation. Command outcomes may be **uncertain** after timeouts: never retry
side effects blindly. This is not yet a durable task queue or an agent loop.
See [the M4.1 end-to-end experiment](docs/m4-sandbox-task-execution.md).

## M4.2 — Durable execution intent and completion records

`e2b-taskctl` records a stable task ID, command hash and owning sandbox in
a private fsynced task journal **before** sending a command to E2B. Terminal
process results are stored with exit code, bounded output and a local SHA-256
integrity digest. Repeating a task ID returns its recorded status without
reexecuting it; changed input is rejected. On daemon restart, interrupted
RUNNING tasks become UNCERTAIN and are never automatically retried. Other
tasks on the same VM are refused until uncertainty is investigated. This is
not a durable queued scheduler or remote attestation of side effects.
See [M4.2 design and live verification](docs/m4-durable-task-journal.md).

## M4.3 — VM completion evidence and explicit task recovery

Opt-in `e2b-taskctl --evidence` tasks use a source-built Go helper inside the
owned E2B VM to persist a completion marker, captured output and a bounded
artifact manifest. The host re-reads and verifies the evidence before storing
the result. After a lost response or daemon crash, `e2b-taskctl --execute
--task-id ID recover` can resolve an UNCERTAIN evidence task by inspecting its
original VM, without rerunning the command. Missing or changed evidence stays
uncertain. Configure fleetd with `--task-helper` to enable new evidence tasks.
This is single-host observational evidence, not VM attestation or exactly-once
external effects. See [M4.3 build, recovery experiment and limits](docs/m4-completion-evidence.md).

If fleetd has already stopped, `fleetctl drain` cannot reach it. Run
`e2b-fleet-cleanup --execute --state-file EXISTING_JOURNAL` with E2B credentials
loaded to drain directly under the same exclusive journal lock. It confirms
provider deletion before releasing reservations, preserves fleet/task history
and needs no socket. See [cleanup without a daemon](docs/fleet-offline-cleanup.md).

## Consolidated verification

Run `./scripts/verify.sh all` on the configured lab server to verify formatting,
vet, unit/race tests, Docker/runc + gVisor/runsc integration and the opt-in live
E2B lifecycle/task/crash-recovery gate. The test owns its daemon and disposable
journal and checks standalone cleanup. Use `./scripts/verify.sh e2b` for an
Embed-only host, or `./scripts/verify.sh unit` without live infrastructure.
See [complete commands, coverage and remaining implementation](docs/full-verification.md).

## M4.4 — Asynchronous jobs and durable artifacts

`e2b-jobctl submit` returns a durable queued acceptance while bounded workers
execute evidence-backed commands independently of the client connection. Jobs
share the synchronous task journal's VM fence, survive queued restarts, and
support explicit cancellation and recovery. Interrupted dispatch is never
automatically rerun. Verified completion bundles and artifact bytes are synced
to private host storage before completion, survive VM deletion, and can be
downloaded with integrity checks. No new services or runtimes are installed.

See [M4.4 behavior and complete commands](docs/m4-async-jobs.md), and
[the recorded verification status](docs/verification-status.md). The e2b
verification mode now runs both M4.3 and M4.4 live gates; the all mode still
requires Docker's gVisor runtime registration.
