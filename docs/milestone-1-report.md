# First milestone report — 2026-10-24

The Go runner, both runtime adapters, six experiments, integration tests, benchmark tooling and documentation are implemented. The final race-enabled test suite passed, and all six experiments passed for both runtimes through both the test suite and CLI. No lab containers remained after verification. Earlier failures and raw measurements are retained.

## Environment and provenance

The final benchmark began at 2026-10-24T11:10:44Z on Ubuntu 24.04.5 LTS, Linux 6.8.0-142-generic, amd64, four logical CPUs (DO-Premium-AMD), and 8,326,950,912 bytes of RAM. Docker Engine/CLI were 29.1.3, runc was 1.3.4-0ubuntu1~24.04.1, runsc was release-20261019.0, and the project-local compiler was Go 1.27.3. Docker used cgroup v2, the systemd cgroup driver and overlayfs. AppArmor and Docker's default seccomp profile were enabled. The recorded load averages were 1.16, 0.80 and 0.58; the host was not proven otherwise idle.

Existing daemon configuration registered `/usr/bin/runsc` without runtime arguments or a platform override, so the documented systrap default applies. No service configuration was changed. See [environment.txt](../results/benchmark-final/environment.txt), [runtime registration](../results/runtime-registration.json), and [Go download provenance](../results/go-toolchain.json).

- Probe SHA256: `412add18dc57410497f2e03e1be16d8a64a10ec5425bea2d060a01340ef4d6d4`.
- Tested image: `sha256:387306d43f0e8610819ddbc51a13c42be3fdeee06804bc279a3b0e1bcc81ce45`.
- The official Go archive checksum matched `65efbc80fdc1fa2cd2631cad5b481777a68e8725f34e89f50fc6ac714e379b40`.
- [Source checksum manifest](../results/source.sha256) verified every recorded implementation/tooling file after the final run. There was no existing Git history; no commit identifier is claimed.

## Commands and verification

Direct `go test ./...` and `go vet ./...` were run using `.tools/go/bin/go`, project-local GOCACHE/GOMODCACHE, and GOTOOLCHAIN=local. The default test invocation passed with integration tests disabled; it was not treated as runtime verification. Final verification used:

```sh
./scripts/build.sh
./scripts/test.sh integration
./bin/lab experiments --backend all --output results/experiments.json
./scripts/benchmark.sh 20 3 results/benchmark-final
./bin/lab run --backend docker --timeout 3s --memory-mib 64 --network none -- /lab-probe echo 7
./bin/lab run --backend gvisor --timeout 3s --memory-mib 64 --pids 64 -- /lab-probe noop
./scripts/cleanup.sh
```

The integration script ran vet and `go test -race -count=1 -cover -v ./...` with LAB_INTEGRATION=1. No enabled integration checks were skipped. Sandbox unit coverage was 82.8%; this is not whole-project coverage. Probe executables run outside the host test binary and were not coverage-instrumented. See the [full final test log](../results/integration-tests.log).

| Experiment | runc | runsc | Actual observation |
| --- | --- | --- | --- |
| Filesystem | passed | passed | UID 65532, empty effective capabilities, host canary/socket absent, root write denied, /tmp writable; host canary unchanged. |
| Network | passed | passed | Bridge positive control read the local peer greeting; none returned network unreachable. The peer remained running until explicit cancellation. |
| Memory | passed | passed | The 256 MiB allocation completed under 512 MiB. Under 64 MiB, both workloads exited 137 with Docker OOMKilled=true and no timeout. |
| Process limits | passed | passed | Both controls started 24 children at limit 256. At limit 64, runc refused after 21 successful start calls with EAGAIN; runsc refused after 12 with ENOMEM. RAM was 512 MiB throughout. |
| Startup | passed | passed | No-op printed ready, exited zero and produced real lifecycle timestamps. |
| Cleanup | passed | passed | Two-second deadlines terminated marker-writing parents with children; containers disappeared and fresh sandboxes had no prior marker. |

Counts above match [final CLI observations](../results/experiments.json) and [integration evidence](../results/integration-docker.json), [runsc evidence](../results/integration-gvisor.json). Process counts are successful start calls, not concurrently live children or an exact task ceiling.

Additional integration checks passed for stream separation, exit code 7, parent cancellation, repeated Exec rejection, idempotent Destroy and output bounds. Each flood stream retained 1,048,576 bytes and dropped 180,224 bytes. CLI smoke results are saved in [cli-docker.json](../results/cli-docker.json) and [cli-gvisor.json](../results/cli-gvisor.json).

## Measured latency

The final benchmark used the same immutable image, 20 measured samples per runtime and three excluded warmups each. Runtime order alternated. All 46 measured/warmup workloads exited zero. Values below are milliseconds; percentiles use nearest rank.

| Runtime | Metric | Mean | p50 | p95 | Min | Max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| runc | attach-to-exit | 227.402 | 224.347 | 256.020 | 194.519 | 278.110 |
| runsc | attach-to-exit | 259.339 | 254.072 | 286.005 | 226.502 | 329.123 |
| runc | total lifecycle | 444.493 | 432.657 | 522.458 | 391.481 | 527.027 |
| runsc | total lifecycle | 483.321 | 475.283 | 527.825 | 430.904 | 607.421 |

See [raw samples](../results/benchmark-final/samples.json) and the [complete summary](../results/benchmark-final/summary.md), including create, daemon-timestamp workload and cleanup measurements. These are warm-image Docker pipeline measurements with a concurrent stats observer, not pure OCI startup, cold image pulls or throughput benchmarks. The small local sample supports no universal performance ranking.

## What failed and what we learned

The [first integration run](../results/first-integration/integration-tests.log) failed on two adapter assumptions: modern Docker exposes bridge addresses under NetworkSettings.Networks rather than the legacy top-level field, and missing-container diagnostics used lowercase text. Both were fixed with regression tests; repeated Destroy now passes on both real backends.

The initial process probe expected EAGAIN, but runsc reported ENOMEM. Accepting that diagnostic without a control would confuse memory pressure with process enforcement. The final experiment holds RAM fixed and adds a higher-process-limit control. Its paired observations support limit-associated refusal while leaving the exact failing accounting mechanism unresolved: both cgroup pids and RLIMIT_NPROC change together, and virtual tasks and Go runtime threads differ across runtimes.

Resource sampling is incomplete by design. Longer memory runs produced samples, while most no-op workloads produced none. Sampled maxima missed the actual OOM growth; some Docker samples around exit reported zeros. Neither a sampled maximum nor an exit-time zero establishes true peak memory. No missing sample was replaced with a made-up value. Cumulative workload CPU time is not collected; only optional CPU-percentage samples are available.

Docker emitted a legacy-builder deprecation notice because buildx was unavailable; scratch-image builds still succeeded. No packages or services were changed to suppress it. The Go compiler was installed locally from the verified official archive, and no downloaded setup script was executed.

## Cleanup, limitations and remaining work

[cleanup.log](../results/cleanup.log) records successful label-scoped cleanup; [remaining-containers.txt](../results/remaining-containers.txt) is empty. The probe image, earlier lab build layers, local compiler/caches, binaries and evidence remain for reproduction. `scripts/cleanup.sh --image` removes the current labeled image and dangling lab-labeled images; shared build caches are retained. Local file-removal instructions are in [setup.md](setup.md).

No unrelated project or system service was modified. The original parent AGENTS.md SHA256 remained `901d3cb312cac7a98c9640d5226c3efcb7c276e9ea97a99b8c6b576612828ed4`. Scoped AGENTS.md and always-applied Cursor rules preserve the requested direct-Go editing workflow.

The probes do not prove escape resistance, exact peak accounting, crash recovery, hostile multi-tenant safety or broad application compatibility. SIGKILL/daemon-outage recovery, sophisticated egress policy and concurrency remain untested or deferred. See [architecture](architecture.md) for runtime guarantees and limitations. Firecracker, Kubernetes, Temporal, APIs and AI agents remain outside this milestone.
