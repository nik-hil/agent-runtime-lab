# Process-limit investigation

The process experiment now varies the cgroup PID limit and RLIMIT_NPROC independently. All existing test cases remain, the full race-enabled suite passed, and all six investigation cases completed with raw accounting retained. The observations below are from the final CLI run on 2026-10-24; a successful investigation means valid bounded observations, not proof of a universal runtime behavior.

## Reproduce

```sh
export LAB_RESULTS=results/process-limit-investigation
./scripts/build.sh
./scripts/test.sh integration
./scripts/metadata.sh > "$LAB_RESULTS/environment.txt"
image_id="$(docker image inspect --format '{{.Id}}' agent-runtime-lab:local)"
./bin/lab experiments --backend all --experiment processes --image "$image_id" --output "$LAB_RESULTS/observations.json"
docker ps -a --filter label=agent-runtime-lab.managed=true
```

Each runtime runs exactly three configurations, stated as cgroup PIDs / NPROC soft=hard: **256/256**, **64/256**, and **256/64**. Every case uses 536,870,912 bytes (512 MiB), a maximum of 24 child-launch attempts, one CPU, no network, and a 12-second deadline. The probe stops at the first failed Start call. It waits 300 ms before launching, uses 20 ms launch intervals, and holds children for another 500 ms before killing and reaping them, allowing the host observer to sample.

The CLI exposes `--pids` and independent `--nproc`. NPROC zero follows `--pids`, preserving old callers and least-privilege defaults. An individual case can be reproduced with:

```sh
./bin/lab run --backend gvisor --timeout 12s --memory-mib 512 --pids 64 --nproc 256 --process-accounting -- /lab-probe processes 24 observe
```

The old `allow` and `reject` probe modes still work. The experiment uses `observe` so the restricted cases do not assume a particular refusal count or errno. Control validity still requires 24 successful Starts; integration checks also require all 24 control children to emit readiness markers.

## Environment and evidence

Recorded at **2026-10-24T19:13:03Z**: Ubuntu 24.04.5 LTS, Linux 6.8.0-142-generic, amd64, four logical CPUs (DO-Premium-AMD), 8,326,950,912 bytes of RAM, Docker 29.1.3, runc 1.3.4-0ubuntu1~24.04.1, runsc release-20261019.0 and Go 1.27.3. Docker used cgroup v2/systemd. No runsc platform override was configured, so the documented systrap default applies. No service configuration was changed.

- [Full raw observations](evidence/process-limit-investigation/observations.json): all six cases, exact parent errors/errno, per-child diagnostics and readiness, workload snapshots and host samples.
- [Derived summary](evidence/process-limit-investigation/summary.json), [environment](evidence/process-limit-investigation/environment.txt), [runtime registration](evidence/process-limit-investigation/runtime-registration.json), and [image metadata](evidence/process-limit-investigation/image.json).
- [Source checksums](evidence/process-limit-investigation/source.sha256) and [probe checksum](evidence/process-limit-investigation/probe.sha256). The source manifest verifies the implementation tested; original milestone evidence was preserved separately.
- Image: `sha256:2f4cd938514f6d6725254db1065f75b8b65b0553b52239d896f29c6b7cbaf4ee`.
- Probe SHA256: `533a80cd0857a6d0c27e8b80d1c0a52047deab6d5e90b68473d9c16e79c685dd`.

## Measured outcomes

“Starts” means successful `exec.Cmd.Start` calls. “Ready” means that the child completed Go runtime initialization and emitted its marker; it does not establish survival until teardown. Host values are maxima observed by sampling, not exact kernel peaks. The effective workload NPROC soft/hard values matched the configured value in every case.

Workload limits come from `/proc/self/limits` (with its raw contents retained), and workload thread counts come from its visible `/proc/<pid>/task` entries. Under runsc these interfaces are virtualized; they are not native host measurements. Host samples read the separately verified native cgroup and corresponding host proc entries.

| Runtime | Case: PIDs/NPROC | Starts / attempts | Ready | Host tasks, max observed | Workload-visible threads, max observed | Observed increase in pids.events:max | Launch error |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| runc | Control: 256/256 | 24 / 24 | 24 | 78 | 78 | 0 | none |
| runc | Cgroup: 64/256 | 21 / 22 | 20 | 64 | 64 | 21 | EAGAIN (11) |
| runc | RLIMIT: 256/64 | 20 / 21 | 20 | 64 | 64 | 0 | EAGAIN (11) |
| runsc | Control: 256/256 | 24 / 24 | 24 | 99 | 76 | 0 | none |
| runsc | Cgroup: 64/256 | 12 / 13 | 12 | 64 | 40 | 1 | ENOMEM (12) |
| runsc | RLIMIT: 256/64 | 20 / 21 | 20 | 87 | 64 | 0 | EAGAIN (11) |

Native `/proc/<pid>/task` counts and cgroup.threads counts independently produced the same observed maxima as the host-task column in all six cases. There were 26–34 host samples per case and no dropped host samples. Individual fields are sequential reads, so agreement of maxima does not imply an atomic snapshot. Guest `/proc` reads can race child exit; incomplete totals remain null with their read errors.

The exact parent launch diagnostics were:

```text
fork/exec /lab-probe: resource temporarily unavailable
fork/exec /lab-probe: cannot allocate memory
```

In runc's cgroup-restricted case, Start succeeded for child 21, but that child exited 2 without becoming ready. Its stderr began:

```text
runtime: failed to create new OS thread (have 2 already; errno=11)
runtime: may need to increase max user processes (ulimit -u)
fatal error: newosproc
```

The complete stderr and stack trace are in the raw observations. No parent output or child diagnostics were truncated in this run. All six parent probes exited zero, had no timeout and reported OOMKilled=false; intentional launch refusals and recorded child startup failures are observations rather than runner errors. Cleanup-induced child signals are recorded separately through Wait errors and termination-request flags and are not counted as startup diagnostics.

## What the measurements support

**The two controls cover different accounting scopes.** Linux's PID cgroup controller accounts tasks; its max-event counter records enforcement events. Native RLIMIT_NPROC is associated with the calling process's real UID and, on Linux, counts threads. These limits are not simply counts of child executable launches. [Kernel cgroup documentation](https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html?highlight=freezer#pid), [Linux getrlimit documentation](https://man7.org/linux/man-pages/man2/getrlimit.2.html).

**Host cgroup enforcement was measured in both 64/256 cases.** Both reached 64 host tasks and incremented the host PID-limit event counter while effective workload NPROC remained 256. That is stronger evidence than interpreting errno alone. An event increment is not one failed child-launch call: other task creations, including a child's runtime thread initialization, also contribute.

**gVisor's host task budget includes runtime machinery.** In the runsc control sample with 99 host tasks, the host process snapshot included an `exe` process with seven threads, one `gvisor_sentry` with 15 threads, 76 single-thread `gvisor_sentry` processes and one single-thread `runsc-fd-parkin`. These host entries had real UID 0 and native NPROC unlimited. The workload separately ran as UID 65532 with its configured virtual NPROC. This identifies different scopes; it does not mean the workload ran as host root. The [gVisor resource model](https://gvisor.dev/docs/architecture_guide/resources/) describes virtual tasks and possible platform-specific host stubs. We did not identify the internal role of every recorded process from its name alone.

Consequently, the same host PID budget need not accommodate the same number of initialized workload children. Here runsc reached the host cap with 12 ready children while runc had 20. The recorded runsc ENOMEM coincided with host PID-limit enforcement; it does not establish exhaustion of the 512 MiB RAM limit. We did not trace the runtime's internal allocation/errno translation path, so its exact ENOMEM provenance remains unresolved.

**The 256/64 cases instead match the workload-visible NPROC boundary.** Both reported effective NPROC 64, reached a workload-visible thread count of 64, returned EAGAIN and had no observed host PID-limit event increase. Runsc's host maximum was 87, below its cgroup cap of 256 and different from its virtual count of 64. Together with native host NPROC unlimited, this supports separating runsc's virtual NPROC accounting from native host limits. These controlled observations are consistent with NPROC enforcement, but are not a syscall trace proving it was the sole possible failure source. Host-wide usage by other processes sharing a real UID was not exhaustively measured; system-wide task/address-space constraints were not traced either.

The Go diagnostic suggesting `ulimit -u` appeared in the cgroup-restricted case even though NPROC was 256. Error messages alone therefore cannot distinguish the controls. Scheduling, Go runtime thread creation, measurement overhead and child initialization failures can change counts between runs; these are observations, not fixed capacity guarantees.

## Verification, safety and limits

`gofmt`, direct `go test ./...` and `go vet ./...` passed. Plain tests leave integration disabled; the separate full run used `go test -race -count=1 -cover -v ./...` with LAB_INTEGRATION=1. Every existing integration test and all six original experiment categories passed on both runtimes. Added checks cover independent Docker arguments/default compatibility, actual configured NPROC in workload snapshots, bounded launch reports, counter parsing, incomplete reads and refusal to use remote Docker PIDs as local host PIDs. See the [complete test log](evidence/process-limit-investigation/integration-tests.log).

The trusted host runner reads only the owned container cgroup subtree and corresponding process metadata. No host proc/cgroup directory or Docker socket is mounted into a workload. Host sampling requires a local Unix Docker endpoint, cgroup v2 and a native cgroup path tied to the owned container ID. Unsupported or inaccessible readings carry nulls/errors. Samples are bounded to 256, child stdout to 2 KiB and stderr to 16 KiB each, with dropped-data counts; no unavailable measurement is invented.

The [remaining-container record](evidence/process-limit-investigation/remaining-containers.txt) is empty. Runtime-managed cleanup killed/reaped children and removed the lab containers; no unrelated services or projects were changed. This investigation does not prove escape resistance, universal errno mappings or an exact live-process ceiling, and introduces no additional runtime or agent.
