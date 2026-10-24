# Architecture and tradeoffs

## Scope and lifecycle

The Go module has no external dependencies. `Sandbox` exposes `Create(ctx, Config)`, `Exec(ctx) (Result, error)`, and idempotent `Destroy(ctx)`. A sandbox executes one argument vector exactly once. Create resolves the trusted local image to an immutable ID and creates a stopped container; Exec starts and attaches to that configured command. Destroy verifies a random ownership label before forced removal.

Docker and gVisor adapters share lifecycle code and explicitly pass `--runtime runc` or `--runtime runsc`. There is no silent fallback to the daemon default. This compares Docker-managed runtimes, including CLI overhead, rather than raw OCI calls. A Docker CLI dependency avoids introducing an SDK and its dependency tree.

`Run` attempts Destroy after every Create/Exec outcome. Its cleanup context is independent of the workload deadline or parent cancellation. Killing the attached CLI does not stop a container, so timeout/cancellation explicitly sends KILL before inspection and removal. Infrastructure errors and workload exit codes remain separate. Host SIGINT/SIGTERM trigger cancellation; SIGKILL, host crashes and daemon failures require the label-scoped cleanup command. Cleanup errors propagate.

## Defaults and input boundary

The host runner is trusted and controls Docker; the workload does not. Commands are passed directly as arguments, never interpreted on the host. No host environment variables are injected into workloads. Trusted image contents and image-declared environment remain part of the input.

Workloads use UID/GID 65532, drop all capabilities, prohibit gaining privileges, retain runtime security defaults, and use private PID, IPC, mount and cgroup namespaces. Root is read-only; bounded temporary storage and ordinary runtime-provided filesystems remain available. No host directories, devices, Docker sockets, host namespaces or published ports are exposed by the lab. Image-declared volumes are rejected; Docker itself still provides files such as resolv.conf and hosts.

RAM defaults to 128 MiB; equal memory and memory-swap caps disable additional swap. CPU quota is one CPU. Host task and per-user process limits default to 64, file descriptors to 256 and core size to zero. RLIMIT_NPROC is included because host PID accounting does not necessarily count gVisor virtual processes identically. With runc it counts tasks for the host UID, potentially across containers sharing UID 65532; run sequentially on a dedicated host. Limits depend on kernel/cgroup/runtime support and do not form a complete denial-of-service defense. See [Docker resource controls](https://docs.docker.com/engine/containers/resource_constraints/).

`Config.PidsLimit` controls only cgroup pids. `Config.NprocLimit` independently controls RLIMIT_NPROC; zero follows PidsLimit to preserve existing callers and the CLI's previous `--pids` behavior. Both limits remain bounded to 512. The process experiment now changes one control at a time at a fixed 512 MiB, and separates successful Start calls, initialized children and workload-visible threads.

## Isolation guarantees and limitations

| Runtime | Isolation mechanism | Practical limits |
| --- | --- | --- |
| Docker / runc | Linux namespaces, cgroups, capability restrictions and Docker's default seccomp/LSM policy isolate workloads. | Workload system calls reach the shared host kernel. Kernel/runtime vulnerabilities and configuration errors can break isolation. This is not a VM boundary. |
| gVisor / runsc | A user-space kernel (Sentry) implements application system calls and reduces direct host-kernel exposure; restricted runtime components provide host access. | The host kernel, runtime and Docker control plane remain trusted. Syscall/device compatibility and performance differ; shared resources and side channels remain. This is not a dedicated hardware VM. |

Sources: [Docker security](https://docs.docker.com/engine/security/), [gVisor security model](https://gvisor.dev/docs/architecture_guide/security/), [resource model](https://gvisor.dev/docs/architecture_guide/resources/), and [application compatibility](https://gvisor.dev/docs/user_guide/compatibility/). The default runsc platform is systrap; record deployment overrides ([platform guide](https://gvisor.dev/docs/user_guide/platforms/)). The lab never changes registration or platform settings.

Network `none` removes external interfaces while retaining loopback ([Docker documentation](https://docs.docker.com/engine/network/drivers/none/)). Opt-in `bridge` allows connectivity according to Docker/firewall configuration, including possible host/LAN/internet access. No domain allowlist, proxy or host firewall manipulation is implemented.

## Output, accounting and timing

Each stream retains at most 1 MiB and drains excess bytes while recording the dropped count. Docker logging is disabled to avoid unbounded daemon log files. Metadata is bounded separately. Results include streams, nullable exit code, timeout/cancellation flags and create/attach/cleanup/total durations.

Daemon StartedAt/FinishedAt timestamps provide optional workload duration. A concurrent Docker stats CLI supplies optional memory, host-PID and CPU-percentage samples. Null indicates unavailable data, especially for short jobs; an observed maximum is not a true peak, CPU percentage is not accumulated CPU time, and runtime memory includes differing overhead. Docker can emit zeros around process exit; those do not establish zero workload consumption. OOMKilled is Docker's reported state, not independently inferred. Unavailable measurements are never replaced with zero.

The process experiment additionally opts into read-only host cgroup v2 sampling. It verifies a local Unix endpoint and ties the inspected native PID's cgroup path to the owned container ID before reading that subtree. Native task entries and cgroup counts are labeled as host accounting; gVisor's workload-visible `/proc` and limits are separate. Read failures and unsupported cgroup v1/remote setups are recorded rather than guessed. Sampling is capped at 256 records (with an explicit dropped count and latest sample retained); fields within a sample are sequential reads, not an atomic snapshot. Child diagnostics retain 2 KiB stdout and 16 KiB stderr per child with explicit dropped-byte counters. Original output, timeout and teardown protections remain in effect.

The startup experiment uses a static no-op probe. Benchmarks alternate runtime order, retain excluded warmups and raw samples, pin each run to its image ID and report nearest-rank percentiles. Attach time includes OCI startup, probe execution and attachment; total also includes metadata, sampler shutdown and cleanup. Sampling adds observer overhead. These support local pipeline comparisons, not claims about pure runtime startup or application throughput.

## Deferred work

No Firecracker, Kubernetes, Temporal, HTTP API or AI agent is included. Remote-daemon host assertions, concurrency, per-tenant quotas, persistent sandboxes, sophisticated networking, crash reconciliation and a production security audit are outside this milestone.
