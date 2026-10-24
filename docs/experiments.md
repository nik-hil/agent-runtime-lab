# Experiments

```sh
./bin/lab experiments --backend all --output results/experiments.json
./bin/lab experiments --backend gvisor --experiment filesystem
```

Integration tests run the same six definitions and fail on unexpected behavior. Additional tests cover stream separation, exit codes, cancellation, output bounds and lifecycle rules. Experiments run sequentially against a local Linux daemon, without bind mounts or public network dependencies. Infrastructure errors never count as successful isolation.

## 1. Filesystem isolation

Create a harmless random canary in the host temporary directory. The probe receives its absolute path and must see ENOENT, see neither conventional Docker socket path, run with nonzero UID and empty effective capabilities, fail a root write, and successfully write to /tmp. The host verifies unchanged canary contents and removes it.

Expected: all checks pass on both runtimes. A denied write alone does not distinguish read-only root from Unix permissions. This checks normal visibility and writes, not kernel escape resistance.

## 2. Network restrictions

Start a lab-owned non-root TCP peer on its private bridge address and port 8080, without a published host port. A bridge-connected client must read its fixed greeting. A client with network=none must fail to reach the same endpoint. Confirm the peer is still running, then cancel it, collect its final result and remove it. Peer and clients use the selected runtime.

Expected: positive control connects; denied client does not. A failed positive control invalidates the experiment. Loopback remains usable with none. Bridge connectivity depends on Docker/firewall configuration and does not establish an internet allowlist.

## 3. Memory limits

Retain and touch 256 MiB in 1 MiB increments. A 512 MiB control should finish. Repeat at 64 MiB with swap capped to RAM, a 12-second deadline, one CPU and bounded process count. Wait two seconds before allocation to permit optional baseline sampling.

Expected: control exits zero; restricted run exits nonzero with Docker's OOMKilled flag or an explicit out-of-memory diagnostic. Timeout, startup failure or unrelated exit alone does not count as enforcement. Samples may miss the true peak; runtime overhead is part of cgroup memory use.

## 4. Process limits

At a fixed 512 MiB RAM, first create 24 source-built children with both process limits set to 256. Repeat with pids-limit=64 and RLIMIT_NPROC=64. Detect EAGAIN or ENOMEM, record the successful spawn count and exact diagnostic, then kill and reap children. CPU, memory and timeout remain bounded; this is not an unbounded fork bomb.

Expected: the control starts all 24 children; the restricted run refuses creation before that bound. ENOMEM alone cannot establish a process limit, so the equal-memory positive control is mandatory. The count need not be 64: Go children create threads, runc accounts host tasks, and gVisor models virtual processes differently. Both process controls apply, so refusal cannot be attributed to one alone. See the shared-UID caveat in [architecture](architecture.md).

The recorded count is successful process-start calls, not a measurement of concurrently live children. A child may itself fail during Go runtime thread initialization. The experiment checks bounded creation refusal and does not claim an exact concurrent-process ceiling.

## 5. Startup latency

Create the static no-op process, start/attach, capture its ready message and exit, inspect and destroy. Record create, attach-to-exit, optional workload, cleanup and total milliseconds.

Expected: exit zero and real timestamps; there is no speed threshold. `scripts/benchmark.sh 20 3 results/benchmark` repeats with alternating runtime order, excludes three warmups per runtime from summaries, retains raw data and records the environment. Image pulls/builds and host boot cold starts are not measured.

## 6. Cleanup after termination

A parent creates a /tmp marker, starts a child and sleeps. A two-second deadline stops attachment, explicitly kills the workload and removes its owned container. Verify its exact generated name is absent. A fresh sandbox must not contain the old marker.

Expected: timeout after marker/child creation, absent container and fresh temporary storage. This verifies Docker/runtime-managed cleanup, not hypothetical processes escaping the runtime. Host SIGKILL and daemon outages require manual cleanup and are not exercised automatically.

## Evidence

Observations include expectation, passed/failed/error status, raw results, diagnostics and real timings. Unsupported samples remain null. Tests save per-runtime JSON and verbose logs under results/. Benchmark summaries reject unsuccessful runs. Use [report-template.md](report-template.md) to record outcomes without filling in unmeasured values.
