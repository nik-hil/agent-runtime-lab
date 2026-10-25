# Milestone 1B: task completion, failure recovery and idempotency

## Four different questions

Starting a program, completing a task, committing a side effect and safely retrying are different events. A readiness message says that application code initialized. Exit code 0 says the program chose to report success. Neither proves that the expected output exists or that its contents are correct. This lab therefore checks the artifact or durable ledger independently of the child's success message.

The deterministic task adds the numbers 1 through 10. The complete artifact must contain exactly `sum(1..10)=55` followed by a newline. Partial work contains `sum(1..5)=15` followed by a newline. No AI agent, real GitHub operation or third-party business API participates.

## Setup and exact commands

Run from the repository root on a disposable Linux Docker host with both runtimes already registered. Follow [setup.md](setup.md) for the compiler and runtime prerequisites. Nothing here installs software or restarts host services.

```sh
source scripts/env.sh
docker info --format 'cgroup={{.CgroupVersion}} runtimes={{json .Runtimes}}'
mkdir -p results/task-completion
LAB_RESULTS="$PWD/results/task-completion" ./scripts/build.sh
go test ./...
go vet ./...
LAB_RESULTS="$PWD/results/task-completion" ./scripts/test.sh integration
```

`source` selects the project-local compiler and caches. `docker info` checks infrastructure. The build compiles the Go runner/probe and builds a local scratch image without downloading an image. Plain Go tests exercise ledger/verification logic; their default integration skips are **not** runtime verification. The final script requires both runtimes and runs vet plus the complete race-enabled suite, including every original experiment. A missing runtime or failed assertion makes it fail.

Run each experiment manually; `--backend all` executes both Docker/runc and gVisor/runsc:

```sh
# A: compare the independently read artifact with the expected exact bytes.
./bin/lab experiments --backend all --experiment task-success --output results/task-completion/task-success.json

# B: inspect partial work AFTER the child crashes, then destroy its sandbox.
./bin/lab experiments --backend all --experiment task-partial --output results/task-completion/task-partial.json

# C: lose an acknowledgement, restart the receiver, retry and check the ledger.
./bin/lab experiments --backend all --experiment task-retry --output results/task-completion/task-retry.json
```

Use `--backend docker` or `--backend gvisor` to isolate one runtime. Each command fails if its assertions fail, even when a workload exited 0. The intentionally failing workloads in B/C are expected observations, so a correctly verified experiment itself passes.

These optional `jq` commands expose the evidence rather than just the command's exit status:

```sh
jq '.[] | {backend,status,attempts:.task.attempts}' results/task-completion/task-success.json
jq '.[] | {backend,status,attempts:.task.attempts,fresh_tmp:.task.fresh_sandbox_artifact_absent}' results/task-completion/task-partial.json
jq '.[] | {backend,status,attempts:.task.attempts,checks:.task.independent_ledger_checks,receivers:.task.receivers,cleanup:.task.cleanup_confirmed}' results/task-completion/task-retry.json
jq -e 'all(.[]; .status == "passed")' results/task-completion/task-retry.json
```

The first two show the independently read artifact bytes. The third separates failed process attempts from committed ledger state. The last checks the experiment assertions; it does not replace inspecting the underlying evidence.

## A — Successful execution

The child emits a task-ID/attempt-specific `ready` event, computes the sum and writes `/tmp/task-artifact.txt`. It emits `artifact_written` and waits at a bounded observation barrier. The trusted parent runs a second source-built process inside the same sandbox to read the file. That reader exports its contents to the parent's JSON and releases the barrier. The child emits `work_completed` and exits 0.

Expected: readiness, a zero exit and the exact artifact agree. The parent compares the bytes against the task specification; it does not accept a claimed hash or completion marker alone. Unit tests explicitly reject missing, incorrect and partial artifacts despite a zero exit. The exported JSON keeps the verified artifact available after sandbox destruction. This does not make `/tmp` durable: a fresh sandbox must have no file there.

## B — Partial failure

A small supervisor launches one child. The child emits readiness, writes the partial sum and deliberately exits 42. The supervisor calls `Wait`, records that exit and keeps the sandbox alive briefly. A separate reader then observes the partial file **after the child has failed**, together with the recorded child exit. After observation, the supervisor also exits 42. The orchestrator destroys the sandbox and checks a fresh sandbox for the old file.

Expected: some work happened, the task did not finish, and the file survives child failure while the sandbox is alive. It does not survive as a file in a new sandbox. The evidence copy of its contents intentionally remains in JSON.

There is no automatic rollback when a process fails: the partial write was still visible after `Wait` reported failure. Removing a temporary filesystem later is resource cleanup, not a transaction undo. A previously exported or externally committed effect can outlive that cleanup, as C demonstrates. The partial-file experiment does not attempt forensic recovery of destroyed tmpfs pages.

## C — Crash after external commit

The receiver simulates creation of a resource such as a PR using a filesystem ledger. It runs in a trusted **runc** container for both comparisons; only the task backend changes. This keeps receiver behavior constant. Its dedicated Docker volume is mounted only into the receiver. Tasks receive an internal IP address, never the volume, host paths or a Docker socket.

The receiver and tasks join a newly created, labeled `--internal` bridge. It has no publicly published port or connection to an external bridge. Normal workloads still default to `network=none`; this experiment explicitly opts into `NetworkLabInternal`. The adapter verifies the network's driver, internal flag and exact ownership labels. Docker's host/control plane remains trusted: an internal bridge is not protection from a host operator ([Docker internal-network documentation](https://docs.docker.com/reference/cli/docker/network/create/#network-internal-mode---internal)). This fixture has no authentication and must not be exposed as a service.

### Execution timeline

```mermaid
sequenceDiagram
    participant P as Parent / verifier
    participant T as Task sandbox, attempt 1
    participant R as Receiver 1
    participant V as Dedicated ledger volume
    participant R2 as Receiver 2
    participant T2 as Fresh task sandbox, attempt 2
    T->>P: ready(task ID, attempt 1)
    T->>R: commit(task ID, stable key, payload)
    R->>V: lock, write, file sync, rename, directory sync
    Note over R,V: Commit is persisted before acknowledgement
    R--xT: receiver exits 43 before sending acknowledgement
    T->>P: acknowledgement error, exit 45
    P->>P: destroy task sandbox and receiver container
    P->>R2: start new container using the same volume
    P->>R2: run independent ledger reader
    R2->>V: locked snapshot confirms one record
    Note over P,V: Task retry has not happened yet
    P->>T2: start attempt 2 with original task ID and key
    T2->>R2: retry original operation
    R2->>V: locked lookup finds existing resource
    R2-->>T2: existing receipt; no second record
    T2->>P: work completed, exit 0
    P->>R2: independently read and verify final ledger
```

The first task cannot infer whether its request committed from the missing reply. It records `external_commit_exists: null` until the parent obtains a ledger snapshot; the completed report records the independently verified value. The receiver crash is source-defined after persistence, not a randomly timed kill. The first task sandbox has already been destroyed when the receiver restarts in a different container.

The retry receives the stored resource ID with `reused: true`. The experiment also runs two concurrent same-key retries, rejects another task trying that key, commits a different task with a different key, and crashes a third task before it sends any commit. That third task's retry creates its first record. These give a final expected ledger of **three distinct resources**, not three retries of one resource.

### What durable commit and idempotency mean here

The ledger is the simulated side effect itself. Under an exclusive `flock` on a stable lock file, the receiver validates the current records and checks both task identity and key. It writes a same-directory temporary file, calls file `Sync`, closes it, renames it over `ledger.json`, then syncs the directory before responding or triggering the crash. Independent readers take the same lock. Directory sync matters because file sync alone does not persist its directory entry ([Linux fsync documentation](https://man7.org/linux/man-pages/man2/fsync.2.html)). Errors propagate; corrupt ledgers fail closed.

Identical task ID, key and payload return the existing record. Reusing either identity inconsistently is a conflict, not a new effect. The lock serializes concurrent checks and writes across receiver/verifier processes; a rename alone would not prevent two writers from reading the same old state. Unit tests race new duplicate commits as well as testing retries and conflicts. Runtime experiments race retries against an already committed record.

An acknowledgement is only a message about a commit. Losing that message cannot undo the synced record. Durable storage lets a new receiver find it; the stable key lets a retry recognize it. The task runner does not promise that arbitrary external operations become exactly-once. A real PR service would need compatible idempotency or a reconciliation strategy; atomically writing this local ledger cannot transact with an unrelated API.

## Evidence fields and code flow

`cmd/lab` selects an experiment; `src/experiments/task_completion.go` coordinates bounded attempts and independent verification. `cmd/lab-probe/tasks.go` implements the child/supervisor, observer and receiver modes. `src/tasklab` implements the fixed operation protocol, HTTP handler and ledger. `task_receiver.go` owns receiver/container/network/volume setup and teardown. The original `Sandbox` Create/Exec/Destroy interface and runc/runsc selection remain intact.

Each attempt records task ID, attempt, key and purpose; inspected process start/exit; parsed readiness; child-reported completion; independently verified task completion; partial artifact evidence; verified external commit existence; retries, duplicates and final state. `runs[run_index]` preserves exact stdout/stderr, exit code, timeout, truncation, timings and available resource samples. `process_started` uses the runtime's inspected exited state, not successful `docker create`. In B, `child_started` records the real Go `Start` PID and `child_exited` records the `Wait` status. Readiness is a separate child-emitted event, not proof of later completion.

`work_completed` requires both normal task completion and matching independent artifact/receipt/ledger evidence. A failed attempt can therefore have `work_completed: false` and `external_commit_exists: true`. External commit `null` means unavailable/unverified. The fresh-sandbox artifact field applies only to A/B. Cleanup checks and overall experiment status are separate from task state. Short processes can have no usage samples; null metrics are not zero consumption.

## Actual observations

Verified **2026-10-24 at 20:00:38 UTC** on Linux 6.8.0-142-generic, amd64, cgroup v2, Docker 29.1.3, runc 1.3.4, runsc release-20261019.0 and Go 1.27.3. The probe image was `sha256:98a408c71b48614324ad548331b49ca481094afaff268e7329aefeb4dd4c3ef9`; its binary SHA256 was `a5f92486d972f0c0066d3702e11bc2475ce9538a1cf1e2328bdd7991a60ffd79`.

The direct `go test ./...` and `go vet ./...` commands passed. The explicit integration script passed vet and all race-enabled tests, with no skipped integration tests. This includes the original six experiments and existing cancellation, output-bound and lifecycle checks. The integration test package reported 60.035 seconds; this is a suite duration, not a runtime performance comparison. The three separate manual commands above then all passed on both backends.

| Experiment | Task runtime | Started / ready | Task exit | Independently verified work | External commit | Retry / duplicate effects | Final observed state |
| --- | --- | --- | --- | --- | --- | --- | --- |
| A | runc | yes / yes | 0 | exact complete artifact | none | no / none | complete; exported bytes retained; fresh `/tmp` empty |
| A | runsc | yes / yes | 0 | exact complete artifact | none | no / none | complete; exported bytes retained; fresh `/tmp` empty |
| B | runc | yes / yes | child 42; supervisor 42 | partial artifact read after child exit | none | no / none | failed; partial file did not appear in fresh sandbox |
| B | runsc | yes / yes | child 42; supervisor 42 | partial artifact read after child exit | none | no / none | failed; partial file did not appear in fresh sandbox |
| C, primary task | runc | yes / yes, both attempts | 45 then 0 | retry receipt matches persisted record | exactly one for this task | yes / none | one committed resource reused after receiver restart |
| C, primary task | runsc | yes / yes, both attempts | 45 then 0 | retry receipt matches persisted record | exactly one for this task | yes / none | one committed resource reused after receiver restart |

The exact first-attempt diagnostic on both backends was `commit acknowledgement unavailable: Post "http://<lab-internal-ip>:8080/commit": EOF`; only the ephemeral private IP is sanitized here. Full diagnostics remain in the raw JSON. The receiver's log records `durable_commit_before_ack_crash`, and its inspected exit code was 43. The independent snapshot after receiver replacement and before task retry already contained `resource-0001` for that task. That measurement, rather than the task error alone, establishes that the effect committed.

Additional C assertions produced the same outcomes on both task runtimes:

| Check, in execution order | Task exit(s) | Ledger records afterward | Interpretation |
| --- | --- | --- | --- |
| First commit, lost acknowledgement, receiver restart | 45 | 1 | effect survived task destruction and receiver replacement |
| Same task/key retry | 0 | 1 | existing resource returned with `reused: true` |
| Two concurrent same-key retries | 0, 0 | 1 | both reused the original; no additional effect |
| Different task ID using original key | 46 (HTTP 409) | 1 | conflicting identity rejected |
| Different task ID and different key | 0 | 2 | independent work committed a new resource |
| Third task crashes before commit | 44 | 2 | no record for that task |
| Third task retries with its original ID/key | 0 | 3 | exactly one first commit for that task |

Across the final manual runs, all 20 task attempts started and became ready; their exit sequences matched the expected states. No attempt timed out and no parent output was truncated. All six experiment results confirmed cleanup. Final label-filtered queries returned **zero containers, zero networks and zero volumes**. The receiver's second instance exited 143 during normal fixture cleanup; this was separate from task completion. No difference in these task semantics was observed between runc and runsc in this run; no performance or stronger isolation claim follows.

Two development failures were observed and corrected before the final verification: the image build initially excluded the new empty ledger directory, and Docker rejected local-log compression with a one-file log bound. The latter also exposed cleanup incorrectly depending on log retrieval; cleanup now records unavailable diagnostics and continues removal. The leftover lab fixtures from that development failure were removed with the label-scoped cleanup script. Preliminary raw observations remain under `results/task-completion/development/`; they are not counted as successful verification.

The concise [sanitized summary](evidence/task-completion/summary.json), [integration log](evidence/task-completion/integration-tests.log), [source hashes](evidence/task-completion/source.sha256) and [evidence notes](evidence/task-completion/README.md) are retained under `docs/`. Raw scenario and integration JSON, environment metadata and empty-resource queries are also committed in [results/task-completion/](../results/task-completion/). The summary omits private IPs, random resource names and host-identifying metadata; it retains task identities, attempt states, verified artifact bytes, receipts, ledger counts and sanitized failure diagnostics. Source hashes tie the observations to the tested files rather than an unchanged base commit.

## Cleanup and remaining limits

Normal execution snapshots the ledger into JSON, then removes task containers, both receiver containers, the internal network and the volume. Thus persistence is demonstrated across receiver restart **within** the experiment, not across separate fresh invocations. Each invocation intentionally begins with a new ledger. The second receiver's normal cleanup exit may be 143 (SIGTERM); it is not a task failure. File reading and exit injection are bounded; there is no process stressor or random failure timing.

After stopping lab commands, inspect and clean only lab resources:

```sh
docker ps -a --filter label=agent-runtime-lab.managed=true
docker network ls --filter label=agent-runtime-lab.managed=true
docker volume ls --filter label=agent-runtime-lab.managed=true
./scripts/cleanup.sh
```

The final command intentionally deletes any remaining lab ledger volumes; export evidence first. Add `--image` only to remove the lab image as well. It never prunes unrelated resources or stops a service. Preserve `results/task-completion/` if you want the raw execution evidence; sanitized summaries under `docs/evidence/task-completion/` are retained in Git.

Verified restart recovery does not establish physical power-loss survival: this lab does not power-cycle the host, fault the storage device, or crash the receiver during each possible write step. Durability still depends on the filesystem, volume driver and hardware honoring sync. The single-host ledger is limited to 128 fixed lab operations; no garbage collection, authentication, multi-host coordination or general retry engine is provided. The independent reader shares the trusted source-built image and is not adversarial attestation. Sandbox isolation limitations remain those in [architecture.md](architecture.md).

For AI coding agents and distributed workflows, the same distinctions matter: starting a tool is not finishing a change; a successful command can produce an incorrect artifact; a timeout can occur after a remote change already committed; retrying without a stable identity can duplicate effects. A parent should retain attempt evidence, verify the intended result, and consult committed state before deciding whether work must be repeated. This lab demonstrates those ideas without implementing an AI agent or workflow platform.
