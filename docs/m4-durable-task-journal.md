# M4.2 — Durable task IDs, completion evidence and conservative crash recovery

M4.2 builds on the live-verified M4.1 execution bridge. It adds a durable
**single-host** task journal managed by `fleetd`; it does not create a job
queue, make arbitrary commands idempotent, or guarantee exactly-once effects.

## Contract and failure model

1. `e2b-taskctl --execute --task-id ID --id WORKLOAD --command COMMAND submit`
   first validates that WORKLOAD is a confirmed RUNNING E2B sandbox owned by
   the fleet controller. It writes **task ID, workload ID, sandbox ID,
   SHA-256 of the exact command bytes, timeout, and RUNNING** into a private
   task journal *and fsyncs it before contacting E2B*.
2. The daemon executes the command once over the existing loopback-only E2B
   envd proxy (no host shell execution). An explicit process terminal event is
   converted to **SUCCEEDED** (exit 0) or **FAILED** (nonzero exit), including
   bounded stdout/stderr, sandbox ID and a deterministic `result_sha256`.
   A local digest detects accidental journal damage; it is **not remote
   attestation** or proof that all side effects committed.
3. Transport errors, timeouts, cancellation, a changing/draining workload or
   missing terminal events become **UNCERTAIN** and retain the ownership
   binding. This is **never** automatically retried.
4. If fleetd dies after the task intent was flushed but before a terminal
   result was flushed, the next daemon startup durably converts RUNNING to
   UNCERTAIN. The same task ID (same VM, command and timeout) returns its
   existing record, even after restart. Reusing the task ID with any changed
   request returns a conflict.
5. A RUNNING or UNCERTAIN task prevents other tasks from being started on the
   same sandbox through the M4.2 API. A completed task doesn't block the VM.
   Explicit `fleetctl drain` remains available and non-blocking.
6. `GET /v1/tasks`, `GET /v1/tasks/{id}` and `POST /v1/tasks` use the **existing
   private Unix socket**. Only trusted local operators with socket permission
   may execute work; there is no tenant-facing authentication yet.

The default task file is `<fleet state-file>.tasks.json`; its parent must be
private mode `0700`. Both task journal and task lock are mode `0600`.
The store enforces an exclusive OS file lock, validates journal records on
restart, writes a temporary file, calls `fsync`, atomically renames and syncs
the directory. If persistence becomes uncertain it refuses further operations.
The journal holds at most 128 task records; it does not prune them silently.
Commands themselves are **not** saved, only SHA-256 digests. Output is saved
and might contain sensitive information, so protect the task journal.

### Successful command reported as UNCERTAIN

The shared M4.1 executor previously required `exitCode` to be present in
every terminal event. E2B declares it as an implicit-presence proto3 scalar,
so a successful command can emit `exited=true` while omitting the default
zero exit code. This made both `taskctl submit` and legacy `fleetctl exec`
reject a valid successful completion. The parser now accepts an omitted
zero only after a start event and with `exited=true` and no terminal error.
Empty end events, missing terminal events and transport failures remain
uncertain; explicit nonzero exits still produce completed failed commands.

After updating, rebuild **fleetd** and restart it with the same fleet and
task journals and the original flags. Rebuilding only the CLIs will not
update the parser. Existing UNCERTAIN records remain unchanged, because
their original completion evidence was discarded. Do not delete or edit
the journal or submit the append command under another task ID.

For the `m42-once` experiment, inspect the sandbox without changing it:

```sh
/tmp/agentlab-m42-fleetctl --socket .state/m4-2-fleet.sock \
  --execute --id taskvm42 --command 'wc -l /tmp/m42-once' exec
```

This legacy read-only inspection does not resolve the task record. If the
file exists, its count shows the current sandbox-local effect. A missing
file is a completed nonzero command, not evidence of a transport failure.
The uncertain task still blocks new M4.2 tasks on that sandbox. Preserve
its journal, drain the disposable fleet when finished, and use a new VM
and a new task ID for another durable completion experiment.

## One integrated live experiment

Use a **new fleet journal** (prior test journals are intentionally drained).
Build from the M4.2 PR branch and run the Go checks:

```sh
cd ~/src/agent-runtime-lab
git fetch origin
git switch --track origin/feat/m4-2-durable-tasks
CGO_ENABLED=1 go test -race -count=1 ./...
go vet ./...
go build -o /tmp/agentlab-m42-fleetd ./cmd/e2b-fleetd
go build -o /tmp/agentlab-m42-fleetctl ./cmd/e2b-fleetctl
go build -o /tmp/agentlab-m42-taskctl ./cmd/e2b-taskctl
```

**Terminal 1 — daemon** (load credentials in this terminal only):

```sh
cd ~/e2b
eval "$(sudo docker compose exec -T ready cat /run/e2b/sdk.env)"
cd ~/src/agent-runtime-lab
/tmp/agentlab-m42-fleetd --execute \
  --state-file .state/m4-2-fleet.json \
  --socket .state/m4-2-fleet.sock \
  --interval 2s --ttl 300 \
  --audit-interval 30s --audit-timeout 15s \
  --renew-ttl --renew-interval 60s \
  --exec-proxy http://127.0.0.1:3002
```

The corresponding task journal is
`.state/m4-2-fleet.json.tasks.json`. **Do not delete it to bypass an
uncertain task**.

**Terminal 2 — create a VM and wait until it reaches `running`:**

```sh
cd ~/src/agent-runtime-lab
/tmp/agentlab-m42-fleetctl --socket .state/m4-2-fleet.sock \
  --execute --id taskvm42 --tenant alpha submit
/tmp/agentlab-m42-fleetctl --socket .state/m4-2-fleet.sock status
```

Submit a command with a harmless, observable **sandbox-local side effect**:

```sh
/tmp/agentlab-m42-taskctl --socket .state/m4-2-fleet.sock \
  --execute --task-id m42-once --id taskvm42 \
  --command 'echo marker >> /tmp/m42-once; wc -l /tmp/m42-once' submit
```

Expected `state=succeeded`, `result.exit_code=0`, stdout containing `1`,
and a nonempty `result_sha256`.

**Repeat the exact same command with the same task ID**; you must receive the
same stored completion record. To verify the side effect wasn't repeated:

```sh
/tmp/agentlab-m42-taskctl --socket .state/m4-2-fleet.sock \
  --execute --task-id m42-once --id taskvm42 \
  --command 'echo marker >> /tmp/m42-once; wc -l /tmp/m42-once' submit
/tmp/agentlab-m42-fleetctl --socket .state/m4-2-fleet.sock \
  --execute --id taskvm42 --command 'wc -l /tmp/m42-once' exec
```

Expect line count **1**. A changed command with the old `m42-once` ID must
return HTTP 409 rather than executing.

Confirm nonzero exits are durably **FAILED**, not uncertain:

```sh
/tmp/agentlab-m42-taskctl --socket .state/m4-2-fleet.sock \
  --execute --task-id m42-fail --id taskvm42 \
  --command 'echo BEFORE_EXIT; exit 17' submit
echo "taskctl exit status: $?"
/tmp/agentlab-m42-taskctl --socket .state/m4-2-fleet.sock \
  --task-id m42-fail status
/tmp/agentlab-m42-taskctl --socket .state/m4-2-fleet.sock list
```

The CLI exits nonzero for `state=failed`, but the persisted record includes
`result.exit_code=17` and the captured stdout. Inspecting it never reruns.

**Crash-recovery proof (optional but important):** Create an in-flight task
using `--task-id m42-interrupted --command 'sleep 45; echo after' --timeout 60`
in Terminal 2. While it is running, use `pgrep -af agentlab-m42-fleetd`
and send `kill -9 PID` only to the identified fleet daemon PID. This does
**not** kill E2B itself or reliably cancel the sandbox command. Stop any
old taskctl request still waiting. The Unix socket may remain as a stale
filesystem inode; confirm there is no listener with
`ss -xlpn | grep m4-2-fleet.sock` before removing **only** the stale socket.
Restart the daemon with the **same fleet and task journals**. Check:

```sh
/tmp/agentlab-m42-taskctl --socket .state/m4-2-fleet.sock \
  --task-id m42-interrupted status
```

Expected `state=uncertain`, `result` absent, and an explicit warning note.
Submitting the same `task-id` cannot rerun that command. Other M4.2 tasks
against the *same sandbox* are blocked while uncertainty remains. Do not
infer that the sandbox command failed or that it was canceled.

**Always drain when done:**

```sh
/tmp/agentlab-m42-fleetctl --socket .state/m4-2-fleet.sock \
  --execute --drain-timeout 120s drain
/tmp/agentlab-m42-fleetctl --socket .state/m4-2-fleet.sock status
```

Confirm DELETED and CPU/RAM/pending all zero, then Ctrl+C in Terminal 1.
Keep task journal as evidence; draining never erases task history.

## Limitations / what comes after

- **No independent in-VM completion marker** or remote proof of side effects.
  A process terminal event is evidence of process exit, not a transaction.
- **No automatic task recovery or rerun** after ambiguity. Operators must
  investigate side effects; M4.3 can add explicit per-task completion markers,
  an artifact manifest and a safe, opt-in recovery procedure.
- No distributed workers, high-availability journal, tenant-facing auth,
  long-running detached tasks, globally shared VM ownership or exactly-once
  semantics. A command can still overlap an externally initiated drain.
- The legacy M4.1 `fleetctl exec` endpoint is intentionally *not* durable and
  must not be used for commands requiring replay protection. M4.2 protects
  only commands submitted through `e2b-taskctl`.
