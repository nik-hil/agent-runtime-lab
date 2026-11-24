# M4.3 — VM completion evidence, artifact manifests and explicit recovery

M4.2 persists intent and results on the orchestration host. If that host loses
the response or crashes before recording the result, a task remains UNCERTAIN.
M4.3 adds a second completion record **inside the original E2B VM**, allowing an
operator to resolve uncertainty when matching evidence is still available.

## Contract

1. Submit with `e2b-taskctl --evidence` to opt in. Without that flag, existing
   M4.2 execution and conservative uncertainty behaviour are preserved.
2. Before any upload or execution, the host journal fsyncs the original task
   identity plus a random 256-bit nonce, helper binary SHA-256, protocol version
   and canonical list of requested artifact paths.
3. The daemon uploads a **source-built, static Linux amd64 Go helper** over the
   existing authenticated loopback envd `/files` endpoint. It checks the uploaded
   bytes' hash and invokes the helper through the existing sandbox process RPC.
   It never executes the task or helper on the orchestration host. The helper
   checks its own binary hash, unlinks its uploaded executable and reserves an
   exclusive nonce directory before running the user's command.
4. After process exit, the helper captures bounded stdout/stderr and observes
   every declared artifact. It writes a completion JSON envelope to a temporary
   mode-0600 file, fsyncs it, renames it to `completion.json` and fsyncs its
   directory. The mode-0700 nonce directory is never reused, even if the marker
   is missing. The marker contains no raw command.
5. The daemon independently downloads that marker and each declared artifact.
   It validates task/workload/VM/command/timeout/nonce/helper bindings, output
   hashes, artifact paths, lengths and SHA-256 hashes before recording the
   result on the host. Exit 0 is SUCCEEDED; a known nonzero exit is FAILED.
6. Missing responses, helper failures, deadlines, missing/changed artifacts or
   a changing/draining VM leave the task UNCERTAIN. No automatic inspection,
   task replay or state resolution occurs after an execution error.
7. `e2b-taskctl --execute --task-id ID recover` explicitly inspects an UNCERTAIN
   evidence task. It never uploads a helper or starts a process. It only reads
   evidence from the original owned healthy RUNNING VM, rechecks fleet state,
   and durably records a matching result. The note says the command was not
   rerun. A recovered failed command stays FAILED and the CLI exits nonzero.
8. Missing, invalid, stale, oversized or changed evidence leaves the task and
   its VM fence unchanged. A missing marker is **never permission to retry**.
   Completed records are returned as stored without another provider call.

The marker is `/tmp/agentlab-m43/<nonce>/completion.json`. stdout/stderr share
a 64 KiB budget; invalid UTF-8 is replaced with `?`. There are at most eight
declared artifacts, each a regular file of at most 1 MiB, with distinct clean
absolute paths of at most 256 bytes outside the evidence directory. The helper
rejects final-component symlinks, directories, devices and FIFOs. The envelope
is capped at 512 KiB. Existing task, command and timeout bounds remain 128 task
records, 4000 command bytes and 1..60 seconds. JSON request overhead has its own
64 KiB limit. Inspection uses a bounded 30-second budget.

Recovery's control-plane `/connect` request obtains an envd token and can extend
the VM's TTL. "No execution" does not mean this control request is mutation-free.
API keys never enter the sandbox proxy; envd tokens and file bodies are not
included in API error messages.

## Host journal compatibility

Task journals now write schema version 2. Opening an M4.2 version-1 journal
validates it and atomically persists a migration that retains its task IDs,
request bindings, states and result hashes. Interrupted RUNNING tasks still
become UNCERTAIN. Older daemons reject version 2 rather than silently discarding
evidence metadata; do not downgrade a migrated task journal.

Legacy tasks have no evidence intent and cannot acquire recoverable evidence
retroactively. In particular, the original `m42-once` remains UNCERTAIN. Keep
all old journals as evidence. This milestone adds no pruning or bypass for
uncertain tasks and does not clear a drained fleet's admission fence.

## Scope and limits

- The helper, marker and artifacts live in the same VM. A task that controls
  that VM user can modify or forge its evidence. SHA-256 provides binding and
  accidental-corruption detection, **not cryptographic VM attestation**.
- A marker proves an observed shell exit and observed output/artifact bytes.
  It does not prove third-party transactions committed, prevent future artifact
  changes, or guarantee exactly-once external effects. Background work may
  outlive a shell; do not treat shell exit as independent business completion.
- Artifact bytes are verified in the VM and re-read by the host; the journal
  retains their manifest, not a durable off-VM copy of the artifact contents.
  Artifacts must remain unchanged until verification/recovery completes.
- VM deletion or expiry can remove the only recoverable marker. A killed or
  timed-out helper might never publish a marker. Cancellation does not undo
  effects. There is no detached task resurrection or automatic rerun.
- This remains a trusted-operator, single-host Unix-socket API. No new cloud
  services, SDK/runtime upgrades, public listeners, distributed workers or
  tenant-facing auth are introduced. The existing nondurable `fleetctl exec`
  path remains available for careful operator inspection.

## Build and run one bounded live experiment

These steps use a new fleet journal. Fetch and build the M4.3 branch first:

```sh
git fetch origin
git switch --track origin/feat/m4-3-completion-evidence
CGO_ENABLED=1 go test -race -count=1 ./...
go vet ./...
go build -o /tmp/agentlab-m43-fleetd ./cmd/e2b-fleetd
go build -o /tmp/agentlab-m43-fleetctl ./cmd/e2b-fleetctl
go build -o /tmp/agentlab-m43-taskctl ./cmd/e2b-taskctl
go build -o /tmp/agentlab-fleet-cleanup ./cmd/e2b-fleet-cleanup
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -buildvcs=false -trimpath \
  -ldflags='-s -w' -o /tmp/agentlab-m43-task-agent ./cmd/e2b-task-agent
```

**Terminal 1:** Load the existing E2B credentials in this terminal only. If an
older test daemon is running, stop that daemon with Ctrl+C first so its journal
lock is released. Then clean up the M4.2 verification fleet directly:

```sh
cd ~/e2b
eval "$(sudo docker compose exec -T ready cat /run/e2b/sdk.env)"
cd ~/src/agent-runtime-lab
/tmp/agentlab-fleet-cleanup --execute \
  --state-file .state/m4-2-verify.json --timeout 120s
```

Confirm `complete: true`, all selected jobs DELETED and CPU/RAM/pending zero.
This cleanup works even if the old daemon/socket is absent or stale. Retain
the old journals; do not reuse them. If cleanup reports unresolved work, inspect
that report before proceeding. See [daemon-independent cleanup](fleet-offline-cleanup.md).

Now start fleetd with a fresh journal and socket. The helper is a local build
artifact, never a downloaded precompiled program. It is read once at startup
and uploaded only to owned VMs.

```sh
/tmp/agentlab-m43-fleetd --execute \
  --state-file .state/m4-3-fleet.json \
  --socket .state/m4-3-fleet.sock \
  --task-helper /tmp/agentlab-m43-task-agent \
  --interval 2s --ttl 300 --audit-interval 30s --audit-timeout 15s \
  --renew-ttl --renew-interval 60s --exec-proxy http://127.0.0.1:3002
```

**Terminal 2:** Create a disposable VM and wait for `running`.

```sh
cd ~/src/agent-runtime-lab
/tmp/agentlab-m43-fleetctl --socket .state/m4-3-fleet.sock \
  --execute --id taskvm43 --tenant alpha submit
/tmp/agentlab-m43-fleetctl --socket .state/m4-3-fleet.sock status
```

Then submit a task with a declared artifact:

```sh
/tmp/agentlab-m43-taskctl --socket .state/m4-3-fleet.sock \
  --execute --evidence --task-id m43-once --id taskvm43 \
  --artifact /tmp/m43-output.txt \
  --command 'echo marker >> /tmp/m43-output.txt; wc -l /tmp/m43-output.txt' submit
```

Expected: `state=succeeded`, exit 0, stdout containing `1`, a nonempty
`result_sha256`, `evidence_intent` with nonce/helper hash, and `evidence`
containing the completion manifest and the output file's size/SHA-256.

Repeat the **exact same submission**. Its timestamps, result hash and manifest
must remain unchanged. Read the side effect without appending:

```sh
/tmp/agentlab-m43-fleetctl --socket .state/m4-3-fleet.sock \
  --execute --id taskvm43 --command 'wc -l /tmp/m43-output.txt' exec
```

Expected count: **1**. Changing command, timeout, evidence mode or artifact list
under `m43-once` returns an explicit HTTP 409 conflict.

Verify known failure handling with evidence:

```sh
/tmp/agentlab-m43-taskctl --socket .state/m4-3-fleet.sock \
  --execute --evidence --task-id m43-fail --id taskvm43 \
  --command 'echo BEFORE_EXIT; exit 17' submit
/tmp/agentlab-m43-taskctl --socket .state/m4-3-fleet.sock \
  --task-id m43-fail status
```

Expect FAILED with exit 17 and captured output. The CLI's nonzero exit is
intentional. It must not become UNCERTAIN merely because the command failed.

## Controlled daemon-crash recovery check

Use the [deterministic crash-recovery proof](m4-crash-proof.md). The previous
manual SIGSTOP/SIGKILL timing procedure did not establish the intended window
in the operator run: its task was already durably SUCCEEDED before inspection.
That is stored-result retrieval, not UNCERTAIN recovery.

The explicit test-only flag `--test-crash-after-evidence-task TASK_ID` exits
fleetd with code 86 after that named task's VM evidence has been verified,
before the host result is committed. It is off by default, requires the
configured helper, and must be omitted when restarting the normal daemon.
The runbook includes credential loading, a 30-minute TTL, an immediate normal
restart on the same journals with a fresh socket, and complete commands.

## Cleanup and verification status

Always drain the disposable fleet before stopping fleetd:

```sh
/tmp/agentlab-m43-fleetctl --socket .state/m4-3-fleet.sock \
  --execute --drain-timeout 120s drain
/tmp/agentlab-m43-fleetctl --socket .state/m4-3-fleet.sock status
```

Confirm DELETED and zero CPU/RAM/pending, then Ctrl+C in Terminal 1. Preserve
the journals. A drained journal cannot be used for another new workload.

If fleetd has already stopped, use `/tmp/agentlab-fleet-cleanup --execute
--state-file .state/m4-3-fleet.json --timeout 120s` in the credential-loaded
shell. That path needs no socket and preserves the task journal.

Automated coverage includes helper execution/nonce reservation, output bounds,
failed process exits, file bounds/symlinks, journal migration and crash recovery,
request binding, caller-mutation protection, lost terminal responses followed
by GET-only evidence recovery, stale nonce rejection, changed/missing/oversized
evidence, explicit recovery fencing and concurrent drain. The sandbox helper is
also cross-compiled in CI. Real E2B upload/marker/crash compatibility remains
pending until the operator supplies the above live output; deterministic HTTP
fixtures are not reported as a live provider test.
