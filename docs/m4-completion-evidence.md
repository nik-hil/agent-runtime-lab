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

These steps use a new fleet journal. First finish and drain any earlier test
daemon. For the M4.2 verification fleet from the previous run:

```sh
cd ~/src/agent-runtime-lab
/tmp/agentlab-m42-fleetctl --socket .state/m4-2-verify.sock \
  --execute --drain-timeout 120s drain
/tmp/agentlab-m42-fleetctl --socket .state/m4-2-verify.sock status
```

Confirm all old journal workloads DELETED and CPU/RAM/pending zero before
Ctrl+C in that daemon's terminal. Retain its journals; do not reuse them.

Fetch and build the M4.3 branch:

```sh
git fetch origin
git switch --track origin/feat/m4-3-completion-evidence
CGO_ENABLED=1 go test -race -count=1 ./...
go vet ./...
go build -o /tmp/agentlab-m43-fleetd ./cmd/e2b-fleetd
go build -o /tmp/agentlab-m43-fleetctl ./cmd/e2b-fleetctl
go build -o /tmp/agentlab-m43-taskctl ./cmd/e2b-taskctl
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -buildvcs=false -trimpath \
  -ldflags='-s -w' -o /tmp/agentlab-m43-task-agent ./cmd/e2b-task-agent
```

**Terminal 1:** Load the existing E2B credentials in this terminal only, then
start fleetd. The helper path is a local build artifact, never a downloaded
precompiled program. It is read once at startup and uploaded only to owned VMs.

```sh
cd ~/e2b
eval "$(sudo docker compose exec -T ready cat /run/e2b/sdk.env)"
cd ~/src/agent-runtime-lab
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

This optional live check needs the daemon shell and an operator shell. The
local process pause creates a window in which the VM can finish while fleetd
cannot consume the terminal response or persist it. Do not pause E2B, Docker,
the VM, or any other process. Use only the identified disposable fleetd PID.

In Terminal 2, start the task in the background so that the shell stays usable:

```sh
/tmp/agentlab-m43-taskctl --socket .state/m4-3-fleet.sock \
  --execute --evidence --task-id m43-crash --id taskvm43 --timeout 60 \
  --artifact /tmp/m43-crash.txt \
  --command 'echo marker >> /tmp/m43-crash.txt; sleep 40; wc -l /tmp/m43-crash.txt' \
  submit > /tmp/m43-crash-submit.log 2>&1 &
```

Before the 40-second sleep ends, confirm that the VM command actually started:

```sh
/tmp/agentlab-m43-fleetctl --socket .state/m4-3-fleet.sock \
  --execute --id taskvm43 --command 'test -f /tmp/m43-crash.txt' exec
pgrep -af /tmp/agentlab-m43-fleetd
```

If the file test has not succeeded, wait briefly and inspect again; do not infer
that the original command failed. Once it succeeds, identify the **one correct
fleetd PID** in the process listing. Substitute it for `FLEETD_PID` below:

```sh
kill -STOP FLEETD_PID
sleep 45
kill -KILL FLEETD_PID
```

The 45-second wait is intentionally bounded and allows the VM's 40-second
command to finish while the local daemon is frozen. This does not guarantee
the helper survived every provider-specific stream lifecycle behaviour; an
absent marker must remain UNCERTAIN, even in this experiment.

Confirm no listener remains with `ss -xlpn` before removing **only** the stale
`.state/m4-3-fleet.sock` inode. Never remove the fleet/task journals or locks.
Restart fleetd in Terminal 1 with the **same command and journals**. Then:

```sh
/tmp/agentlab-m43-taskctl --socket .state/m4-3-fleet.sock \
  --task-id m43-crash status
/tmp/agentlab-m43-taskctl --socket .state/m4-3-fleet.sock \
  --execute --task-id m43-crash recover
/tmp/agentlab-m43-fleetctl --socket .state/m4-3-fleet.sock \
  --execute --id taskvm43 --command 'wc -l /tmp/m43-crash.txt' exec
```

Expected: startup shows UNCERTAIN; `recover` verifies the marker and artifact,
records SUCCEEDED with a note saying no rerun, and the count remains **1**.
An ordinary repeated submission must never implicitly recover or rerun an
uncertain task. A missing/modified marker or artifact returns 409 and retains
UNCERTAIN. Do not bypass that by editing the journal or changing task IDs.

## Cleanup and verification status

Always drain the disposable fleet before stopping fleetd:

```sh
/tmp/agentlab-m43-fleetctl --socket .state/m4-3-fleet.sock \
  --execute --drain-timeout 120s drain
/tmp/agentlab-m43-fleetctl --socket .state/m4-3-fleet.sock status
```

Confirm DELETED and zero CPU/RAM/pending, then Ctrl+C in Terminal 1. Preserve
the journals. A drained journal cannot be used for another new workload.

Automated coverage includes helper execution/nonce reservation, output bounds,
failed process exits, file bounds/symlinks, journal migration and crash recovery,
request binding, caller-mutation protection, lost terminal responses followed
by GET-only evidence recovery, stale nonce rejection, changed/missing/oversized
evidence, explicit recovery fencing and concurrent drain. The sandbox helper is
also cross-compiled in CI. Real E2B upload/marker/crash compatibility remains
pending until the operator supplies the above live output; deterministic HTTP
fixtures are not reported as a live provider test.
