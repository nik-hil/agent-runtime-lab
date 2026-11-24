# Repository release gate

M4.3 completes the current single-host VM execution foundation. It does not
complete a multi-node agent platform.

| Area | Implemented in this branch |
| --- | --- |
| Isolation experiments | Docker/runc and gVisor/runsc contracts and experiments |
| VM ownership | Durable create/pause/resume/delete intents, restart recovery |
| Admission | CPU/RAM reservations, tenant limits, bounded fair pending queue |
| Operations | Reconciliation, backoff, private socket, metrics, inventory audit, drift fence, opt-in TTL renewal |
| Commands | Bounded synchronous execution, exit codes and captured output |
| Durable tasks | Stable task IDs, persisted intent/results, replay conflicts, uncertain-outcome fence |
| Completion evidence | VM helper, bound manifest/output/artifact hashes, explicit recovery without reexecution |
| Cleanup | Explicit live drain and daemon-independent journal cleanup |
| Still ahead | Actual host telemetry; asynchronous task scheduling/cancellation; agent/model loop and tools; multi-node durable ownership, leases/fencing, worker failure recovery; public tenant API/authentication |

Reservations are configured promises, not measured host utilization. Local
flock journals provide single-process ownership, not distributed leadership.
Evidence hashes check observed bytes; they do not attest a hostile VM or
guarantee exactly-once changes in external systems.

## One command per verification scope

Run these commands on the existing Linux lab server. No new services, Go SDKs,
runtimes or E2B versions are installed. The current E2B Compose project must
already be running at ~/e2b, with API/proxy ports 3000/3002 and working credentials.

Load credentials in the shell that will run verification; do not print them:

~~~bash
cd ~/e2b
eval "$(sudo docker compose exec -T ready cat /run/e2b/sdk.env)"
test -n "${E2B_API_KEY:-}" || { echo "E2B credentials were not loaded"; exit 1; }
cd ~/src/agent-runtime-lab
git switch feat/m4-3-completion-evidence
git pull --ff-only origin feat/m4-3-completion-evidence
~~~

For every implemented code path's automated checks, including both installed
container runtimes and the live E2B gate:

~~~bash
cd ~/src/agent-runtime-lab
./scripts/verify.sh all
~~~

If only verifying the E2B milestones on an Embed-only host, use:

~~~bash
cd ~/src/agent-runtime-lab
./scripts/verify.sh e2b
~~~

The e2b mode runs formatting, vet, all unit/race tests, and live E2B verification.
It does not claim Docker/gVisor live validation. The all mode additionally
builds the source probe image and requires both Docker/runc and gVisor/runsc;
missing infrastructure fails instead of producing a misleading pass.
The unit mode needs no E2B credentials and skips all real-runtime tests.

The all mode checks Docker access before running Go checks. If direct access
fails but the existing passwordless `sudo -n docker` route works, it creates a
private Docker CLI wrapper for this run. Builds and Go test subprocesses inherit
that wrapper through PATH. Verification and journals remain under the invoking
user; only Docker commands use sudo. It never requests a password, changes group
membership, or changes sudoers. If both routes fail, verification stops before
running the checks. The e2b and unit modes do not require host Docker access.

## What the live E2B gate does

TestE2BFullLifecycle builds fleetd and a static Linux/amd64 task helper from
this checkout. It owns a fresh private journal and its own daemon processes.
It creates at most two VM identities and reserves only one active 2 CPU /
2048 MiB VM at a time. A paused VM may retain provider snapshot/storage costs.

The test checks, in sequence:

1. Admission accounting, host-capacity queueing, and logical tenant quota.
2. Pause, capacity release, queued dispatch, resume queueing, and stable VM identity.
3. Successful commands and nonzero process exit results.
4. Successful evidence, artifact bytes, cached submission replay, and ID conflicts.
5. Persisted failed task evidence and output.
6. Inventory audit, TTL renewal, and private monitor metrics.
7. Named daemon exit 86 after VM evidence verification, before host result persistence.
8. Restart with the same journal: UNCERTAIN task, unchanged replay, blocked new task.
9. Missing/corrupt completion marker and missing/changed artifact rejection.
10. Explicit evidence recovery, unchanged repeated recovery, and marker count exactly one.
11. Timeout remaining UNCERTAIN, blocking later tasks on that VM.
12. Daemon-independent cleanup, zero reservations, and independent provider absence
    observations for every VM ID seen by the test.

Mutation requests are sent once. Polling repeats read-only observations.
The timeout assertion does not claim remote cancellation or rollback.
The named crash is deliberate and scoped to the test task.

Cleanup runs after normal completion and assertion failures. The test stops only
its own daemon, drains only its own journal, and leaves private journals,
daemon logs and cleanup.json under .state/verify-*. Test failure retains enough
state to investigate and run standalone cleanup again. A killed test process
or power failure cannot run Go cleanup callbacks; use its printed journal path.

Shell verification logs are saved separately under results/verify-*. Both
directories are private. A failing test, incomplete cleanup, vet error or
formatting failure makes the command exit nonzero. Read the complete go test
result, including parent test cleanup, before treating subtest passes as a pass.

## Existing proof fleet

The automated gate uses a fresh journal; it does not drain older fleets.
Stop the manually running proof daemon with Ctrl+C in its terminal, then
clean only the known proof journal before starting the new gate:

~~~bash
cd ~/src/agent-runtime-lab
source scripts/env.sh
go build -buildvcs=false -o /tmp/agentlab-m43-fleet-cleanup ./cmd/e2b-fleet-cleanup
/tmp/agentlab-m43-fleet-cleanup --execute \
  --state-file .state/m4-3-proof.json \
  --timeout 120s
~~~

This requires E2B credentials in the cleanup shell. Do not remove journals or
sockets to bypass the exclusive owner lock. A historical unsafe flag may remain
latched after complete deletion; complete=true and zero reservations describe
cleanup of that selected journal.

## Validation status

The operator demonstrated a real named crash for m43-proof, an UNCERTAIN
host record after restart, explicit verified recovery, and a separate wc
observation of exactly one marker. This validates that recovery scenario.

The new consolidated gate must also pass on the user's live server before it
can be called live-verified. Default GitHub CI has no local E2B server: it
compiles this test, runs unit/race tests and vet, checks formatting, and builds
the static helper. Skipped live tests are never evidence of a live pass.

A passing gate covers the implemented foundation and its regression cases.
It is not a load/capacity benchmark, hostile-workload security certification,
or validation of the future distributed platform.
