# Repository release gate

M4.4 adds asynchronous jobs and host-durable artifacts. M5 adds distributed
placement, fenced ownership, conservative failover and replicated artifacts.
Use the [M5 complete command block](m5-multi-server.md#one-complete-release-gate)
to verify the entire Embed-based implementation on the existing server.

| Area | Implemented in this branch |
| --- | --- |
| Isolation experiments | Docker/runc and gVisor/runsc contracts and experiments |
| VM ownership | Durable create/pause/resume/delete intents, restart recovery |
| Admission | CPU/RAM reservations, tenant limits, bounded fair pending queue |
| Operations | Reconciliation, backoff, private socket, metrics, inventory audit, drift fence, opt-in TTL renewal |
| Commands | Bounded synchronous execution, exit codes and captured output |
| Durable tasks | Stable task IDs, persisted intent/results, replay conflicts, uncertain-outcome fence |
| Completion evidence | VM helper, bound manifest/output/artifact hashes, explicit recovery without reexecution |
| Async jobs | Durable queue, bounded workers, cancellation, shared VM fence, restart-safe dispatch |
| Durable artifacts | Fsynced host bundles, verified downloads and recovery after VM deletion |
| Cleanup | Explicit live drain and daemon-independent journal cleanup |
| Multi-server | etcd-backed placement, global tenant quotas, coordinator/node leases, ownership epochs, durable node intents and safe pending reassignment |
| Artifact failover | Verified copies on distinct node agents, surviving-replica downloads and repair |
| Still ahead | Measured host telemetry, physical failure-domain deployment validation, production PKI lifecycle, public tenant API/authentication |

Reservations are configured promises, not measured host utilization. Local
flock journals provide local process ownership; M5 adds etcd lease and CAS
fencing for distributed leadership and dispatch.
Evidence hashes check observed bytes; they do not attest a hostile VM or
guarantee exactly-once changes in external systems.

## One command per verification scope

Run these commands on the existing Linux lab server. No system services, Go SDKs,
runtimes or E2B versions are installed. M5 explicitly starts isolated etcd test
containers and removes only those selected containers. The current E2B Compose project must
already be running at ~/e2b, with API/proxy ports 3000/3002 and working credentials.

Load credentials in the shell that will run verification; do not print them:

~~~bash
sdk_env="$(sudo -n docker compose --project-directory "$HOME/e2b" exec -T ready cat /run/e2b/sdk.env)" || exit 1
eval "$sdk_env"
unset sdk_env
test -n "${E2B_API_KEY:-}" || { echo "E2B credentials were not loaded"; exit 1; }
cd ~/src/agent-runtime-lab
git fetch origin feat/m5-multi-server
git switch feat/m5-multi-server
git pull --ff-only origin feat/m5-multi-server
~~~

For every implemented code path's automated checks, including both installed
container runtimes, the live E2B gates and M5 cluster gates:

~~~bash
cd ~/src/agent-runtime-lab
./scripts/verify.sh all
~~~

If only verifying the E2B milestones on an Embed-only host, use:

~~~bash
cd ~/src/agent-runtime-lab
./scripts/verify.sh e2b
~~~

For M4 plus the whole M5 implementation without the separate gVisor requirement:

~~~bash
cd ~/src/agent-runtime-lab
./scripts/verify.sh cluster
~~~

For real three-member etcd and TLS protocol tests without E2B credentials:

~~~bash
cd ~/src/agent-runtime-lab
./scripts/verify.sh quorum
~~~

The e2b mode runs formatting, vet, all unit/race tests, and live E2B verification.
It now includes both TestE2BFullLifecycle and TestE2BAsyncJobs. The M4.4 gate
recovers a genuinely uncertain host record from committed artifact bytes after
deleting the original VM. See [M4.4 commands and semantics](m4-async-jobs.md).
The server's missing runsc registration is recorded in
[verification status](verification-status.md); it is not a full all-mode pass.
It does not claim Docker/gVisor live validation. The all mode additionally
builds the source probe image and requires both Docker/runc and gVisor/runsc;
missing infrastructure fails instead of producing a misleading pass.
The unit mode needs no E2B credentials and skips all real-runtime tests.

The all, cluster and quorum modes check Docker access before Go checks. If direct access
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

The operator subsequently reported the M4.3/M4.4 E2B gate passed, then
confirmed the corrected M5 cluster gate passed on 2026-11-25. The M5 pass
covers single-host process failover and does not establish physical-server HA.
See verification-status.md for the operator report and evidence limits.
Default GitHub CI has no local E2B server: it
compiles this test, runs unit/race tests and vet, checks formatting, and builds
the static helper. Skipped live tests are never evidence of a live pass.

A passing gate covers the implemented foundation and its regression cases.
It is not a load/capacity benchmark, hostile-workload security certification,
or validation of physical-server HA. The M5 CI quorum/protocol gate uses real
etcd/TLS with a synthetic fleet, while the opt-in cluster gate uses real E2B
commands and process crash injection. Neither replaces a physical multi-host
failure test. See [M5 deployment and evidence limits](m5-multi-server.md).
