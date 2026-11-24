# M4.4: asynchronous agent jobs and durable artifact storage

A job submission now returns HTTP 202 after durable queue acceptance. A bounded
worker pool executes evidence-backed sandbox commands independently of the
submitting connection. Jobs use an existing owned RUNNING workload; they share
the task journal and per-VM execution fence with synchronous durable tasks.
An agent process can be the submitted command; model selection and a model/tool
loop remain workload concerns.
Host artifact copying applies to the new jobs API; existing synchronous tasks
retain their M4.3 evidence semantics.

## Behavior

| Trigger | Result |
| --- | --- |
| New job ID | Persist command and immutable VM/request/evidence binding; return QUEUED immediately |
| Identical submission replay | Return the saved job; never execute again |
| Changed input under an existing ID | HTTP 409 |
| Queued job after daemon restart | Eligible for dispatch with its original identity and matching helper |
| Interrupted RUNNING job | UNCERTAIN; never automatically replayed; same-VM queue stays held |
| Queued cancellation | CANCELLED, command removed before dispatch |
| Running cancellation or timeout | Cancel request/context; unknown remote outcome remains UNCERTAIN |
| Verified process exit and committed host bundle | SUCCEEDED for exit 0; FAILED for a known nonzero exit |
| Lost artifact copy or unknown command completion | UNCERTAIN; inspect and explicitly recover |
| Recovery with valid host bundle | Resolve using host bytes, even after the VM has been deleted |
| Recovery without host bundle | Inspect original owned healthy VM, copy verified bytes, then save completion |
| Corrupt host bundle | Fail closed; do not silently replace it from the VM |
| Fleet drain | Fence admission, cancel queued jobs and request running-job cancellation |

The bounded FIFO queue uses persisted sequence numbers, so wall-clock changes
cannot reorder accepted work. It skips VMs already held by RUNNING or UNCERTAIN tasks.
The list API/CLI reports state counts, worker count, storage used/reserved/quota
and each queued job's hold reason (paused workers, changed helper, unready/fenced
workload, busy/uncertain VM, or waiting for a worker).
Different eligible VMs can use different workers; one VM runs only one durable
task/job at a time. A paused workload holds its queue until it resumes. A deleted
or replaced VM causes never-dispatched jobs to be cancelled rather than rebound.
The legacy diagnostic exec endpoint is still an explicit operator tool and does
not participate in the durable task/job fence.

## Persistence and limits

Version 3 of the existing private task journal adds QUEUED/CANCELLED records,
dispatch timestamps, cancellation requests and durable artifact receipts.
Versions 1 and 2 migrate while preserving existing task IDs/results. Earlier
daemons refuse the new version. Use backups rather than downgrading a journal.
The existing bound remains 128 total task/job records, including history.

Raw queued commands must be persisted to resume queued work. They are stored
only in the private 0600 checkpoint under a 0700 directory, omitted from every
API/summary, and removed from the current checkpoint when claimed or cancelled.
This is not secure erasure of filesystem history. Avoid literal secrets in
commands. Once dispatch is persisted, a crash cannot authorize replay.

Each job has an immutable host bundle: the verified VM completion envelope and
every declared artifact's bytes. Names are hashes of job IDs; artifact filenames
are numeric indices, never user-supplied host paths. Files are 0600 and
directories 0700. The store refuses symlink files/directories, checks hashes on
download/recovery, and takes an exclusive lock. It stages and fsyncs all files,
syncs the directory, atomically publishes the whole bundle, then syncs its
parent before a terminal result is persisted. New nested root directory entries
are synced independently of the journal. If the final sync is unconfirmed,
storage is poisoned until restart/inspection; no completion is invented.
Startup verifies all existing completed-job receipts against their host bundles;
pointing an existing journal at an empty/damaged artifact directory fails early.

Default storage: `<task-state-file>.artifacts`; quota 128 MiB, configurable
1..1024 MiB. Concurrent copies reserve space before reading provider files.
At most 8 artifacts, 1 MiB each; output is at most 64 KiB and manifests 512 KiB.
Existing bundles and crash-left staging files count against quota on restart.
There is no automatic artifact deletion/expiry. A host disk backup or object
storage replication is needed to survive host/disk loss. VM deletion alone
cannot remove committed host bundles.

Downloads use `GET /v1/jobs/ID/artifacts/INDEX`, with SHA-256 headers. The CLI
verifies bytes and creates a private new output file atomically without
overwriting an existing file. Artifact indices follow sorted declared paths.

## Build and automatic verification

The existing local Embed API/proxy and Compose project at `~/e2b` must be up.
No new runtimes or services are installed. Run the complete block on the server:

~~~bash
bash <<'VERIFY'
set -euo pipefail
cd "$HOME/src/agent-runtime-lab"
git fetch origin
git switch feat/m4-4-async-jobs
git pull --ff-only origin feat/m4-4-async-jobs
cd "$HOME/e2b"
agentlab_sdk_environment="$(sudo -n docker compose exec -T ready cat /run/e2b/sdk.env)"
eval "$agentlab_sdk_environment"
unset agentlab_sdk_environment
: "${E2B_API_KEY:?E2B credentials were not loaded}"
export E2B_API_KEY
cd "$HOME/src/agent-runtime-lab"
./scripts/verify.sh e2b
VERIFY
~~~

This runs formatting, vet, unit/race tests, the M4.3 live gate and the M4.4 live
gate. Tests start/stop their own daemons and clean up only their own VM journals.
They preserve journals, artifact bundles, daemon logs and cleanup reports.
See [the recorded runsc verification blocker](verification-status.md).

## Manual use with complete commands

Build source binaries:

~~~bash
cd ~/src/agent-runtime-lab
source scripts/env.sh
go build -buildvcs=false -o /tmp/agentlab-m44-fleetd ./cmd/e2b-fleetd
go build -buildvcs=false -o /tmp/agentlab-m44-fleetctl ./cmd/e2b-fleetctl
go build -buildvcs=false -o /tmp/agentlab-m44-jobctl ./cmd/e2b-jobctl
go build -buildvcs=false -o /tmp/agentlab-m44-cleanup ./cmd/e2b-fleet-cleanup
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -buildvcs=false -trimpath -o /tmp/agentlab-m44-task-agent ./cmd/e2b-task-agent
~~~

Start a fresh manually managed fleet in terminal A. Reuse this journal on
restart; omit test crash flags in normal operation:

~~~bash
bash <<'DAEMON'
set -euo pipefail
cd "$HOME/e2b"
agentlab_sdk_environment="$(sudo -n docker compose exec -T ready cat /run/e2b/sdk.env)"
eval "$agentlab_sdk_environment"
unset agentlab_sdk_environment
: "${E2B_API_KEY:?E2B credentials were not loaded}"
export E2B_API_KEY
cd "$HOME/src/agent-runtime-lab"
/tmp/agentlab-m44-fleetd --execute \
  --state-file .state/m4-4-fleet.json \
  --socket .state/m4-4-fleet.sock \
  --task-helper /tmp/agentlab-m44-task-agent \
  --job-workers 2 --artifact-max-mib 128 \
  --ttl 1800 --renew-ttl --renew-interval 60s \
  --audit-interval 30s --audit-timeout 15s \
  --exec-proxy http://127.0.0.1:3002
DAEMON
~~~

In terminal B, submit the workload. Wait for its status to show RUNNING before
submitting a job (the automated verifier handles this polling itself):

~~~bash
cd ~/src/agent-runtime-lab
/tmp/agentlab-m44-fleetctl --socket .state/m4-4-fleet.sock \
  --execute --tenant alpha --id agentvm44 submit
/tmp/agentlab-m44-fleetctl --socket .state/m4-4-fleet.sock status
~~~

Submit once; wait only polls status and never resubmits:

~~~bash
cd ~/src/agent-runtime-lab
/tmp/agentlab-m44-jobctl --socket .state/m4-4-fleet.sock \
  --execute --job-id agentjob44 --id agentvm44 \
  --artifact /tmp/m44-output.txt \
  --command 'echo marker >> /tmp/m44-output.txt; wc -l /tmp/m44-output.txt' submit
/tmp/agentlab-m44-jobctl --socket .state/m4-4-fleet.sock \
  --job-id agentjob44 --wait-timeout 120s wait
/tmp/agentlab-m44-jobctl --socket .state/m4-4-fleet.sock \
  --job-id agentjob44 artifacts
/tmp/agentlab-m44-jobctl --socket .state/m4-4-fleet.sock \
  --job-id agentjob44 --index 0 --out /tmp/m44-downloaded-output.txt download
~~~

Explicit cancellation and recovery are separate operations:

~~~bash
cd ~/src/agent-runtime-lab
/tmp/agentlab-m44-jobctl --socket .state/m4-4-fleet.sock \
  --execute --job-id agentjob44 cancel
/tmp/agentlab-m44-jobctl --socket .state/m4-4-fleet.sock \
  --job-id agentjob44 status
/tmp/agentlab-m44-jobctl --socket .state/m4-4-fleet.sock \
  --execute --job-id agentjob44 recover
~~~

For cleanup while the daemon is available:

~~~bash
cd ~/src/agent-runtime-lab
/tmp/agentlab-m44-fleetctl --socket .state/m4-4-fleet.sock \
  --execute --drain-timeout 120s drain
~~~

After the daemon stops, use daemon-independent cleanup with credentials loaded
in that shell. It preserves job journals and artifacts:

~~~bash
bash <<'CLEANUP'
set -euo pipefail
cd "$HOME/e2b"
agentlab_sdk_environment="$(sudo -n docker compose exec -T ready cat /run/e2b/sdk.env)"
eval "$agentlab_sdk_environment"
unset agentlab_sdk_environment
: "${E2B_API_KEY:?E2B credentials were not loaded}"
export E2B_API_KEY
cd "$HOME/src/agent-runtime-lab"
/tmp/agentlab-m44-cleanup --execute --state-file .state/m4-4-fleet.json --timeout 120s
CLEANUP
~~~

`--job-workers 0` pauses dispatch without discarding queued jobs and is useful
for read-only artifact serving/recovery. The test-only flag
`--test-crash-after-artifacts-job ID` exits 87 after the named job's bundle is
durable but before completion is saved. It is off by default. The live gate
uses it automatically, then deletes the VM before recovering from local bytes.

This is a private trusted-operator single-host service. It does not provide
public tenant authentication, distributed leases or automatic retry of effects.
