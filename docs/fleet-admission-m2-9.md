# M2.9 — Crash-safe single-host E2B fleet admission

This milestone connects the M2.8 CPU/RAM scheduler to E2B's real sandbox
Create, Observe, Pause, Resume, and Delete APIs. It reuses the existing
`admission.Scheduler` and persists the *entire* scheduling checkpoint alongside
each sandbox's identity and desired state before remote mutations. This is a
single-host learning lab, **not a distributed cluster scheduler**.

## Invariants

1. A `Submit` is queued; no E2B Create occurs until `tick` reserves enough
   host **and tenant** CPU/RAM. Requests use the one configured template's
   conservative reservation, not an arbitrary client-selected size.
2. The reservation and `create_pending=true` are atomically fsynced **before**
   `POST /sandboxes`. If the controller is killed or the Create reply is lost,
   it cannot repeat Create. Metadata recovery adopts exactly one matching VM.
3. Resuming a paused VM must first reacquire an active reservation. The logical
   tenant quota remains occupied while paused. Resume failures keep capacity
   reserved until reconciliation.
4. Pause frees CPU/RAM **only after GET reports PAUSED**. Delete releases
   resources and the logical slot **only after GET reports ABSENT**. Successful
   HTTP acknowledgments do not directly free resources.
5. The first observation after Create checks `cpuCount` and `memoryMB` from
   E2B against the conservative reservation. An unexpectedly larger or
   malformed VM marks the journal `unsafe=true`, preventing further admissions.
   Explicit deletion remains available to clean up an oversize VM.
6. An external disappearance while desired RUNNING fails closed, retaining the
   reservation until the operator requests deletion and verifies absence.
7. An exclusive Linux `flock` prevents simultaneous CLI controller processes
   from mutating a state file. State is written by atomic rename, fsync of the
   file, and fsync of its directory; corruption and inconsistent accounting
   are rejected on restart.

## Verification without live E2B

```sh
cd ~/src/agent-runtime-lab
CGO_ENABLED=1 go test -race -count=1 ./...
go vet ./...
```

The new tests use a fake E2B runtime and cover restart, fair admission,
conservative pause/delete accounting, lost Create and Pause replies, exact
metadata recovery, refusal of corrupted reservations, an exclusive state lock,
and a template that consumes more resources than reserved.

## Live E2B experiment (opt-in)

**Important:** each successful `tick` can create live Firecracker sandboxes.
The CLI does **not** automatically delete them when it exits. Reserve a fresh
state path, and explicitly delete every VM afterward. The default E2B TTL is
900 seconds. Do not run the experiment if you cannot complete cleanup before
expiry; a VM may otherwise expire and require manual reconciliation.

```sh
cd ~/e2b
eval "$(sudo docker compose exec -T ready cat /run/e2b/sdk.env)"

cd ~/src/agent-runtime-lab
STATE=.state/m2-9-experiment-01.json

# Two tenants. Host admission budget=4 vCPU/4096 MiB; every VM reserves
# an upper bound of 2 vCPU/2048 MiB until actual E2B shape is inspected.
go run ./cmd/e2b-fleet --state-file "$STATE" --execute --id a1 --tenant alpha submit
go run ./cmd/e2b-fleet --state-file "$STATE" --execute --id b1 --tenant beta submit
go run ./cmd/e2b-fleet --state-file "$STATE" --execute tick

# Third VM must stay queued while two reservation slots are in use.
go run ./cmd/e2b-fleet --state-file "$STATE" --execute --id a2 --tenant alpha submit
go run ./cmd/e2b-fleet --state-file "$STATE" --execute tick
go run ./cmd/e2b-fleet --state-file "$STATE" status

# Pause a1; confirmed PAUSED frees one active reservation.
go run ./cmd/e2b-fleet --state-file "$STATE" --execute --id a1 pause
go run ./cmd/e2b-fleet --state-file "$STATE" --execute tick
go run ./cmd/e2b-fleet --state-file "$STATE" status

# Resume a1 must queue while a2 and b1 occupy all active reservations.
go run ./cmd/e2b-fleet --state-file "$STATE" --execute --id a1 resume
go run ./cmd/e2b-fleet --state-file "$STATE" --execute tick

# Delete b1, observe absence, then readmit a1 for resume.
go run ./cmd/e2b-fleet --state-file "$STATE" --execute --id b1 delete
go run ./cmd/e2b-fleet --state-file "$STATE" --execute tick
go run ./cmd/e2b-fleet --state-file "$STATE" status

# Cleanup ALL surviving VMs; rerun tick until deleted entries are confirmed.
go run ./cmd/e2b-fleet --state-file "$STATE" --execute --id a1 delete
go run ./cmd/e2b-fleet --state-file "$STATE" --execute --id a2 delete
go run ./cmd/e2b-fleet --state-file "$STATE" --execute tick
go run ./cmd/e2b-fleet --state-file "$STATE" status
```

The commands intentionally start separate controller processes. A state file
cannot be safely deleted or reused while it may still reference a VM.

## Ambiguous Create recovery

If `tick` returns `Create outcome uncertain` or the controller is killed
after a reservation but before persisting the ID:

```sh
go run ./cmd/e2b-fleet --state-file "$STATE" status
go run ./cmd/e2b-fleet --state-file "$STATE" --execute --id JOB_ID recover
go run ./cmd/e2b-fleet --state-file "$STATE" --execute tick
```

`recover` requires exactly one remote sandbox with the stored logical-key
metadata. Zero or multiple matches are **not evidence that creation failed**:
leave the reservation intact and investigate before taking further action.

## Scope and caveats

- **Single host:** the journal and `flock` are not replicated or suitable for
  NFS. This is not multi-node placement, leader election or distributed fencing.
- **Conservative admission:** CPU and memory budgets are reservations. Actual
  Firecracker template resources are checked after creation; the API does not
  permit arbitrary resource sizing in `POST /sandboxes`. Misconfigured
  reservations can still cause physical overcommit at the instant a VM is
  created, so use conservative sizes and a small test fleet.
- **Not physical RAM reclamation:** pausing a microVM does not prove the host
  huge-page pool was returned to ordinary system RAM, and paused snapshots
  still consume storage.
- **No global quota enforcement:** other controllers or E2B API clients are not
  visible to this scheduler. The host budget applies only to this fleet journal.
- **No crash-proof exactly-once effects:** recovery relies on provider metadata
  and may require manual intervention. A lost VM due to TTL expiry remains
  fenced until the operator explicitly deletes its logical record.
- **No autonomous event loop:** `tick` is user-triggered, not a background
  controller; there is no guaranteed service uptime, preemption, persistence
  across different hosts, or external admission enforcement.
- The running E2B runtime and Python SDK 2.46.0 remain unchanged. All Go code
  uses the standard library plus this repository's existing packages.
