# M3.3a — Explicit drain with confirmation (single-host E2B)

This stage makes **drain** a separate operator action. It does **not** change
the non-destructive semantics of Ctrl+C / SIGTERM. The controller never
deletes a VM just because its process is shutting down.

The new `POST /v1/drain` endpoint is reachable only over the daemon's private
Unix socket. It serializes with new submissions, refuses submissions and
non-deletion intent changes, and **atomically persists** a durable drain
admission fence plus `desired=deleted` for each active, paused, queued, or uncertain job. The
existing reconciler then observes remote ABSENT before returning reservations.

`fleetctl --execute drain` waits until every workload is *confirmed* deleted
and the journal's CPU, RAM and pending counters all equal zero. It reports
unresolved IDs and exits nonzero on timeout. **Timeout does not cancel or undo
deletion intent; leave the daemon running, investigate, and retry drain.**

`recover` stays enabled while draining, because a lost E2B Create reply may
require explicit metadata recovery before the known sandbox can be deleted.
If E2B cannot prove absence, the journal intentionally retains the reservation.

## Run on the existing Droplet

First fetch the M3.3 PR branch and run tests:

```sh
cd ~/src/agent-runtime-lab
git fetch origin
git switch --track origin/feat/m3-3-explicit-drain
CGO_ENABLED=1 go test -race -count=1 ./...
go vet ./...
```

**Terminal 1 — start the daemon** with a NEW journal and socket. Never reuse
the older M3.2 state path with these experimental commands.

```sh
cd ~/e2b
eval "$(sudo docker compose exec -T ready cat /run/e2b/sdk.env)"
cd ~/src/agent-runtime-lab
go run ./cmd/e2b-fleetd --execute \
  --state-file .state/m3-3-drain.json \
  --socket .state/m3-3-drain.sock \
  --interval 2s --max-backoff 30s \
  2> /tmp/m3-3-daemon.jsonl
```

**Terminal 2 — create two sandboxes and then drain them:**

```sh
cd ~/src/agent-runtime-lab
go run ./cmd/e2b-fleetctl --socket .state/m3-3-drain.sock \
  --execute --id a1 --tenant alpha submit
go run ./cmd/e2b-fleetctl --socket .state/m3-3-drain.sock \
  --execute --id b1 --tenant beta submit

go run ./cmd/e2b-fleetctl --socket .state/m3-3-drain.sock \
  --execute --drain-timeout 120s drain

# Confirm terminal states without dumping every intermediate tick.
go run ./cmd/e2b-fleetctl --socket .state/m3-3-drain.sock status \
  > /tmp/m3-3-final.json
tail -n 30 /tmp/m3-3-daemon.jsonl
```

Expected success output:

```
Drain requested; awaiting observed sandbox deletion ...
PASS: all journal workloads confirmed deleted; CPU=0 MiB=0 pending=0.
```

Now **stop Terminal 1 with Ctrl+C**. It prints an accurate journal summary,
but performs no additional VM mutations on signal handling. Re-run a
read-only local check if desired:

```sh
go run ./cmd/e2b-fleet --state-file .state/m3-3-drain.json status
```

If you want a short output instead of full JSON, you can run:

```sh
go run ./cmd/e2b-fleetctl --socket .state/m3-3-drain.sock metrics \
  | grep -E 'fleetd_admission_(used_cpu|used_memory_mib|pending)|fleetd_workloads'
```

## Failure/recovery tests and caveats

- `go test -race ./src/fleetdaemon ./cmd/e2b-fleetctl` simulates E2B Create
  with a lost response, explicit metadata recovery, and a Delete returning a
  transient provider error. The tests verify retained reservations until
  provider absence is observed. These faults are not injected into your real
  running E2B services.
- A drain launched while VMs are being created may encounter an uncertain
  Create; in that case `fleetctl drain` times out and lists the outstanding ID.
  Run `fleetctl --execute --id <id> recover` and retry drain. Do **not** reset
  or remove the journal to unblock an ambiguous Create.
- Drain's admission fence and per-job delete intents survive daemon restarts.
  A drained journal cannot accept new workloads through the daemon. Use a
  **different, fresh journal** for subsequent experiments. The legacy standalone
  `e2b-fleet` CLI is not a participant in the new daemon admission guard: do
  not use that CLI for mutations against a draining journal.
- Drain waits for **journal-confirmed** deletion; it does not independently
  audit the E2B infrastructure for sandboxes owned by other clients.
- No new cloud instances, network listeners, Go dependencies or system
  services are installed by checking out this PR.
- An explicit drain will delete **all** managed workloads in the selected
  journal; do not point it at a production/customer fleet.
- The default E2B TTL remains 900 seconds; automatic TTL renewal is out of
  scope. Continue using a fresh state file for each new experiment.
