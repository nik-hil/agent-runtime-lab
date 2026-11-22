# M3.1 — Continuously running E2B fleet reconciliation

## Motivation

M2.9 required the operator to run `e2b-fleet tick` after each desired-state
change. A real control plane must apply queued work and converge automatically,
without racing a second process for the same durable state file.

This milestone introduces two Go commands:

- `e2b-fleetd` — **one** owner of the existing M2.9 journal and E2B backend;
  runs bounded reconciliation periodically and when intent changes.
- `e2b-fleetctl` — sends local commands over a Unix-domain HTTP socket. It
  **does not** open the state file and **does not** need the E2B API key.

No services are installed or started automatically. The daemon is launched
manually and remains alive only until you terminate it. Existing E2B
containers, Firecracker settings, Go modules, and SDK versions are unchanged.

## Architecture

```text
     CLI e2b-fleetctl (terminal 2)
           |  HTTP over private Unix socket (mode 0600)
           v
     e2b-fleetd (terminal 1)
       |      |
       |      +-- periodic + event-triggered bounded Tick loop
       v
     fleet.Controller (serialized mutations)
       |                  |
       v                  v
     atomic journal    E2B Embed API
     + flock             /sandboxes
```

`fleet.Controller` still performs at most one remote operation per `Tick`
and retains resource reservations through uncertain results. The journal
stores requests, desired/observed state and CPU/RAM reservations before
remote mutations. Only the daemon has the journal lock. Clients connect to
the local Unix socket and never race a separate journal writer.

## Run: live E2B experiment

First fetch the PR branch and verify it:

```sh
cd ~/src/agent-runtime-lab
git fetch origin
git switch --track origin/feat/m3-1-continuous-reconciler
CGO_ENABLED=1 go test -race -count=1 ./...
go vet ./...
```

### Terminal 1 — start the server

```sh
cd ~/e2b
eval "$(sudo docker compose exec -T ready cat /run/e2b/sdk.env)"
cd ~/src/agent-runtime-lab

go run ./cmd/e2b-fleetd \
  --execute \
  --state-file .state/m3-1.json \
  --socket .state/m3-1.sock \
  --interval 2s \
  --steps 12
```

Leave this process running. Only this terminal needs E2B credentials.

### Terminal 2 — send intent via socket

Use a **second SSH connection** to the same Droplet. The control socket is
local: it does not listen on a TCP port. Use the full Go commands, not a
temporary `fleet` shell function:

```sh
cd ~/src/agent-runtime-lab

go run ./cmd/e2b-fleetctl --execute --id a1 --tenant alpha submit
go run ./cmd/e2b-fleetctl --execute --id b1 --tenant beta submit
go run ./cmd/e2b-fleetctl --execute --id a2 --tenant alpha submit
go run ./cmd/e2b-fleetctl status
```

The automatic worker should admit `a1` and `b1` and hold `a2` as queued.
Use `status` again as necessary. **Do not manually call `tick`**; the daemon
owns reconciliation.

Pause one VM, free resources, and request a queued resume:

```sh
go run ./cmd/e2b-fleetctl --execute --id a1 pause
go run ./cmd/e2b-fleetctl status
go run ./cmd/e2b-fleetctl --execute --id a1 resume
go run ./cmd/e2b-fleetctl status
```

Expected: `a1` is paused before `a2` is admitted; then `a1` becomes
`resume_queued` because `a2` and `b1` occupy the admission budget.

Release capacity, finish the resume, and **clean up**:

```sh
go run ./cmd/e2b-fleetctl --execute --id b1 delete
go run ./cmd/e2b-fleetctl status
go run ./cmd/e2b-fleetctl --execute --id a1 delete
go run ./cmd/e2b-fleetctl --execute --id a2 delete
go run ./cmd/e2b-fleetctl status > /tmp/m3-1-final.json
cat /tmp/m3-1-final.json
```

The final status must show all jobs `deleted`, zero pending work, and zero
active reservations. A status request provides a non-secret fleet snapshot
and the most recent reconciliation error, if any.

Press `Ctrl+C` in terminal 1 **after cleanup**. This stops only the daemon.
The daemon never automatically deletes VMs on shutdown. If the daemon exits
while VMs are alive, restart it with the same journal and socket path promptly,
or explicitly delete the VMs before their TTL expires.

## Crash, restart and errors

- The daemon holds the exclusive M2.9 `flock` for its entire process lifetime.
  The old `e2b-fleet` CLI cannot safely mutate the same journal while the
  daemon runs; it will fail to acquire the lock. Use `e2b-fleetctl` instead.
- SIGINT/SIGTERM cancels work and gracefully shuts down HTTP. A SIGKILL or
  host power loss can leave a stale Unix socket pathname; after verifying
  that **no** daemon owns the state file, remove only that stale socket and
  restart the daemon. Never delete the journal to work around a lock failure.
- If E2B Create succeeds but its response is lost, the journal keeps
  `create_pending: true`, and the worker does **not** blindly retry Create.
  The operator may request `--execute --id <id> recover` over the socket.
  Zero or multiple metadata matches remain unresolved.
- Failed API calls are reported in `/v1/status` as `reconciler.last_error`.
  The worker retries a bounded reconciliation batch on the next interval;
  a persistent failure may require operator intervention. A successful idle
  pass clears the most recent error; always inspect job states too.
- The new socket is created in a private (0700) directory with mode 0600.
  Existing socket paths are **never** overwritten. The daemon is not an
  internet-facing or multi-user API and supplies no TCP listener.
- The E2B TTL remains 900 seconds by default. Restarting this daemon does
  **not** automatically extend TTL for already-running VMs. A VM can expire
  while a controller is offline, which will be surfaced as unexpected loss.

## Not yet production-grade

This is a **single-node, single-process** controller. There is no leader
election, distributed fencing, workload authorizer, priority/preemption,
autoscaling across E2B hosts, durable multi-node queue, metrics service or
centralized log pipeline. HTTP over the Unix socket uses filesystem permissions
for local access control; do not expose that socket to untrusted users or
containers. The daemon does not automatically extend sandbox TTL or guarantee
orphan cleanup on sudden host failure.

Tested with deterministic fake E2B runtimes, simulated ambiguous Create,
daemon restart with the same journal, real Unix HTTP requests, and repeated
Go race tests. **A live daemon experiment on the Droplet is required before
merging this milestone.**
