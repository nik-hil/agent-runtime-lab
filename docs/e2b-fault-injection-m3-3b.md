# M3.3b — E2B failure injection and crash recovery

M3.3a established successful graceful drain against a live E2B Embed instance.
M3.3b targets the more dangerous case: a durable controller making progress
while the provider returns ambiguous outcomes or the controller process dies.

## Deterministic tests: no live E2B required

The fault suite uses `httptest.NewServer` on loopback with the **real**
`src/e2bapi.Backend` adapter and the unchanged durable `src/fleet.Controller`.
It does not use the Droplet's E2B API key, create an actual microVM, alter
Docker, or install dependencies. It tests these concrete failure boundaries:

| Injection | Asserted safety property |
|---|---|
| Create commits a remote VM, but returns HTTP 503 | `create_pending` and CPU/RAM reservation remain; no duplicate POST after restart |
| E2B metadata listing returns HTTP 503 | Recovery fails closed; cannot conclude that the VM does not exist |
| Describe returns HTTP 503 | Remote state is not falsely confirmed; resource reservation retained |
| Delete returns HTTP 429, then 503 | Retry is possible, but capacity is not freed on error |
| Delete returns HTTP 204 | Capacity is still held until a subsequent GET reports 404 |
| Create returns HTTP 429 before any VM exists | Missing metadata match is **not** permission to retry Create; manual intervention remains necessary |
| Create commits a remote VM, but HTTP response exceeds the request deadline | Timeout retains Create ambiguity until exact metadata recovery; no duplicate POST |
| Delete succeeds remotely but returns HTTP 503 | After restart, GET 404 confirms cleanup without a repeated Delete |
| Real child process receives SIGKILL during an in-flight Create | Pre-Create durable fence survives; recovery discovers the existing VM exactly once |

Run from the PR checkout:

```sh
cd ~/src/agent-runtime-lab
CGO_ENABLED=1 go test -race -count=1 -v ./src/fleet -run 'Test(HTTPFaults|RejectedCreate|DeleteCommitted|DaemonSIGKILL)'
CGO_ENABLED=1 go test -race -count=1 ./...
go vet ./...
```

The **SIGKILL** test is itself local: it launches the Go test binary as a
child, lets a loopback fake E2B provider allocate exactly one fake VM, and
kills the child while the provider holds the HTTP response. The parent then
reopens the *same* journal, verifies the reservation/creation fence, performs
metadata recovery, and drains the fake VM. It is not a claim of testing a
Droplet reboot, kernel panic, or real E2B control-plane outage.

## Optional real E2B restart test (one sandbox)

This separately verifies that real E2B identity and reservations survive
controller restart. Unlike the test above, it does **not** inject an ambiguous
Create outcome. Never reuse a drained journal from M3.3a.

**Terminal 1:** Load the credentials and run the actual built binary to avoid
the extra parent process introduced by `go run`:

```sh
cd ~/src/agent-runtime-lab
go build -o /tmp/agentlab-m33b-fleetd ./cmd/e2b-fleetd
cd ~/e2b
eval "$(sudo docker compose exec -T ready cat /run/e2b/sdk.env)"
cd ~/src/agent-runtime-lab
/tmp/agentlab-m33b-fleetd --execute \
  --state-file .state/m3-3b-live.json \
  --socket .state/m3-3b-live.sock --interval 2s \
  2> /tmp/m3-3b-daemon.jsonl
```

**Terminal 2:** Submit one sandbox, and query status until the response shows
`running` and contains its `sandbox_id`:

```sh
cd ~/src/agent-runtime-lab
go run ./cmd/e2b-fleetctl --socket .state/m3-3b-live.sock \
  --execute --id crash1 --tenant alpha submit
go run ./cmd/e2b-fleetctl --socket .state/m3-3b-live.sock status
```

For this live test, prefer a **graceful controller restart** first: stop
Terminal 1 with Ctrl+C and start the same daemon binary again with the same
journal and socket paths. On restart, `crash1` should retain its sandbox ID;
the controller must not Create a second VM. Then explicitly drain:

```sh
go run ./cmd/e2b-fleetctl --socket .state/m3-3b-live.sock \
  --execute --drain-timeout 120s drain
go run ./cmd/e2b-fleetctl --socket .state/m3-3b-live.sock status \
  > /tmp/m3-3b-final.json
cat /tmp/m3-3b-final.json
```

Confirm `crash1=deleted`, `used.cpu=0`, `used.memory_mib=0`, `pending=0`, and
`unsafe=false`, then stop Terminal 1 with Ctrl+C. The journal must be retained
as evidence and must not be reused for new admission after the drain.

## Safety boundaries still open

- A missing sandbox during recovery can be caused by eventual consistency.
  The controller intentionally **does not** clear a Create ambiguity when
  metadata lookup returns zero matches. This can require manual intervention.
- The separate legacy `e2b-fleet` command currently lacks a direct guard
  against submitting to a drained journal after the daemon has stopped. Do
  **not** use that command for mutations on drained journals. This is a known
  control-plane hardening gap, not proof that all possible local callers
  respect the drain fence.
- These tests don't prove cross-host quorum/fencing, node reboot recovery,
  provider eventual-consistency bounds, or real provider HTTP 429 semantics.
  The local fault provider deliberately implements non-idempotent Create.
- No E2B SDK/runtime upgrades or service changes are part of this milestone.
