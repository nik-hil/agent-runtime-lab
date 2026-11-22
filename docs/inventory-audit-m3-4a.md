# M3.4a — E2B inventory audit and TTL drift detection (read-only)

This milestone adds an **offline** `e2b-audit` command. It cross-checks the
single-host fleet journal against the live E2B Embed API, using both
`GET /sandboxes/{id}` and exact-key `GET /v2/sandboxes?metadata=...` queries.
It does not mutate E2B, the journal, or the admission scheduler.

## Why a separate auditor?

M3.3 established durable ownership and verified cleanup, but the controller
does not yet periodically re-observe stable RUNNING sandboxes. In particular,
E2B can expire a VM because of its TTL while the journal still records RUNNING
and retains a CPU/RAM reservation. Without provider reconciliation, controller
status alone may be stale. Similarly, a VM matching the metadata key of a
DELETED record is a possible managed orphan.

`e2b-audit` is a read-only diagnostic foundation. M3.4b can integrate bounded
periodic provider inspection and operator-acknowledged drift handling into
the running daemon after this auditor has been validated.

## What it can detect

| Code | Meaning | Operator response |
|---|---|---|
| `unexpected_absence` | Journal owns an ID, GET returns 404 without delete intent | Check TTL/provider; do not blindly recreate or free resources |
| `duplicate_metadata_key` | Multiple active VMs share one journal correlation key | Investigate all IDs; stop automatic recreation |
| `orphan_after_deletion` | A DELETED journal workload still has a matching active VM | Investigate ownership; manual cleanup only after verification |
| `identity_mismatch` | Metadata lists a VM whose ID differs from journal's ID | Resolve identity before changing reservations |
| `ambiguous_create_match` | CreatePending with exactly one metadata match | Use the existing explicit `fleetctl recover` workflow |
| `ambiguous_create_no_match` | CreatePending with no active listing result | **Do not** assume no VM was created |
| `unexpected_resource_shape` | GET reports zero or more CPU/RAM than reserved | Block unsafe admission in a future controller implementation |
| `observed_state_mismatch` | Stable desired/admitted state differs from provider | Inspect unexpected pause/resume before acting |
| `missing_from_metadata_inventory` | GET finds ID, metadata list does not | Could be an eventually consistent listing; recheck |
| `deletion_not_yet_reconciled` | Desired deleted, provider GET 404, journal still owns ID | Informational; restart reconciler to confirm absence |

The auditor rejects corrupted checkpoints, duplicate journal keys, missing
credentials and API failures. HTTP 429/503 and timeouts are errors, not empty
inventory. On failure it exits nonzero without claiming a complete audit.

**Scope limitation:** E2B's metadata-filtered API can identify only sandboxes
associated with **keys already known in this journal**. It cannot enumerate
arbitrary user VMs or discover keys erased from a lost/corrupted journal. A
clean audit is **not** proof that the entire E2B host has no orphan sandboxes.
The snapshot is momentary, not an atomic provider-wide read.

## Exit codes and access

- `0`: audit completed with no review-required finding (information-level
  findings may appear).
- `1`: configuration, lock, checkpoint, request or provider error; audit
  incomplete and unsafe to infer absence.
- `2`: completed, but one or more warning/critical discrepancies need review.

The audit takes the journal's exclusive local `flock`. **Stop `fleetd` before
running it.** Do not remove any lock or socket while the daemon is alive.
Only run with credentials sourced into the same shell as this command; never
paste `sdk.env` or an API key into shared logs. API calls are limited to HTTP
loopback by the existing E2B adapter.

## Smoke test: audit an already-clean completed experiment

Fetch the PR branch, then:

```sh
cd ~/src/agent-runtime-lab
CGO_ENABLED=1 go test -race -count=1 ./src/fleetaudit ./src/e2bapi
CGO_ENABLED=1 go test -race -count=1 ./...
go vet ./...

cd ~/e2b
eval "$(sudo docker compose exec -T ready cat /run/e2b/sdk.env)"
cd ~/src/agent-runtime-lab
go build -o /tmp/agentlab-e2b-audit ./cmd/e2b-audit

# Run only AFTER the old M3.3b daemon has stopped.
/tmp/agentlab-e2b-audit --state-file .state/m3-3b-live.json \
  > /tmp/m3-4a-clean-audit.json
cat /tmp/m3-4a-clean-audit.json
```

Expected for the previously drained `crash1` experiment: `workloads: 1`,
`scope: known_journal_metadata_keys_only`, `findings: []` and
`requires_review: false`. A different result is evidence to investigate;
never delete remote sandboxes merely because of a diagnostic finding.

## Optional live TTL experiment (creates one temporary E2B VM)

Use fresh paths; never reuse an old/drained journal. Start a new daemon in
Terminal 1, with your normal SDK credentials already exported:

```sh
cd ~/src/agent-runtime-lab
go run ./cmd/e2b-fleetd --execute \
  --state-file .state/m3-4a-ttl.json \
  --socket .state/m3-4a-ttl.sock \
  --ttl 60 --interval 2s \
  2> /tmp/m3-4a-daemon.jsonl
```

In Terminal 2:

```sh
cd ~/src/agent-runtime-lab
go run ./cmd/e2b-fleetctl --socket .state/m3-4a-ttl.sock \
  --execute --id ttl1 --tenant alpha submit
go run ./cmd/e2b-fleetctl --socket .state/m3-4a-ttl.sock status
```

Verify `ttl1` is RUNNING with a sandbox ID. **Do not drain yet.** Stop only
Terminal 1 with `Ctrl+C`; it must preserve the running VM. After the provider's
60-second TTL has elapsed, use Terminal 2 (with E2B credentials loaded) to
perform the read-only comparison:

```sh
/tmp/agentlab-e2b-audit --state-file .state/m3-4a-ttl.json \
  > /tmp/m3-4a-expired-audit.json
echo "audit exit code: $?"
cat /tmp/m3-4a-expired-audit.json
```

If E2B has expired the sandbox and returned 404, the auditor should emit
`unexpected_absence` and exit `2`. Provider expiration timing is not
guaranteed to equal the nominal TTL; an active VM may still be present when
checked. The auditor **never modifies reservations** in either case.

To finish cleanup, restart Terminal 1 **with the same state-file, socket,
resource flags and credentials**. In Terminal 2:

```sh
go run ./cmd/e2b-fleetctl --socket .state/m3-4a-ttl.sock \
  --execute --drain-timeout 120s drain
go run ./cmd/e2b-fleetctl --socket .state/m3-4a-ttl.sock status \
  > /tmp/m3-4a-final.json
cat /tmp/m3-4a-final.json
```

Confirm DELETED, zero CPU/RAM and zero pending, then press `Ctrl+C` in
Terminal 1. If any operation times out, keep the journal and inspect the
status; do not erase ownership records. Audit can be rerun after shutdown.

## What is not implemented

- No periodic health polling inside `fleetd` yet; that is M3.4b.
- No automatic repair, recreation, TTL renewal or orphan deletion.
- No provider-wide inventory of VMs lacking known journal keys.
- No independent verification of physical RAM/CPU or paused snapshot storage.
