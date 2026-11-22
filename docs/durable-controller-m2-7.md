# M2.7 — Crash-safe sandbox lifecycle reconciliation

This milestone adds a *durable*, **single-host** lifecycle controller. Unlike
M2.6, it preserves desired state, sandbox ID and uncertain-create status across
controller restarts. There are no new Go modules and no E2B upgrades. The
storage adapter is a fsync + atomic rename JSON file; SQLite can be added behind
the `durable.Store` interface later. This is intentionally not a production
distributed-controller implementation.

## Key invariant

**Never reissue a Create when the previous Create may have succeeded.** The
controller first persists `create_pending: true`, then calls `POST /sandboxes`.
If the process crashes before recording the returned sandbox ID, startup sees
the fence and refuses to create another sandbox. `recover` lists E2B sandboxes
by metadata `agent_runtime_lab_key`, verifies the metadata on every returned
record, and adopts the ID **only if exactly one** live sandbox matches.
An empty or ambiguous listing is not treated as proof that creation failed.

Other operations are safe to re-observe: pause/resume/delete requests may have
completed even if the API response was lost. A restart fetches the current
remote state before performing the next mutation. Remote deletion while desired
is RUNNING or PAUSED does not trigger an automatic replacement.

## Use on the E2B Embed Droplet

```sh
cd ~/src/agent-runtime-lab
go test -race ./src/durable ./src/e2bapi ./src/lifecycle
go test ./...
go vet ./...

cd ~/e2b
eval "$(sudo docker compose exec -T ready cat /run/e2b/sdk.env)"
cd ~/src/agent-runtime-lab

# The CLI keeps one logical sandbox in the local state file across invocations.
go run ./cmd/e2b-durable --execute start
go run ./cmd/e2b-durable status
go run ./cmd/e2b-durable --execute pause
go run ./cmd/e2b-durable --execute resume
go run ./cmd/e2b-durable --execute delete
go run ./cmd/e2b-durable status
```

If a Create is interrupted:

```sh
go run ./cmd/e2b-durable status
go run ./cmd/e2b-durable --execute recover
# Then run start/pause/delete as appropriate.
```

The state is stored at `.state/m2-7.json` (override with `--state-file`). The
directory is ignored by Git. Do **not** delete the state file while its
sandbox may be alive. Each new test needs a new state-file path; a terminal
record cannot be restarted. The CLI requires `--execute` for E2B operations.

The default sandbox TTL is 900 seconds (`--ttl` can set 30–3600 seconds).
This is a finite experimental lease, not guaranteed perpetual retention: a
long gap between CLI invocations may cause E2B to expire or destroy the VM.
Explicitly run `delete` after experiments and verify the result.

## Failure matrix

| Failure | Expected behavior |
|---|---|
| Crash before POST Create | Pending-create fence remains; operator can inspect before retry |
| Crash after remote Create but before saving ID | Recovery adopts exactly one metadata match; never blind re-create |
| Pause succeeded but client timed out | Re-observation sees PAUSED; no unnecessary repeat |
| Delete succeeded but client timed out | Re-observation sees ABSENT; record becomes terminal |
| Sandbox vanished while desired RUNNING | Returns error; no implicit replacement |
| Two controllers open same state file | Advisory lock prevents concurrent ownership |
| Invalid/corrupt state | Fail closed; no remote action |
| Zero/multiple metadata matches | Recovery refuses to guess |

## Limitations

- The journal requires a local filesystem with atomic rename, `fsync` and
  advisory `flock`. It is **not suitable for NFS or multi-node replicas**.
- A power failure between writing the first new state file and the first
  remote Create is safe (fenced), but may require operator action despite no
  sandbox having been created. This is deliberate fail-closed behavior.
- E2B's correlation metadata is *not* a server-side idempotency guarantee.
  If a sandbox has expired before recovery, an empty list cannot prove whether
  it existed. We do not automatically recreate it.
- The listing API filters only visible running/paused sandboxes and may be
  eventually consistent. Always treat an empty match as inconclusive.
- The controller does not implement multi-node ownership, fencing epochs,
  leases, durable operation queues, or crash-safe external side-effect
  transactions. Those are later milestones.
- E2B API credentials are read from the environment and never persisted.
  The store contains IDs and correlation keys only.
- Tests are deterministic fakes + HTTP test servers. They do not simulate a
  real host power outage. Real E2B end-to-end validation must be performed
  separately on the Droplet.
