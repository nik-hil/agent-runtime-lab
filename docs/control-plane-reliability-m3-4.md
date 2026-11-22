# M3.4 — Provider reconciliation, TTL policy and bounded orphan inventory

This completes the M3.4 work in one implementation on top of the M3.3
durable multi-VM controller and the initial M3.4a read-only auditor. E2B Embed,
Docker Compose and the E2B SDK are **not** upgraded or reconfigured.

## Safety contract

- The daemon periodically compares an **isolated journal snapshot** with
  E2B's live sandbox details and exact known-key metadata. No provider I/O
  occurs under the controller mutex or while writing the journal.
- A provider 429/503, timeout, invalid data or incomplete scan is an **audit
  failure**, NOT proof the VM is absent. The previous reservations persist.
- A completed audit reporting critical drift starts a two-confirmation safety
  rule. Two consecutive audits containing the identical critical finding set
  may durably mark the fleet `unsafe`. Before writing the fence, the
  controller verifies **all journal workloads and admission state are still
  identical** to the snapshot that was audited. An admitted or changed job
  during the scan prevents a stale observation from fencing it.
- A durable unsafe fence blocks **new submissions**, but allows the operator
  to request `drain`. Neither the audit nor the fence creates, deletes,
  resurrects or releases reservations for VMs.
- There is deliberately **no automatic unfence**, repair, remote orphan
  deletion or automatic stale-reservation release. A new journal should be
  created only after the old journal is safely drained/investigated.
- `Ctrl+C` remains non-destructive. The monitor and worker stop on the same
  context and the Unix socket is removed by the existing safe cleanup logic.
- The older `e2b-fleet` standalone CLI must **not** be used for mutations on
  a drained or unsafe journal. Prefer `e2b-fleetctl` while fleetd owns it.

## New daemon controls

| Flag | Default | Description |
|---|---|---|
| `--audit-interval` | `60s` | Poll known journal metadata and live VM details |
| `--audit-timeout` | `30s` | Whole-audit time budget (must fit within interval) |
| `--audit-max-jobs` | `100` | Cap full-scan workload count; fail audit instead of sampling silently |
| `--audit-global` | false | Also scan all active **lab-tagged** VMs visible to the API key |
| `--renew-ttl` | false | Opt in to extending known running sandbox lifetime |
| `--renew-interval` | `2m` | Renewal cadence (must be at most TTL/2 when enabled) |
| `--ttl` | `900` | Desired TTL passed to E2B in seconds; renewal uses same value |

The TTL extension calls `POST /sandboxes/{id}/timeout` with a JSON
`{"timeout": <seconds>}` body. It is only applied to stable RUNNING workloads
with desired=running and known IDs. Queued, paused, uncertain, deleting,
draining and unsafe workloads are skipped. The controller does not presume
that an E2B timeout call succeeded if it errors; a subsequent audit detects
actual absence. The pinned Embed deployment must support that route.

The `--audit-global` mode is optional and read-only. It enumerates paginated
active `/v2/sandboxes` inventory, filters records whose metadata starts with
`agent-runtime-lab/`, and reports `untracked_lab_sandbox` findings when a
remote VM's key is missing from **this particular journal**. Such records
may belong to a separate experiment, and must **never** be auto-deleted.
Pagination is bounded to 50 pages and fails closed on cursor cycles, duplicate
IDs, bad responses or provider errors. This is a team-scoped best-effort view,
not a guarantee of global cloud-wide inventory.

## Observability

- `fleetctl audit` uses `GET /v1/audit` over the existing Unix socket to
  display the most recent full audit result, error state, critical confirmation
  count, durable fence state and renewal attempts/failures.
- `fleetctl metrics` retains the original fleet metrics and additionally
  exports `fleetd_inventory_audits_total`,
  `fleetd_inventory_failures_total`, `fleetd_inventory_findings`,
  `fleetd_ttl_renew_attempts_total`, and `fleetd_ttl_renew_failures_total`.
- Structured logs include `fleet_inventory_drift`,
  `fleet_inventory_audit_failed`, `fleet_inventory_admission_fenced`, and
  `fleet_ttl_renew_failed`. Metrics expose only aggregate values, not sandbox
  IDs or tenant labels. Historical counters reset on daemon restart; the
  journal's unsafe fence and workload identities persist.

## One end-to-end Droplet experiment

**Prepare:** E2B Embed is already running on the Droplet. Stop any old fleetd
before accessing its journal. Do not reuse old drained journals.

Fetch the PR branch and run the complete suite:

```sh
cd ~/src/agent-runtime-lab
git fetch origin
git switch --track origin/feat/m3-4a-readonly-inventory-audit
CGO_ENABLED=1 go test -race -count=1 ./...
go vet ./...
```

**Terminal 1**, source E2B credentials without printing them:

```sh
cd ~/e2b
eval "$(sudo docker compose exec -T ready cat /run/e2b/sdk.env)"
cd ~/src/agent-runtime-lab
go run ./cmd/e2b-fleetd --execute \
  --state-file .state/m3-4-full.json \
  --socket .state/m3-4-full.sock \
  --interval 2s --ttl 120 \
  --audit-interval 10s --audit-timeout 8s \
  --audit-global \
  --renew-ttl --renew-interval 30s \
  2> /tmp/m3-4-full.jsonl
```

**Terminal 2**, submit one live E2B sandbox:

```sh
cd ~/src/agent-runtime-lab
go run ./cmd/e2b-fleetctl --socket .state/m3-4-full.sock \
  --execute --id health1 --tenant alpha submit
go run ./cmd/e2b-fleetctl --socket .state/m3-4-full.sock status
```

After `health1` is RUNNING and at least one audit interval has elapsed:

```sh
go run ./cmd/e2b-fleetctl --socket .state/m3-4-full.sock audit
go run ./cmd/e2b-fleetctl --socket .state/m3-4-full.sock metrics
tail -n 25 /tmp/m3-4-full.jsonl
```

Verify the auditor has completed (`runs >= 1`). An advisory
`untracked_lab_sandbox` from another experiment is possible with global
inventory enabled; it is not proof of leakage or permission to delete.
After at least one 30-second renew interval, `renew_attempts >= 1` should be
reported for `health1`, unless it was deleted, paused or marked unsafe. If
the provider rejects the timeout endpoint, observe `renew_failures` and
check the provider compatibility before retrying.

Then **drain and confirm remote absence** while fleetd is running:

```sh
go run ./cmd/e2b-fleetctl --socket .state/m3-4-full.sock \
  --execute --drain-timeout 120s drain
go run ./cmd/e2b-fleetctl --socket .state/m3-4-full.sock status
```

Confirm every job DELETED, CPU=0, RAM=0, pending=0, no retained sandbox ID
or CreatePending. Press Ctrl+C in Terminal 1 **only after** this confirmation.
Keep the state journal and logs as evidence. Never delete the journal to
"reset" a failure with uncertain ownership.

## Independent TTL-expiry/drift experiment (optional)

With a **different fresh journal**, start a daemon with `--ttl 60`,
`--audit-interval 10s --audit-timeout 8s`, **without** `--renew-ttl`.
Submit one sandbox, confirm RUNNING, then stop the controller with Ctrl+C
without draining. After E2B expires the VM, restart the daemon with the SAME
journal and observe two consecutive complete audits. If Describe returns 404
and the provider listing confirms no active VM, the report contains
`unexpected_absence`. The journal should remain reserved and become unsafe;
it must **not** create a new sandbox automatically. Explicitly run `drain`
and confirm deletion before stopping. Provider expiration is asynchronous;
do not assume expiration precisely at 60 seconds.

## Known boundaries

- The audit compares snapshots rather than an atomic provider-wide view;
  two confirmations and current-journal validation reduce false fences but
  cannot eliminate every external consistency issue.
- There is no cross-journal admission accounting, distributed leader election,
  lease fencing, alert delivery, or automatic recovery from unexpected loss.
- A slow provider may make a bounded audit fail. Failed audits do not block
  new admission by themselves; only two confirmed critical findings persist
  a fence. These are educational controls, not a provider SLA.
- A monotonic TTL renewal guarantee requires continuous controller uptime;
  no lease renewals occur after Ctrl+C. Opt-in renewal costs additional E2B
  API traffic and extends VM lifetime.
