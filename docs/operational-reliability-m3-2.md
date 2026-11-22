# M3.2 — Operational reliability for the local E2B fleet reconciler

M3.1 proved continuous reconciliation against real E2B. M3.2 adds operational
visibility and bounded retry traffic to the same **single-host** daemon.
It does not install a system service, modify existing E2B containers or
upgrade the SDK/runtime. No Go dependencies are added.

## Behavior

- Errors generate single-line JSON structured logs on **stderr** using Go's
  `log/slog`. Progress cycles also log transitions. Idle reconciliations do
  not emit a line every two seconds, avoiding noisy terminal scrollback.
- A failing `Tick` increases a consecutive-failure counter and delays the next
  **timer-driven** retry exponentially, capped by `--max-backoff` (30 seconds
  by default). A new explicit client intent or `recover` wakes the worker
  immediately so cleanup does not wait for the retry timer. A successful
  reconciliation resets the delay; the last error and its timestamp remain
  available for historical diagnosis.
- No implicit retry of an uncertain Create is introduced. The M2.9 durable
  journal still fences ambiguous creation. Any exposed `create_pending=true`
  must be recovered through metadata and an exact single provider match.
- Workloads not converged after `--stuck-after` (default 2 minutes) appear in
  `reconciler.stuck_jobs` and emit one `fleet_workload_stalled` warning per
  unchanged state. Waiting for available capacity may be **normal**, not a
  failure. Running sandboxes with desired `running` and paused sandboxes with
  desired `paused` do *not* produce stall warnings.
- Stall age and counters live only in daemon memory and reset on restart;
  durable VM identity and resource reservations remain in the existing fleet
  journal. This milestone does not persist metrics or ship logs anywhere.

## Private metrics

`GET /v1/metrics` over the **existing Unix socket** returns Prometheus text:

| Series | Meaning |
|---|---|
| `fleetd_reconcile_cycles_total` | Reconciliation cycles executed |
| `fleetd_reconcile_failures_total` | Cycles ending in an error |
| `fleetd_reconcile_transitions_total` | Controller steps that changed state or attempted a remote mutation |
| `fleetd_reconcile_consecutive_failures` | Current streak of errors |
| `fleetd_reconcile_backoff_seconds` | Approximate remaining timer retry delay |
| `fleetd_stuck_jobs` | Nonconverged jobs past the threshold |
| `fleetd_admission_used_cpu` | Reserved CPU slots |
| `fleetd_admission_used_memory_mib` | Reserved active memory MiB |
| `fleetd_admission_pending` | Queued requests |
| `fleetd_admission_unsafe` | Unsafe capacity flag (0 or 1) |
| `fleetd_workloads{state="..."}` | Count by bounded built-in admission state |

These are **reservation** metrics, not physical RSS, Firecracker overhead,
hugepage availability or paused snapshot disk usage. Metrics contain no
API keys, sandbox IDs, logical keys, tenant names or free-form labels.
The endpoint is local Unix HTTP only, not internet-facing. No Prometheus
server is installed or configured automatically.

## Reproduce on the Droplet

After fetching PR branch `feat/m3-2-operational-reliability`:

```sh
cd ~/src/agent-runtime-lab
CGO_ENABLED=1 go test -race -count=1 ./...
go vet ./...
```

For a live run, use **new paths** instead of the finished M3.1 journal.
First load the existing E2B credentials in Terminal 1 without printing them:

```sh
cd ~/e2b
eval "$(sudo docker compose exec -T ready cat /run/e2b/sdk.env)"
cd ~/src/agent-runtime-lab
go run ./cmd/e2b-fleetd --execute \
  --state-file .state/m3-2.json \
  --socket .state/m3-2.sock \
  --interval 2s --max-backoff 30s --stuck-after 20s \
  2> /tmp/m3-2-daemon.jsonl
```

In Terminal 2, submit one job and inspect the observability surface:

```sh
cd ~/src/agent-runtime-lab
go run ./cmd/e2b-fleetctl --socket .state/m3-2.sock \
  --execute --id test1 --tenant alpha submit
go run ./cmd/e2b-fleetctl --socket .state/m3-2.sock metrics \
  > /tmp/m3-2-metrics.txt
go run ./cmd/e2b-fleetctl --socket .state/m3-2.sock status \
  > /tmp/m3-2-status.json

# Explicitly destroy the live test VM, then confirm deleted state:
go run ./cmd/e2b-fleetctl --socket .state/m3-2.sock \
  --execute --id test1 delete
go run ./cmd/e2b-fleetctl --socket .state/m3-2.sock status
```

After status shows `test1` deleted, `used.cpu=0`, `used.memory_mib=0` and
`pending=0`, stop Terminal 1 with Ctrl+C. The daemon never automatically
deletes VMs on shutdown and does not renew E2B sandbox TTL. Capture logs with
`tail -n 30 /tmp/m3-2-daemon.jsonl`; avoid pasting environment variables or
credentials into shared logs.

## Limitations and follow-ups

- Error classification is not yet provider-specific. Exponential backoff
  limits request frequency after all errors, including persistent faults.
  Explicit client writes bypass backoff for urgent cleanup.
- A stalled job alert is based on unchanged observed state since this daemon
  started or since the last transition; it does **not** establish VM liveness.
- Metrics are process-local and reset on restart. A future deployment can
  scrape the private endpoint via an authorized local collector.
- Alert delivery, durable rate-limit budgets, idempotent provider operations,
  TTL renewal, physical host telemetry, and multi-node fencing are not part
  of this milestone.
- The socket remains protected by filesystem permissions; it must not be
  exported to untrusted containers or a public TCP proxy.
