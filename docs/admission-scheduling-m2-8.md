# M2.8 — Single-node CPU/RAM admission and tenant quotas

This milestone implements the **decision to admit a sandbox**, rather than
calling E2B. All schedules are in-memory and unit-testable. Nothing in this
milestone launches or modifies a VM.

## Run

```sh
cd ~/src/agent-runtime-lab
CGO_ENABLED=1 go test -race -count=1 ./src/admission
go vet ./...
go run ./cmd/admission-demo
```

## Rules

1. Each request has a unique stable ID, a tenant, a CPU reservation, and a
   memory reservation in MiB. Repeating the exact request is idempotent.
   Reusing an ID for different workload parameters is rejected.
2. Host and tenant *active* CPU/RAM budgets cannot be overcommitted, even with
   simultaneous callers. The scheduler locks around reservation decisions.
3. Each tenant has a maximum number of **logical sandboxes**. Queued, running,
   reserved, uncertain and paused sandboxes count. Pausing does *not* free
   logical quota.
4. Pending work has a bounded queue. The scheduler selects tenants in
   round-robin order, preserving FIFO order within each tenant. A request that
   does not fit won't block a different tenant whose head request fits.
5. `Dispatch` reserves resources **before** an external create or resume
   operation. `ConfirmRunning` records remote evidence; API success alone is
   not enough to infer the running state.
6. `MarkUncertain` retains the reservation after an ambiguous create/resume
   result. A later `ConfirmRunning` may reconcile the real VM without charging
   capacity again. `ConfirmDeleted` may release it **only after** remote
   absence is authoritatively confirmed (or an operator has resolved the
   uncertainty). A momentary 404/empty list can be eventually consistent.
7. `ConfirmPaused` frees active CPU/RAM **only after** remote pause is
   observed. The VM still occupies a logical slot and durable snapshot/disk
   space. `QueueResume` requires a **new active resource reservation** before
   contacting E2B to resume the VM.
8. `ConfirmDeleted` frees both active resources (if held) and the logical
   sandbox quota; past request IDs remain terminal to prevent accidental
   duplicate logical work from retries.

## State flow

```text
Submit --> queued --> reserved --> running --> paused
                       |            |             |
                   uncertain         |        resume_queued
                       |             |             |
                 (hold resources)     |        resume_held
                       |             |             |
                   running <---------+-------------+

Any non-deleted state -- confirmed remote absence --> deleted
```

The scheduler does **not** run reconciliation or remote observations itself.
The controller (`src/durable`) owns those operations. The next milestone
will integrate admission and durable execution using one coordinated owner.

## Safety boundaries and open questions

- **In-memory only:** no crash recovery for reservations, tenant quotas, or
  queue positions. Do not deploy this as a distributed production scheduler.
- **Single node:** quota and host usage are not authoritative across nodes or
  orchestrators. A production scheduler needs persistent reservations,
  ownership leases/fencing, and a node inventory with utilization feedback.
- **Reservation is not measured usage:** committed vCPU/RAM are an accounting
  budget. This doesn't implement CPU-share scheduling, actual utilization
  sampling, ballooning, huge-page fragmentation, swap, or memory overcommit.
- **Paused snapshots still consume disk and network/storage bandwidth.** This
  prototype does not model disk capacity, snapshot object retention, network
  limits, or Firecracker host kernel huge-page pool reservation.
- **Conservative ambiguity:** a timeout can hold capacity until reconciliation
  proves the remote result. This favors avoiding oversubscription over maximum
  utilization; later add lease-based recovery and operator diagnostics.
- **No priority/preemption, weighted fair queuing, starvation guarantee,
  deadline tracking, rate limits, or multi-node placement** yet. Request IDs
  and completed records are retained indefinitely until an explicit GC policy
  is added.

These are deliberate limitations rather than claims of a complete scheduler.

## Reproduction matrix

| Scenario | Expected outcome |
|---|---|
| 500 concurrent submit/dispatch callers | Used resources never exceed limits |
| Tenant at max active CPU | Other tenant can use available host capacity |
| Tenant logical quota reached | New sandbox request rejected |
| Pause observed | Active CPU/RAM freed; logical slot kept |
| Resume requested with full host | Queued, not resumed prematurely |
| Create response lost | Capacity remains reserved until reconciled |
| Remote VM missing, confirmed | Reservation and logical quota released |
| Duplicate identical request ID | Existing entry returned, no double-charge |
| Queue full | Further new work rejected |
