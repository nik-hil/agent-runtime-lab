# M2.5 — Sandbox lifecycle controller (simulation)

This lab adds `src/lifecycle`, a separate package from the existing one-shot
`src/sandbox` container runner. The latter's public API and defaults remain
unchanged.

## Run

```sh
go test -race ./src/lifecycle
go run ./cmd/lifecycle-demo
go test ./...
go vet ./...
```

The demo uses an **in-memory fake backend**; it does not talk to E2B or
launch Firecracker. It demonstrates the desired/observed control loop:

```text
desired running  -> create -> observe running
desired paused   -> pause  -> observe paused
desired running  -> resume -> observe running
desired deleted  -> delete -> observe absent
```

Each `Reconcile` call performs at most one mutation. A successful backend
request is not blindly treated as proof that the target state exists: the next
pass observes the state again. Backend calls run with a per-operation timeout.

## Failure behavior verified by unit tests

- A failed pause can be retried after a new observation.
- A timed-out create is **ambiguous**, not necessarily absent. Reconciliation
  stops creating further sandboxes until an operator has found the remote ID
  and called `AttachAfterAmbiguousCreate`.
- An externally deleted sandbox is not automatically recreated.
- Concurrent calls to `Reconcile` on one controller do not duplicate `Create`.
- Deletion is terminal for the same controller instance.

## Limitations before production or live E2B use

1. **No persistence:** desired state, remote ID and uncertain-create fence are
   lost on process restart. Add a durable record before exposing this as an API.
2. **No distributed ownership:** a mutex only protects goroutines within one
   process. Multiple replicas need leases and fencing or compare-and-swap.
3. **No guaranteed create idempotency:** `logicalKey` provides correlation, not
   an E2B idempotency guarantee. Do not auto-retry a Create that may have
   succeeded without returning an ID.
4. **No live E2B adapter yet:** next milestone will map the backend operations
   to E2B's supported API and verify status semantics and authentication.
5. **Minimal state model:** creating, stopping, failed, and concurrent external
   transitions must be represented explicitly before production use.
6. **No persistent state handoff or callback support:** this is synchronous
   educational code, not a distributed production operator.

The running E2B Embed installation and the Python SDK `e2b==2.46.0` remain
unchanged. E2B runtime source `7278c2a380767c9989da73cf4c04b1af1b32da18`
is available for studying the actual orchestrator independently of this code.
