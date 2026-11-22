# M2.7 — Live crash-after-create fault injection

The normal lifecycle smoke test proves persistent reconciliation across clean
process exits. This **optional destructive lab test** exercises the narrower
failure window after E2B has created a sandbox but before the controller has
saved its ID.

It uses the existing E2B Embed API and **no new dependencies**. The command
targets its own process with SIGKILL; it does **not** kill E2B, Firecracker,
Docker, SSH, or the host.

## Preconditions

- E2B Embed is already healthy on this host; Go and GCC are installed.
- The normal `.state/m2-7.json` lifecycle has been deleted/terminated.
- Choose a **new dedicated** state path for each injection. Never delete or
  overwrite an existing state file to repeat the test.
- Run the recovery immediately after injection; the VM has a finite TTL.

```sh
cd ~/src/agent-runtime-lab
CGO_ENABLED=1 go test -race -count=1 ./cmd/e2b-crashlab ./src/durable ./src/e2bapi
go vet ./...

cd ~/e2b
eval "$(sudo docker compose exec -T ready cat /run/e2b/sdk.env)"
cd ~/src/agent-runtime-lab

# This command will exit nonzero with "signal: killed" BY DESIGN. It prints
# the sandbox ID before terminating its Go child process.
go run ./cmd/e2b-crashlab --execute --confirm-sigkill \
  --state-file .state/m2-7-crash-01.json --ttl 900

# This must show create_pending=true with no sandbox_id.
go run ./cmd/e2b-durable --state-file .state/m2-7-crash-01.json status

# Recover must reuse the existing E2B VM, not call Create again.
go run ./cmd/e2b-durable --state-file .state/m2-7-crash-01.json --execute recover

# Observe the same VM, then clean it up.
go run ./cmd/e2b-durable --state-file .state/m2-7-crash-01.json --execute start
go run ./cmd/e2b-durable --state-file .state/m2-7-crash-01.json --execute delete
go run ./cmd/e2b-durable --state-file .state/m2-7-crash-01.json status
```

## Expected evidence

1. The injected command prints `EXPECTED CRASH: E2B created sandbox <id>`.
2. The persisted state contains a randomized `logical_key`, empty `sandbox_id`
   and `create_pending: true`.
3. `recover` finds exactly one live sandbox whose E2B metadata contains the
   recorded logical key; it stores the **same ID printed before SIGKILL**.
4. `start` observes RUNNING without creating a new sandbox.
5. `delete` converges to `desired=deleted`, `observed=absent` and
   `terminated=true`.

## Failure / cleanup guidance

- If recovery reports **no matching sandbox**, do not reset the state or issue
  a fresh Create. E2B's listing is not proof that no sandbox existed. It may
  not be immediately visible or may already have expired.
- If multiple sandboxes match, investigate manually; the controller refuses to
  guess which one it owns.
- On failure, use the **printed E2B sandbox ID** to inspect or explicitly kill
  that sandbox through the installed E2B SDK. Never bulk-delete by metadata.
- A signal kills only the Go child. The provider sandbox remains alive until
  deleted or its TTL expires. A `go run` message `signal: killed` is expected;
  other errors are not a passing outcome.
- The file journal and lock require a local Linux filesystem. They do not
  provide distributed leases, fenced cross-node ownership, or transactional
  creation across the network. This experiment verifies only the specific
  crash window on one host.
