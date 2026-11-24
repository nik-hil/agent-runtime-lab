# M4.1 — Execute a real command inside a managed E2B microVM

M4.1 adds a **synchronous, operator-only** execution path. It reuses the
single-host fleet daemon, its private Unix socket, and the already-created
E2B sandbox. The command is not executed on the Ubuntu host.

## Execution contract

- `fleetctl --execute --id taskvm --command 'echo hello' exec` invokes
  `POST /v1/execute` on the existing mode-0600 Unix socket.
- The daemon checks that the journal owns the sandbox and that it is confirmed
  RUNNING with desired RUNNING, no ambiguity, no unsafe fence and no drain.
- The daemon authenticates to the loopback E2B API's `/sandboxes/{id}/connect`
  endpoint to obtain an ephemeral envd access token (which is never printed),
  then submits `/process.Process/Start` through the **local** E2B client proxy
  (default `http://127.0.0.1:3002`, sandbox envd port 49983).
- The backend speaks Connect's 5-byte-framed JSON streaming protocol,
  requires a process start event and explicit end event, and captures stdout,
  stderr and the exit code. It sends E2B's sandbox ID, envd port, and token
  as headers, **never the E2B API key** to the sandbox proxy.
- The caller supplies a timeout in 1..60 seconds (30s default); the command
  body is capped at 4096 bytes; captured stdout+stderr are capped at 64 KiB.
  A truncated result says `truncated=true`. No redirects are followed, and
  API/proxy endpoints are restricted to loopback HTTP origins.
- An exit code other than zero is a completed **failed command**, not an RPC
  failure. `fleetctl` prints the exit code and returns nonzero.
- If envd does not deliver a terminal event, the result is **uncertain**.
  We never retry side-effecting commands blindly. A concurrent drain/VM state
  change causes the result to be rejected even if the VM executed the command.

## Scope and limitations

This is **not yet** a production-grade durable task queue. The command and
result are not journaled; there is no task idempotency key, artifact storage,
detached-process recovery, or guaranteed remote cancellation. A timed-out
command may still have produced side effects inside its VM. M4.2 will build
durable task identity and completion attestation before any automatic retry.

The new execution endpoint is trusted **local operator access only**. Do not
expose the Unix socket to tenants or provide this endpoint over public TCP.
Never put secrets in the `--command` flag: shell history and `ps` can expose
arguments. Command output may itself be sensitive; capture/redact it with
operator discipline. Use disposable test VMs, not customer workloads.

This implementation is tested against a simulated E2B control plane and
Connect-framed envd proxy. **Live compatibility with your pinned E2B Embed
deployment is not yet confirmed** and must be verified before treating this
as an integration-tested feature.

## One end-to-end Droplet test

Use a **fresh** journal and socket, never the drained M3.4 journals.
Load E2B credentials only in the daemon shell.

```sh
cd ~/src/agent-runtime-lab
git switch main
git pull --ff-only origin main
CGO_ENABLED=1 go test -race -count=1 ./...
go vet ./...
go build -o /tmp/agentlab-m4-fleetd ./cmd/e2b-fleetd
go build -o /tmp/agentlab-m4-fleetctl ./cmd/e2b-fleetctl

cd ~/e2b
eval "$(sudo docker compose exec -T ready cat /run/e2b/sdk.env)"
cd ~/src/agent-runtime-lab
/tmp/agentlab-m4-fleetd --execute \
  --state-file .state/m4-task.json --socket .state/m4-task.sock \
  --interval 2s --ttl 300 --audit-interval 30s --audit-timeout 15s \
  --renew-ttl --renew-interval 60s \
  --exec-proxy http://127.0.0.1:3002 \
  2> >(tee /tmp/m4-task-daemon.jsonl >&2)
```

In another terminal without API credentials:

```sh
cd ~/src/agent-runtime-lab
/tmp/agentlab-m4-fleetctl --socket .state/m4-task.sock \
  --execute --id taskvm --tenant alpha submit
/tmp/agentlab-m4-fleetctl --socket .state/m4-task.sock status

# Only once taskvm reaches RUNNING:
/tmp/agentlab-m4-fleetctl --socket .state/m4-task.sock \
  --execute --id taskvm --exec-timeout 30 \
  --command 'echo M4_OK; uname -s; id -u' exec

# A nonzero exit must be surfaced, not mistaken for success:
/tmp/agentlab-m4-fleetctl --socket .state/m4-task.sock \
  --execute --id taskvm --command 'exit 17' exec

# Always clean up, even if execution reports a proxy/protocol error:
/tmp/agentlab-m4-fleetctl --socket .state/m4-task.sock \
  --execute --drain-timeout 120s drain
/tmp/agentlab-m4-fleetctl --socket .state/m4-task.sock status
```

Confirm `taskvm` deleted, no sandbox ID, and CPU/RAM/pending all zero before
stopping the daemon with Ctrl+C. The `/tmp/m4-task-daemon.jsonl` log may
contain operational errors but never the API key or envd token.

Do not delete the journal to recover a failed command; preserve evidence.
