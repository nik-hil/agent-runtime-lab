# M2.6 — Live E2B adapter for the Go lifecycle controller

This adds a **real but deliberately local-only** HTTP adapter to the existing
`src/lifecycle` controller. The HTTP request shapes are taken from the E2B
runtime OpenAPI specification at commit
`7278c2a380767c9989da73cf4c04b1af1b32da18`.

## Prerequisites

- E2B Embed already running on the same host, or reached through an SSH tunnel.
- Go 1.23+ and GCC for the race detector. No Go dependencies are added.
- No E2B service changes, Docker updates, or Python SDK upgrades.

```sh
cd ~/src/agent-runtime-lab
go test -race ./src/lifecycle ./src/e2bapi
go test ./...
go vet ./...

cd ~/e2b
eval "$(sudo docker compose exec -T ready cat /run/e2b/sdk.env)"
cd ~/src/agent-runtime-lab
go run ./cmd/e2b-lifecycle --execute
```

The CLI uses `E2B_API_URL` (defaults to `http://127.0.0.1:3000`) and
`E2B_API_KEY`, and rejects non-loopback API endpoints, redirects, and HTTPS
connections to avoid accidentally sending credentials to remote services.
Use a local tunnel if you need a remote API; do not weaken this check.

## REST contract

| Controller | HTTP request | Expected result |
|---|---|---|
| Create | `POST /sandboxes` (`templateID`, `timeout`, `metadata`) | `201`, `sandboxID` |
| Observe | `GET /sandboxes/{id}` | `200`, `state=running\|paused`; `404` means absent |
| Pause | `POST /sandboxes/{id}/pause`, `{ "memory": true }` | `204` |
| Resume | `POST /sandboxes/{id}/connect`, `{ "timeout": 300 }` | `200` or `201` |
| Delete | `DELETE /sandboxes/{id}` | `204` or `404` |

The adapter uses the full-memory pause mode; filesystem-only behavior has
already been verified separately with Python SDK 2.46.0 via
`pause(keep_memory=False)`.

## Safety and interpretation

- The executable requires `--execute`, creates only one sandbox, and attempts
  independent cleanup on failure. `autoPause` is disabled; sandbox TTL is
  300 seconds. A cleanup failure prints the sandbox ID for manual remediation.
- The metadata correlation key is **not** an idempotency key. If create times
  out after succeeding server-side, the controller blocks duplicate creates.
  Look up the remote sandbox using `agent_runtime_lab_key=agent-runtime-lab/m2-6`
  before retrying or deleting it.
- **No durable storage:** the controller loses state if its Go process exits.
  The independent deferred cleanup runs only on normal termination or SIGINT/
  SIGTERM handled by Go; SIGKILL and power loss bypass it.
- **No distributed ownership:** this is one controller instance; introducing
  multiple replicas requires leases, versioned records, and fencing.
- A successful API response does not by itself mean the target state was
  reached; subsequent `Observe` calls validate it.
- Unit tests use `httptest`; live verification happens only when the user
  explicitly invokes the CLI against E2B Embed.
