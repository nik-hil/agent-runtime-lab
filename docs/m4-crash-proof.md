# Deterministic M4.3 crash-recovery proof

The manual SIGSTOP/SIGKILL timing experiment can miss the intended window.
A saved SUCCEEDED task followed by `recover` only proves stored-result retrieval:
terminal tasks do not recontact the provider. It does not prove recovery from
UNCERTAIN, nor does it prove the VM is still alive.

Use this explicit test-only fault instead:

`--test-crash-after-evidence-task m43-proof`

For that named task only, fleetd exits with code **86** after downloading and
validating the VM completion envelope and before TaskHandler saves its result.
The original intent is already fsynced. The VM remains alive; it is not killed
or paused. The flag is off by default, requires a helper, and is never needed
in a normal daemon. Cached submissions and recovery do not trigger it.

The new experiment uses a 30-minute VM TTL and an immediate normal restart.
No command is automatically replayed: only the daemon process is restarted.

## Preparation

Stop the current test daemon with Ctrl+C in its terminal. Then run this whole
block in that terminal to update, build, load credentials and reconcile the
earlier M4.3 fleet (including already-expired VMs):

```sh
cd ~/src/agent-runtime-lab
git fetch origin
git switch feat/m4-3-completion-evidence
git pull --ff-only origin feat/m4-3-completion-evidence
go build -o /tmp/agentlab-m43-fleetd ./cmd/e2b-fleetd
go build -o /tmp/agentlab-m43-fleetctl ./cmd/e2b-fleetctl
go build -o /tmp/agentlab-m43-taskctl ./cmd/e2b-taskctl
go build -o /tmp/agentlab-fleet-cleanup ./cmd/e2b-fleet-cleanup
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -buildvcs=false -trimpath \
  -ldflags='-s -w' -o /tmp/agentlab-m43-task-agent ./cmd/e2b-task-agent

cd ~/e2b
eval "$(sudo docker compose exec -T ready cat /run/e2b/sdk.env)"
cd ~/src/agent-runtime-lab
/tmp/agentlab-fleet-cleanup --execute \
  --state-file .state/m4-3-fleet.json --timeout 120s
```

Confirm cleanup reports `complete: true` and zero reservations. Preserve the old
journals. The proof paths below are new; do not delete existing journals or
reuse a previously completed proof task to try to trigger another crash.

## Terminal 1: arm the fault and restart immediately after exit 86

Credentials were loaded in this same terminal during preparation. This full
block also loads them explicitly so it works in a fresh shell:

```sh
cd ~/e2b
eval "$(sudo docker compose exec -T ready cat /run/e2b/sdk.env)"
cd ~/src/agent-runtime-lab

if /tmp/agentlab-m43-fleetd --execute \
  --state-file .state/m4-3-proof.json \
  --socket .state/m4-3-proof.sock \
  --task-helper /tmp/agentlab-m43-task-agent \
  --test-crash-after-evidence-task m43-proof \
  --interval 2s --ttl 1800 \
  --renew-ttl --renew-interval 60s \
  --audit-interval 30s --audit-timeout 15s \
  --exec-proxy http://127.0.0.1:3002; then
  printf '%s\n' 'Daemon stopped normally; the test crash was not triggered.'
else
  M43_CRASH_EXIT=$?
  if [ "$M43_CRASH_EXIT" -eq 86 ]; then
    /tmp/agentlab-m43-fleetd --execute \
      --state-file .state/m4-3-proof.json \
      --socket .state/m4-3-proof-recovery.sock \
      --task-helper /tmp/agentlab-m43-task-agent \
      --interval 2s --ttl 1800 \
      --renew-ttl --renew-interval 60s \
      --audit-interval 30s --audit-timeout 15s \
      --exec-proxy http://127.0.0.1:3002
  else
    printf 'Unexpected daemon exit %s; inspect the error before proceeding.\n' "$M43_CRASH_EXIT"
  fi
fi
```

The first process logs `fleet_test_crash_armed`. After the proof task runs,
it logs `fleet_test_crash_after_evidence` and exits 86. The second process
opens the **same fleet/task journals**, marks the interrupted task UNCERTAIN
and serves a **fresh socket without the crash flag**. No PID replacement,
sleeping VM command or stale-socket removal is needed.

## Terminal 2: create the VM and submit the proof task once

```sh
cd ~/src/agent-runtime-lab
/tmp/agentlab-m43-fleetctl --socket .state/m4-3-proof.sock \
  --execute --id taskvm43proof --tenant alpha submit
/tmp/agentlab-m43-fleetctl --socket .state/m4-3-proof.sock status
```

Wait until `taskvm43proof` is RUNNING. Then submit exactly once:

```sh
cd ~/src/agent-runtime-lab
/tmp/agentlab-m43-taskctl --socket .state/m4-3-proof.sock \
  --execute --evidence --task-id m43-proof --id taskvm43proof \
  --artifact /tmp/m43-proof.txt \
  --command 'echo marker >> /tmp/m43-proof.txt; wc -l /tmp/m43-proof.txt' submit
```

A lost-response/EOF error is expected: the daemon deliberately exits before
answering. Do not resubmit to make the command succeed. Terminal 1 automatically
starts the normal daemon on the new recovery socket.

## Terminal 2: prove uncertainty, recover, then inspect the side effect

```sh
cd ~/src/agent-runtime-lab
sleep 2
/tmp/agentlab-m43-taskctl --socket .state/m4-3-proof-recovery.sock \
  --task-id m43-proof status
```

The required precondition is **UNCERTAIN with no result**. An already-SUCCEEDED
record does not satisfy this experiment. After confirming UNCERTAIN:

```sh
cd ~/src/agent-runtime-lab
/tmp/agentlab-m43-taskctl --socket .state/m4-3-proof-recovery.sock \
  --execute --task-id m43-proof recover
/tmp/agentlab-m43-fleetctl --socket .state/m4-3-proof-recovery.sock \
  --execute --id taskvm43proof --command 'wc -l /tmp/m43-proof.txt' exec
```

Required result: SUCCEEDED, matching evidence, a note explicitly saying the
command was not rerun, and line count **1**. Missing or changed evidence stays
UNCERTAIN and never authorizes a new execution. The normal daemon continues
renewing the VM TTL while it remains healthy.

## Cleanup

```sh
cd ~/src/agent-runtime-lab
/tmp/agentlab-m43-fleetctl --socket .state/m4-3-proof-recovery.sock \
  --execute --drain-timeout 120s drain
/tmp/agentlab-m43-fleetctl --socket .state/m4-3-proof-recovery.sock status
```

Confirm deletion and zero reservations, then Ctrl+C in Terminal 1. If the
daemon has stopped, use the standalone cleanup command with credentials:

```sh
cd ~/e2b
eval "$(sudo docker compose exec -T ready cat /run/e2b/sdk.env)"
cd ~/src/agent-runtime-lab
/tmp/agentlab-fleet-cleanup --execute \
  --state-file .state/m4-3-proof.json --timeout 120s
```

Preserve all journals. The earlier `m43-crash` remains the original completed
task; it is not relabelled as a recovered task.
