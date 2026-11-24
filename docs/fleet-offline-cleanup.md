# Clean up a fleet when its daemon is stopped

`fleetctl drain` sends an API request to a running fleet daemon. A stopped
daemon cannot receive it, even when its old Unix socket file remains on disk.
`connection refused` is not evidence that the VMs were deleted. Removing the
socket also does not delete a VM or release journal reservations.

Use the explicit **daemon-independent cleanup** command when fleetd is stopped:

```sh
cd ~/src/agent-runtime-lab
git fetch origin
git switch feat/m4-3-completion-evidence
git pull --ff-only origin feat/m4-3-completion-evidence
go build -o /tmp/agentlab-fleet-cleanup ./cmd/e2b-fleet-cleanup

cd ~/e2b
eval "$(sudo docker compose exec -T ready cat /run/e2b/sdk.env)"
cd ~/src/agent-runtime-lab
/tmp/agentlab-fleet-cleanup --execute \
  --state-file .state/m4-2-verify.json --timeout 120s
```

Replace the state-file only when intentionally selecting another test fleet.
There is **no default state-file** and no socket argument. The command needs
the existing E2B credentials because it talks to the control API directly.
It does not need the task helper, a task-journal migration or a running daemon.

Expected output includes `complete: true`, all selected journal jobs DELETED,
zero CPU/RAM/pending reservations, and:

```text
PASS: selected journal workloads confirmed deleted; CPU=0 MiB=0 pending=0. Journals preserved.
```

An already-expired VM is handled by observing provider absence. Cleanup never
assumes deletion from elapsed TTL, a stopped daemon or a successful DELETE ACK.
Repeating the command on an already-cleaned journal is safe and returns success.

## Ownership, scope and failure handling

- The command takes the same exclusive fleet-journal lock as fleetd. If the
  daemon or another cleanup process still owns it, cleanup refuses to proceed.
  Stop the correct daemon or use its normal `fleetctl drain` endpoint instead.
- It restores the journal's recorded resource policies and template shape,
  validates the state, and atomically persists the drain fence and every delete
  intent **before** provider mutations. It never dispatches queued work, creates
  new VMs, resumes them or runs user commands.
- Only workload identities in the selected journal are eligible for cleanup.
  It does not scan and delete other fleets, unknown VMs or provider-wide assets.
- Known VMs are reconciled individually. A failing provider request for one
  workload cannot prevent cleanup of another workload in the same sweep.
- A pending ambiguous Create is looked up by its original metadata key. One
  unique match can be bound and deleted. A complete zero/multiple-match result
  remains fenced with reservations retained; it never authorizes a new Create
  or deletion of all matches. That ambiguity is not repeatedly polled in the
  same invocation. Investigate before explicitly trying cleanup again.
- Provider errors or a verification timeout produce a nonzero exit and a report
  of unresolved workloads. Saved delete intent survives interruption; rerunning
  the same cleanup continues from that state. `--timeout` is bounded to 1s..10m;
  per-workload polling defaults to 500ms and is bounded to 100ms..5s.
- Fleet/task journals, lock files and socket paths are preserved. No manual
  socket removal is needed for this cleanup path. It refuses a missing, corrupt
  or symlinked state-file rather than initializing an empty replacement fleet.

A drained journal stays drained and cannot admit new work through fleetd.
Use a fresh journal/socket pair for the next experiment. Retain old task
records, including UNCERTAIN tasks, as evidence of what was and was not observed.

This cleanup removes the need to restart a stopped daemon just to ask it to
drain. Ctrl+C and process termination remain non-destructive; they do not
implicitly delete workloads or prove that the provider cleaned them up.
