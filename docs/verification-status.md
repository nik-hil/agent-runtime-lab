# Verification status

M5 adds multi-server scheduling, ownership fencing, conservative failover and
replicated artifact storage. See [the full gate and operating guide](m5-multi-server.md).
Its live gate is `./scripts/verify.sh cluster`; M4.4's earlier operator pass is
not evidence of an M5 live pass. The gVisor/runsc registration issue remains a
separate prerequisite for `verify.sh all`.

## 2026-11-24: ubuntu-c-8-sfo2

The operator ran `./scripts/verify.sh all` on branch
`feat/m4-3-completion-evidence`, commit
`d3cca3f032ea1931e2fd14c0da13170d9dc816ae`.
Logs: `results/verify-qVl5MFbf` on that server.

| Check | Observed result |
| --- | --- |
| Formatting, vet, repository unit/race tests | Passed |
| Passwordless Docker routing | Passed; Docker commands used the existing root `sudo -n` route |
| Probe image build | Passed; image tagged `agent-runtime-lab:local` |
| Docker server | 29.1.3; registered runtimes `io.containerd.runc.v2`, `runc` |
| Container integration | Blocked: `required runtime runsc is not registered` |
| Dedicated live E2B gate | Not reached because all mode stopped at container integration |

The buildx deprecation message did not fail the image build. The E2B test's
SKIP inside the container test phase is expected: that phase disables live E2B;
the dedicated E2B phase was never reached. This is not a full verification pass.
The earlier operator-provided named-crash recovery proof for `m43-proof` did
succeed and observed a single marker; it does not replace the consolidated gate.

The intended lab setup keeps `agentlab` out of the Docker group and uses the
existing passwordless sudo rule. Do not use `sudo -u agentlab -g docker` or ask
for a password. The verifier automatically uses a private per-run Docker CLI
wrapper with `sudo -n` when direct access fails. No passwords, sudoers, runtime
registrations or Docker services were changed to bypass this failure.

Use `./scripts/verify.sh e2b` for the current Embed milestones. It still runs
formatting, vet and all repository unit/race tests, then the required live E2B
gates. It does not claim live Docker/gVisor validation. Full all-mode validation
remains outstanding until `runsc` is separately installed/configured for Docker.
See [the complete verification commands](full-verification.md).

## M4.4 additions

The operator subsequently reported "everything passed" after the supplied
M4.4 `./scripts/verify.sh e2b` command block. Record the M4.3 and M4.4 live gates
as operator-confirmed passed on 2026-11-24. No new runsc-registration evidence
was supplied, so the separate all-mode container blocker remains outstanding.

The same e2b/all entrypoints now include `TestE2BAsyncJobs` after the M4.3 gate.
It uses its own private journal, verifies queued restart, VM serialization,
queued/running cancellation, failed-job evidence, and named exit 87 after
artifact fsync but before host completion. It then deletes the VM before
explicit recovery and downloads both earlier and recovered artifacts from
host storage. Successful unit/CI checks do not count as a live E2B pass.

This editing workspace cannot create Unix sockets (`operation not permitted`).
Targeted in-process and filesystem tests run here; GitHub CI must run the full
unit/race suite including existing Unix-socket tests. No socket tests are
silently skipped to claim success.
