# Milestone report: <date / revision>

## Environment and provenance

- OS, kernel, architecture, CPU model/count and RAM:
- Docker, runc, runsc, Go versions; cgroup version/driver:
- runsc platform and nondefault settings:
- Source revision or source checksum manifest:
- Probe SHA256 and immutable image ID:
- Host load, storage driver and known background work:
- Exact commands, UTC timestamps and evidence paths:

## Verification

| Check | Runtime | Expected | Actual | Status | Evidence |
| --- | --- | --- | --- | --- | --- |
| Unit tests / race detector / vet | host | pass | unmeasured | not run | |
| Filesystem | runc / runsc | see experiments.md | unmeasured | not run | |
| Network | runc / runsc | control connects; none denied | unmeasured | not run | |
| Memory | runc / runsc | control succeeds; restricted OOM | unmeasured | not run | |
| Processes | runc / runsc | bounded refusal | unmeasured | not run | |
| Startup | runc / runsc | no-op exits zero | unmeasured | not run | |
| Cleanup | runc / runsc | timeout; absent container; fresh tmp | unmeasured | not run | |

Separate infrastructure errors from workload failures. List skipped checks explicitly. Never replace unavailable values with zero.

## Benchmark observations

- Iterations, excluded warmups, order, image state and timer definitions:
- Attach-to-exit and total: mean, nearest-rank p50/p95, min/max:
- Optional workload times and resource sample availability:
- Raw samples and summary artifact:

## What worked, what failed, what we learned

Describe observed outcomes, anomalies and fixes. Distinguish evidence from explanations or hypotheses. State untested cases and why. Do not infer escape resistance or production readiness from these probes.

## Cleanup and remaining work

Record remaining resources, cleanup command/result, unresolved failures and next experiments. State whether unrelated projects/services were changed.
