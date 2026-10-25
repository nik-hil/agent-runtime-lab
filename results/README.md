# Recorded experiment results

This directory is committed so a fresh clone includes the actual execution evidence. Files record their original runs; publishing them does not represent a new execution. Reports explain the environment, expectations, failures and limits of each investigation.

## Where to look

| Path | Contents |
| --- | --- |
| [`experiments.json`](experiments.json), `integration-*.json`, `integration-tests.log` | Original milestone's six experiments and runtime integration results. See [the milestone report](../docs/milestone-1-report.md). |
| [`benchmark-final/`](benchmark-final/) | Original milestone's final benchmark samples, summary and environment. `benchmark/` retains an earlier run. |
| [`filesystem-only.json`](filesystem-only.json) | Separate filesystem-only run using both runtimes. |
| [`process-limit-investigation/observations.json`](process-limit-investigation/observations.json) | Final six-case process-limit investigation, with exact errors and host/virtual accounting. See [the investigation](../docs/process-limit-investigation.md). |
| [`task-completion/task-success.json`](task-completion/task-success.json) | A: complete artifact independently verified on both runtimes. |
| [`task-completion/task-partial.json`](task-completion/task-partial.json) | B: partial artifact observed after child failure, then absent in a fresh sandbox. |
| [`task-completion/task-retry.json`](task-completion/task-retry.json) | C: persisted commit, receiver restart, safe retries, concurrency and identity-conflict checks. |
| [`task-completion/integration-tests.log`](task-completion/integration-tests.log), `task-completion/integration-*.json` | Full Milestone 1B race-enabled suite, including all original experiments. See [the task-completion report](../docs/task-completion-investigation.md). |

`first-integration/`, `continuation-verification/`, `process-only.json`, `process-limit-investigation/initial-observations.json` and `task-completion/development/` preserve earlier or diagnostic runs. These may include failures and older probe behavior; do not treat them as final successful verification. Use the reports and final files above for conclusions.

## Verify and reproduce

Environment files, image metadata, probe/source checksums, timestamps and cleanup-query outputs accompany the runs. Empty `remaining-*.txt` files mean that the corresponding query found no matching lab resources. Raw records retain ephemeral lab IPs, container names and runtime diagnostics; concise sanitized task evidence is also available under [`docs/evidence/task-completion/`](../docs/evidence/task-completion/).

From the repository root, verify the committed Milestone 1B snapshot:

```sh
sha256sum --check docs/evidence/task-completion/raw.sha256
jq '.[] | {backend,experiment,status,task}' results/task-completion/task-retry.json
```

Follow the linked reports for exact build and reproduction commands. Running them again can overwrite these paths and produce Git changes. New names, timings and raw hashes are expected; compare verified task states and committed operations. Historical source manifests describe the source revision used for that run, which can differ from the latest checkout. Toolchains, caches and binaries belong outside this evidence directory and remain ignored.
