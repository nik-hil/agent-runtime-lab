# Milestone 1B evidence

Verified at **2026-10-24T20:00:38Z**. These are real local executions of both runc and runsc task backends, with a trusted runc receiver. See [the investigation](../../task-completion-investigation.md) for the commands, interpretation and limitations.

- `summary.json`: sanitized projection of the three final manual experiment outputs. It retains all 20 task attempts, exact artifact bytes, task IDs/keys, event phases, receipts, ledger counts/final records, failure diagnostics and receiver-restart checks. Private endpoint IPs are replaced with `<lab-internal-ip>`; random container/network/volume names and host-identifying metadata are omitted. Null means not applicable or unavailable, not a measured false/zero.
- `integration-tests.log`: full successful vet/race integration run. No integration tests were skipped. All original experiments and the new assertions passed; the integration package reported 60.035 seconds. Coverage is per Go test package; the separate static probe image is not coverage-instrumented.
- `source.sha256`: hashes of the actual Go sources, tooling, build inputs and module configuration tested. No source changed between this build, the passing integration run and final manual runs.
- `probe.sha256`: the source-built workload binary hash. The image ID was `sha256:98a408c71b48614324ad548331b49ca481094afaff268e7329aefeb4dd4c3ef9`.
- `raw.sha256`: hashes of the committed raw scenario/integration JSON, test log and final resource queries under `results/task-completion/`.
- `verified-at.txt`: capture timestamp.

Environment: Linux 6.8.0-142-generic, amd64, Ubuntu 24.04.5 LTS, Docker 29.1.3, cgroup v2/systemd, overlayfs storage, runc 1.3.4-0ubuntu1~24.04.1, runsc release-20261019.0, Go 1.27.3. Existing AppArmor/seccomp defaults were retained; no daemon settings or services changed.

The three `remaining-*.txt` raw query outputs were all empty: no lab containers, networks or volumes remained. Empty files share the standard SHA256 of an empty input. Evidence is exported before the receiver's volume is intentionally removed.

From the repository root, verify the checked-out source and locally retained raw snapshot:

```sh
sha256sum --check docs/evidence/task-completion/source.sha256
sha256sum --check docs/evidence/task-completion/raw.sha256
jq -e 'all(.observations[]; .status == "passed" and .cleanup_confirmed and (.duplicate_side_effects == false))' docs/evidence/task-completion/summary.json
```

A new clone includes both this summary/log and the full raw evidence in [results/task-completion/](../../../results/task-completion/); compiled binaries remain excluded. Recreate raw outputs using the investigation's build/test and three individual experiment commands. New raw hashes will differ because resource names, endpoint IPs and timings change; compare task states and ledger contents, not byte identity. No measurements from the preliminary `results/task-completion/development/` runs are counted as final verification.
