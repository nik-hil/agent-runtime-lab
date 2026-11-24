#!/usr/bin/env bash
# One release gate for this repository. No service installation or credential output.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
mode="${1:-e2b}"
if [[ $# -gt 1 || ( "$mode" != e2b && "$mode" != all && "$mode" != unit && "$mode" != quorum && "$mode" != cluster ) ]]; then
  echo "usage: ./scripts/verify.sh [unit|e2b|all|quorum|cluster]" >&2
  exit 2
fi
if [[ "$mode" != unit && "$mode" != quorum && -z "${E2B_API_KEY:-}" ]]; then
  echo "E2B_API_KEY is required; load sdk.env from the running E2B Compose project first." >&2
  exit 1
fi
umask 077
mkdir -p results
report_dir="$(mktemp -d "$LAB_ROOT/results/verify-XXXXXXXX")"
echo "Verification logs: $report_dir"
if [[ "$mode" == all || "$mode" == quorum || "$mode" == cluster ]] && ! docker info >/dev/null 2>&1; then
  if ! docker_executable="$(type -P docker)"; then
    echo "Docker CLI is required for $mode mode." >&2
    exit 1
  fi
  if ! sudo -n -- "$docker_executable" info >/dev/null 2>&1; then
    echo "Docker is unavailable through both direct access and passwordless sudo; no password was requested." >&2
    exit 1
  fi
  # Go tests launch Docker directly, so a shell function cannot route their
  # subprocesses. Use a private executable wrapper inherited through PATH.
  mkdir -m 700 "$report_dir/docker-bin"
  cat > "$report_dir/docker-bin/docker" <<'DOCKER'
#!/bin/sh
exec sudo -n -- "$LAB_DOCKER_EXECUTABLE" "$@"
DOCKER
  chmod 700 "$report_dir/docker-bin/docker"
  export LAB_DOCKER_EXECUTABLE="$docker_executable"
  export PATH="$report_dir/docker-bin:$PATH"
  echo "Using passwordless sudo for Docker commands; verification remains under your user."
fi
formatting="$(gofmt -l src cmd tests)"
if [[ -n "$formatting" ]]; then
  echo "Formatting failed:" >&2
  echo "$formatting" >&2
  exit 1
fi
go vet ./... 2>&1 | tee "$report_dir/vet.log"
LAB_INTEGRATION=0 LAB_E2B_INTEGRATION=0 LAB_CLUSTER_INTEGRATION=0 LAB_M5_E2B_INTEGRATION=0 CGO_ENABLED=1 \
  go test -race -count=1 -timeout 15m ./... 2>&1 | tee "$report_dir/unit.log"
if [[ "$mode" == all ]]; then
  ./scripts/build.sh 2>&1 | tee "$report_dir/build.log"
  LAB_INTEGRATION=1 LAB_E2B_INTEGRATION=0 CGO_ENABLED=1 \
    go test -race -count=1 -timeout 15m -v ./tests 2>&1 | tee "$report_dir/runtimes.log"
fi
if [[ "$mode" == e2b || "$mode" == all || "$mode" == cluster ]]; then
  LAB_INTEGRATION=0 LAB_E2B_INTEGRATION=1 CGO_ENABLED=1 \
    go test -race -count=1 -timeout 20m -v ./tests -run '^TestE2B(FullLifecycle|AsyncJobs)$' 2>&1 | tee "$report_dir/e2b.log"
fi
if [[ "$mode" == quorum || "$mode" == cluster || "$mode" == all ]]; then
  LAB_INTEGRATION=0 LAB_E2B_INTEGRATION=0 LAB_CLUSTER_INTEGRATION=1 LAB_M5_E2B_INTEGRATION=0 CGO_ENABLED=1 \
    go test -race -count=1 -timeout 10m -v ./tests -run '^TestM5(Quorum|Protocol|PendingFailover)$' 2>&1 | tee "$report_dir/quorum.log"
fi
if [[ "$mode" == cluster || "$mode" == all ]]; then
  LAB_INTEGRATION=0 LAB_E2B_INTEGRATION=0 LAB_CLUSTER_INTEGRATION=0 LAB_M5_E2B_INTEGRATION=1 CGO_ENABLED=1 \
    go test -race -count=1 -timeout 25m -v ./tests -run '^TestM5Cluster$' 2>&1 | tee "$report_dir/cluster.log"
fi
echo "PASS: requested verification mode $mode; logs preserved in $report_dir"
