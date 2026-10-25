#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
export LAB_RESULTS="${LAB_RESULTS:-$LAB_ROOT/results}"
mkdir -p "$LAB_RESULTS"
export LAB_RESULTS="$(cd "$LAB_RESULTS" && pwd)"
case "${1:-integration}" in
  integration) export LAB_INTEGRATION=1; log="$LAB_RESULTS/integration-tests.log" ;;
  unit) export LAB_INTEGRATION=0; log="$LAB_RESULTS/unit-tests.log" ;;
  *) echo "usage: scripts/test.sh [unit|integration]" >&2; exit 2 ;;
esac
go vet ./...
go test -race -count=1 -cover -v ./... 2>&1 | tee "$log"
