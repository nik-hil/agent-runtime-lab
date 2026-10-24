#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
mkdir -p results
case "${1:-integration}" in
  integration) export LAB_INTEGRATION=1; log=results/integration-tests.log ;;
  unit) export LAB_INTEGRATION=0; log=results/unit-tests.log ;;
  *) echo "usage: scripts/test.sh [unit|integration]" >&2; exit 2 ;;
esac
export LAB_RESULTS="$LAB_ROOT/results"
go vet ./...
go test -race -count=1 -cover -v ./... 2>&1 | tee "$log"
