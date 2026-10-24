#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
command -v go >/dev/null
docker info --format 'Docker {{.ServerVersion}}; runtimes={{range $key, $value := .Runtimes}}{{$key}} {{end}}'
lab_results="${LAB_RESULTS:-results}"
mkdir -p bin "$lab_results"
go build -trimpath -buildvcs=false -o bin/lab ./cmd/lab
go build -trimpath -buildvcs=false -o bin/lab-summary ./cmd/lab-summary
CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false -ldflags='-s -w -buildid=' -o images/probe/lab-probe ./cmd/lab-probe
docker build --network=none --pull=false --tag agent-runtime-lab:local images/probe
docker image inspect agent-runtime-lab:local > "$lab_results/image.json"
sha256sum images/probe/lab-probe > "$lab_results/probe.sha256"
find src cmd tests scripts images/probe -type f \( -name '*.go' -o -name '*.sh' -o -name Dockerfile \) -print0 | sort -z | xargs -0 sha256sum > "$lab_results/source.sha256"
sha256sum go.mod Makefile >> "$lab_results/source.sha256"
