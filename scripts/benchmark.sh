#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
iterations="${1:-20}"
warmups="${2:-3}"
out="${3:-results/benchmark-$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$out"
scripts/metadata.sh > "$out/environment.txt"
image_id="$(docker image inspect --format '{{.Id}}' agent-runtime-lab:local)"
# Keep raw failures, but do not produce a successful benchmark summary for them.
bin/lab bench --backend all --image "$image_id" --n "$iterations" --warmup "$warmups" --output "$out/samples.json"
bin/lab-summary "$out/samples.json" | tee "$out/summary.md"
