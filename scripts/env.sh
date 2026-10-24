#!/usr/bin/env bash
# Source from project tooling; confine Go caches and configuration to this lab.
LAB_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ -x "$LAB_ROOT/.tools/go/bin/go" ]]; then
  export PATH="$LAB_ROOT/.tools/go/bin:$PATH"
fi
export GOCACHE="$LAB_ROOT/.tools/go-cache"
export GOMODCACHE="$LAB_ROOT/.tools/go-mod"
export GOTOOLCHAIN=local
export GOENV=off
cd "$LAB_ROOT"
