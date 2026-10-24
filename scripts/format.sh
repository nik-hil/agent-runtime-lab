#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
gofmt -w src cmd tests
