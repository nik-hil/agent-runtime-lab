#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
if [[ "${1:-}" != "" && "${1:-}" != "--image" ]]; then
  echo "usage: scripts/cleanup.sh [--image]" >&2
  exit 2
fi
# Stop all lab invocations first. No global prune or service changes.
docker info --format 'Cleaning lab resources on Docker {{.ServerVersion}}'
owned="$(docker ps -aq --filter label=agent-runtime-lab.managed=true)"
if [[ -n "$owned" ]]; then
  mapfile -t ids <<< "$owned"
  docker rm --force --volumes "${ids[@]}"
fi
if [[ "${1:-}" == "--image" ]]; then
  current="$(docker image ls --quiet --filter reference=agent-runtime-lab:local)"
  if [[ -n "$current" ]]; then
    owner="$(docker image inspect --format '{{index .Config.Labels "agent-runtime-lab.managed"}}' agent-runtime-lab:local)"
    [[ "$owner" == "true" ]] || { echo "Refusing to remove an image without the lab label" >&2; exit 1; }
    docker image rm agent-runtime-lab:local
  fi
  docker image prune --force --filter label=agent-runtime-lab.managed=true
fi
docker ps -a --filter label=agent-runtime-lab.managed=true --format '{{.Names}} {{.Status}}'
