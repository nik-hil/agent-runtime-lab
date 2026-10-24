#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
printf 'UTC: '
date -u +%FT%TZ
uname -a
awk -F ': ' '/model name/ { print "CPU model: " $2; exit }' /proc/cpuinfo
printf 'Host load: '
cat /proc/loadavg
go version
printf 'Probe SHA256: '
sha256sum images/probe/lab-probe
docker version
docker info --format 'OS={{.OperatingSystem}} kernel={{.KernelVersion}} arch={{.Architecture}} cpus={{.NCPU}} memory_bytes={{.MemTotal}} cgroup={{.CgroupVersion}} driver={{.CgroupDriver}} storage={{.Driver}} default_runtime={{.DefaultRuntime}} security={{json .SecurityOptions}}'
docker image inspect --format 'image_id={{.Id}}' agent-runtime-lab:local
if command -v runsc >/dev/null; then runsc --version; fi
if command -v runc >/dev/null; then runc --version; fi
