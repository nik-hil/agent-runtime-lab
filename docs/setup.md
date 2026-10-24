# Setup and cleanup

## Host prerequisites

Use a disposable Linux host with cgroups and Docker Engine/CLI. Both runc and runsc must already be registered. Docker access is privileged; grant it to the trusted host runner, never to workloads. Explicitly enabled integration tests fail when either runtime is missing.

Check prerequisites without changing services:

```sh
cd /root/sanduboxes/agent-runtime-lab  # or your own checkout
docker version
docker info --format 'cgroup={{.CgroupVersion}} runtimes={{json .Runtimes}}'
runsc --version
runc --version
cc --version
```

If prerequisites are absent, provision a dedicated host using official [Docker installation](https://docs.docker.com/engine/install/) and [gVisor installation](https://gvisor.dev/docs/user_guide/install/) instructions. Follow [Docker integration](https://gvisor.dev/docs/user_guide/quick_start/docker/) to register runsc there. That host-level step may require a daemon restart; obtain host-owner approval and avoid a host with unrelated workloads. Lab scripts never install packages, edit daemon configuration, restart services or change runtime settings.

## Project-local Go

Use an existing Go 1.23+ compiler, or install this pinned Linux/amd64 release locally. Only a verified official compiler archive is downloaded; no downloaded shell script is executed.

```sh
mkdir -p .tools
curl --fail --location https://go.dev/dl/go1.27.3.linux-amd64.tar.gz --output .tools/go1.27.3.linux-amd64.tar.gz
printf '%s  %s\n' 65efbc80fdc1fa2cd2631cad5b481777a68e8725f34e89f50fc6ac714e379b40 .tools/go1.27.3.linux-amd64.tar.gz | sha256sum --check -
tar -xzf .tools/go1.27.3.linux-amd64.tar.gz -C .tools
.tools/go/bin/go version
```

Other architectures need matching archives/checksums from [Go downloads](https://go.dev/dl/). Record the installed version and checksum. A C compiler enables the race detector; the probe uses CGO_ENABLED=0. There are no Go module dependencies to download.

## Build and reproduce

```sh
./scripts/build.sh
./scripts/test.sh unit
./scripts/test.sh integration
./bin/lab experiments --backend all --output results/experiments.json
./scripts/benchmark.sh 20 3 results/benchmark
./scripts/metadata.sh > results/environment.txt
docker ps -a --filter label=agent-runtime-lab.managed=true
```

The image uses FROM scratch, local source and disabled build networking. Its only copied file is the static probe. results/image.json and results/probe.sha256 identify the build. Caches and local artifacts stay under this project apart from automatically removed host canaries and labeled Docker resources. Scripts are Bash commands for the host; they contain no embedded Go source.

Set `LAB_RESULTS=results/process-limit-investigation` when invoking build/test scripts to retain investigation artifacts separately from the original milestone. The process experiment samples host accounting without mounting any host files into containers; unsupported or inaccessible host readings are reported explicitly. The reproduction commands and archived measurements are in [process-limit-investigation.md](process-limit-investigation.md).

## Cleanup

Stop lab commands first, then inspect and remove only lab resources:

```sh
docker ps -a --filter label=agent-runtime-lab.managed=true
./scripts/cleanup.sh --image
docker ps -a --filter label=agent-runtime-lab.managed=true
```

Omit --image to retain images. With --image, cleanup removes the labeled current image and prunes only dangling images carrying the lab label from earlier builds. It does not prune unrelated containers, images, volumes, networks or shared builder caches, and does not stop Docker. Ordinary build-cache entries may remain.

After preserving evidence, remove generated files from the project root if desired:

```sh
rm -rf -- .tools bin images/probe/lab-probe
```

Results remain under results/. Do not delete the parent directory or its existing AGENTS.md.
