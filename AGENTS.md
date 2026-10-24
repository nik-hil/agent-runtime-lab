# Repository Guidelines

## Required Workspace Rules

Before working in this project or any subdirectory, read and follow the [project Go development rules](.cursor/rules/go-development.mdc). In the original workspace, also read and follow `../.cursor/rules/go-development.mdc` when it exists. The project copy preserves the workspace rules for standalone clones. The Go rules below repeat those requirements for agents that load only this `AGENTS.md`. Keep these copies aligned.

## Go Development Rules

Write application code directly in `.go` files with simple, idiomatic Go. Use Go for the runner, probes, experiments, and tests. Edit files with direct file-editing tools and ordinary patches. Do not use Python, JavaScript, shell scripts, or other languages to generate, modify, or write Go source. Never embed Go source in wrapper scripts. If code generation becomes necessary, explain why first.

Shell tooling may invoke existing Go commands; it must not contain or generate Go source.

Run `gofmt` on all modified Go files. Run `go test ./...` and `go vet ./...` directly in the terminal when appropriate. Prefer the project-local compiler in `.tools/go/bin/` when installed; keep Go caches inside `.tools/`.

Do not ask for approval for routine edits or Go commands that current permissions allow. Briefly explain additional permissions when required. Show meaningful changes as readable diffs in fenced `diff` code blocks.

## Project Structure

`src/sandbox/` contains the common interface and Docker/runc and gVisor/runsc adapters. `src/experiments/` defines the six experiments. `cmd/` contains the CLI, probe, and benchmark summary. `tests/` contains integration tests; `docs/`, `scripts/`, and `images/` contain design notes and reproducible tooling. Generated artifacts belong in ignored `bin/`, `.tools/`, or `results/` paths.

## Build and Verification

Use `scripts/build.sh`, `scripts/test.sh unit`, and `scripts/test.sh integration`. Real-runtime tests require both runtimes and must fail explicitly when infrastructure is unavailable. Use `scripts/benchmark.sh` for raw samples and reproducible summaries. Never fabricate measurements or treat skipped integration tests as verified behavior.

## Safety and Scope

Keep least-privilege defaults. Never expose a Docker socket or sensitive host directories to workloads. Do not execute arbitrary downloaded scripts, alter unrelated projects or system services, or add deferred runtimes and orchestration systems. Preserve the parent `AGENTS.md`. Document isolation limitations and report actual failures.
