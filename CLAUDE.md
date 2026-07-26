# tickbatch - Agent Instructions & Architecture Guidelines

## Project Identity
- **Name:** tickbatch
- **Language:** Go (1.25+)
- **Purpose:** A lock-free, zero-allocation telemetry batching engine with vectorized delta encoding and pluggable transports.

## 1. Dependency Stance & Standard Library Limits
- **Zero-Dependency Default:** Default to the Go standard library. 
- **Allowed Core Packages:** We rely heavily on `sync/atomic`, `time`, `net`, and `unsafe`.
- **The `unsafe` Mandate:** The `unsafe` package is explicitly ALLOWED and REQUIRED in Phase 3 (bare-metal serialization bypassing `encoding/binary`) and Phase 5 (64-bit vectorized XORing).
- **Pure-Go Only:** CGO is strictly prohibited. The library must cross-compile without external C/C++ toolchains.
- **Burden of Proof:** Any third-party dependency must be justified by extreme, benchmark-proven performance gains (e.g., `github.com/klauspost/compress`).

## 2. Unbreakable Design Principles
1. **Zero-Allocation Hot Path:** The `Push(item T)` method is the primary API. It must be strictly non-blocking and completely free of heap allocations (0 allocs/op). 
2. **Bring Your Own Transport:** The library does not enforce a specific network protocol. All batched outputs must be routed through the `Sink` interface.
3. **Graceful Degradation:** The library must never panic or crash the host application under backpressure. It must silently drop packets or handle overflow according to the user's config, while keeping the main thread unblocked.

## 3. Testing Strategy & Commands
Testing in this repository is heavily focused on concurrency safety and memory profiling.

- **Standard Tests & Race Detection (Mandatory):**
  `go test -v -race ./...`
  *Rule:* No code is merged if the race detector flags an issue.

- **Memory & Allocation Benchmarks:**
  `go test -bench=. -benchmem ./...`
  *Rule:* The `Push()` benchmark must register `0 B/op` and `0 allocs/op`. This is an automated CI gate - the Zero-Allocation Enforcer job parses benchmark output and fails the build on any `allocs/op > 0`. It is not just a guideline.

- **CI Platform Matrix:** Tests run on all three targets - ubuntu-latest (amd64), macos-latest (arm64), windows-latest (amd64). Low-level `unsafe` operations and atomic alignment must be verified across architectures.

- **Linting:**
  `golangci-lint config verify && golangci-lint run`
  *Config:* `.golangci.yml` controls all linting behaviour. Always run `config verify` first - the CI action runs it as a strict preflight and will fail on schema errors that `run` silently ignores.
  *Enabled linters (beyond the standard set):* `gocritic`, `godot`, `revive`, `misspell`, `prealloc`, `unconvert`, `wastedassign`.
  *Intentional suppressions - do not remove:*
  - `gosec` unsafe warnings: `unsafe` is mandated by this project (see §1).
  - `revive` unused-parameter on `ringbuf.go`: the `_ [64]byte` padding fields are intentional false-sharing guards, not dead code.

## Code Style & Conventions
- **Generics Constraint:** ALWAYS use the generic constraint `[T Serializable]` for payload ingestion. Do not use `[T any]`. This ensures the compiler enforces our zero-allocation byte-packing contract.
- **Error Handling:** Background goroutines must not swallow errors. Route errors to the caller via channels or a structured logging interface.
- **GoDoc presence:** Every exported identifier MUST have a GoDoc comment (enforced by `revive` with `checkPrivateReceivers`).
- **GoDoc punctuation:** Every doc comment sentence must start with a capital letter and end with a period (enforced by `godot`, scope: declarations).

## Definition of Done
A feature or PR is only complete if:
1. `go test -v -race ./...` passes.
2. The benchmark confirms zero allocations on the `Push()` ingest method.
3. `golangci-lint config verify && golangci-lint run` reports no issues.
4. No CGO dependencies were introduced.
5. The behavior under backpressure/queue-full states is explicitly tested and verified.

## Git Commit Standard (Conventional Commits)
You must strictly adhere to the [Conventional Commits](https://www.conventionalcommits.org/) specification for all generated commit messages. This repository relies on structured commit history for automated changelogs and release versioning.

**Format:**
`<type>[optional scope]: <description>`

**Allowed Types:**
* `feat`: A new feature or major phase completion (e.g., `feat(engine): implement vectorized delta encoding`).
* `perf`: A code change specifically targeting throughput or memory optimization (e.g., `perf(ringbuf): remove drainSlice to eliminate L1 cache double-copy`).
* `fix`: A bug fix or pipeline repair (e.g., `fix(ci): correct golangci-lint schema validation`).
* `refactor`: A code change that neither fixes a bug nor adds a feature, but improves structure.
* `test`: Adding missing tests or correcting existing tests (e.g., `test(fuzz): add boundary fuzzer for tail-byte fallback`).
* `docs`: Documentation only changes (e.g., `docs(readme): pivot use cases to HFT and L2 market data`).
* `chore`: Build process or auxiliary tool changes.

**Strict Rules:**
1. The description must be written in the imperative, present tense (e.g., "add" not "added" or "adds").
2. Do not capitalize the first letter of the description.
3. Do not place a period at the end of the description.