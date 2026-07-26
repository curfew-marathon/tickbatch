# Contributing to tickbatch

Contributions are welcome. Before opening a pull request, run the full gate locally to confirm
all checks pass.

## Quality gate

```bash
# Race detector + all tests (mandatory)
go test -v -race ./...

# Zero-allocation enforcement — Push must show 0 allocs/op
go test -bench=. -benchmem ./...

# Linter — config verify is a strict preflight; run it first
golangci-lint config verify && golangci-lint run
```

A pull request is not mergeable if any of the following are true:

- The race detector flags an issue.
- `BenchmarkPush` reports `allocs/op > 0`.
- `golangci-lint run` reports any issue.
- CGO was introduced.
- Backpressure behavior under a full queue is untested.

## Commit messages

This repository uses [Conventional Commits](https://www.conventionalcommits.org/) for automated
changelog generation and release versioning.

**Format:** `<type>[optional scope]: <description>`

Allowed types: `feat`, `perf`, `fix`, `refactor`, `test`, `docs`, `chore`.

Rules: imperative present tense (`add`, not `added`), no capital first letter, no trailing period.

## Security

To report a vulnerability, see [SECURITY.md](SECURITY.md).
