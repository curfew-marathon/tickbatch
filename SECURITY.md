# Security Policy

## Supported versions

| Version | Supported |
|---------|-----------|
| v1.x    | Yes       |
| v0.x    | No        |

## Reporting a vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Email **hertyk.estate@gmail.com** with the subject line `[tickbatch] Security Report`. Include:

- A description of the vulnerability and its potential impact.
- Steps to reproduce or a minimal proof-of-concept.
- The tickbatch version and Go version affected.

You will receive an acknowledgment within 72 hours. Please allow up to 14 days for a fix before
any public disclosure.

## Scope

tickbatch is a pure-Go, zero-CGO library with no network listener or server component. The primary
attack surface is:

- The `unsafe.Pointer` serialization path in `Serializable` implementations.
- The `Sink.Flush` contract - callers must not retain the payload slice beyond the call.
- Delta encoding desync when `Config.DeltaEncoding` is enabled over an unreliable transport.
