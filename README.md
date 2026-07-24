# tickbatch

A lock-free, zero-allocation telemetry batching engine for Go.

Designed for high-frequency data pipelines (game telemetry, sensor feeds, financial ticks) where the ingest path must never allocate, never block, and never crash the host process under backpressure.

## Benchmark

```
BenchmarkPush-8   970M ops/s   12.4 ns/op   0 B/op   0 allocs/op
```

Tested on Apple M1, sustained over 68 seconds (~4.84 billion round-trips).

## Install

```bash
go get github.com/curfew-marathon/tickbatch
```

## Quick start

```go
// 1. Define a flat, pointer-free struct and implement Serializable.
type VehicleTelemetry struct {
    RPM      float32
    Speed    float32
    Steering float32
}

func (v VehicleTelemetry) Marshal(buf []byte) int {
    // pack bytes — see Phase 3 for unsafe fast-path
    return 12
}

// 2. Create a batcher and push from any goroutine.
b := tickbatch.New[VehicleTelemetry](tickbatch.Config{
    QueueSize: 1 << 12, // must be power of two
})

b.Push(VehicleTelemetry{RPM: 3500, Speed: 120, Steering: -0.25})
```

`Push` is non-blocking. If the queue is full, the item is silently dropped — the caller is never stalled or panicked.

## Design

| Property | Mechanism |
|---|---|
| Zero allocations | Pre-allocated ring buffer slots; items stored by value |
| Lock-free MPMC | Vyukov sequence-based CAS queue |
| False Sharing eliminated | 64-byte cache-line padding between head and tail cursors |
| GC-invisible | Pointer-free `Serializable` constraint keeps GC scan time O(1) |
| No CGO | Pure Go; cross-compiles without a C toolchain |

## Roadmap

- [x] Phase 1 — Zero-alloc ingest & lock-free ring buffer
- [ ] Phase 2 — Tick engine & zero-alloc drain loop
- [ ] Phase 3 — Bare-metal `unsafe` serialization
- [ ] Phase 4 — Backpressure policies & stress suite
- [ ] Phase 5 — Vectorized 64-bit delta encoding
- [ ] Phase 6 — UDP sink & pluggable compression
