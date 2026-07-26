# tickbatch — zero-allocation, lock-free telemetry batching for Go

## The zero-allocation, lock-free telemetry & compliance exhaust pipe for Go.

Feed it a firehose of risk events, audit records, or market data telemetry. It absorbs everything lock-free into a pre-allocated ring buffer and drains to your transport at a rock-steady Hz — without ever touching the heap, blocking the producer, or coupling your hot thread to downstream I/O.

[![Build Status](https://img.shields.io/github/actions/workflow/status/curfew-marathon/tickbatch/ci.yml?branch=main)](https://github.com/curfew-marathon/tickbatch/actions)
[![Go Version](https://img.shields.io/badge/go-1.25%2B-00ADD8?logo=go)](https://go.dev)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![GoDoc](https://pkg.go.dev/badge/github.com/curfew-marathon/tickbatch.svg)](https://pkg.go.dev/github.com/curfew-marathon/tickbatch)

**`0 B/op` · `0 allocs/op` · ~80M pushes/sec · zero deps · zero CGO · 2 fuzz harnesses**

---

## The Problem

High-throughput Go services generate telemetry events — risk snapshots, audit log entries, compliance records, structured traces — at rates that make naive emission untenable.

A risk engine emitting 50,000 position updates per second to a compliance bus. An order management system stamping every order lifecycle event to a durable audit store. A market data aggregator forwarding telemetry to a monitoring pipeline. They all share the same failure mode: the producer goroutine stalls waiting on a downstream write that is blocked on a full TCP buffer, a slow disk, or a GC pause triggered by the serialization layer.

The naive fixes trade one failure mode for another:

- A `make([]byte, n)` allocation per emission is a future GC pause waiting to happen.
- A mutex between the producer and the flusher is a direct coupling of your hot path to network I/O latency.
- A channel blocks under burst load. A `sync.Pool` helps on average but guarantees nothing.

You patch one leak and spring another. The producer stalls. The p99 latency climbs.

| Metric | `tickbatch` | Buffered `chan` | `sync.Pool` + mutex |
|---|---|---|---|
| Allocs per push | **0** | 0 | 0 (hit) / 1 (miss) |
| Blocks producer under burst | **Never** | Yes (full channel) | Yes (lock contention) |
| Producer/IO coupling | **None** | Goroutine + scheduler | Lock held during I/O |
| Backpressure policy | **Drop / evict** | Block caller | Block caller |
| MPMC out of the box | **Yes** | Yes | No |

---

## The Solution

tickbatch is a zero-impact exhaust pipe. It completely decouples the event producer from the downstream transport.

**The producer never touches the network. The network never touches the producer.**

The ingest path is a single atomic compare-and-swap against a pre-allocated ring buffer slot. No locks. No channels. No goroutine handoffs. No heap activity. If the buffer is full, the new item is silently dropped (or, when `DropOldest` is configured, the oldest item is evicted) — the caller is never stalled, never panicked, and never blocked behind a slow Kafka producer or a saturated UDP socket.

A background goroutine drains the ring at a fixed Hz, serializes directly into a pre-allocated byte buffer via the `Serializable` interface, and hands the batch to your `Sink`. One function call to your transport. Zero allocations. The GC has nothing to scan on the hot path.

**The heap stays flat. The GC stays quiet. Your producer thread is never the bottleneck.**

---

## Features

- **Zero-Allocation Hot Path.** `Push` is a single compare-and-swap. Verified `0 B/op, 0 allocs/op` by a CI gate that parses benchmark output and fails the build on any regression.
- **Lock-Free MPMC Ring Buffer.** The Dmitry Vyukov sequence-based algorithm. No mutexes. Multiple producers, single consumer. Scales to any number of pushing goroutines.
- **Cache-Line Padding.** The head and tail cursors are physically separated by 64 bytes of padding. They live on different CPU cache lines. False sharing between producer and consumer cores is structurally impossible.
- **Bare-Metal `unsafe` Serialization.** The `Serializable` interface encodes your struct directly into a caller-supplied `[]byte` via `unsafe.Pointer` casting. No `encoding/binary`. No reflection. C-level throughput.
- **Bring Your Own Transport.** The `Sink` interface is a single method: `Flush(payload []byte) error`. UDP, TCP, shared memory, Kafka: anything goes.
- **Graceful Shutdown.** Canceling the context triggers a final drain: remaining ring-buffer items are serialized and flushed before the goroutine exits. No records are silently abandoned on shutdown.
- **Pluggable Compression.** The optional `Compressor` interface lets you apply `zstd`, `lz4`, or any codec to each batch payload inside the pre-allocated compress buffer — zero additional allocations.
- **Vectorized Delta Encoding.** Optional XOR-delta mode diffs each batch against the previous frame using 64-bit word-level vectorization via `unsafe.Slice`, then falls back to a byte-wise tail loop for non-8-byte-aligned payloads.
- **Pure Go.** Zero CGO. Zero external dependencies. Cross-compiles to every GOOS/GOARCH without a C toolchain.

---

## Benchmarks

### Baseline (no race detector)

The raw ingest cost on a single core. This is what your hot path pays.

```
$ go test -bench=BenchmarkPush -benchmem -benchtime=30s ./...

goos: darwin
goarch: arm64
pkg: github.com/curfew-marathon/tickbatch
cpu: Apple M1

BenchmarkPush-8    93002511    12.44 ns/op    0 B/op    0 allocs/op
```

**80 million pushes per second. Zero heap activity. Ever.**

At 12.44 ns/op, the throughput is ~80M pushes/sec. The iteration count (93,002,511) reflects the total samples collected over the 30-second bench run — divide by the bench duration, not the iteration count, to get per-second throughput.

### Stress Test (race detector enabled, 8 cores, 3 runs each)

The same benchmark under Go's race detector, which instruments every atomic operation with shadow memory writes. This is the worst-case latency floor. The allocation count does not move.

```
$ go test -bench=BenchmarkPush -benchmem -benchtime=30s -count=3 -cpu=1,2,4,8 -race ./...

goos: darwin
goarch: arm64
pkg: github.com/curfew-marathon/tickbatch
cpu: Apple M1

BenchmarkPush        80139657    469.4 ns/op    0 B/op    0 allocs/op
BenchmarkPush        72361881    519.6 ns/op    0 B/op    0 allocs/op
BenchmarkPush        68007879    540.0 ns/op    0 B/op    0 allocs/op
BenchmarkPush-2      65918344    552.3 ns/op    0 B/op    0 allocs/op
BenchmarkPush-2      64945124    560.9 ns/op    0 B/op    0 allocs/op
BenchmarkPush-2      64378368    569.9 ns/op    0 B/op    0 allocs/op
BenchmarkPush-4      64174353    556.3 ns/op    0 B/op    0 allocs/op
BenchmarkPush-4      65978332    565.8 ns/op    0 B/op    0 allocs/op
BenchmarkPush-4      64096766    552.1 ns/op    0 B/op    0 allocs/op
BenchmarkPush-8      66106965    546.4 ns/op    0 B/op    0 allocs/op
BenchmarkPush-8      66391826    547.1 ns/op    0 B/op    0 allocs/op
BenchmarkPush-8      66093150    549.0 ns/op    0 B/op    0 allocs/op
PASS
```

Note the scaling behavior: throughput is essentially flat from 2 to 8 cores. The 64-byte cache-line padding between head and tail eliminates the cross-core false-sharing that collapses throughput in naive ring buffer implementations.

---

## Installation

```bash
go get github.com/curfew-marathon/tickbatch
```

Requires Go 1.25+. No other dependencies.

---

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "time"
    "unsafe"

    "github.com/curfew-marathon/tickbatch"
)

// RiskSnapshot is a flat, pointer-free struct representing a single position
// risk record emitted by a trading engine. Rule: pointer-free structs only.
// Pointer fields force the GC to scan every ring buffer slot on every collection
// cycle, turning O(1) scan time into O(n). Keep it flat.
type RiskSnapshot struct {
    InstrumentID uint32
    NetPosition  float64
    MarketValue  float64
    DeltaExposure float32
    Flags        uint32
}

// Marshal implements tickbatch.Serializable. It encodes RiskSnapshot into buf
// via a direct unsafe memory copy: no reflection, no encoding/binary,
// no allocations. Returns the number of bytes written.
func (r RiskSnapshot) Marshal(buf []byte) int {
    const size = int(unsafe.Sizeof(RiskSnapshot{}))
    if len(buf) < size {
        return 0
    }
    copy(buf[:size], (*[size]byte)(unsafe.Pointer(&r))[:])
    return size
}

// ComplianceSink forwards each flushed batch to the downstream audit store.
type ComplianceSink struct{}

func (s ComplianceSink) Flush(payload []byte) error {
    // Write payload to Kafka topic, S3, ClickHouse, durable UDP socket, etc.
    fmt.Printf("flushed %d bytes\n", len(payload))
    return nil
}

func main() {
    b := tickbatch.New[RiskSnapshot](tickbatch.Config{
        QueueSize:    1 << 12, // 4096 slots, must be a power of two
        MaxBatchSize: 64 * 1024,
        MaxItemSize:  int(unsafe.Sizeof(RiskSnapshot{})),
        TickRate:     100, // drain and flush 100 times per second
        Sink:         ComplianceSink{},
    })

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    // Start the tick engine. It runs until ctx is canceled.
    // On cancellation, any remaining ring-buffer items are flushed before exit.
    // The returned channel closes when the goroutine exits cleanly.
    done := b.Start(ctx)

    // Push from any goroutine. Non-blocking. Zero allocations.
    // If the queue is full, the new item is silently dropped (or the oldest evicted
    // when DropOldest is configured) — the producer is never stalled behind downstream I/O.
    // Synthetic firehose: real producers should pace with time.Ticker or event-driven pushes.
    go func() {
        for {
            select {
            case <-ctx.Done():
                return
            default:
                b.Push(RiskSnapshot{
                    InstrumentID:  4217,
                    NetPosition:   150.0,
                    MarketValue:   734_875.50,
                    DeltaExposure: 0.42,
                    Flags:         0x01,
                })
            }
        }
    }()

    <-done
}
```

---

## Architecture

### The Vyukov MPMC Ring Buffer

The ring buffer uses Dmitry Vyukov's sequence-based MPMC algorithm. Each slot carries an atomic sequence number alongside the payload. Producers claim slots via a compare-and-swap on the tail cursor; consumers claim them via a CAS on the head cursor. No mutex is ever acquired. The sequence number encodes whether a slot is empty, being written, or ready to read, eliminating the ABA problem without a generation counter.

### Cache-Line Isolation

The head and tail cursors are separated by 64 bytes of padding inside the `ringbuf` struct:

```
head atomic.Uint64
_    [64]byte        // isolates head from tail on a separate cache line
tail atomic.Uint64
_    [64]byte        // isolates tail from subsequent fields
```

On a modern NUMA or multi-socket system, without this padding, a write to `tail` by a producer invalidates the cache line holding `head` on every consumer core. The result is a coherence storm that can collapse throughput by an order of magnitude. The padding places each cursor on its own 64-byte cache line, making producer and consumer operations fully independent at the hardware level.

### Memory Barriers and Atomic Ordering

All cursor and sequence operations use `sync/atomic`. In Go's memory model, atomic loads and stores provide sequentially consistent ordering. This means the sequence number store that publishes a slot to consumers acts as a full memory barrier: a consumer that observes the updated sequence is guaranteed to also observe the item written before it. No additional fences or `unsafe` ordering tricks are required.

### The Tick Engine

`Start` spawns a single background goroutine running a `time.Ticker` at `Config.TickRate` Hz. On each tick, the drain loop calls `popMarshal` in a tight loop, atomically dequeuing each item and marshaling it directly into a pre-allocated `[]byte` buffer via the `Serializable` interface. When the buffer is full or the ring is empty, the loop stops and `Sink.Flush` is called with the accumulated payload. The byte buffer is sized at construction and never reallocated. When the context is canceled, a final drain executes before the goroutine exits — no records are silently abandoned.

### Producer Isolation

The critical design property is that `Sink.Flush` never executes on the producer's goroutine. The producer touches only the ring buffer (a single atomic CAS). The drain goroutine owns all serialization, compression, and network I/O. A 200 ms disk stall, a TCP backpressure event, or a slow Kafka broker never propagates back to the producer. The ring buffer absorbs the burst; backpressure is applied by silently dropping or evicting items — never by blocking the caller.

### Wire Format

Every payload delivered to `Sink.Flush` uses the following fixed layout:

```
Bytes [0:4]  — sequence ID, little-endian uint32 (monotonically increasing per Batcher)
Bytes [4:6]  — item count, little-endian uint16
Bytes [6:8]  — reserved, always zero
Bytes [8:N]  — packed items, each written by T.Marshal() back-to-back with no separator
```

When `Config.DeltaEncoding` is true, the payload delivered to `Sink.Flush` is the XOR of the current raw frame against the previous raw frame. Receivers must maintain a copy of the prior raw frame and XOR it with each received frame to reconstruct the original batch. If a frame is lost in transit (e.g. over UDP), all subsequent frames produce corrupt output — only enable delta encoding over reliable transports.

### Correctness & Safety

tickbatch bypasses `encoding/binary` and uses `unsafe.Pointer` arithmetic throughout the hot path. To validate these low-level invariants, the library ships two fuzzing harnesses:

- **`FuzzXORBytes`** stress-tests the vectorized XOR engine — both the 8-byte word loop and the `len%8` tail-byte fallback — asserting that XOR-ing a payload twice recovers the original byte-for-byte. Exercises the alignment edge cases that are hardest to catch with hand-written unit tests.
- **`FuzzTickSerialization`** stress-tests the `Marshal`/unmarshal round-trip with randomly generated field values including bit patterns that produce NaN floats, infinities, and denormals.

To extend the corpus locally:

```bash
go test -fuzz=FuzzXORBytes -fuzztime=60s ./...
go test -fuzz=FuzzTickSerialization -fuzztime=60s ./...
```

### Observability

Every component is designed for production instrumentation. The following zero-allocation counters are available on every `Batcher`:

| Method | Description |
|---|---|
| `DroppedCount()` | Items discarded because the ring was full under `DropNewest`. |
| `EvictedCount()` | Items evicted from the ring head under `DropOldest`. |
| `TruncatedCount()` | Items dequeued but discarded because `Marshal` returned zero bytes — indicates a bug in `T.Marshal` or a `MaxItemSize` configured smaller than the actual encoded size. |
| `FlushedBatches()` | Total batches successfully delivered to `Sink.Flush`. |
| `FlushedItems()` | Total items serialized across all flushes. |

`FlushedItems() / FlushedBatches()` gives the average batch fill rate. `DroppedCount() + EvictedCount()` gives cumulative data loss across both backpressure policies. All counters are `atomic.Uint64` reads — zero allocations, safe to call from any goroutine at any time.

---

## Use Cases

tickbatch is purpose-built for infrastructure where the producer thread must never stall behind downstream I/O.

**Compliance & Audit Exhaust.** Risk engines, order management systems, and matching engines generate a continuous stream of lifecycle events that must be captured without imposing latency on the trading path. tickbatch provides a non-blocking ingest point that the hot thread pushes into at full speed. A separate drain goroutine delivers ordered, serialized records to durable storage — Kafka, S3, ClickHouse, or a UDP compliance bus — at a controlled rate. The producer never waits on a disk write or a network round-trip.

**Risk Telemetry Pipelines.** Portfolio risk systems emit position snapshots, Greeks, and margin utilization at high frequency. tickbatch absorbs burst spikes into a lock-free buffer and coalesces them into batched payloads before forwarding to downstream analytics. GC pauses in the serialization layer never interrupt the risk calculation loop.

**Market Data Telemetry.** Quote and trade events from exchange feeds arrive in microsecond bursts. tickbatch ingests them lock-free and delivers coalesced payloads to monitoring pipelines, latency dashboards, and SRE alerting systems. The ingest goroutine is never coupled to the network write that delivers the telemetry.

**Structured Event Logging.** High-throughput structured logs — request traces, latency samples, error events — can be ingested at any rate and forwarded to Kafka, Prometheus remote-write, or ClickHouse at a controlled Hz. Per-event serialization allocations are eliminated; the GC sees a flat heap.

**UDP Multicast and Network Telemetry.** High-throughput UDP pipelines benefit directly from the zero-allocation flush model. Push raw events from the ingest goroutine; receive a single coalesced payload in `Sink.Flush` ready for `conn.WriteTo`. The `UDPSink` implementation is included in the library.

**Zero-Allocation Go Infrastructure.** Any Go service operating under a strict GC pause SLO benefits from moving hot-path data through pre-allocated structures. tickbatch provides the ingest buffer, the tick-driven drain, and the serialization contract as a composable primitive, not a monolithic framework.

**Lock-Free Queue Primitive.** The underlying `ringbuf` is a general-purpose, lock-free MPMC queue with cache-line padding and Vyukov sequencing. It can be used directly as a high-performance inter-goroutine communication primitive wherever `channel` overhead is measurable.

**Cross-Platform Systems Infrastructure.** Pure Go, zero CGO, zero external dependencies. Compiles for linux/arm64, linux/amd64, darwin/arm64, windows/amd64, and any other GOOS/GOARCH without a C toolchain. Identical behavior on co-located bare metal and cloud VMs.

---

## Contributing

Contributions are welcome. Before opening a pull request, run the full gate locally:

```bash
# Race detector + all tests
go test -v -race ./...

# Zero-allocation enforcement (Push must show 0 allocs/op)
go test -bench=. -benchmem ./...

# Linter (config verify is mandatory before run)
golangci-lint config verify && golangci-lint run
```

A pull request is not mergeable if any of the following are true:

- The race detector flags an issue.
- `BenchmarkPush` reports any `allocs/op > 0`.
- `golangci-lint run` reports any issue.
- CGO was introduced.
- Backpressure behavior under a full queue is untested.

---

## License

Apache 2.0. See [LICENSE](LICENSE).
