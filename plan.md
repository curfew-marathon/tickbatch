# tickbatch - Implementation Plan

This document outlines the phased delivery of the `tickbatch` library. Development is structured into small, user-deliverable milestones. At the end of every phase, the library must compile, pass all tests, and perform a demonstrable function. 

To push Go's performance to the absolute limit, this project aggressively avoids Garbage Collector (GC) overhead, mitigates CPU cache false sharing, and utilizes vectorized operations. We rely strictly on Vanilla Go (`sync/atomic`, `time`, `net`, `unsafe`) for the core library to ensure zero dependencies.

## Phase 1: The Zero-Alloc Ingest & Contract Definition
**Goal:** Define the generic constraints and build a fixed-size, lock-free queue that defeats False Sharing.
- [x] Define the `Serializable` interface (`Marshal(buf []byte) int`). Document strictly that implementors must use **pointer-free** flat structs to guarantee O(1) GC scan times.
- [x] Define `Config` and the core `Batcher[T Serializable]` struct. 
- [x] Implement `ringbuf[T Serializable]` using atomic pointers. 
- [x] **Hardware Optimization:** Inject Cache-Line Padding (`[56]byte`, sizing each 8-byte pointer to a full 64-byte line) between the read and write pointers in the `ringbuf` struct to physically separate them in the CPU cache, completely eliminating False Sharing.
- [x] Implement `Batcher.Push(item T)`.
- [x] **Testable Deliverable:** A unit test proving data can be pushed and popped sequentially.
- [x] **Performance Gate:** `go test -bench=. -benchmem` must guarantee `0 allocs/op` on `Push()`.

## Phase 2: The Tick Engine & Zero-Alloc Drain
**Goal:** Extract data at a specific Hz rate without allocating new memory per tick.
- [x] Define the `Sink` interface (`Flush(payload []byte) error`).
- [x] Implement a simple `StdoutSink` for testing.
- [x] Pre-allocate a `[]byte` buffer inside the `Batcher` struct during initialization.
- [x] Write `runLoop()` utilizing `time.Ticker` to safely drain the ring buffer into the pre-allocated buffer (preventing per-tick allocations).
- [x] **Testable Deliverable:** `marketsim` sample app batching 8-instrument market data at 60 Hz and printing a live throughput table.

## Phase 3: Bare-Metal Serialization (The Bytes)
**Goal:** Utilize the `Serializable` interface and `unsafe` to pack the pre-allocated buffers at C-level speeds.
- [x] Add `popMarshal` to `ringbuf`: marshals each item directly from its ring slot into `byteBuffer`, eliminating the intermediate `drainSlice` (ring slot → byteBuffer in one step).
- [x] Implement 8-byte payload header: `[0:4]` uint32 sequence ID, `[4:6]` uint16 item count (both little-endian via byte-shifting), `[6:8]` integrity tag (bit 15 keyframe flag, bits 0-14 low 15 bits of CRC-32/IEEE over the body).
- [x] **Hardware Optimization:** `Tick.Marshal` uses `unsafe.Pointer` field-level casts (`*(*float64)(unsafe.Pointer(&buf[8]))`) to bypass `encoding/binary`.
- [x] **Testable Deliverable:** `TestTickSerialization` asserts exact payload byte length, header sequence ID, header item count, and full field-level data integrity of the decoded first `Tick`.

## Phase 4: Backpressure & The Stress Test Suite
**Goal:** Prove the library holds up under extreme load and handles full queues gracefully.
- [x] Implement backpressure policies (e.g., if the fixed-size ring buffer is full, drop the oldest `T` to make room for the new one).
- [x] Create `perf_test.go` (The Stress Suite).
- [x] **Testable Deliverable:** Run the Stress Suite simulating 10,000 pushes per frame at 144Hz. The library must not crash, and the backpressure policy must execute predictably without memory leaks.

## Phase 5: Vectorized Delta Encoding (The Brain)
**Goal:** Shrink the payload by sending only what changed, utilizing 64-bit chunking for violent speed increases while maintaining memory safety.
- [x] Pre-allocate a `previousState []byte` buffer during `New()`.
- [x] **Hardware Optimization:** Implement 64-bit Vectorized XORing. Use `unsafe.Slice` to cast the underlying `[]byte` buffer into `[]uint64`, allowing the CPU to process the delta diffs in 8-byte chunks.
- [x] **Alignment Safety:** Implement a tail-byte fallback loop. Safely handle the `len % 8` remainder with a standard byte-wise XOR to prevent out-of-bounds panics on structs that do not align perfectly to 8-byte boundaries.
- [x] **Testable Deliverable:** A unit test mathematically proving a reduction in payload byte size, alongside benchmarks proving the vectorized XOR outperforms standard byte looping. Explicitly include tests with non-8-byte aligned struct sizes (e.g., 35 bytes) to guarantee the tail-byte fallback executes correctly.

## Phase 6: Standard UDP Sink & Pluggable Compression
**Goal:** Ship it over a real network and offer optional, opt-in compression.
- [x] Implement `UDPSink` using the standard `net` package (fire-and-forget).
- [x] Define a `Compressor` interface so users can optionally wrap their sinks in a library like `klauspost/compress/zstd`.
- [x] **Testable Deliverable:** Two local UDP scripts (a sender using `tickbatch` and a vanilla Go UDP listener) successfully syncing a stream of structs in real-time.

## Phase 7: Fuzzing, Chaos & Reliability
**Goal:** Defend the zero-allocation hot path and memory safety against malicious payloads, awkward memory boundaries, and downstream network failures.
- [x] **The Fuzzer (`unsafe` Guard):** Implement Go native fuzzing (`go test -fuzz`) targeting the Phase 3 byte-packing and Phase 5 Vectorized XOR engines. 
- [x] **Alignment & Boundary Verification:** Ensure the fuzzer injects randomized, non-standard byte slice lengths (e.g., 37 bytes) to mathematically prove the tail-byte fallback logic cannot segfault or panic under chaotic conditions.
- [x] **The Chaos Sink (Stall Protection):** Implement a `BlockingMockSink` that intentionally sleeps or hangs during the `Flush()` call. 
- [x] **Non-Blocking Verification:** Write a test proving that a stalled downstream Sink will not lock up the `runLoop`, ensuring the batcher continues to drain the queue and apply backpressure policies without freezing the main thread.
- [x] **Graceful Shutdown:** Implement a context-cancellation test (`ctx.Done()`) that proves the `runLoop` stops accepting new pushes, flushes the final remaining items in the queue to the sink, and exits cleanly without leaking goroutines or dropping the final batch.
- [x] **Testable Deliverable:** The library must survive a 5-minute fuzzing gauntlet and a simulated network outage chaos test while maintaining 0 allocations and 0 memory leaks.

## Phase 8: CI/CD Automation & Release Readiness
**Goal:** Enforce strict memory invariants via automated CI and ship a fully runnable demonstration of the library.
- [x] **CI Pipeline (`.github/workflows/ci.yml`):** Trigger on push to `main` and `pull_request`. Run `go test -v -race ./...` across a build matrix of `ubuntu-latest` (amd64), `macos-latest` (arm64), and `windows-latest` (amd64) to catch unsafe-cast regressions on all three targets.
- [x] **Lint Step:** Run `golangci-lint config verify && golangci-lint run` via `golangci/golangci-lint-action` to enforce all project style and correctness rules.
- [x] **Zero-Allocation Memory Gate:** Run `go test -bench=BenchmarkPush -benchmem ./... > bench.txt`, then parse `bench.txt` with `grep`/`awk` and exit 1 if `allocs/op > 0`. Mathematically prevents any PR from introducing a heap escape on the hot path.
- [x] **Runnable Example (`examples/telemetry_ingest/main.go`):** A fully functional demonstration that dials `UDPSink` to `127.0.0.1:9999`, pushes `RiskEvent` structs in a hot loop for 3 seconds under `DropOldest` backpressure, then blocks on the `done` channel to prove graceful shutdown. (No delta encoding: `UDPSink` is fire-and-forget, not a `ReliableSink`, so `New` rejects pairing it with `DeltaEncoding`.)
- [x] **Testable Deliverable:** `go run ./examples/telemetry_ingest/` exits cleanly after 3 seconds, printing the total dropped count. CI gates prevent any allocation regression from merging.

## Phase 9: Enterprise Hardening & Bleeding-Edge Optimization
**Goal:** Bulletproof the `unsafe` memory boundaries, eliminate OS scheduler jitter, and leverage modern Go compiler advancements for absolute maximum throughput.

> **Status:** Phase 9 items are post-v1.0 stretch goals, intentionally deferred - they do not gate the v1.0 release. Shipped fuzzing coverage lives in Phase 7 (`FuzzXORBytes` in `fuzz_test.go`).
- [ ] **Native Fuzzing (`testing.F`):** Implement `FuzzTickSerialization` in `batcher_test.go`. Pump millions of randomly mutated byte slices and struct bounds into the `Marshal` and `Push` functions to mathematically prove the `unsafe` pointer arithmetic cannot trigger a segfault under hostile or malformed ingest conditions.
- [ ] **Hardware Affinity (`runtime.LockOSThread`):** Add a `LockThread bool` to `Config`. When enabled, the `runLoop` immediately locks itself to the underlying OS thread, preventing the Go scheduler from migrating the goroutine across CPU cores. This guarantees the L1/L2 hardware caches remain perfectly hot during the batching cycle.
- [ ] **Profile-Guided Optimization (PGO):** Run a sustained `pprof` CPU profile against the stress benchmarks to generate a `default.pgo` file. Commit this profile to the repository so the Go compiler automatically aggressively inlines `push`, `popMarshal`, and the `unsafe` casting paths for all future builds.
- [ ] **Execution Tracing (`go tool trace`):** Build a dedicated integration test that wraps a heavily loaded `Batcher` with `trace.Start()` and `trace.Stop()`. Output a `trace.out` artifact that visually proves to consumers that the garbage collector and Go scheduler never interrupt the `SpinTick` critical path.