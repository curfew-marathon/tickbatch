# tickbatch - Implementation Plan

This document outlines the phased delivery of the `tickbatch` library. Development is structured into small, user-deliverable milestones. At the end of every phase, the library must compile, pass all tests, and perform a demonstrable function. 

To push Go's performance to the absolute limit, this project aggressively avoids Garbage Collector (GC) overhead, mitigates CPU cache false sharing, and utilizes vectorized operations. We rely strictly on Vanilla Go (`sync/atomic`, `time`, `net`, `unsafe`) for the core library to ensure zero dependencies.

## Phase 1: The Zero-Alloc Ingest & Contract Definition
**Goal:** Define the generic constraints and build a fixed-size, lock-free queue that defeats False Sharing.
- [x] Define the `Serializable` interface (`Marshal(buf []byte) int`). Document strictly that implementors must use **pointer-free** flat structs to guarantee O(1) GC scan times.
- [x] Define `Config` and the core `Batcher[T Serializable]` struct. 
- [x] Implement `ringbuf[T Serializable]` using atomic pointers. 
- [x] **Hardware Optimization:** Inject Cache-Line Padding (`[64]byte`) between the read and write pointers in the `ringbuf` struct to physically separate them in the CPU cache, completely eliminating False Sharing.
- [x] Implement `Batcher.Push(item T)`.
- [x] **Testable Deliverable:** A unit test proving data can be pushed and popped sequentially.
- [x] **Performance Gate:** `go test -bench=. -benchmem` must guarantee `0 allocs/op` on `Push()`.

## Phase 2: The Tick Engine & Zero-Alloc Drain
**Goal:** Extract data at a specific Hz rate without allocating new memory per tick.
- [ ] Define the `Sink` interface (`Flush(payload []byte) error`).
- [ ] Implement a simple `StdoutSink` for testing.
- [ ] Pre-allocate a `[]T` slice and a `[]byte` buffer inside the `Batcher` struct during initialization.
- [ ] Write `runLoop()` utilizing `time.Ticker` to safely drain the ring buffer into the pre-allocated slices (preventing per-tick allocations).
- [ ] **Testable Deliverable:** A functional main program batching dummy state and printing it exactly 60 times a second.

## Phase 3: Bare-Metal Serialization (The Bytes)
**Goal:** Utilize the `Serializable` interface and `unsafe` to pack the pre-allocated buffers at C-level speeds.
- [ ] Implement the logic inside the drain loop to call `item.Marshal(buf)` on every popped item.
- [ ] Implement lightweight payload headers (e.g., Sequence ID, Batch Count) at the front of the byte buffer.
- [ ] **Hardware Optimization:** Bypass `encoding/binary` overhead inside the implementation examples by utilizing `unsafe.Pointer` casting for primitive types (e.g., casting a `float32` directly into 4 bytes) for absolute zero-overhead serialization.
- [ ] **Testable Deliverable:** A `MockSink` test asserting the exact byte length, structure, and endianness of a flushed payload.

## Phase 4: Backpressure & The Stress Test Suite
**Goal:** Prove the library holds up under extreme load and handles full queues gracefully.
- [ ] Implement backpressure policies (e.g., if the fixed-size ring buffer is full, drop the oldest `T` to make room for the new one).
- [ ] Create `perf_test.go` (The Stress Suite).
- [ ] **Testable Deliverable:** Run the Stress Suite simulating 10,000 pushes per frame at 144Hz. The library must not crash, and the backpressure policy must execute predictably without memory leaks.

## Phase 5: Vectorized Delta Encoding (The Brain)
**Goal:** Shrink the payload by sending only what changed, utilizing 64-bit chunking for violent speed increases while maintaining memory safety.
- [ ] Pre-allocate a `previousState []byte` buffer during `New()`.
- [ ] **Hardware Optimization:** Implement 64-bit Vectorized XORing. Use `unsafe.Slice` to cast the underlying `[]byte` buffer into `[]uint64`, allowing the CPU to process the delta diffs in 8-byte chunks.
- [ ] **Alignment Safety:** Implement a tail-byte fallback loop. Safely handle the `len % 8` remainder with a standard byte-wise XOR to prevent out-of-bounds panics on structs that do not align perfectly to 8-byte boundaries.
- [ ] **Testable Deliverable:** A unit test mathematically proving a reduction in payload byte size, alongside benchmarks proving the vectorized XOR outperforms standard byte looping. Explicitly include tests with non-8-byte aligned struct sizes (e.g., 35 bytes) to guarantee the tail-byte fallback executes correctly.

## Phase 6: Standard UDP Sink & Pluggable Compression
**Goal:** Ship it over a real network and offer optional, opt-in compression.
- [ ] Implement `UDPSink` using the standard `net` package (fire-and-forget).
- [ ] Define a `Compressor` interface so users can optionally wrap their sinks in a library like `klauspost/compress/zstd`.
- [ ] **Testable Deliverable:** Two local UDP scripts (a sender using `tickbatch` and a vanilla Go UDP listener) successfully syncing a stream of structs in real-time.