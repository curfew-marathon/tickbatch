// Package tickbatch is a lock-free, zero-allocation telemetry batching engine for Go.
//
// tickbatch completely decouples event producers from downstream I/O. The producer
// calls [Batcher.Push] - a single atomic compare-and-swap against a pre-allocated
// ring buffer slot - and returns immediately. A background goroutine drains the ring
// at a fixed Hz, serializes payloads directly into a pre-allocated byte buffer, and
// hands each batch to a pluggable [Sink]. No heap activity ever occurs on the ingest
// path. The GC has nothing to scan.
//
// Ring buffer design
//
// The internal ring buffer implements Dmitry Vyukov's sequence-based MPMC algorithm.
// Each slot carries an atomic sequence number alongside the payload. Producers claim
// slots via a compare-and-swap on the tail cursor; the single drain goroutine consumes
// via the head cursor. The sequence number encodes slot state - empty, being written,
// or ready to read - eliminating the ABA problem without a generation counter.
//
// The head and tail cursors are separated by 64 bytes of padding so they reside on
// different CPU cache lines. This structural isolation eliminates producer/consumer
// false sharing and keeps throughput flat across core counts.
//
// Serialization
//
// Payload types must implement the [Serializable] interface, which encodes a value
// into a caller-supplied []byte via a single Marshal call. The recommended pattern
// uses unsafe.Pointer casting for a direct memory copy - no encoding/binary, no
// reflection, no allocations. Flat, pointer-free structs guarantee O(1) GC scan time
// regardless of ring buffer capacity; pointer fields cause the collector to scan every
// slot on every cycle, degrading throughput from O(1) to O(n).
//
// Backpressure
//
// When the ring buffer is full, [Batcher.Push] applies a [BackpressurePolicy] rather
// than blocking. [DropNewest] (the default) silently discards the incoming item;
// [DropOldest] evicts the oldest queued item via a lock-free CAS on the consumer head.
// Either way the caller is never stalled, panicked, or coupled to downstream I/O
// latency. Data loss under sustained overload is a deliberate design choice.
//
// Usage
//
// Define a flat, pointer-free struct and implement [Serializable]:
//
//	type Event struct {
//	    ID    uint64
//	    Value float64
//	}
//
//	func (e Event) Marshal(buf []byte) int {
//	    const size = int(unsafe.Sizeof(Event{}))
//	    if len(buf) < size {
//	        return 0
//	    }
//	    copy(buf[:size], (*[size]byte)(unsafe.Pointer(&e))[:])
//	    return size
//	}
//
// Construct a [Batcher], start the tick engine, and push events from any goroutine:
//
//	b := tickbatch.New[Event](tickbatch.Config{
//	    QueueSize:    1 << 12,
//	    MaxBatchSize: 64 * 1024,
//	    MaxItemSize:  int(unsafe.Sizeof(Event{})),
//	    TickRate:     100,
//	    Sink:         mySink,
//	})
//	done := b.Start(ctx)
//	b.Push(Event{ID: 1, Value: 3.14}) // non-blocking, zero allocations
//	cancel()
//	<-done
//
// See the package examples for complete, runnable demonstrations.
package tickbatch
