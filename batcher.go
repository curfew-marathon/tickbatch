package tickbatch

import (
	"context"
	"fmt"
	"log"
	"math"
	"sync/atomic"
	"time"
)

// headerSize is the byte length of the fixed header prepended to every flushed batch.
// Layout: bytes [0:4] sequence ID (uint32, little-endian), [4:6] item count (uint16,
// little-endian), [6:8] reserved and zeroed.
const headerSize = 8

// BackpressurePolicy controls what happens when [Batcher.Push] is called on a full ring buffer.
type BackpressurePolicy int

const (
	// DropNewest silently discards the incoming item when the ring buffer is full.
	// Already-queued items are preserved unchanged. This is the default policy.
	DropNewest BackpressurePolicy = iota

	// DropOldest evicts the oldest queued item to make room for the incoming one.
	// It performs a lock-free CAS on the consumer head and never blocks or allocates.
	DropOldest
)

// Config holds construction parameters for a [Batcher].
//
// Pointer-containing fields are grouped at the top of the struct so the GC
// pointer bitmap covers only 40 bytes instead of the full 80, halving the
// per-collection scan cost of every live [Batcher].
type Config struct {
	// Sink is the transport that receives each flushed payload.
	// If nil, drained batches are serialized but not delivered.
	Sink Sink

	// Compressor, if non-nil, is applied to each batch payload immediately before
	// Sink.Flush. The [Compressor.Compress] call writes into a pre-allocated buffer
	// so the zero-allocation contract on [Batcher.Push] is preserved end-to-end.
	Compressor Compressor

	// OnFlushError, if non-nil, is called whenever Sink.Flush returns an error.
	// It is invoked from the drain goroutine and must be goroutine-safe.
	OnFlushError func(error)

	// QueueSize is the capacity of the internal ring buffer.
	// It must be a positive power of two (e.g. 1<<10 for 1024 slots).
	QueueSize uint64

	// MaxBatchSize is the maximum size in bytes of a single serialized batch.
	// The internal byte buffer is pre-allocated to this size during [New].
	MaxBatchSize int

	// TickRate is the number of drain cycles executed per second.
	// A value of 60 causes the engine to wake and flush the queue 60 times per second.
	// It must be positive when [Batcher.Start] is called.
	TickRate int

	// Backpressure selects the policy applied when Push is called on a full queue.
	// The zero value is [DropNewest].
	Backpressure BackpressurePolicy

	// DeltaEncoding, when true, XORs each flushed payload against the previous
	// frame before passing it to Sink.Flush. Receivers must XOR with their own
	// copy of the prior frame to reconstruct the original batch. When false
	// (the default), Sink.Flush receives the fully-serialized raw payload,
	// preserving the original Sink contract.
	DeltaEncoding bool
}

// Batcher is a generic, lock-free telemetry batching engine.
//
// The zero value is not usable; construct via [New].
type Batcher[T Serializable] struct {
	byteBuffer     []byte
	previousState  []byte
	deltaBuffer    []byte
	compressBuffer []byte
	ring           *ringbuf[T]
	cfg            Config
	sequenceID     atomic.Uint32
	started        atomic.Bool
	dropped        atomic.Uint64
}

// New allocates and returns a ready-to-use Batcher.
// It panics if cfg.QueueSize is zero or not a power of two, if
// cfg.MaxBatchSize is smaller than headerSize, or if cfg.Backpressure
// is not a known [BackpressurePolicy] constant.
func New[T Serializable](cfg Config) *Batcher[T] {
	if cfg.MaxBatchSize < headerSize {
		panic("tickbatch: Config.MaxBatchSize must be at least headerSize bytes")
	}
	if cfg.Backpressure != DropNewest && cfg.Backpressure != DropOldest {
		panic("tickbatch: Config.Backpressure is not a valid BackpressurePolicy")
	}
	b := &Batcher[T]{
		ring:       newRingbuf[T](cfg.QueueSize),
		cfg:        cfg,
		byteBuffer: make([]byte, cfg.MaxBatchSize),
	}
	if cfg.DeltaEncoding {
		b.previousState = make([]byte, cfg.MaxBatchSize)
		b.deltaBuffer = make([]byte, cfg.MaxBatchSize)
	}
	if cfg.Compressor != nil {
		b.compressBuffer = make([]byte, cfg.MaxBatchSize)
	}
	return b
}

// Push enqueues item into the ring buffer.
//
// This is the hot-path method. It is non-blocking and performs zero heap
// allocations. If the buffer is full, the configured [BackpressurePolicy] is
// applied — the caller is never stalled or panicked (graceful degradation contract).
// Under [DropNewest], the incoming item is silently discarded and [Batcher.DroppedCount]
// is incremented. Under [DropOldest], the oldest queued item is evicted and the
// new item always succeeds; DroppedCount is never incremented.
func (b *Batcher[T]) Push(item T) {
	if !b.ring.push(item, b.cfg.Backpressure) {
		b.dropped.Add(1)
	}
}

// DroppedCount returns the cumulative number of items silently discarded because
// the ring buffer was full under the [DropNewest] policy. It is always zero when
// using [DropOldest], which evicts the oldest item rather than dropping the new one.
func (b *Batcher[T]) DroppedCount() uint64 {
	return b.dropped.Load()
}

// Start launches the tick engine in a background goroutine and returns a channel
// that is closed once the goroutine has fully exited.
//
// The engine runs until ctx is canceled, allowing callers to wait for a clean
// shutdown by receiving from the returned channel. It panics if Config.TickRate
// is not positive, or if Start has already been called on this Batcher.
func (b *Batcher[T]) Start(ctx context.Context) <-chan struct{} {
	if b.cfg.TickRate <= 0 {
		panic("tickbatch: Config.TickRate must be positive")
	}
	if !b.started.CompareAndSwap(false, true) {
		panic("tickbatch: Start called more than once")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.runLoop(ctx)
	}()
	return done
}

// drainAndFlush dequeues all available items from the ring buffer, serializes
// them into the pre-allocated byteBuffer, and delivers the payload to the
// configured Sink. The prevOffset argument tracks the previous frame's byte
// length for delta-encoding stale-byte zeroing. It performs zero heap
// allocations.
func (b *Batcher[T]) drainAndFlush(prevOffset *int) {
	offset := headerSize
	n := 0
	for n < math.MaxUint16 {
		written, ok := b.ring.popMarshal(b.byteBuffer[offset:])
		if !ok {
			break
		}
		if written == 0 {
			break
		}
		offset += written
		n++
	}
	if n == 0 || b.cfg.Sink == nil {
		return
	}

	seq := b.sequenceID.Add(1)
	b.byteBuffer[0] = byte(seq)
	b.byteBuffer[1] = byte(seq >> 8)
	b.byteBuffer[2] = byte(seq >> 16)
	b.byteBuffer[3] = byte(seq >> 24)
	count := uint16(n)
	b.byteBuffer[4] = byte(count)
	b.byteBuffer[5] = byte(count >> 8)
	b.byteBuffer[6] = 0
	b.byteBuffer[7] = 0

	if !b.cfg.DeltaEncoding {
		payload := b.byteBuffer[:offset]
		if b.cfg.Compressor != nil {
			cn, cerr := b.cfg.Compressor.Compress(b.compressBuffer, payload)
			if cerr != nil {
				if b.cfg.OnFlushError != nil {
					b.cfg.OnFlushError(cerr)
				} else {
					log.Printf("tickbatch: %v", cerr)
				}
				return
			}
			if cn < 0 || cn > len(b.compressBuffer) {
				oobErr := fmt.Errorf("tickbatch: compressor returned out-of-range n=%d", cn)
				if b.cfg.OnFlushError != nil {
					b.cfg.OnFlushError(oobErr)
				} else {
					log.Printf("tickbatch: %v", oobErr)
				}
				return
			}
			payload = b.compressBuffer[:cn]
		}
		if err := b.cfg.Sink.Flush(payload); err != nil {
			if b.cfg.OnFlushError != nil {
				b.cfg.OnFlushError(err)
			} else {
				log.Printf("tickbatch: %v", err)
			}
		}
		return
	}

	XORBytes(b.deltaBuffer[:offset], b.byteBuffer[:offset], b.previousState[:offset])
	payload := b.deltaBuffer[:offset]
	if b.cfg.Compressor != nil {
		cn, cerr := b.cfg.Compressor.Compress(b.compressBuffer, payload)
		if cerr != nil {
			if b.cfg.OnFlushError != nil {
				b.cfg.OnFlushError(cerr)
			} else {
				log.Printf("tickbatch: %v", cerr)
			}
			return
		}
		if cn < 0 || cn > len(b.compressBuffer) {
			oobErr := fmt.Errorf("tickbatch: compressor returned out-of-range n=%d", cn)
			if b.cfg.OnFlushError != nil {
				b.cfg.OnFlushError(oobErr)
			} else {
				log.Printf("tickbatch: %v", oobErr)
			}
			return
		}
		payload = b.compressBuffer[:cn]
	}
	if err := b.cfg.Sink.Flush(payload); err != nil {
		if b.cfg.OnFlushError != nil {
			b.cfg.OnFlushError(err)
		} else {
			log.Printf("tickbatch: %v", err)
		}
	} else {
		copy(b.previousState[:offset], b.byteBuffer[:offset])
		if *prevOffset > offset {
			clear(b.previousState[offset:*prevOffset])
		}
		*prevOffset = offset
	}
}

// runLoop is the internal drain loop. It ticks at cfg.TickRate Hz, draining
// the ring buffer into the pre-allocated slices and flushing serialized payloads
// to the configured Sink on every wake. When the context is canceled, it performs
// one final drain to flush any remaining items before exiting.
func (b *Batcher[T]) runLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second / time.Duration(b.cfg.TickRate))
	defer ticker.Stop()

	var prevOffset int

	for {
		select {
		case <-ctx.Done():
			b.drainAndFlush(&prevOffset)
			return
		case <-ticker.C:
			b.drainAndFlush(&prevOffset)
		}
	}
}
