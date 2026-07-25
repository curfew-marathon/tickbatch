package tickbatch

import (
	"context"
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
type Config struct {
	// Sink is the transport that receives each flushed payload.
	// If nil, drained batches are serialized but not delivered.
	Sink Sink

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
}

// Batcher is a generic, lock-free telemetry batching engine.
//
// The zero value is not usable; construct via [New].
type Batcher[T Serializable] struct {
	byteBuffer []byte
	ring       *ringbuf[T]
	cfg        Config
	sequenceID atomic.Uint32
	started    atomic.Bool
}

// New allocates and returns a ready-to-use Batcher.
// It panics if cfg.QueueSize is zero or not a power of two, or if
// cfg.MaxBatchSize is smaller than headerSize.
func New[T Serializable](cfg Config) *Batcher[T] {
	if cfg.MaxBatchSize < headerSize {
		panic("tickbatch: Config.MaxBatchSize must be at least headerSize bytes")
	}
	return &Batcher[T]{
		ring:       newRingbuf[T](cfg.QueueSize),
		cfg:        cfg,
		byteBuffer: make([]byte, cfg.MaxBatchSize),
	}
}

// Push enqueues item into the ring buffer.
//
// This is the hot-path method. It is non-blocking and performs zero heap
// allocations. If the buffer is full, the configured [BackpressurePolicy] is
// applied — the caller is never stalled or panicked (graceful degradation contract).
func (b *Batcher[T]) Push(item T) {
	b.ring.push(item, b.cfg.Backpressure)
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

// runLoop is the internal drain loop. It ticks at cfg.TickRate Hz, draining
// the ring buffer into the pre-allocated slices and flushing serialized payloads
// to the configured Sink on every wake.
func (b *Batcher[T]) runLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second / time.Duration(b.cfg.TickRate))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
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
				continue
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

			if err := b.cfg.Sink.Flush(b.byteBuffer[:offset]); err != nil && b.cfg.OnFlushError != nil {
				b.cfg.OnFlushError(err)
			}
		}
	}
}
