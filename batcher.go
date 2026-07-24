package tickbatch

import (
	"context"
	"time"
)

// Config holds construction parameters for a [Batcher].
type Config struct {
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

	// Sink is the transport that receives each flushed payload.
	// If nil, drained batches are serialized but not delivered.
	Sink Sink

	// OnFlushError, if non-nil, is called whenever Sink.Flush returns an error.
	// It is invoked from the drain goroutine and must be goroutine-safe.
	OnFlushError func(error)
}

// Batcher is a generic, lock-free telemetry batching engine.
//
// The zero value is not usable; construct via [New].
type Batcher[T Serializable] struct {
	ring       *ringbuf[T]
	cfg        Config
	drainSlice []T
	byteBuffer []byte
}

// New allocates and returns a ready-to-use Batcher.
// It panics if cfg.QueueSize is zero or not a power of two.
func New[T Serializable](cfg Config) *Batcher[T] {
	return &Batcher[T]{
		ring:       newRingbuf[T](cfg.QueueSize),
		cfg:        cfg,
		drainSlice: make([]T, cfg.QueueSize),
		byteBuffer: make([]byte, cfg.MaxBatchSize),
	}
}

// Push enqueues item into the ring buffer.
//
// This is the hot-path method. It is non-blocking and performs zero heap
// allocations. If the buffer is full, item is silently dropped — the caller
// is never stalled or panicked (graceful degradation contract).
func (b *Batcher[T]) Push(item T) {
	b.ring.push(item)
}

// Start launches the tick engine in a background goroutine and returns a channel
// that is closed once the goroutine has fully exited.
//
// The engine runs until ctx is canceled, allowing callers to wait for a clean
// shutdown by receiving from the returned channel. It panics if Config.TickRate
// is not positive.
func (b *Batcher[T]) Start(ctx context.Context) <-chan struct{} {
	if b.cfg.TickRate <= 0 {
		panic("tickbatch: Config.TickRate must be positive")
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
			n := 0
			for n < len(b.drainSlice) {
				if !b.ring.pop(&b.drainSlice[n]) {
					break
				}
				n++
			}
			if n == 0 {
				continue
			}
			offset := 0
			for i := 0; i < n; i++ {
				offset += b.drainSlice[i].Marshal(b.byteBuffer[offset:])
			}
			if b.cfg.Sink == nil || offset == 0 {
				continue
			}
			if err := b.cfg.Sink.Flush(b.byteBuffer[:offset]); err != nil && b.cfg.OnFlushError != nil {
				b.cfg.OnFlushError(err)
			}
		}
	}
}
