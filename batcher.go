package tickbatch

// Config holds construction parameters for a [Batcher].
type Config struct {
	// QueueSize is the capacity of the internal ring buffer.
	// It must be a positive power of two (e.g. 1<<10 for 1024 slots).
	QueueSize uint64
}

// Batcher is a generic, lock-free telemetry batching engine.
//
// The zero value is not usable; construct via [New].
type Batcher[T Serializable] struct {
	ring *ringbuf[T]
	cfg  Config
}

// New allocates and returns a ready-to-use Batcher.
// It panics if cfg.QueueSize is zero or not a power of two.
func New[T Serializable](cfg Config) *Batcher[T] {
	return &Batcher[T]{
		ring: newRingbuf[T](cfg.QueueSize),
		cfg:  cfg,
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
