package tickbatch

import (
	"context"
	"errors"
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
	// Must satisfy MaxBatchSize >= headerSize + MaxItemSize.
	MaxBatchSize int

	// MaxItemSize is the maximum number of bytes that any single item's Marshal
	// method can write. The drain loop stops before calling popMarshal when the
	// remaining buffer space is smaller than this value, leaving the item queued
	// for the next tick instead of dequeuing it into a buffer that cannot hold it.
	// Must be positive.
	MaxItemSize int

	// TickRate is the number of drain cycles executed per second.
	// A value of 60 causes the engine to wake and flush the queue 60 times per second.
	// Must be in the range [1, 1_000_000_000]; [New] panics if either bound is violated.
	TickRate int

	// Backpressure selects the policy applied when Push is called on a full queue.
	// The zero value is [DropNewest].
	Backpressure BackpressurePolicy

	// ShutdownTimeout bounds the duration of the final Sink.Flush call that runs
	// when the context passed to [Batcher.Start] is canceled. If the flush does
	// not complete within this duration, runLoop returns anyway, leaving the flush
	// goroutine to finish in the background. A zero value disables the timeout
	// (the default), meaning runLoop blocks until the final flush returns.
	//
	// When the timeout fires, the channel returned by Start is closed immediately.
	// The background flush goroutine may still be writing to the Sink. Callers
	// must not call Sink.Close until they are certain the goroutine has finished
	// (for example, by waiting an additional ShutdownTimeout after <-done).
	// The abandoned flush is reported via [Config.OnFlushError] if set, or via
	// log.Printf otherwise.
	ShutdownTimeout time.Duration

	// FlushTimeout bounds how long a single Sink.Flush call may block before the
	// drain goroutine treats it as an error. Zero disables the per-flush timeout
	// (the default). When the timeout fires, [ErrFlushTimeout] is passed to
	// [Config.OnFlushError] (or logged) and the flush is abandoned; the underlying
	// goroutine may still be running until the OS unblocks the I/O, but the drain
	// loop is immediately free to process subsequent ticks.
	//
	// This is the primary lever for bounding the tarpit failure mode: without it,
	// a slow or partitioned sink parks the sole drain goroutine indefinitely,
	// silently stalling delivery while producers continue filling the ring.
	FlushTimeout time.Duration

	// DeltaEncoding, when true, XORs each flushed payload against the previous
	// frame before passing it to Sink.Flush. Receivers must XOR with their own
	// copy of the prior frame to reconstruct the original batch. When false
	// (the default), Sink.Flush receives the fully-serialized raw payload,
	// preserving the original Sink contract.
	//
	// DeltaEncoding is semantically unsafe over fire-and-forget transports such
	// as [UDPSink]. A lost datagram advances the sender's delta baseline while the
	// receiver never processes that frame; every subsequent XOR produces permanently
	// corrupt output. Sink must implement [ReliableSink] when DeltaEncoding is true;
	// [New] panics otherwise.
	DeltaEncoding bool
}

// ErrFlushTimeout is passed to [Config.OnFlushError] when a Sink.Flush call
// exceeds [Config.FlushTimeout]. The underlying flush goroutine may still be
// running; the drain loop proceeds to subsequent ticks without waiting for it.
var ErrFlushTimeout = errors.New("tickbatch: flush timeout exceeded")

// Batcher is a generic, lock-free telemetry batching engine.
//
// The zero value is not usable; construct via [New].
type Batcher[T Serializable] struct {
	// Immutable after construction.
	byteBuffer     []byte
	previousState  []byte
	deltaBuffer    []byte
	compressBuffer []byte
	timeoutBuf     []byte // snapshot buffer for timed-out flushes; nil when FlushTimeout == 0
	ring           *ringbuf[T]
	cfg            Config
	started        atomic.Bool

	// Producer-written on every overflow (any Push() goroutine). Isolated on its
	// own cache line to prevent false sharing with the consumer group below.
	dropped atomic.Uint64
	_       [56]byte // 8 (dropped) + 56 = 64 bytes: one full cache line

	// Consumer-written exclusively by the background flush goroutine. Grouped
	// together — no cross-thread writes within this group. The block spans exactly
	// 128 bytes (two cache lines): 4+4+8+8+8+8+8+8+8+4+60 = 128.
	sequenceID     atomic.Uint32
	_              [4]byte // align next uint64
	truncated      atomic.Uint64
	flushedBatches atomic.Uint64
	flushedItems   atomic.Uint64
	flushErrs      atomic.Uint64
	bytesFlushed   atomic.Uint64
	lastFlushAt    atomic.Int64 // unix nanos; 0 = never flushed
	coalescedTicks atomic.Uint64
	flushInFlight  atomic.Bool     // serializes Sink.Flush when FlushTimeout is set
	_              [60]byte        // pad to 128 bytes: two full cache lines
}

// New allocates and returns a ready-to-use Batcher.
// It panics if cfg.QueueSize is zero or not a power of two, if
// cfg.MaxBatchSize is smaller than the fixed batch header, if cfg.MaxItemSize
// is not positive, if cfg.TickRate is not positive, or if cfg.Backpressure
// is not a known [BackpressurePolicy] constant.
func New[T Serializable](cfg Config) *Batcher[T] {
	if cfg.MaxBatchSize < headerSize {
		panic(fmt.Sprintf("tickbatch: Config.MaxBatchSize must be at least %d bytes (the fixed batch header)", headerSize))
	}
	if cfg.Backpressure != DropNewest && cfg.Backpressure != DropOldest {
		panic("tickbatch: Config.Backpressure is not a valid BackpressurePolicy")
	}
	if cfg.MaxItemSize <= 0 {
		panic("tickbatch: Config.MaxItemSize must be positive")
	}
	if cfg.MaxBatchSize < headerSize+cfg.MaxItemSize {
		panic(fmt.Sprintf("tickbatch: Config.MaxBatchSize must be >= headerSize + MaxItemSize (%d + %d = %d minimum)", headerSize, cfg.MaxItemSize, headerSize+cfg.MaxItemSize))
	}
	if cfg.TickRate <= 0 {
		panic("tickbatch: Config.TickRate must be positive")
	}
	if cfg.TickRate > 1_000_000_000 {
		panic("tickbatch: Config.TickRate must not exceed 1_000_000_000 (1 GHz; interval would round to zero)")
	}
	if cfg.DeltaEncoding && cfg.Sink != nil {
		if _, ok := cfg.Sink.(ReliableSink); !ok {
			panic("tickbatch: DeltaEncoding requires a ReliableSink; UDPSink and other fire-and-forget sinks are incompatible")
		}
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
	if cfg.FlushTimeout > 0 {
		b.timeoutBuf = make([]byte, cfg.MaxBatchSize)
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

// EvictedCount returns the cumulative number of items forcibly evicted from the
// head of the ring buffer under the [DropOldest] backpressure policy. It is always
// zero when using [DropNewest]. Use DroppedCount() + EvictedCount() to observe
// total data loss across both policies.
func (b *Batcher[T]) EvictedCount() uint64 {
	return b.ring.evicted.Load()
}

// TruncatedCount returns the number of items that were dequeued by the drain loop
// but discarded because their Marshal method returned zero bytes. A non-zero value
// indicates either a bug in T.Marshal or a [Config.MaxItemSize] that is smaller than
// the actual encoded size of T.
func (b *Batcher[T]) TruncatedCount() uint64 {
	return b.truncated.Load()
}

// FlushedBatches returns the total number of batches delivered to [Sink].
func (b *Batcher[T]) FlushedBatches() uint64 {
	return b.flushedBatches.Load()
}

// FlushedItems returns the total number of items serialized and delivered across
// all flushes. Dividing by [Batcher.FlushedBatches] gives the average batch fill rate.
func (b *Batcher[T]) FlushedItems() uint64 {
	return b.flushedItems.Load()
}

// FlushErrorCount returns the cumulative number of failed Sink.Flush calls,
// including flushes abandoned due to [Config.FlushTimeout]. A rising count while
// [Batcher.LastFlushAt] is stale is the primary indicator of a tarpitted or
// partitioned downstream sink.
func (b *Batcher[T]) FlushErrorCount() uint64 {
	return b.flushErrs.Load()
}

// BytesFlushed returns the total number of payload bytes successfully delivered
// to [Sink.Flush] across all batches. Together with [Batcher.FlushedItems] this
// enables ingested-vs-delivered reconciliation to quantify silent data loss.
func (b *Batcher[T]) BytesFlushed() uint64 {
	return b.bytesFlushed.Load()
}

// LastFlushAt returns the wall-clock time of the most recent successful
// Sink.Flush call. Returns the zero [time.Time] if no flush has completed yet.
// Use time.Since(b.LastFlushAt()) as the MTTR clock during a partition: a value
// exceeding your SLO tolerance means no data has been delivered since that point.
func (b *Batcher[T]) LastFlushAt() time.Time {
	ns := b.lastFlushAt.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// QueueDepth returns a best-effort snapshot of the number of items currently
// waiting in the ring buffer. Because head and tail are read independently
// without a lock, the value may be transiently inconsistent; use it for
// saturation alerting (e.g. QueueDepth()/QueueCap() > 0.8) rather than exact
// accounting. A rising depth is a leading indicator of loss — it signals
// backpressure before DroppedCount or EvictedCount become non-zero.
func (b *Batcher[T]) QueueDepth() uint64 {
	tail := b.ring.tail.Load()
	head := b.ring.head.Load()
	if tail >= head {
		return tail - head
	}
	// Transient read ordering: head appears ahead of tail; report zero rather
	// than wrapping a uint64 to ~18 quintillion.
	return 0
}

// QueueCap returns the ring buffer capacity as set by [Config.QueueSize].
// [New] panics if QueueSize is not already a positive power of two.
func (b *Batcher[T]) QueueCap() uint64 {
	return b.ring.mask + 1
}

// CoalescedTicks returns the number of drain cycles that were skipped because a
// full drain cycle (serialization, compression, and Sink.Flush) overran the
// configured tick interval. The count is computed arithmetically
// (floor(drain_duration/tick_interval) - 1) rather than from the ticker channel
// depth, which silently caps at 1 regardless of actual slippage.
// A rising value means the engine is draining slower than [Config.TickRate];
// consider lowering [Config.TickRate] to reduce the tick interval pressure, or
// setting [Config.FlushTimeout] to bound how long a single Sink.Flush may block.
func (b *Batcher[T]) CoalescedTicks() uint64 {
	return b.coalescedTicks.Load()
}

// Start launches the tick engine in a background goroutine and returns a channel
// that is closed once the goroutine has fully exited.
//
// The engine runs until ctx is canceled, allowing callers to wait for a clean
// shutdown by receiving from the returned channel. It panics if Start has already
// been called on this Batcher.
//
// Callers must halt all producers before canceling the context. Any [Batcher.Push]
// call that races with the final drain after cancellation may be silently lost;
// quiescing producers is the caller's responsibility.
func (b *Batcher[T]) Start(ctx context.Context) <-chan struct{} {
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

// flushWithTimeout calls Sink.Flush, optionally bounding it with a deadline.
// If [Config.FlushTimeout] is zero, the call is direct and allocation-free.
// If non-zero:
//   - flushInFlight serializes concurrent calls: if a previous flush is still
//     running (timed-out but not yet returned), [ErrFlushTimeout] is returned
//     immediately rather than stacking a second concurrent Sink.Flush.
//   - payload is copied into the pre-allocated timeoutBuf before the goroutine
//     launches, so the drain loop may freely overwrite byteBuffer/deltaBuffer/
//     compressBuffer without racing the abandoned goroutine.
//   - The buffered result channel guarantees the goroutine can always write its
//     result and GC cleanly, even if this call has already returned on timeout.
func (b *Batcher[T]) flushWithTimeout(payload []byte) error {
	if b.cfg.FlushTimeout == 0 {
		return b.cfg.Sink.Flush(payload)
	}
	if !b.flushInFlight.CompareAndSwap(false, true) {
		return ErrFlushTimeout
	}
	n := copy(b.timeoutBuf, payload)
	snapshot := b.timeoutBuf[:n]
	ch := make(chan error, 1)
	go func() {
		ch <- b.cfg.Sink.Flush(snapshot)
		b.flushInFlight.Store(false)
	}()
	t := time.NewTimer(b.cfg.FlushTimeout)
	defer t.Stop()
	select {
	case err := <-ch:
		return err
	case <-t.C:
		return ErrFlushTimeout
	}
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
		if len(b.byteBuffer)-offset < b.cfg.MaxItemSize {
			break
		}
		written, ok := b.ring.popMarshal(b.byteBuffer[offset:])
		if !ok {
			break
		}
		if written == 0 {
			b.truncated.Add(1)
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
		if err := b.flushWithTimeout(payload); err != nil {
			b.flushErrs.Add(1)
			if b.cfg.OnFlushError != nil {
				b.cfg.OnFlushError(err)
			} else {
				log.Printf("tickbatch: %v", err)
			}
		} else {
			b.flushedBatches.Add(1)
			b.flushedItems.Add(uint64(n))
			b.bytesFlushed.Add(uint64(len(payload)))
			b.lastFlushAt.Store(time.Now().UnixNano())
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
	if err := b.flushWithTimeout(payload); err != nil {
		b.flushErrs.Add(1)
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
		b.flushedBatches.Add(1)
		b.flushedItems.Add(uint64(n))
		b.bytesFlushed.Add(uint64(len(payload)))
		b.lastFlushAt.Store(time.Now().UnixNano())
	}
}

// runLoop is the internal drain loop. It ticks at cfg.TickRate Hz, draining
// the ring buffer into the pre-allocated slices and flushing serialized payloads
// to the configured Sink on every wake. When the context is canceled, it performs
// one final drain to flush any remaining items before exiting.
func (b *Batcher[T]) runLoop(ctx context.Context) {
	tickInterval := time.Second / time.Duration(b.cfg.TickRate)
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	var prevOffset int

	for {
		select {
		case <-ctx.Done():
			if b.cfg.ShutdownTimeout > 0 {
				ch := make(chan struct{})
				go func() {
					b.drainAndFlush(&prevOffset)
					close(ch)
				}()
				timer := time.NewTimer(b.cfg.ShutdownTimeout)
				select {
				case <-ch:
					timer.Stop()
				case <-timer.C:
					timeoutErr := fmt.Errorf("tickbatch: shutdown flush did not complete within %v; drain goroutine still running", b.cfg.ShutdownTimeout)
					if b.cfg.OnFlushError != nil {
						b.cfg.OnFlushError(timeoutErr)
					} else {
						log.Printf("%v", timeoutErr)
					}
				}
			} else {
				b.drainAndFlush(&prevOffset)
			}
			return
		case <-ticker.C:
			tickStart := time.Now()
			b.drainAndFlush(&prevOffset)
			// Measure true tick slippage arithmetically rather than via len(ticker.C).
			// Go's ticker channel has depth 1 and silently drops overflowed ticks, so
			// a 500ms flush against a 10ms tick interval would count as 1 missed tick
			// from channel length, but floor(500/10)-1 = 49 is the true slippage.
			if elapsed := time.Since(tickStart); elapsed > tickInterval {
				b.coalescedTicks.Add(uint64(elapsed/tickInterval) - 1)
			}
		}
	}
}
