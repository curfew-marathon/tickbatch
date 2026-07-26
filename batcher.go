package tickbatch

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"math"
	"sync/atomic"
	"time"
	"unsafe"
)

// headerSize is the byte length of the fixed header prepended to every flushed batch.
// Layout: bytes [0:4] sequence ID (uint32, little-endian), [4:6] item count (uint16,
// little-endian), [6:8] integrity tag (uint16, little-endian): bit 15 signals a keyframe
// (full-frame baseline reset for delta-encoding receivers), bits 0-14 carry the low 15 bits
// of CRC-32/IEEE over the raw payload body (bytes [8:N]). An empty body causes an early
// return before this tag is written, so receivers never observe a tag for an empty frame.
// The sequence ID is a uint32 that wraps to zero after 2^32-1 batches; no epoch or MAC is provided.
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
// Pointer-containing fields (the two interface values, OnFlushError, and the
// unexported newTicker) are grouped at the top of the struct so the GC pointer
// bitmap covers only the leading words rather than the whole struct, keeping the
// per-collection scan cost of every live [Batcher] low.
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

	// newTicker, when non-nil, overrides the internal time.NewTicker so tests can
	// drive the drain loop deterministically instead of depending on wall-clock
	// ticks. It is unexported and not part of the stable public surface; production
	// callers leave it nil and get the real monotonic ticker. It is grouped with the
	// other pointer-containing fields so the GC pointer bitmap stays compact.
	newTicker tickerFactory

	// QueueSize is the capacity of the internal ring buffer.
	// It must be a positive power of two (e.g. 1<<10 for 1024 slots).
	//
	// The type is uint64 (not int like the size/rate fields below) deliberately:
	// it is the ring's modular index space, matched to the atomic uint64 head/tail
	// counters and the uint64 returns of [Batcher.QueueDepth]/[Batcher.QueueCap].
	// This asymmetry is a conscious v1.0 API freeze, not an oversight.
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
	// when the context passed to [Batcher.Start] is canceled. A zero value disables
	// the timeout (the default), meaning runLoop blocks until the final flush
	// returns.
	//
	// When set, the deadline is carried on the context passed to the final
	// Sink.Flush, so a ctx-aware Sink (for example [TCPSink], which applies it as a
	// write deadline) cancels cleanly and leaves no goroutine writing after the
	// channel returned by Start is closed. For sinks that ignore the context, a
	// goroutine backstop still lets runLoop return at the deadline; in that case the
	// abandoned flush is reported via [Config.OnFlushError] if set (or a rate-limited
	// slog fallback), and callers must not call Sink.Close until they are certain the
	// goroutine has finished. Prefer a ctx-aware Sink to make shutdown leak-free.
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

	// KeyframeInterval, when non-zero and DeltaEncoding is true, emits a full
	// un-XORed payload every KeyframeInterval successful flushes. Keyframe batches
	// set bit 15 of the header integrity tag so receivers know to reset their delta
	// baseline. This bounds error propagation: a receiver that detects a bad CRC can
	// wait for the next keyframe rather than accumulating permanent drift.
	// A value of 1 effectively disables delta compression (every frame is a keyframe).
	// Ignored when DeltaEncoding is false.
	KeyframeInterval uint32
}

// tickerFactory produces a tick channel and a stop function for the drain loop.
// It abstracts time.NewTicker so [Config.newTicker] can inject a deterministic
// clock in tests.
type tickerFactory func(d time.Duration) (tickC <-chan time.Time, stop func())

// defaultTicker is the production [tickerFactory]: a real monotonic time.Ticker.
func defaultTicker(d time.Duration) (<-chan time.Time, func()) {
	t := time.NewTicker(d)
	return t.C, t.Stop
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
	// together - no cross-thread writes within this group. The block spans exactly
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
	lastLogTime    atomic.Int64 // unix nanos of last slog fallback; rate-limits duplicate errors
	flushInFlight  atomic.Bool  // serializes Sink.Flush when FlushTimeout is set
	_              [52]byte     // pad to 128 bytes: two full cache lines
}

// ErrInvalidConfig is the base error returned by [New] when the supplied
// [Config] is invalid. It is always wrapped with a specific reason; test for it
// with errors.Is(err, ErrInvalidConfig).
var ErrInvalidConfig = errors.New("tickbatch: invalid config")

// New allocates and returns a ready-to-use Batcher.
//
// It returns an error wrapping [ErrInvalidConfig] if cfg.QueueSize is zero or not
// a power of two, if cfg.MaxBatchSize is smaller than the fixed batch header or
// than headerSize+MaxItemSize, if cfg.MaxItemSize is not positive, if cfg.TickRate
// is outside [1, 1_000_000_000], if cfg.Backpressure is not a known
// [BackpressurePolicy], or if cfg.DeltaEncoding is set with a non-[ReliableSink].
// Use [MustNew] for the panic-on-error variant.
func New[T Serializable](cfg Config) (*Batcher[T], error) {
	if cfg.QueueSize == 0 || cfg.QueueSize&(cfg.QueueSize-1) != 0 {
		return nil, fmt.Errorf("%w: QueueSize must be a positive power of two, got %d", ErrInvalidConfig, cfg.QueueSize)
	}
	if cfg.MaxBatchSize < headerSize {
		return nil, fmt.Errorf("%w: MaxBatchSize must be at least %d bytes (the fixed batch header)", ErrInvalidConfig, headerSize)
	}
	if cfg.Backpressure != DropNewest && cfg.Backpressure != DropOldest {
		return nil, fmt.Errorf("%w: Backpressure is not a valid BackpressurePolicy", ErrInvalidConfig)
	}
	if cfg.MaxItemSize == 0 {
		// Default to the compile-time size of T. Valid for flat, pointer-free
		// structs (the enforced Serializable contract); unsafe.Sizeof does not
		// evaluate its argument, so no allocation occurs.
		var zero T
		cfg.MaxItemSize = int(unsafe.Sizeof(zero))
	}
	if cfg.MaxItemSize <= 0 {
		return nil, fmt.Errorf("%w: MaxItemSize must be positive", ErrInvalidConfig)
	}
	if cfg.MaxBatchSize < headerSize+cfg.MaxItemSize {
		return nil, fmt.Errorf("%w: MaxBatchSize must be >= headerSize + MaxItemSize (%d + %d = %d minimum)", ErrInvalidConfig, headerSize, cfg.MaxItemSize, headerSize+cfg.MaxItemSize)
	}
	if cfg.TickRate <= 0 {
		return nil, fmt.Errorf("%w: TickRate must be positive", ErrInvalidConfig)
	}
	if cfg.TickRate > 1_000_000_000 {
		return nil, fmt.Errorf("%w: TickRate must not exceed 1_000_000_000 (1 GHz; interval would round to zero)", ErrInvalidConfig)
	}
	if cfg.DeltaEncoding && cfg.Sink != nil {
		if _, ok := cfg.Sink.(ReliableSink); !ok {
			return nil, fmt.Errorf("%w: DeltaEncoding requires a ReliableSink; UDPSink and other fire-and-forget sinks are incompatible", ErrInvalidConfig)
		}
	}
	if cfg.newTicker == nil {
		cfg.newTicker = defaultTicker
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
	return b, nil
}

// MustNew is like [New] but panics if the config is invalid. It is the ergonomic
// fail-fast constructor for callers that treat misconfiguration as a programming
// error (the regexp.MustCompile precedent), and is safe to use in package-level
// var initializers.
func MustNew[T Serializable](cfg Config) *Batcher[T] {
	b, err := New[T](cfg)
	if err != nil {
		panic(err)
	}
	return b
}

// Push enqueues item into the ring buffer.
//
// This is the hot-path method. It is non-blocking and performs zero heap
// allocations. If the buffer is full, the configured [BackpressurePolicy] is
// applied - the caller is never stalled or panicked (graceful degradation contract).
// Under [DropNewest], the incoming item is silently discarded and [Batcher.DroppedCount]
// is incremented. Under [DropOldest], the oldest queued item is normally evicted and
// the new item always succeeds; in the rare case where eviction retries are exhausted
// because a stalled producer holds the head slot, Push degrades to DropNewest and
// DroppedCount is incremented.
func (b *Batcher[T]) Push(item T) {
	if !b.ring.push(item, b.cfg.Backpressure) {
		b.dropped.Add(1)
	}
}

// DroppedCount returns the cumulative number of items discarded because the ring
// buffer was full. Under [DropNewest] this occurs on every overflowing Push.
// Under [DropOldest] it is normally zero; it becomes non-zero only when the
// eviction spin exceeds its retry cap because a stalled producer has claimed
// the head slot but not yet published its sequence number, and Push degrades
// to DropNewest to preserve the non-blocking guarantee.
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
// accounting. A rising depth is a leading indicator of loss - it signals
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

// Stats is a snapshot of a [Batcher]'s operational counters, returned by
// [Batcher.Stats]. It mirrors the individual accessor methods in a single value so
// callers can sample every counter in one call instead of racing several.
//
// Consistency: the fields are gathered with independent atomic loads and no global
// lock (by design - a mutex on the drain path would defeat the lock-free engine), so
// a Stats value is eventually consistent and may be torn across fields. For example
// FlushedItems may reflect a flush that QueueDepth, read a moment later, does not.
// Use it for monitoring and alerting, not for exact cross-field accounting.
type Stats struct {
	// LastFlushAt is the time of the last successful flush ([Batcher.LastFlushAt]).
	// It is placed first because time.Time carries a pointer, keeping the struct's
	// pointer region compact for the GC (fieldalignment).
	LastFlushAt time.Time
	// Dropped is the cumulative items discarded on a full queue ([Batcher.DroppedCount]).
	Dropped uint64
	// Evicted is the cumulative items evicted under DropOldest ([Batcher.EvictedCount]).
	Evicted uint64
	// Truncated is the cumulative items dropped for a bad Marshal ([Batcher.TruncatedCount]).
	Truncated uint64
	// FlushedBatches is the total batches delivered ([Batcher.FlushedBatches]).
	FlushedBatches uint64
	// FlushedItems is the total items delivered ([Batcher.FlushedItems]).
	FlushedItems uint64
	// FlushErrors is the cumulative failed flushes ([Batcher.FlushErrorCount]).
	FlushErrors uint64
	// BytesFlushed is the total payload bytes delivered ([Batcher.BytesFlushed]).
	BytesFlushed uint64
	// CoalescedTicks is the cumulative skipped ticks ([Batcher.CoalescedTicks]).
	CoalescedTicks uint64
	// QueueDepth is a best-effort pending-item count ([Batcher.QueueDepth]).
	QueueDepth uint64
	// QueueCap is the ring capacity ([Batcher.QueueCap]).
	QueueCap uint64
}

// Stats returns a snapshot of all operational counters in a single call. See
// [Stats] for the eventual-consistency contract across fields.
func (b *Batcher[T]) Stats() Stats {
	return Stats{
		Dropped:        b.dropped.Load(),
		Evicted:        b.ring.evicted.Load(),
		Truncated:      b.truncated.Load(),
		FlushedBatches: b.flushedBatches.Load(),
		FlushedItems:   b.flushedItems.Load(),
		FlushErrors:    b.flushErrs.Load(),
		BytesFlushed:   b.bytesFlushed.Load(),
		CoalescedTicks: b.coalescedTicks.Load(),
		QueueDepth:     b.QueueDepth(),
		QueueCap:       b.QueueCap(),
		LastFlushAt:    b.LastFlushAt(),
	}
}

// Start launches the tick engine in a background goroutine and returns a channel
// that is closed once the goroutine has fully exited.
//
// The engine runs until ctx is canceled, allowing callers to wait for a clean
// shutdown by receiving from the returned channel. It panics if Start has already
// been called on this Batcher: double-start is a programming error, not a runtime
// condition, so it is a panic (not an error return) by deliberate v1.0 design.
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

// reportError routes err to [Config.OnFlushError] when set, otherwise to a
// rate-limited [slog.Default] fallback that emits at most one line per second.
// The rate limit is a lock-free atomic CAS on lastLogTime, so a partitioned sink
// ticking at [Config.TickRate] cannot flood the log with identical errors.
//
// When [Config.OnFlushError] is set it is invoked synchronously on the drain
// goroutine: a slow or blocking callback stalls the flush loop and backs up the
// ring. Keep it fast and non-blocking.
func (b *Batcher[T]) reportError(err error) {
	if b.cfg.OnFlushError != nil {
		b.cfg.OnFlushError(err)
		return
	}
	last := b.lastLogTime.Load()
	now := time.Now().UnixNano()
	if now-last > int64(time.Second) && b.lastLogTime.CompareAndSwap(last, now) {
		slog.Error("tickbatch flush failed", "err", err)
	}
}

// flushWithTimeout calls Sink.Flush, optionally bounding it with a deadline
// derived from base and [Config.FlushTimeout].
// If [Config.FlushTimeout] is zero, the call is direct and allocation-free; base
// (its cancellation and any deadline) is passed straight through to the Sink.
// If non-zero:
//   - A child context with the FlushTimeout deadline is derived and passed to
//     Sink.Flush so ctx-aware transports cancel cooperatively (net sinks apply it
//     as a write deadline). Deriving the context heap-allocates; this happens only
//     on the drain goroutine, never on the [Batcher.Push] hot path.
//   - flushInFlight serializes concurrent calls: if a previous flush is still
//     running (timed-out but not yet returned), [ErrFlushTimeout] is returned
//     immediately rather than stacking a second concurrent Sink.Flush. This is the
//     backstop for sinks that ignore ctx and keep blocking past the deadline.
//   - payload is copied into the pre-allocated timeoutBuf before the goroutine
//     launches, so the drain loop may freely overwrite byteBuffer/deltaBuffer/
//     compressBuffer without racing the abandoned goroutine.
//   - The buffered result channel guarantees the goroutine can always write its
//     result and GC cleanly, even if this call has already returned on timeout.
func (b *Batcher[T]) flushWithTimeout(base context.Context, payload []byte) error {
	if b.cfg.FlushTimeout == 0 {
		return b.cfg.Sink.Flush(base, payload)
	}
	fctx, cancel := context.WithTimeout(base, b.cfg.FlushTimeout)
	defer cancel()
	if !b.flushInFlight.CompareAndSwap(false, true) {
		return ErrFlushTimeout
	}
	n := copy(b.timeoutBuf, payload)
	snapshot := b.timeoutBuf[:n]
	ch := make(chan error, 1)
	go func() {
		ch <- b.cfg.Sink.Flush(fctx, snapshot)
		b.flushInFlight.Store(false)
	}()
	select {
	case err := <-ch:
		return err
	case <-fctx.Done():
		return ErrFlushTimeout
	}
}

// drainAndFlush dequeues all available items from the ring buffer, serializes
// them into the pre-allocated byteBuffer, and delivers the payload to the
// configured Sink. PrevOffset tracks the previous frame's byte length for
// delta-encoding stale-byte zeroing; keyframeN tracks position within the
// keyframe cycle when [Config.KeyframeInterval] is set. The base context bounds
// each Sink.Flush (see [Batcher.flushWithTimeout]). It performs zero heap
// allocations on the serialization path.
func (b *Batcher[T]) drainAndFlush(base context.Context, prevOffset *int, keyframeN *uint32) {
	offset := headerSize
	n := 0
	for n < math.MaxUint16 {
		avail := len(b.byteBuffer) - offset
		if avail < b.cfg.MaxItemSize {
			break
		}
		written, ok := b.ring.popMarshal(b.byteBuffer[offset:])
		if !ok {
			break
		}
		if written == 0 || written > avail {
			// written == 0: Marshal wrote nothing (bug or MaxItemSize too small).
			// written > avail: a misbehaving Marshal reported more than the buffer
			// holds; advancing offset would overrun byteBuffer and panic in the
			// header/CRC slicing below, violating the never-panic invariant. Both
			// cases discard the item and count it as truncated.
			b.truncated.Add(1)
			break
		}
		offset += written
		n++
	}
	if n == 0 || b.cfg.Sink == nil {
		return
	}

	// Header bytes [0:8] are explicit little-endian regardless of host byte order.
	// Body bytes [8:N] are native-endian: T.Marshal writes raw in-memory
	// representations via unsafe.Pointer. A big-endian receiver requires a
	// bespoke decoder that accounts for this asymmetry.
	seq := b.sequenceID.Add(1)
	b.byteBuffer[0] = byte(seq)
	b.byteBuffer[1] = byte(seq >> 8)
	b.byteBuffer[2] = byte(seq >> 16)
	b.byteBuffer[3] = byte(seq >> 24)
	count := uint16(n)
	b.byteBuffer[4] = byte(count)
	b.byteBuffer[5] = byte(count >> 8)

	// Integrity tag [6:8]: low 15 bits of CRC-32/IEEE over the raw payload body
	// so receivers can detect frame corruption and delta desync. Bit 15 signals a
	// keyframe (delta-encoding receivers reset their baseline on this flag).
	// CRC is computed over the raw body before any XOR transform so receivers
	// verify the reconstructed payload after reversing the delta.
	isKeyframe := b.cfg.DeltaEncoding && b.cfg.KeyframeInterval > 0 && *keyframeN == 0
	crc16 := uint16(crc32.ChecksumIEEE(b.byteBuffer[8:offset]) & 0x7fff)
	if isKeyframe {
		crc16 |= 0x8000
	}
	binary.LittleEndian.PutUint16(b.byteBuffer[6:8], crc16)

	if !b.cfg.DeltaEncoding {
		payload := b.byteBuffer[:offset]
		if b.cfg.Compressor != nil {
			cn, cerr := b.cfg.Compressor.Compress(b.compressBuffer, payload)
			if cerr != nil {
				b.reportError(cerr)
				return
			}
			if cn < 0 || cn > len(b.compressBuffer) {
				b.reportError(fmt.Errorf("tickbatch: compressor returned out-of-range n=%d", cn))
				return
			}
			payload = b.compressBuffer[:cn]
		}
		if err := b.flushWithTimeout(base, payload); err != nil {
			b.flushErrs.Add(1)
			b.reportError(err)
		} else {
			b.flushedBatches.Add(1)
			b.flushedItems.Add(uint64(n))
			b.bytesFlushed.Add(uint64(len(payload)))
			b.lastFlushAt.Store(time.Now().UnixNano())
		}
		return
	}

	// Keyframe: send the raw frame so receivers can reset their delta baseline.
	// Normal delta: XOR the full buffer (header + body) against previousState.
	// XORing the header ensures the CRC and flags are also differentially encoded,
	// and the receiver recovers the correct header by reversing the XOR.
	var payload []byte
	if isKeyframe {
		payload = b.byteBuffer[:offset]
	} else {
		xorBytes(b.deltaBuffer[:offset], b.byteBuffer[:offset], b.previousState[:offset])
		payload = b.deltaBuffer[:offset]
	}
	if b.cfg.Compressor != nil {
		cn, cerr := b.cfg.Compressor.Compress(b.compressBuffer, payload)
		if cerr != nil {
			b.reportError(cerr)
			return
		}
		if cn < 0 || cn > len(b.compressBuffer) {
			b.reportError(fmt.Errorf("tickbatch: compressor returned out-of-range n=%d", cn))
			return
		}
		payload = b.compressBuffer[:cn]
	}
	if err := b.flushWithTimeout(base, payload); err != nil {
		b.flushErrs.Add(1)
		b.reportError(err)
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
		if b.cfg.KeyframeInterval > 0 {
			(*keyframeN)++
			if *keyframeN >= b.cfg.KeyframeInterval {
				*keyframeN = 0
			}
		}
	}
}

// runLoop is the internal drain loop. It ticks at cfg.TickRate Hz, draining
// the ring buffer into the pre-allocated slices and flushing serialized payloads
// to the configured Sink on every wake. When the context is canceled, it performs
// one final drain to flush any remaining items before exiting.
func (b *Batcher[T]) runLoop(ctx context.Context) {
	tickInterval := time.Second / time.Duration(b.cfg.TickRate)
	tickC, stop := b.cfg.newTicker(tickInterval)
	defer stop()

	var prevOffset int
	var keyframeN uint32

	for {
		select {
		case <-ctx.Done():
			b.finalDrain(&prevOffset, &keyframeN)
			return
		case <-tickC:
			tickStart := time.Now()
			// Normal-tick flushes use a fresh background base rather than ctx so an
			// in-flight flush is never torn by cancellation mid-write; shutdown is
			// handled separately by finalDrain. FlushTimeout still bounds each flush.
			b.drainAndFlush(context.Background(), &prevOffset, &keyframeN)
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

// finalDrain performs the single drain that runs after the Start context is
// canceled. It flushes any items still queued.
//
// The base context is derived from context.Background (never the already-canceled
// Start context) so the final flush is not aborted before it can deliver. When
// [Config.ShutdownTimeout] is set, that base carries the timeout, so a ctx-aware
// Sink cancels cleanly at the deadline - the well-behaved path leaks nothing.
// A goroutine backstop still bounds runLoop's return for sinks that ignore ctx and
// keep blocking; that abandoned goroutine is reported via [Batcher.reportError].
// When ShutdownTimeout is zero, the drain runs inline and blocks until the final
// flush returns (the historical default).
func (b *Batcher[T]) finalDrain(prevOffset *int, keyframeN *uint32) {
	if b.cfg.ShutdownTimeout <= 0 {
		b.drainAndFlush(context.Background(), prevOffset, keyframeN)
		return
	}
	base, cancel := context.WithTimeout(context.Background(), b.cfg.ShutdownTimeout)
	defer cancel()
	ch := make(chan struct{})
	go func() {
		b.drainAndFlush(base, prevOffset, keyframeN)
		close(ch)
	}()
	select {
	case <-ch:
	case <-base.Done():
		b.reportError(fmt.Errorf("tickbatch: shutdown flush did not complete within %v; drain goroutine still running", b.cfg.ShutdownTimeout))
	}
}
