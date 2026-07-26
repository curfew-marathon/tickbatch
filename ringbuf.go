package tickbatch

import "sync/atomic"

// paddedSeq is a sequence-number cell padded to a full CPU cache line.
//
// Each cell in the ring's seqs array sits on its own 64-byte line, so no two
// producers ever contend the same cache line when publishing adjacent slots.
// Without this padding, MPSC/MPMC fan-in produces MESI Read-For-Ownership
// storms on the shared lines, collapsing throughput under high producer counts.
type paddedSeq struct {
	val atomic.Uint64
	_   [56]byte // pad atomic.Uint64 (8 B) to a full 64-byte cache line
}

// ringbuf is a fixed-capacity, lock-free MPMC queue using the Dmitry Vyukov
// sequence-based algorithm.
//
// Sequence numbers and item payloads are stored in two parallel slices:
//
//   - seqs holds one paddedSeq per slot. Each paddedSeq occupies a full
//     64-byte cache line, so adjacent producers never contend the same line
//     when they race to publish their slot's sequence number.
//
//   - data holds the raw items, packed without inter-item padding. The consumer
//     reads sequentially through data, so dense packing maximizes prefetcher
//     efficiency on the drain path.
//
// The head and tail cursors are separated by 64 bytes of padding to place them
// on distinct CPU cache lines. Without this, every write to tail invalidates the
// cache line holding head on all other cores — a phenomenon called false sharing
// that can collapse throughput by an order of magnitude.
type ringbuf[T Serializable] struct {
	head atomic.Uint64
	_    [56]byte // pad head (8 B) to a full cache line

	tail atomic.Uint64
	_    [56]byte // pad tail (8 B) to a full cache line

	mask uint64
	seqs []paddedSeq // one 64-byte cell per slot; never shares a line between slots
	data []T         // item payloads, packed for consumer spatial locality
	_    [8]byte     // mask(8)+seqs_hdr(24)+data_hdr(24) = 56 B; pad to 64 B
	evicted atomic.Uint64
}

// newRingbuf allocates a ring buffer. The size must be a positive power of two;
// it panics otherwise.
func newRingbuf[T Serializable](size uint64) *ringbuf[T] {
	if size == 0 || size&(size-1) != 0 {
		panic("tickbatch: QueueSize must be a positive power of two")
	}
	r := &ringbuf[T]{
		mask: size - 1,
		seqs: make([]paddedSeq, size),
		data: make([]T, size),
	}
	// Each slot's initial sequence equals its index. This is the starting
	// invariant that the Vyukov algorithm depends on.
	for i := range r.seqs {
		r.seqs[i].val.Store(uint64(i))
	}
	return r
}

// maxEvictRetries caps the number of consecutive eviction attempts in push.
// If the head slot is claimed-but-not-published by a stalled producer,
// evictOldest is a no-op and the DropOldest loop would spin indefinitely.
// After this many attempts the loop degrades to DropNewest (returns false)
// so Push always returns promptly and the caller's DroppedCount is incremented.
const maxEvictRetries = 128

// push attempts to enqueue item. Returns false only when policy is [DropNewest]
// and the buffer is full, or when [DropOldest] exhausts its eviction retry cap.
//
// The fast path (diff == 0) is ordered first in the if-chain so the static
// branch predictor, which treats forward jumps as not-taken, favors the
// common case. The full/overtaken cases are cold and become forward jumps.
func (r *ringbuf[T]) push(item T, policy BackpressurePolicy) bool {
	var evictAttempts int
	for {
		pos := r.tail.Load()
		idx := pos & r.mask
		diff := int64(r.seqs[idx].val.Load() - pos)

		if diff == 0 {
			// Slot is ready for a producer at this position. Race to claim it.
			if r.tail.CompareAndSwap(pos, pos+1) {
				r.data[idx] = item
				// Publish to consumers: seq advances to pos+1, which is the
				// value the pop path waits for.
				r.seqs[idx].val.Store(pos + 1)
				return true
			}
			// Another producer won the CAS; retry from the new tail.
			continue
		}

		if diff < 0 {
			// seq has fallen behind pos: the slot has not been recycled yet,
			// meaning the buffer is full.
			if policy == DropNewest {
				return false
			}
			// DropOldest: evict the oldest item on a separate, non-inlined
			// path so its node weight is not charged against push's inline
			// budget, preserving the compiler's ability to inline push itself.
			// Bound the spin so a stalled producer cannot cause livelock.
			if evictAttempts++; evictAttempts > maxEvictRetries {
				return false
			}
			r.evictOldest()
		}
		// diff > 0: another producer already advanced tail past pos.
		// Reload tail and retry.
	}
}

// evictOldest forcibly advances the consumer head by one to orphan the oldest
// item, then recycles its slot so a producer can reuse it.
//
// Guard: only evict a slot whose producer has already published (seq ==
// headPos+1). If the head slot is still being written (producer won the tail
// CAS but has not yet stored seq+1), we spin rather than recycle — otherwise a
// second producer could claim the same slot concurrently and race on the item
// field.
//
// Marked noinline to keep this cold path out of push's inline budget.
//
//go:noinline
func (r *ringbuf[T]) evictOldest() {
	headPos := r.head.Load()
	idx := headPos & r.mask
	if r.seqs[idx].val.Load() == headPos+1 {
		if r.head.CompareAndSwap(headPos, headPos+1) {
			r.seqs[idx].val.Store(headPos + r.mask + 1)
			r.evicted.Add(1)
		}
	}
}

// popMarshal atomically dequeues the next item and marshals it directly into
// buf, returning the bytes written and whether an item was available.
// Marshal is called while the slot is claimed and before recycling, so no
// producer can observe the slot during this window.
func (r *ringbuf[T]) popMarshal(buf []byte) (int, bool) {
	for {
		pos := r.head.Load()
		idx := pos & r.mask
		diff := int64(r.seqs[idx].val.Load() - (pos + 1))

		switch {
		case diff == 0:
			if r.head.CompareAndSwap(pos, pos+1) {
				n := r.data[idx].Marshal(buf)
				r.seqs[idx].val.Store(pos + r.mask + 1)
				return n, true
			}
		case diff < 0:
			return 0, false
		}
	}
}

// pop attempts to dequeue into item. Returns false if the buffer is empty.
func (r *ringbuf[T]) pop(item *T) bool {
	for {
		pos := r.head.Load()
		idx := pos & r.mask
		diff := int64(r.seqs[idx].val.Load() - (pos + 1))

		switch {
		case diff == 0:
			// Slot has been written by a producer at this position. Race to consume it.
			if r.head.CompareAndSwap(pos, pos+1) {
				*item = r.data[idx]
				// Recycle the slot: advance seq to pos+mask+1, which signals
				// that a producer may use this slot again in the next lap.
				r.seqs[idx].val.Store(pos + r.mask + 1)
				return true
			}
		case diff < 0:
			// seq has not yet reached pos+1; the producer hasn't written here.
			// The buffer is empty from this consumer's perspective.
			return false
		default:
			// Another consumer already advanced head; reload and retry.
		}
	}
}
