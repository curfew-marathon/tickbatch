package tickbatch

import "sync/atomic"

// slot is a single cell in the ring buffer. It pairs an item with a sequence
// number used by the Vyukov MPMC algorithm to coordinate producers and consumers
// without locks.
type slot[T Serializable] struct {
	seq  atomic.Uint64
	item T
}

// ringbuf is a fixed-capacity, lock-free MPMC queue using the Dmitry Vyukov
// sequence-based algorithm.
//
// The head and tail cursors are separated by 64 bytes of padding to place them
// on distinct CPU cache lines. Without this, every write to tail invalidates the
// cache line holding head on all other cores — a phenomenon called False Sharing
// that can collapse throughput by an order of magnitude.
type ringbuf[T Serializable] struct {
	head atomic.Uint64
	_    [64]byte // padding: isolates head from tail in the CPU cache

	tail atomic.Uint64
	_    [64]byte // padding: isolates tail from subsequent fields

	mask uint64
	data []slot[T]
	_    [16]byte      // padding: places evicted on its own cache line (mask+data=32 B)
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
		data: make([]slot[T], size),
	}
	// Each slot's initial sequence equals its index. This is the starting
	// invariant that the Vyukov algorithm depends on.
	for i := range r.data {
		r.data[i].seq.Store(uint64(i))
	}
	return r
}

// push attempts to enqueue item. Returns false only when policy is [DropNewest]
// and the buffer is full; with [DropOldest] it always retries after eviction.
func (r *ringbuf[T]) push(item T, policy BackpressurePolicy) bool {
	for {
		pos := r.tail.Load()
		s := &r.data[pos&r.mask]
		seq := s.seq.Load()
		diff := int64(seq - pos)

		switch {
		case diff == 0:
			// Slot is ready for a producer at this position. Race to claim it.
			if r.tail.CompareAndSwap(pos, pos+1) {
				s.item = item
				// Publish to consumers: seq advances to pos+1, which is the
				// value the pop path waits for.
				s.seq.Store(pos + 1)
				return true
			}
			// Another producer won the CAS; retry from the new tail.
		case diff < 0:
			// seq has fallen behind pos: the slot has not been recycled yet,
			// meaning the buffer is full.
			if policy == DropOldest {
				// Forcibly advance the consumer head by one to orphan the
				// oldest item, then recycle its slot so a producer can reuse
				// it. This is the lock-free eviction path: no mutex, no alloc.
				//
				// Guard: only evict a slot whose producer has already published
				// (seq == headPos+1). If the head slot is still being written
				// (producer won the tail CAS but has not yet stored seq+1), we
				// spin rather than recycle — otherwise a second producer could
				// claim the same slot concurrently and race on the item field.
				headPos := r.head.Load()
				hs := &r.data[headPos&r.mask]
				if hs.seq.Load() == headPos+1 {
					if r.head.CompareAndSwap(headPos, headPos+1) {
						hs.seq.Store(headPos + r.mask + 1)
						r.evicted.Add(1)
					}
				}
				// Retry regardless: if the CAS lost or the slot was not yet
				// published, another goroutine made progress; loop to retry.
			} else {
				return false
			}
		default:
			// seq > pos: another producer already advanced tail past pos.
			// Reload tail and retry.
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
		s := &r.data[pos&r.mask]
		seq := s.seq.Load()
		diff := int64(seq - (pos + 1))

		switch {
		case diff == 0:
			if r.head.CompareAndSwap(pos, pos+1) {
				n := s.item.Marshal(buf)
				s.seq.Store(pos + r.mask + 1)
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
		s := &r.data[pos&r.mask]
		seq := s.seq.Load()
		diff := int64(seq - (pos + 1))

		switch {
		case diff == 0:
			// Slot has been written by a producer at this position. Race to consume it.
			if r.head.CompareAndSwap(pos, pos+1) {
				*item = s.item
				// Recycle the slot: advance seq to pos+mask+1, which signals
				// that a producer may use this slot again in the next lap.
				s.seq.Store(pos + r.mask + 1)
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
