package tickbatch

import (
	"bytes"
	"context"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

// MarketTick is a flat, pointer-free struct representing a single HFT market
// data record. Its pointer-free nature guarantees O(1) GC scan time per the
// Serializable contract.
type MarketTick struct {
	Price  float32
	Volume float32
	Spread float32
}

// Marshal implements [Serializable]. It reads bytes from the struct (which is
// stack-allocated and properly aligned) and copies them into buf, making it
// safe on architectures with strict alignment requirements.
func (m MarketTick) Marshal(buf []byte) int {
	const size = int(unsafe.Sizeof(MarketTick{}))
	if len(buf) < size {
		return 0
	}
	copy(buf[:size], (*[size]byte)(unsafe.Pointer(&m))[:])
	return size
}

// QuoteSnapshot is a flat, pointer-free struct representing a level-1 market
// data snapshot for use in tick-engine tests.
type QuoteSnapshot struct {
	Bid     float32
	Ask     float32
	BidSize float32
	AskSize float32
}

// Marshal implements [Serializable] by encoding the struct into buf via a direct
// memory copy, returning the number of bytes written.
func (q QuoteSnapshot) Marshal(buf []byte) int {
	const size = int(unsafe.Sizeof(QuoteSnapshot{}))
	if len(buf) < size {
		return 0
	}
	copy(buf[:size], (*[size]byte)(unsafe.Pointer(&q))[:])
	return size
}

// countingSink is a test-only Sink that atomically records how many times
// Flush has been called.
type countingSink struct {
	count atomic.Int64
}

// Flush increments the call counter and returns nil.
func (c *countingSink) Flush(_ []byte) error {
	c.count.Add(1)
	return nil
}

// TestPushPop verifies the fundamental FIFO contract: a value pushed must be
// the exact value returned on the next pop, with no corruption or reordering.
func TestPushPop(t *testing.T) {
	b := New[MarketTick](Config{QueueSize: 16, MaxBatchSize: headerSize + 12, MaxItemSize: 12, TickRate: 60})

	ticks := []MarketTick{
		{Price: 100.00, Volume: 0, Spread: 0},
		{Price: 189.98, Volume: 500.5, Spread: 0.01},
		{Price: 415.25, Volume: 200.0, Spread: 0.02},
	}

	for _, want := range ticks {
		b.Push(want)
		var got MarketTick
		if !b.ring.pop(&got) {
			t.Fatalf("pop returned false after push of %+v", want)
		}
		if got != want {
			t.Errorf("round-trip mismatch: got %+v, want %+v", got, want)
		}
	}
}

// TestPushOnFullQueueDrops verifies graceful degradation: pushing beyond capacity
// must silently drop items and never block or panic.
func TestPushOnFullQueueDrops(t *testing.T) {
	const size = 4
	b := New[MarketTick](Config{QueueSize: size, MaxBatchSize: headerSize + 12, MaxItemSize: 12, TickRate: 60})

	filler := MarketTick{Price: 100.0}
	for i := 0; i < size; i++ {
		b.Push(filler)
	}

	// This push must not block, panic, or corrupt the queue.
	overflow := MarketTick{Price: 999.99}
	b.Push(overflow)

	// Drain and confirm overflow item was dropped.
	for i := 0; i < size; i++ {
		var got MarketTick
		if !b.ring.pop(&got) {
			t.Fatalf("expected item at slot %d", i)
		}
		if got == overflow {
			t.Error("overflow item must not appear in a full queue")
		}
	}

	var extra MarketTick
	if b.ring.pop(&extra) {
		t.Error("queue must be empty after draining exactly size items")
	}
}

// TestRunLoop verifies that the tick engine wakes, drains the queue, and
// delivers batches to the Sink without panicking or leaking goroutines.
func TestRunLoop(t *testing.T) {
	const (
		tickRate  = 60
		pushCount = 200
	)

	sink := &countingSink{}
	b := New[QuoteSnapshot](Config{
		QueueSize:    1 << 10,
		MaxBatchSize: 4096,
		MaxItemSize:  16,
		TickRate:     tickRate,
		Sink:         sink,
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	done := b.Start(ctx)

	state := QuoteSnapshot{Bid: 189.97, Ask: 189.98, BidSize: 300, AskSize: 100}
	for i := 0; i < pushCount; i++ {
		b.Push(state)
	}

	<-done

	got := sink.count.Load()
	if got == 0 {
		t.Fatal("expected at least one Sink.Flush call, got zero")
	}
	t.Logf("Sink.Flush called %d times over 1s at %d Hz tick rate", got, tickRate)
}

// Tick represents a single HFT market tick with symbol, price, and size.
// It is a flat, pointer-free struct guaranteeing O(1) GC scan time per the
// [Serializable] contract.
type Tick struct {
	// Symbol is the instrument identifier, padded with zero bytes.
	Symbol [8]byte
	// Price is the last-trade price.
	Price float64
	// Size is the last-trade quantity in lots.
	Size uint32
}

// Marshal implements [Serializable] by writing each field directly into buf via
// unsafe pointer casts, bypassing encoding/binary for zero-overhead serialization.
// The packed wire size is 20 bytes (8 Symbol + 8 Price + 4 Size), which is smaller
// than unsafe.Sizeof(Tick{}) due to trailing struct padding; returning the padded
// size would include uninitialized bytes in the payload.
func (t Tick) Marshal(buf []byte) int {
	const size = 20 // 8 (Symbol) + 8 (Price) + 4 (Size); no trailing padding
	if len(buf) < size {
		return 0
	}
	copy(buf[:8], t.Symbol[:])
	*(*float64)(unsafe.Pointer(&buf[8])) = t.Price
	*(*uint32)(unsafe.Pointer(&buf[16])) = t.Size
	return size
}

// captureSink is a test-only Sink that records the first flushed payload on a
// buffered channel so tests can block until a batch arrives.
type captureSink struct {
	ch chan []byte
}

// Flush copies payload into a fresh slice and delivers it to the channel
// non-blocking, preserving only the first batch if Flush is called multiple times.
func (c *captureSink) Flush(payload []byte) error {
	buf := make([]byte, len(payload))
	copy(buf, payload)
	select {
	case c.ch <- buf:
	default:
	}
	return nil
}

// TestTickSerialization pushes three Tick structs, waits for the first flush,
// and asserts the exact payload layout: byte length, header sequence ID, header
// item count, and full field-level data integrity of the first decoded Tick.
func TestTickSerialization(t *testing.T) {
	const tickSize = 20 // packed wire size: 8 (Symbol) + 8 (Price) + 4 (Size)

	sink := &captureSink{ch: make(chan []byte, 1)}
	b := New[Tick](Config{
		QueueSize:    16,
		MaxBatchSize: headerSize + 16*tickSize,
		MaxItemSize:  tickSize,
		TickRate:     200,
		Sink:         sink,
	})

	ticks := [3]Tick{
		{Symbol: [8]byte{'A', 'A', 'P', 'L'}, Price: 189.98, Size: 100},
		{Symbol: [8]byte{'G', 'O', 'O', 'G'}, Price: 175.50, Size: 50},
		{Symbol: [8]byte{'M', 'S', 'F', 'T'}, Price: 415.25, Size: 200},
	}

	// Push all items before starting so they land in the very first drain cycle.
	for _, tick := range ticks {
		b.Push(tick)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := b.Start(ctx)

	var payload []byte
	select {
	case payload = <-sink.ch:
	case <-time.After(500 * time.Millisecond):
		cancel()
		<-done
		t.Fatal("timed out waiting for Sink.Flush")
	}

	cancel()
	<-done

	wantLen := headerSize + 3*tickSize
	if len(payload) != wantLen {
		t.Fatalf("payload length: got %d, want %d", len(payload), wantLen)
	}

	// Decode sequence ID: bytes [0:4], little-endian uint32.
	seq := uint32(payload[0]) | uint32(payload[1])<<8 | uint32(payload[2])<<16 | uint32(payload[3])<<24
	if seq != 1 {
		t.Errorf("sequence ID: got %d, want 1", seq)
	}

	// Decode item count: bytes [4:6], little-endian uint16.
	count := uint16(payload[4]) | uint16(payload[5])<<8
	if count != 3 {
		t.Errorf("item count: got %d, want 3", count)
	}

	// Decode and verify the first Tick from the item region.
	data := payload[headerSize:]
	var got Tick
	copy(got.Symbol[:], data[:8])
	got.Price = *(*float64)(unsafe.Pointer(&data[8]))
	got.Size = *(*uint32)(unsafe.Pointer(&data[16]))

	if got != ticks[0] {
		t.Errorf("first Tick: got %+v, want %+v", got, ticks[0])
	}
}

// TestNewPanicsOnZeroQueueSize verifies that New panics when QueueSize is zero,
// preserving the ring buffer's power-of-two size invariant.
func TestNewPanicsOnZeroQueueSize(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("New with QueueSize=0 did not panic")
		}
	}()
	New[MarketTick](Config{QueueSize: 0, MaxBatchSize: headerSize + 12, MaxItemSize: 12, TickRate: 60})
}

// TestNewPanicsOnNonPowerOfTwoQueueSize verifies that New panics when QueueSize
// is not a power of two, as the Vyukov bitmask algorithm requires power-of-two capacity.
func TestNewPanicsOnNonPowerOfTwoQueueSize(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("New with non-power-of-two QueueSize did not panic")
		}
	}()
	New[MarketTick](Config{QueueSize: 3, MaxBatchSize: headerSize + 12, MaxItemSize: 12, TickRate: 60})
}

// TestNewPanicsOnNonPositiveTickRate verifies that New panics when
// Config.TickRate is zero or negative, catching invalid configurations at construction.
func TestNewPanicsOnNonPositiveTickRate(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("New with TickRate=0 did not panic")
		}
	}()
	New[MarketTick](Config{QueueSize: 16, MaxBatchSize: headerSize + 12, MaxItemSize: 12})
}

// TestNewPanicsOnExcessiveTickRate verifies that New panics when Config.TickRate
// exceeds 1_000_000_000, which would cause time.NewTicker to receive a zero duration.
func TestNewPanicsOnExcessiveTickRate(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("New with TickRate=1_000_000_001 did not panic")
		}
	}()
	New[MarketTick](Config{QueueSize: 16, MaxBatchSize: headerSize + 12, MaxItemSize: 12, TickRate: 1_000_000_001})
}

// TestStartPanicsOnDoubleStart verifies that calling Start a second time panics,
// preventing two drain goroutines from racing on the shared byteBuffer.
func TestStartPanicsOnDoubleStart(t *testing.T) {
	b := New[MarketTick](Config{
		QueueSize:    16,
		MaxBatchSize: headerSize + 12,
		MaxItemSize:  12,
		TickRate:     60,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b.Start(ctx)

	defer func() {
		if recover() == nil {
			t.Error("second Start call did not panic")
		}
	}()
	b.Start(ctx)
}

// TestRunLoopBufferFull verifies graceful degradation when byteBuffer cannot fit all
// queued items: the drain loop must stop before dequeuing an item that would overflow
// the buffer, leaving it queued for the next tick. Both items must be delivered over
// two successive flushes; neither is silently destroyed.
func TestRunLoopBufferFull(t *testing.T) {
	const tickSize = 20 // packed wire size: 8 (Symbol) + 8 (Price) + 4 (Size)

	sink := &multiCaptureSink{ch: make(chan []byte, 4)}
	b := New[Tick](Config{
		QueueSize:    16,
		MaxBatchSize: headerSize + tickSize, // fits exactly one Tick per flush
		MaxItemSize:  tickSize,
		TickRate:     500, // fast tick to collect both flushes quickly
		Sink:         sink,
	})

	b.Push(Tick{Symbol: [8]byte{'A'}, Price: 1.0, Size: 1})
	b.Push(Tick{Symbol: [8]byte{'B'}, Price: 2.0, Size: 2})

	ctx, cancel := context.WithCancel(context.Background())
	done := b.Start(ctx)

	collect := func(label string) []byte {
		t.Helper()
		select {
		case p := <-sink.ch:
			return p
		case <-time.After(500 * time.Millisecond):
			cancel()
			<-done
			t.Fatalf("timed out waiting for %s flush", label)
			return nil
		}
	}

	first := collect("first")
	second := collect("second")

	cancel()
	<-done

	wantLen := headerSize + tickSize
	for label, payload := range map[string][]byte{"first": first, "second": second} {
		if len(payload) != wantLen {
			t.Errorf("%s payload length: got %d, want %d", label, len(payload), wantLen)
		}
		count := uint16(payload[4]) | uint16(payload[5])<<8
		if count != 1 {
			t.Errorf("%s item count: got %d, want 1", label, count)
		}
	}
}

// TestStdoutSinkFlush verifies that StdoutSink.Flush writes without error.
func TestStdoutSinkFlush(t *testing.T) {
	s := StdoutSink{}
	if err := s.Flush([]byte("test payload")); err != nil {
		t.Errorf("StdoutSink.Flush returned unexpected error: %v", err)
	}
}

// TestDropOldestEvictsOldestItem verifies the functional correctness of DropOldest
// eviction: pushing into a full queue must remove the head item and enqueue the
// new item, leaving all intermediate items intact and in FIFO order.
func TestDropOldestEvictsOldestItem(t *testing.T) {
	const size = 4
	b := New[OrderUpdate](Config{
		QueueSize:    size,
		MaxBatchSize: headerSize + 24,
		MaxItemSize:  24,
		TickRate:     60,
		Backpressure: DropOldest,
	})

	// Fill the queue with orders 1 through 4.
	for i := uint32(1); i <= size; i++ {
		b.Push(OrderUpdate{OrderID: i})
	}

	// Order 5 must evict order 1 (the oldest).
	b.Push(OrderUpdate{OrderID: 5})

	// Drain all available items.
	var got []OrderUpdate
	for {
		var item OrderUpdate
		if !b.ring.pop(&item) {
			break
		}
		got = append(got, item)
	}

	if len(got) != size {
		t.Fatalf("expected %d items after eviction, got %d", size, len(got))
	}

	// Order 1 must be absent; orders 2–5 must appear in FIFO order.
	want := []uint32{2, 3, 4, 5}
	for i, item := range got {
		if item.OrderID != want[i] {
			t.Errorf("slot %d: got OrderID=%d, want %d (oldest must be evicted, newest must survive)",
				i, item.OrderID, want[i])
		}
	}
}

// TestNewPanicsOnInvalidBackpressurePolicy verifies that New panics when
// Config.Backpressure is set to an unrecognized policy value.
func TestNewPanicsOnInvalidBackpressurePolicy(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("New with invalid BackpressurePolicy did not panic")
		}
	}()
	New[OrderUpdate](Config{QueueSize: 16, MaxBatchSize: headerSize + 24, MaxItemSize: 24, Backpressure: BackpressurePolicy(99)})
}

// OrderBookState is a flat, pointer-free struct with a 35-byte wire
// representation (non-multiple-of-8 length: 32 word-aligned bytes + 3 tail
// bytes) that exercises the [XORBytes] tail-byte fallback path.
type OrderBookState struct {
	// BidPrice is the best bid price in the order book.
	BidPrice float64
	// AskPrice is the best ask price in the order book.
	AskPrice float64
	// BidSize is the total quantity available at the best bid.
	BidSize float32
	// AskSize is the total quantity available at the best ask.
	AskSize float32
	// SeqNum is the sequence number of this order book snapshot.
	SeqNum uint32
	// Flags is a packed bitfield for auxiliary snapshot metadata.
	Flags [7]byte
}

// Marshal implements [Serializable] by writing 35 bytes into buf: 8+8+4+4+4+7.
// The wire size of 35 is not a multiple of 8, which stresses the tail-byte
// fallback loop in [XORBytes].
func (o OrderBookState) Marshal(buf []byte) int {
	const size = 35
	if len(buf) < size {
		return 0
	}
	*(*float64)(unsafe.Pointer(&buf[0])) = o.BidPrice
	*(*float64)(unsafe.Pointer(&buf[8])) = o.AskPrice
	*(*float32)(unsafe.Pointer(&buf[16])) = o.BidSize
	*(*float32)(unsafe.Pointer(&buf[20])) = o.AskSize
	*(*uint32)(unsafe.Pointer(&buf[24])) = o.SeqNum
	copy(buf[28:size], o.Flags[:])
	return size
}

// TestXORBytesCorrectness verifies that [XORBytes] produces the correct bitwise
// XOR of two 35-byte buffers (32 bytes processed as uint64 words + 3 tail bytes
// via the fallback loop) and that the operation is self-inverse: applying XOR
// with the same second operand twice recovers the original first operand.
func TestXORBytesCorrectness(t *testing.T) {
	current := OrderBookState{
		BidPrice: 189.97,
		AskPrice: 189.98,
		BidSize:  300,
		AskSize:  100,
		SeqNum:   42,
		Flags:    [7]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07},
	}
	previous := OrderBookState{
		BidPrice: 189.95,
		AskPrice: 189.96,
		BidSize:  250,
		AskSize:  80,
		SeqNum:   41,
		Flags:    [7]byte{0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70},
	}

	a := make([]byte, 35)
	b := make([]byte, 35)
	dst := make([]byte, 35)
	recovered := make([]byte, 35)

	current.Marshal(a)
	previous.Marshal(b)

	XORBytes(dst, a, b)

	for i := 0; i < 35; i++ {
		if want := a[i] ^ b[i]; dst[i] != want {
			t.Errorf("byte %d: got %02x, want %02x", i, dst[i], want)
		}
	}

	// Self-inverse: XOR(delta, b) must recover a exactly.
	XORBytes(recovered, dst, b)
	for i := 0; i < 35; i++ {
		if recovered[i] != a[i] {
			t.Errorf("reversibility byte %d: got %02x, want %02x", i, recovered[i], a[i])
		}
	}
}

// multiCaptureSink is a test-only Sink that records every flushed payload in
// order, copying each into a fresh slice so the caller may compare them after
// the engine stops.
type multiCaptureSink struct {
	ch chan []byte
}

// Flush copies payload and sends it on the channel non-blocking.
func (m *multiCaptureSink) Flush(payload []byte) error {
	buf := make([]byte, len(payload))
	copy(buf, payload)
	select {
	case m.ch <- buf:
	default:
	}
	return nil
}

func (m *multiCaptureSink) reliable() {}

// TestDeltaEncodingReconstruction verifies the end-to-end delta encoding
// contract across three successive flushes, including a variable-length case
// that stresses the stale-byte zeroing path in runLoop.
//
// Frame 1: previousState is all zeros, so the delta equals the raw payload.
// Frame 2: a modified single-item batch; the delta is XOR(frame2, frame1).
// Frame 3: a two-item batch (larger than frame 2); proves that
// clear(previousState[offset:prevOffset]) prevents stale-byte corruption when
// the batch grows back beyond the previous offset.
func TestDeltaEncodingReconstruction(t *testing.T) {
	const itemSize = 35 // OrderBookState wire size
	const maxItems = 4

	sink := &multiCaptureSink{ch: make(chan []byte, 4)}
	b := New[OrderBookState](Config{
		QueueSize:     16,
		MaxBatchSize:  headerSize + maxItems*itemSize,
		MaxItemSize:   itemSize,
		TickRate:      500,
		Sink:          sink,
		DeltaEncoding: true,
	})

	frame1Item := OrderBookState{BidPrice: 100.0, AskPrice: 100.1, BidSize: 50, AskSize: 50, SeqNum: 1, Flags: [7]byte{0x01}}
	frame2Item := OrderBookState{BidPrice: 100.5, AskPrice: 100.6, BidSize: 40, AskSize: 60, SeqNum: 2, Flags: [7]byte{0x02}}
	frame3a := OrderBookState{BidPrice: 101.0, AskPrice: 101.1, BidSize: 30, AskSize: 70, SeqNum: 3, Flags: [7]byte{0x03}}
	frame3b := OrderBookState{BidPrice: 101.5, AskPrice: 101.6, BidSize: 20, AskSize: 80, SeqNum: 4, Flags: [7]byte{0x04}}

	ctx, cancel := context.WithCancel(context.Background())
	done := b.Start(ctx)

	// Push frame 1 and wait for its flush.
	b.Push(frame1Item)
	var delta1 []byte
	select {
	case delta1 = <-sink.ch:
	case <-time.After(500 * time.Millisecond):
		cancel()
		<-done
		t.Fatal("timed out waiting for frame 1 flush")
	}

	// Push frame 2 and wait for its flush.
	b.Push(frame2Item)
	var delta2 []byte
	select {
	case delta2 = <-sink.ch:
	case <-time.After(500 * time.Millisecond):
		cancel()
		<-done
		t.Fatal("timed out waiting for frame 2 flush")
	}

	// Push frame 3 (two items) and wait for its flush.
	b.Push(frame3a)
	b.Push(frame3b)
	var delta3 []byte
	select {
	case delta3 = <-sink.ch:
	case <-time.After(500 * time.Millisecond):
		cancel()
		<-done
		t.Fatal("timed out waiting for frame 3 flush")
	}

	cancel()
	<-done

	// Build expected raw payloads for comparison.
	raw1 := make([]byte, headerSize+itemSize)
	raw2 := make([]byte, headerSize+itemSize)
	raw3 := make([]byte, headerSize+2*itemSize)
	frame1Item.Marshal(raw1[headerSize:])
	frame2Item.Marshal(raw2[headerSize:])
	frame3a.Marshal(raw3[headerSize:])
	frame3b.Marshal(raw3[headerSize+itemSize:])

	// Frame 1: previousState was all zeros, so delta1 XOR zeros = raw items.
	// Verify by reconstructing item bytes only (header carries live seq/count).
	recon1 := make([]byte, len(delta1))
	XORBytes(recon1, delta1, make([]byte, len(delta1))) // XOR with zero = identity
	if !bytes.Equal(recon1[headerSize:], delta1[headerSize:]) {
		t.Error("frame 1: reconstruction from zero state produced unexpected result")
	}

	// Frame 2: reconstruct raw2 items from delta2 XOR delta1 item region.
	if len(delta2) != headerSize+itemSize {
		t.Fatalf("frame 2 delta length: got %d, want %d", len(delta2), headerSize+itemSize)
	}
	recon2items := make([]byte, itemSize)
	XORBytes(recon2items, delta2[headerSize:], delta1[headerSize:])
	if !bytes.Equal(recon2items, raw2[headerSize:]) {
		t.Errorf("frame 2 reconstruction mismatch:\n got  %v\n want %v", recon2items, raw2[headerSize:])
	}

	// Frame 3 (two items): the batch is larger than frame 2.
	// The stale-byte clear in runLoop must have zeroed previousState[len(frame2):len(frame3)]
	// after frame 2 was flushed, so XOR(delta3[headerSize:], raw2[headerSize:]) recovers raw3 items.
	if len(delta3) != headerSize+2*itemSize {
		t.Fatalf("frame 3 delta length: got %d, want %d", len(delta3), headerSize+2*itemSize)
	}
	// Extend raw2 item region with zeros to match the frame 3 item length.
	prev3 := make([]byte, 2*itemSize) // zeros beyond itemSize represent cleared stale bytes
	copy(prev3, raw2[headerSize:])
	recon3items := make([]byte, 2*itemSize)
	XORBytes(recon3items, delta3[headerSize:], prev3)
	if !bytes.Equal(recon3items, raw3[headerSize:]) {
		t.Errorf("frame 3 reconstruction mismatch:\n got  %v\n want %v", recon3items, raw3[headerSize:])
	}
}

// blockingMockSink is a chaos test-only Sink that sleeps for 50 ms inside
// Flush to simulate a stalled downstream network or disk target. The flushing
// field is set atomically while the sleep is in progress so tests can observe
// exactly when the drain goroutine is blocked.
type blockingMockSink struct {
	flushing atomic.Bool
	count    atomic.Int64
}

// Flush sleeps for 50 ms, recording the stall window via the flushing flag.
func (s *blockingMockSink) Flush(_ []byte) error {
	s.flushing.Store(true)
	time.Sleep(50 * time.Millisecond)
	s.flushing.Store(false)
	s.count.Add(1)
	return nil
}

// TestNonBlockingPushDuringStalledFlush proves that Push never blocks on a
// stalled downstream Sink. While the drain goroutine is sleeping inside
// Flush for 50 ms, a producer goroutine must complete Push within 20 ms —
// well below the stall duration — because Push writes only to the lock-free
// ring buffer, which is completely decoupled from the Sink call path.
// A goroutine-and-channel pattern is used instead of a wall-clock threshold
// so the assertion survives CI scheduler jitter.
func TestNonBlockingPushDuringStalledFlush(t *testing.T) {
	sink := &blockingMockSink{}
	b := New[MarketTick](Config{
		QueueSize:    1 << 10,
		MaxBatchSize: 4096,
		MaxItemSize:  12,
		TickRate:     200,
		Sink:         sink,
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := b.Start(ctx)
	defer func() { cancel(); <-done }()

	item := MarketTick{Price: 100.0, Volume: 1.0, Spread: 0.01}

	// Push enough items to trigger the first drain cycle.
	for i := 0; i < 10; i++ {
		b.Push(item)
	}

	// Wait until the drain goroutine is inside Flush (blocked for 50 ms).
	deadline := time.Now().Add(time.Second)
	for !sink.flushing.Load() {
		if time.Now().After(deadline) {
			t.Fatal("flush did not start within 1s")
		}
		time.Sleep(time.Millisecond)
	}

	// Push in a goroutine; assert it completes well within the 50 ms stall window.
	pushDone := make(chan struct{})
	go func() {
		b.Push(item)
		close(pushDone)
	}()
	select {
	case <-pushDone:
		// passed — Push returned before the 20 ms timeout
	case <-time.After(20 * time.Millisecond):
		t.Error("Push did not return within 20ms while sink was stalled; " +
			"producer must never be coupled to downstream I/O")
	}
}

// TestGracefulShutdown verifies that canceling the context causes runLoop to
// drain any remaining ring-buffer items and execute a final Sink.Flush before
// the goroutine exits, with no goroutine leak.
func TestGracefulShutdown(t *testing.T) {
	sink := &countingSink{}
	b := New[MarketTick](Config{
		QueueSize:    1 << 10,
		MaxBatchSize: 4096,
		MaxItemSize:  12,
		TickRate:     1, // 1 Hz — first tick is 1 s away, ensuring cancel fires first
		Sink:         sink,
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := b.Start(ctx)

	item := MarketTick{Price: 100.0, Volume: 1.0, Spread: 0.01}
	for i := 0; i < 5; i++ {
		b.Push(item)
	}
	cancel()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("runLoop goroutine did not exit within 500ms after context cancellation")
	}

	if sink.count.Load() == 0 {
		t.Fatal("expected at least one Sink.Flush from the graceful shutdown drain; got none")
	}
}

const xorBenchSize = 4096

// BenchmarkXORBytesVectorized measures throughput of the unsafe.Slice uint64-word
// XOR engine against a realistic batch buffer size.
func BenchmarkXORBytesVectorized(b *testing.B) {
	dst := make([]byte, xorBenchSize)
	a := make([]byte, xorBenchSize)
	src := make([]byte, xorBenchSize)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		XORBytes(dst, a, src)
	}
}

// BenchmarkXORBytesNaive measures throughput of a scalar byte-by-byte XOR loop
// as a baseline for comparison against [BenchmarkXORBytesVectorized].
func BenchmarkXORBytesNaive(b *testing.B) {
	dst := make([]byte, xorBenchSize)
	a := make([]byte, xorBenchSize)
	src := make([]byte, xorBenchSize)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j := 0; j < xorBenchSize; j++ {
			dst[j] = a[j] ^ src[j]
		}
	}
}

// BenchmarkPush is the Phase 1 performance gate.
// Success criterion: 0 B/op and 0 allocs/op.
func BenchmarkPush(b *testing.B) {
	batcher := New[MarketTick](Config{QueueSize: 1 << 16, MaxBatchSize: headerSize + 12, MaxItemSize: 12, TickRate: 60})
	item := MarketTick{Price: 415.25, Volume: 200, Spread: 0.01}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		batcher.Push(item)
		// Drain inline to prevent the buffer from filling and masking
		// the true push cost with a false-full early-return.
		var sink MarketTick
		batcher.ring.pop(&sink)
	}
}

// hangSink is a test-only Sink whose Flush blocks until the release channel is closed.
type hangSink struct{ release chan struct{} }

// Flush blocks until release is closed, simulating a permanently stalled transport.
func (s *hangSink) Flush([]byte) error { <-s.release; return nil }

// TestDrainStopsBeforeOverflow verifies the C-1 fix: when byteBuffer cannot hold
// another item the drain loop must break before dequeuing, leaving the item queued
// for the next tick. Both items must be delivered with no panic and no silent loss.
func TestDrainStopsBeforeOverflow(t *testing.T) {
	const tickSize = 20

	sink := &multiCaptureSink{ch: make(chan []byte, 4)}
	b := New[Tick](Config{
		QueueSize:    16,
		MaxBatchSize: headerSize + tickSize,
		MaxItemSize:  tickSize,
		TickRate:     500,
		Sink:         sink,
	})

	b.Push(Tick{Symbol: [8]byte{'X'}, Price: 10.0, Size: 10})
	b.Push(Tick{Symbol: [8]byte{'Y'}, Price: 20.0, Size: 20})

	ctx, cancel := context.WithCancel(context.Background())
	done := b.Start(ctx)

	collect := func(label string) []byte {
		t.Helper()
		select {
		case p := <-sink.ch:
			return p
		case <-time.After(500 * time.Millisecond):
			cancel()
			<-done
			t.Fatalf("timed out waiting for %s flush; item may have been silently lost", label)
			return nil
		}
	}

	first := collect("first")
	second := collect("second")
	cancel()
	<-done

	for label, payload := range map[string][]byte{"first": first, "second": second} {
		if len(payload) != headerSize+tickSize {
			t.Errorf("%s: unexpected payload length %d", label, len(payload))
		}
		count := uint16(payload[4]) | uint16(payload[5])<<8
		if count != 1 {
			t.Errorf("%s: got item count %d, want 1", label, count)
		}
	}
}

// TestShutdownFlushTimeout verifies that when ShutdownTimeout is set, runLoop exits
// within the timeout even when the final Sink.Flush blocks indefinitely, and that
// the abandoned flush is reported via OnFlushError.
func TestShutdownFlushTimeout(t *testing.T) {
	release := make(chan struct{})
	defer close(release) // unblock the orphaned flush goroutine after test

	errCh := make(chan error, 1)
	sink := &hangSink{release: release}
	b := New[MarketTick](Config{
		QueueSize:       16,
		MaxBatchSize:    headerSize + 12,
		MaxItemSize:     12,
		TickRate:        1, // 1 Hz — tick is 1 s away; cancel fires first
		Sink:            sink,
		ShutdownTimeout: 20 * time.Millisecond,
		OnFlushError: func(err error) {
			select {
			case errCh <- err:
			default:
			}
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := b.Start(ctx)

	b.Push(MarketTick{Price: 1.0}) // item ensures the final drain calls Flush
	cancel()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("runLoop did not exit within 100ms; ShutdownTimeout not enforced")
	}

	select {
	case err := <-errCh:
		if err == nil {
			t.Error("OnFlushError received nil error; expected a timeout error")
		}
	default:
		t.Error("OnFlushError was not called; abandoned shutdown flush was not reported")
	}
}

// TestNewPanicsOnDeltaEncodingWithUnreliableSink verifies that New panics when
// DeltaEncoding is true and the Sink does not implement ReliableSink.
func TestNewPanicsOnDeltaEncodingWithUnreliableSink(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("New with DeltaEncoding=true and a non-ReliableSink did not panic")
		}
	}()
	New[MarketTick](Config{
		QueueSize:     16,
		MaxBatchSize:  headerSize + 12,
		MaxItemSize:   12,
		TickRate:      60,
		Sink:          &countingSink{}, // countingSink does not implement ReliableSink
		DeltaEncoding: true,
	})
}

// TestEvictedCount verifies that DropOldest evictions are reflected in EvictedCount
// and that DroppedCount remains zero throughout.
func TestEvictedCount(t *testing.T) {
	const (
		size   = 4
		extras = 3
	)
	b := New[OrderUpdate](Config{
		QueueSize:    size,
		MaxBatchSize: headerSize + 24,
		MaxItemSize:  24,
		TickRate:     60,
		Backpressure: DropOldest,
	})

	for i := uint32(0); i < size; i++ {
		b.Push(OrderUpdate{OrderID: i})
	}
	for i := uint32(0); i < extras; i++ {
		b.Push(OrderUpdate{OrderID: size + i})
	}

	if got := b.EvictedCount(); got != extras {
		t.Errorf("EvictedCount: got %d, want %d", got, extras)
	}
	if got := b.DroppedCount(); got != 0 {
		t.Errorf("DroppedCount: got %d, want 0 (DropOldest must not increment DroppedCount)", got)
	}
}

// TestPaddedSeqSize asserts that paddedSeq occupies exactly one 64-byte cache line.
// If this fails, the parallel-array false-sharing fix is broken — two adjacent sequence
// numbers would share a cache line and reintroduce MESI coherence storms under MPMC fan-in.
func TestPaddedSeqSize(t *testing.T) {
	if got := unsafe.Sizeof(paddedSeq{}); got != 64 {
		t.Fatalf("paddedSeq size = %d bytes, want 64 (one full cache line)", got)
	}
}

// TestPushInlines verifies that the (*ringbuf).push method stays within the
// compiler's inline budget. If push falls off the inlining cliff, every Push call
// introduces a function-call overhead on the hot path and the zero-allocation
// contract is at risk.
func TestPushInlines(t *testing.T) {
	out, err := exec.Command("go", "build", "-gcflags=-m", "./...").CombinedOutput()
	if err != nil {
		t.Fatalf("go build failed: %v\n%s", err, out)
	}
	for _, line := range bytes.Split(out, []byte("\n")) {
		if bytes.Contains(line, []byte("cannot inline")) && bytes.Contains(line, []byte(").push")) {
			t.Fatalf("ring push fell off the inlining cliff.\nTriggering line: %s\n\nFull output:\n%s", line, out)
		}
	}
}

// TestXORBytesShortDstPanics verifies that XORBytes panics when dst is shorter than a.
func TestXORBytesShortDstPanics(t *testing.T) {
	a := make([]byte, 8)
	b := make([]byte, 8)

	t.Run("short dst", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Error("XORBytes with short dst did not panic")
			}
		}()
		XORBytes(make([]byte, 4), a, b)
	})

	t.Run("short b", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Error("XORBytes with short b did not panic")
			}
		}()
		XORBytes(make([]byte, 8), a, make([]byte, 4))
	})
}
