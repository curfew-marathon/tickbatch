package tickbatch

import (
	"context"
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
	b := New[MarketTick](Config{QueueSize: 16, MaxBatchSize: headerSize})

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
	b := New[MarketTick](Config{QueueSize: size, MaxBatchSize: headerSize})

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
	New[MarketTick](Config{QueueSize: 0, MaxBatchSize: headerSize})
}

// TestNewPanicsOnNonPowerOfTwoQueueSize verifies that New panics when QueueSize
// is not a power of two, as the Vyukov bitmask algorithm requires power-of-two capacity.
func TestNewPanicsOnNonPowerOfTwoQueueSize(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("New with non-power-of-two QueueSize did not panic")
		}
	}()
	New[MarketTick](Config{QueueSize: 3, MaxBatchSize: headerSize})
}

// TestStartPanicsOnNonPositiveTickRate verifies that Start panics when
// Config.TickRate is zero or negative, catching invalid configurations early.
func TestStartPanicsOnNonPositiveTickRate(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("Start with TickRate=0 did not panic")
		}
	}()
	b := New[MarketTick](Config{QueueSize: 16, MaxBatchSize: headerSize})
	b.Start(context.Background())
}

// TestStartPanicsOnDoubleStart verifies that calling Start a second time panics,
// preventing two drain goroutines from racing on the shared byteBuffer.
func TestStartPanicsOnDoubleStart(t *testing.T) {
	b := New[MarketTick](Config{
		QueueSize:    16,
		MaxBatchSize: headerSize,
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

// TestRunLoopBufferFull verifies graceful degradation when byteBuffer cannot
// fit all queued items: the drain loop must stop at capacity, drop the
// overflow item silently, and still deliver the partial batch to the Sink.
func TestRunLoopBufferFull(t *testing.T) {
	const tickSize = 20 // packed wire size: 8 (Symbol) + 8 (Price) + 4 (Size)

	sink := &captureSink{ch: make(chan []byte, 1)}
	b := New[Tick](Config{
		QueueSize:    16,
		MaxBatchSize: headerSize + tickSize, // fits exactly one Tick; second is dropped
		TickRate:     200,
		Sink:         sink,
	})

	b.Push(Tick{Symbol: [8]byte{'A'}, Price: 1.0, Size: 1})
	b.Push(Tick{Symbol: [8]byte{'B'}, Price: 2.0, Size: 2})

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

	wantLen := headerSize + tickSize
	if len(payload) != wantLen {
		t.Fatalf("payload length: got %d, want %d", len(payload), wantLen)
	}
	count := uint16(payload[4]) | uint16(payload[5])<<8
	if count != 1 {
		t.Errorf("item count: got %d, want 1 (overflow item must be silently dropped)", count)
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
		MaxBatchSize: headerSize,
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
	New[OrderUpdate](Config{QueueSize: 16, MaxBatchSize: headerSize, Backpressure: BackpressurePolicy(99)})
}

// OrderBookState is a flat, pointer-free struct with a purposefully misaligned
// 35-byte wire representation (32 word-aligned bytes + 3 tail bytes) that
// exercises the [XORBytes] tail-byte fallback path on non-multiple-of-8 payloads.
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
	batcher := New[MarketTick](Config{QueueSize: 1 << 16, MaxBatchSize: headerSize})
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
