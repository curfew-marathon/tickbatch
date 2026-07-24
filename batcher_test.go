package tickbatch

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

// UecarrixTelemetry is a flat, pointer-free struct representing vehicle telemetry.
// Its pointer-free nature is what guarantees O(1) GC scan time per the
// Serializable contract.
type UecarrixTelemetry struct {
	RPM      float32
	Speed    float32
	Steering float32
}

// Marshal implements [Serializable]. It reads bytes from the struct (which is
// stack-allocated and properly aligned) and copies them into buf, making it
// safe on architectures with strict alignment requirements.
func (u UecarrixTelemetry) Marshal(buf []byte) int {
	const size = int(unsafe.Sizeof(UecarrixTelemetry{}))
	if len(buf) < size {
		return 0
	}
	copy(buf[:size], (*[size]byte)(unsafe.Pointer(&u))[:])
	return size
}

// NinjaDriftState is a flat, pointer-free struct simulating high-frequency
// car telemetry data for use in tick-engine tests.
type NinjaDriftState struct {
	Throttle   float32
	Brake      float32
	SteerAngle float32
	Velocity   float32
}

// Marshal implements [Serializable] by encoding the struct into buf via a direct
// memory copy, returning the number of bytes written.
func (n NinjaDriftState) Marshal(buf []byte) int {
	const size = int(unsafe.Sizeof(NinjaDriftState{}))
	if len(buf) < size {
		return 0
	}
	copy(buf[:size], (*[size]byte)(unsafe.Pointer(&n))[:])
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
	b := New[UecarrixTelemetry](Config{QueueSize: 16})

	frames := []UecarrixTelemetry{
		{RPM: 800, Speed: 0, Steering: 0},
		{RPM: 3500, Speed: 120.5, Steering: -0.25},
		{RPM: 7200, Speed: 240.0, Steering: 0.88},
	}

	for _, want := range frames {
		b.Push(want)
		var got UecarrixTelemetry
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
	b := New[UecarrixTelemetry](Config{QueueSize: size})

	filler := UecarrixTelemetry{RPM: 1000}
	for i := 0; i < size; i++ {
		b.Push(filler)
	}

	// This push must not block, panic, or corrupt the queue.
	overflow := UecarrixTelemetry{RPM: 9999}
	b.Push(overflow)

	// Drain and confirm overflow item was dropped.
	for i := 0; i < size; i++ {
		var got UecarrixTelemetry
		if !b.ring.pop(&got) {
			t.Fatalf("expected item at slot %d", i)
		}
		if got == overflow {
			t.Error("overflow item must not appear in a full queue")
		}
	}

	var extra UecarrixTelemetry
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
	b := New[NinjaDriftState](Config{
		QueueSize:    1 << 10,
		MaxBatchSize: 4096,
		TickRate:     tickRate,
		Sink:         sink,
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	done := b.Start(ctx)

	state := NinjaDriftState{Throttle: 1.0, Brake: 0.0, SteerAngle: 0.3, Velocity: 88.5}
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
// The written region is unsafe.Sizeof(Tick{}) bytes; the caller must supply a
// buffer of at least that length.
func (t Tick) Marshal(buf []byte) int {
	const size = int(unsafe.Sizeof(Tick{}))
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
	const tickSize = int(unsafe.Sizeof(Tick{}))

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

// BenchmarkPush is the Phase 1 performance gate.
// Success criterion: 0 B/op and 0 allocs/op.
func BenchmarkPush(b *testing.B) {
	batcher := New[UecarrixTelemetry](Config{QueueSize: 1 << 16})
	item := UecarrixTelemetry{RPM: 9000, Speed: 200, Steering: 0.1}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		batcher.Push(item)
		// Drain inline to prevent the buffer from filling and masking
		// the true push cost with a false-full early-return.
		var sink UecarrixTelemetry
		batcher.ring.pop(&sink)
	}
}
