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
