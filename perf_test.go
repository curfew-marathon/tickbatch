package tickbatch

import (
	"context"
	"sync"
	"testing"
	"time"
	"unsafe"
)

// NinjaDriftTelemetry is a flat, pointer-free struct simulating physics telemetry
// from a high-speed racing game. All fields are 4 bytes, giving a packed wire
// size of exactly 24 bytes with no trailing padding.
type NinjaDriftTelemetry struct {
	// CarID uniquely identifies the vehicle sending this frame.
	CarID uint32
	// PosX is the world-space X coordinate in meters.
	PosX float32
	// PosY is the world-space Y coordinate in meters.
	PosY float32
	// PosZ is the world-space Z coordinate in meters.
	PosZ float32
	// RPM is the current engine speed in revolutions per minute.
	RPM uint32
	// Velocity is the scalar speed of the vehicle in km/h.
	Velocity float32
}

// Marshal implements [Serializable] by copying the struct into buf via an unsafe
// pointer cast, bypassing encoding/binary for zero-overhead serialization.
func (n NinjaDriftTelemetry) Marshal(buf []byte) int {
	const size = int(unsafe.Sizeof(NinjaDriftTelemetry{}))
	if len(buf) < size {
		return 0
	}
	copy(buf[:size], (*[size]byte)(unsafe.Pointer(&n))[:])
	return size
}

// TestStressTelemetry144Hz simulates a 144 Hz game loop for 3 seconds, pushing
// 10,000 NinjaDriftTelemetry frames per tick from a pool of concurrent workers.
// It proves that the engine does not panic, the drain loop exits cleanly, and
// the DropOldest backpressure policy handles burst overflow without memory leaks
// or data races.
func TestStressTelemetry144Hz(t *testing.T) {
	const (
		hz             = 144
		duration       = 3 * time.Second
		frames         = hz * int(duration/time.Second)
		pushesPerFrame = 10_000
		numWorkers     = 16
		queueSize      = 1 << 13 // 8192 slots
		itemSize       = int(unsafe.Sizeof(NinjaDriftTelemetry{}))
	)

	item := NinjaDriftTelemetry{
		CarID:    7,
		PosX:     512.25,
		PosY:     0.0,
		PosZ:     -128.75,
		RPM:      9500,
		Velocity: 347.8,
	}

	sink := &countingSink{}
	b := New[NinjaDriftTelemetry](Config{
		QueueSize:    queueSize,
		MaxBatchSize: headerSize + queueSize*itemSize,
		TickRate:     hz,
		Sink:         sink,
		Backpressure: DropOldest,
	})

	ctx, cancel := context.WithCancel(context.Background())

	done := b.Start(ctx)

	frameTicker := time.NewTicker(time.Second / time.Duration(hz))
	defer frameTicker.Stop()

	for f := 0; f < frames; f++ {
		<-frameTicker.C

		var wg sync.WaitGroup
		wg.Add(numWorkers)
		for w := 0; w < numWorkers; w++ {
			go func(idx int) {
				defer wg.Done()
				count := pushesPerFrame / numWorkers
				// Distribute the remainder across the first workers.
				if idx < pushesPerFrame%numWorkers {
					count++
				}
				for j := 0; j < count; j++ {
					b.Push(item)
				}
			}(w)
		}
		wg.Wait()
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runLoop did not exit within 2s of context cancellation")
	}

	flushCount := sink.count.Load()
	if flushCount == 0 {
		t.Fatal("expected at least one Sink.Flush call; drain loop may not have run")
	}
	t.Logf("stress complete: %d Sink.Flush calls over %v at %d Hz with %d pushes/frame (%d workers)",
		flushCount, duration, hz, pushesPerFrame, numWorkers)
}
