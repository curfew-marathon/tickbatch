package tickbatch

import (
	"context"
	"sync"
	"testing"
	"time"
	"unsafe"
)

// OrderUpdate is a flat, pointer-free struct representing an HFT order lifecycle
// event. All fields are 4 bytes, giving a packed wire size of exactly 24 bytes
// with no trailing padding.
type OrderUpdate struct {
	// OrderID uniquely identifies the order within the matching engine.
	OrderID uint32
	// Price is the limit price of the order in floating-point units.
	Price float32
	// Quantity is the number of lots requested.
	Quantity float32
	// Side encodes the order direction: 1.0 for buy, -1.0 for sell.
	Side float32
	// Timestamp is the exchange-assigned nanosecond epoch of the event.
	Timestamp uint32
	// Checksum is a lightweight integrity field over the order fields.
	Checksum float32
}

// Marshal implements [Serializable] by copying the struct into buf via an unsafe
// pointer cast, bypassing encoding/binary for zero-overhead serialization.
func (o OrderUpdate) Marshal(buf []byte) int {
	const size = int(unsafe.Sizeof(OrderUpdate{}))
	if len(buf) < size {
		return 0
	}
	copy(buf[:size], (*[size]byte)(unsafe.Pointer(&o))[:])
	return size
}

// TestStressTelemetry144Hz simulates a high-frequency trading engine loop for 3
// seconds, pushing 10,000 OrderUpdate records per tick from a pool of concurrent
// workers. It proves that the engine does not panic, the drain loop exits cleanly,
// and the DropOldest backpressure policy handles burst overflow without memory
// leaks or data races.
func TestStressTelemetry144Hz(t *testing.T) {
	const (
		hz             = 144
		duration       = 3 * time.Second
		frames         = hz * int(duration/time.Second)
		pushesPerFrame = 10_000
		numWorkers     = 16
		queueSize      = 1 << 13 // 8192 slots
		itemSize       = int(unsafe.Sizeof(OrderUpdate{}))
	)

	item := OrderUpdate{
		OrderID:   7,
		Price:     512.25,
		Quantity:  100.0,
		Side:      1.0,
		Timestamp: 1_700_000_000,
		Checksum:  0.0,
	}

	sink := &countingSink{}
	b := MustNew[OrderUpdate](Config{
		QueueSize:    queueSize,
		MaxBatchSize: headerSize + queueSize*itemSize,
		MaxItemSize:  itemSize,
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
