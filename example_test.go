package tickbatch_test

import (
	"context"
	"unsafe"

	"github.com/curfew-marathon/tickbatch"
)

// event is a flat, pointer-free struct used in the examples below.
// Pointer-free layout guarantees O(1) GC scan time per ring buffer slot.
type event struct {
	Seq   uint32
	Value float64
}

// Marshal implements [tickbatch.Serializable] via a direct unsafe memory copy.
func (e event) Marshal(buf []byte) int {
	const size = int(unsafe.Sizeof(event{}))
	if len(buf) < size {
		return 0
	}
	copy(buf[:size], (*[size]byte)(unsafe.Pointer(&e))[:])
	return size
}

// ExampleNew demonstrates constructing and starting a Batcher with a StdoutSink.
func ExampleNew() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := tickbatch.New[event](tickbatch.Config{
		QueueSize:    1 << 10,
		MaxBatchSize: 64 * 1024,
		MaxItemSize:  int(unsafe.Sizeof(event{})),
		TickRate:     100,
		Sink:         tickbatch.StdoutSink{},
	})
	done := b.Start(ctx)

	b.Push(event{Seq: 1, Value: 3.14})

	cancel()
	<-done
}

// ExampleBatcher_Push demonstrates non-blocking, allocation-free event ingestion.
func ExampleBatcher_Push() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := tickbatch.New[event](tickbatch.Config{
		QueueSize:    1 << 10,
		MaxBatchSize: 64 * 1024,
		MaxItemSize:  int(unsafe.Sizeof(event{})),
		TickRate:     100,
		Sink:         tickbatch.StdoutSink{},
	})
	done := b.Start(ctx)

	for i := uint32(0); i < 10; i++ {
		b.Push(event{Seq: i, Value: float64(i) * 1.5})
	}

	cancel()
	<-done
}
