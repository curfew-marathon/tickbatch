package tickbatch_test

import (
	"context"
	"log"
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

// ExampleNew demonstrates constructing a Batcher with New and handling the
// validation error idiomatically.
func ExampleNew() {
	b, err := tickbatch.New[event](tickbatch.Config{
		QueueSize:    1 << 10,
		MaxBatchSize: 64 * 1024,
		MaxItemSize:  int(unsafe.Sizeof(event{})),
		TickRate:     100,
		Sink:         tickbatch.StdoutSink{},
	})
	if err != nil {
		log.Fatalf("tickbatch: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := b.Start(ctx)

	b.Push(event{Seq: 1, Value: 3.14})

	cancel()
	<-done
}

// ExampleMustNew demonstrates the fail-fast constructor for callers that treat
// misconfiguration as a programming error. It is safe in var initializers.
func ExampleMustNew() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := tickbatch.MustNew[event](tickbatch.Config{
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

	b := tickbatch.MustNew[event](tickbatch.Config{
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

// ExampleNew_dropOldest configures the DropOldest backpressure policy so that,
// under saturation, the newest data always wins and the oldest queued item is
// evicted - the right trade-off for live telemetry where stale samples are useless.
func ExampleNew_dropOldest() {
	b := tickbatch.MustNew[event](tickbatch.Config{
		QueueSize:    1 << 10,
		MaxBatchSize: 64 * 1024,
		MaxItemSize:  int(unsafe.Sizeof(event{})),
		TickRate:     100,
		Sink:         tickbatch.StdoutSink{},
		Backpressure: tickbatch.DropOldest,
	})
	_ = b
}

// ExampleConfig_deltaEncoding enables XOR delta encoding over a reliable TCP
// transport. DeltaEncoding requires a [tickbatch.ReliableSink] (here [tickbatch.TCPSink])
// because a dropped frame would permanently desync the receiver's delta baseline.
// KeyframeInterval bounds that risk by emitting a full frame every N flushes.
func ExampleConfig_deltaEncoding() {
	sink, err := tickbatch.NewTCPSink("tcp", "127.0.0.1:9000")
	if err != nil {
		log.Fatalf("tickbatch: dial: %v", err)
	}
	defer func() { _ = sink.Close() }()

	b := tickbatch.MustNew[event](tickbatch.Config{
		QueueSize:        1 << 10,
		MaxBatchSize:     64 * 1024,
		MaxItemSize:      int(unsafe.Sizeof(event{})),
		TickRate:         60,
		Sink:             sink,
		DeltaEncoding:    true,
		KeyframeInterval: 100, // full frame every 100 flushes bounds desync risk
	})
	_ = b
}

// ExampleUDPSink sends each batch as a fire-and-forget UDP datagram. UDPSink is
// not a [tickbatch.ReliableSink], so it must not be paired with DeltaEncoding.
func ExampleUDPSink() {
	sink, err := tickbatch.NewUDPSink("127.0.0.1:9000")
	if err != nil {
		log.Fatalf("tickbatch: dial udp: %v", err)
	}
	defer func() { _ = sink.Close() }()

	b := tickbatch.MustNew[event](tickbatch.Config{
		QueueSize:    1 << 10,
		MaxBatchSize: 64 * 1024,
		MaxItemSize:  int(unsafe.Sizeof(event{})),
		TickRate:     100,
		Sink:         sink,
	})
	_ = b
}
