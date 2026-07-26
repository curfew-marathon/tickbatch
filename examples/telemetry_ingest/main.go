// Package main demonstrates tickbatch as a zero-impact telemetry exhaust.
// It pushes RiskEvent structs as fast as possible for 3 seconds, then
// blocks until the engine flushes its final batch and exits cleanly.
package main

import (
	"context"
	"fmt"
	"log"
	"time"
	"unsafe"

	"github.com/curfew-marathon/tickbatch"
)

// riskEventSize is the encoded byte size of one RiskEvent.
const riskEventSize = int(unsafe.Sizeof(RiskEvent{}))

// RiskEvent is a flat, pointer-free risk metric snapshot for a single instrument.
// The trailing [6]byte pad aligns the struct to a multiple of 8 bytes so the
// vectorized XOR engine processes every field in the fast 64-bit path.
type RiskEvent struct {
	InstrumentID uint32
	SequenceNum  uint32
	Price        float64
	Delta        float64
	Gamma        float64
	Timestamp    int64
	Flags        uint16
	_            [6]byte
}

// Marshal writes e into buf via a direct unsafe memory copy and returns riskEventSize,
// or 0 if buf is too small to hold the struct.
func (e RiskEvent) Marshal(buf []byte) int {
	if len(buf) < riskEventSize {
		return 0
	}
	*(*RiskEvent)(unsafe.Pointer(&buf[0])) = e
	return riskEventSize
}

func main() {
	sink, err := tickbatch.NewUDPSink("127.0.0.1:9999")
	if err != nil {
		log.Fatalf("tickbatch: dial udp: %v", err)
	}
	defer func() {
		if err := sink.Close(); err != nil {
			log.Printf("tickbatch: close udp sink: %v", err)
		}
	}()

	flushErrs := make(chan error, 16)

	b := tickbatch.New[RiskEvent](tickbatch.Config{
		Sink:         sink,
		QueueSize:    1 << 14,
		MaxBatchSize: 1 << 16,
		MaxItemSize:  riskEventSize,
		TickRate:     60,
		Backpressure: tickbatch.DropOldest,
		OnFlushError: func(err error) {
			select {
			case flushErrs <- err:
			default:
			}
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	done := b.Start(ctx)

	var seq uint32
	for {
		select {
		case <-ctx.Done():
			<-done
			fmt.Printf("graceful shutdown complete — total dropped: %d\n", b.DroppedCount())
			return
		case err := <-flushErrs:
			log.Printf("tickbatch: flush error: %v", err)
		default:
			seq++
			// Zero-initialize before setting fields so inter-field padding bytes
			// are guaranteed to be zero and cannot leak stale process memory on wire.
			var ev RiskEvent
			ev.InstrumentID = seq % 8
			ev.SequenceNum = seq
			ev.Price = 100.0 + float64(seq%100)*0.01
			ev.Delta = 0.45
			ev.Gamma = 0.02
			ev.Timestamp = time.Now().UnixNano()
			ev.Flags = 0x01
			b.Push(ev)
		}
	}
}
