package tickbatch

import (
	"context"
	"net"
	"testing"
	"time"
	"unsafe"
)

// passThroughCompressor is a test-only [Compressor] that copies src into dst
// unchanged, satisfying the interface without pulling in any third-party library.
type passThroughCompressor struct{}

// Compress copies src into dst and returns the number of bytes written.
func (p passThroughCompressor) Compress(dst, src []byte) (int, error) {
	return copy(dst, src), nil
}

// TestUDPSinkIntegration binds a local UDP listener, pushes five [OrderUpdate]
// structs through a [Batcher] backed by [UDPSink] and [passThroughCompressor],
// and asserts that the received datagram length equals the expected packed payload.
func TestUDPSinkIntegration(t *testing.T) {
	const (
		itemCount = 5
		itemSize  = int(unsafe.Sizeof(OrderUpdate{}))
		wantLen   = headerSize + itemCount*itemSize
	)

	laddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("resolve listener addr: %v", err)
	}
	listener, err := net.ListenUDP("udp", laddr)
	if err != nil {
		t.Fatalf("listen UDP: %v", err)
	}
	t.Cleanup(func() {
		if err := listener.Close(); err != nil {
			t.Logf("listener close: %v", err)
		}
	})

	udpSink, err := NewUDPSink(listener.LocalAddr().String())
	if err != nil {
		t.Fatalf("NewUDPSink: %v", err)
	}
	t.Cleanup(func() {
		if err := udpSink.Close(); err != nil {
			t.Logf("udpSink close: %v", err)
		}
	})

	b := New[OrderUpdate](Config{
		QueueSize:    16,
		MaxBatchSize: headerSize + 16*itemSize,
		TickRate:     200,
		Sink:         udpSink,
		Compressor:   passThroughCompressor{},
	})

	item := OrderUpdate{
		OrderID:   42,
		Price:     512.25,
		Quantity:  10.0,
		Side:      1.0,
		Timestamp: 1_700_000_000,
		Checksum:  0.0,
	}
	for i := 0; i < itemCount; i++ {
		b.Push(item)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := b.Start(ctx)

	if err := listener.SetReadDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}

	buf := make([]byte, 65535)
	n, _, readErr := listener.ReadFromUDP(buf)

	cancel()
	<-done

	if readErr != nil {
		t.Fatalf("ReadFromUDP: %v", readErr)
	}
	if n != wantLen {
		t.Errorf("received %d bytes, want %d (header=%d + %d items × %d bytes)",
			n, wantLen, headerSize, itemCount, itemSize)
	}
}

// TestUDPSinkFlushError verifies that NewUDPSink returns an error for an
// unresolvable address rather than panicking.
func TestUDPSinkFlushError(t *testing.T) {
	_, err := NewUDPSink("not-a-valid-address")
	if err == nil {
		t.Error("NewUDPSink with invalid addr must return a non-nil error")
	}
}
