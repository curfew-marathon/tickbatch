package tickbatch_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"
	"unsafe"

	"github.com/curfew-marathon/tickbatch"
)

// tcpTestItem is a minimal Serializable for TCP sink tests.
// Fields are ordered for optimal alignment (float64 first).
type tcpTestItem struct {
	Value float64
	Seq   uint32
	_     [4]byte
}

func (it tcpTestItem) Marshal(buf []byte) int {
	const size = int(unsafe.Sizeof(tcpTestItem{}))
	if len(buf) < size {
		return 0
	}
	*(*tcpTestItem)(unsafe.Pointer(&buf[0])) = it
	return size
}

// readFrame reads one length-prefixed frame from conn.
func readFrame(conn net.Conn) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
		return nil, err
	}
	n := binary.LittleEndian.Uint32(lenBuf[:])
	payload := make([]byte, n)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func TestTCPSinkRoundTrip(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := ln.Close(); err != nil {
			t.Logf("listener close: %v", err)
		}
	})

	frames := make(chan []byte, 8)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() {
			if err := conn.Close(); err != nil {
				t.Logf("conn close: %v", err)
			}
		}()
		for {
			frame, err := readFrame(conn)
			if err != nil {
				return
			}
			frames <- frame
		}
	}()

	sink, err := tickbatch.NewTCPSink("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sink.Close(); err != nil {
			t.Logf("sink close: %v", err)
		}
	})

	itemSize := int(unsafe.Sizeof(tcpTestItem{}))
	b := tickbatch.New[tcpTestItem](tickbatch.Config{
		Sink:         sink,
		QueueSize:    64,
		MaxBatchSize: 4096,
		MaxItemSize:  itemSize,
		TickRate:     1000,
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := b.Start(ctx)

	const nItems = 5
	for i := range nItems {
		b.Push(tcpTestItem{Seq: uint32(i), Value: float64(i) * 1.5})
	}

	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	var totalItems int
	deadline := time.After(2 * time.Second)
	for totalItems < nItems {
		select {
		case frame := <-frames:
			if len(frame) < 8 {
				t.Fatalf("frame too short: %d bytes", len(frame))
			}
			count := binary.LittleEndian.Uint16(frame[4:6])
			totalItems += int(count)
		case <-deadline:
			t.Fatalf("timed out: received %d/%d items", totalItems, nItems)
		}
	}
}

func TestTCPSinkImplementsReliableSink(t *testing.T) {
	// TCPSink must satisfy the ReliableSink check so DeltaEncoding = true
	// does not panic.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := ln.Close(); err != nil {
			t.Logf("listener close: %v", err)
		}
	})

	go func() {
		conn, _ := ln.Accept()
		if conn != nil {
			if err := conn.Close(); err != nil {
				t.Logf("conn close: %v", err)
			}
		}
	}()

	sink, err := tickbatch.NewTCPSink("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sink.Close(); err != nil {
			t.Logf("sink close: %v", err)
		}
	})

	itemSize := int(unsafe.Sizeof(tcpTestItem{}))
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("TCPSink failed ReliableSink check: %v", r)
		}
	}()

	_ = tickbatch.New[tcpTestItem](tickbatch.Config{
		Sink:          sink,
		QueueSize:     16,
		MaxBatchSize:  1024,
		MaxItemSize:   itemSize,
		TickRate:      1000,
		DeltaEncoding: true,
	})
}

func TestTCPSinkErrorCountOnBrokenConn(t *testing.T) {
	// When the remote closes the connection the Batcher must continue running
	// (no panic) and FlushErrorCount must eventually rise. The first write after
	// a peer close may succeed because the kernel buffers it; the RST comes back
	// and causes subsequent writes to fail. We push items over multiple drain cycles
	// so at least one flush attempt sees the broken connection.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := ln.Close(); err != nil {
			t.Logf("listener close: %v", err)
		}
	})

	accepted := make(chan struct{})
	go func() {
		conn, _ := ln.Accept()
		if conn != nil {
			if err := conn.Close(); err != nil {
				t.Logf("conn close: %v", err)
			}
		}
		close(accepted)
	}()

	sink, err := tickbatch.NewTCPSink("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sink.Close(); err != nil {
			t.Logf("sink close: %v", err)
		}
	})

	// Wait for the server to close its side, then sleep long enough for the RST
	// to propagate on loopback before any write is attempted.
	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("server goroutine did not accept connection in time")
	}
	time.Sleep(100 * time.Millisecond)

	itemSize := int(unsafe.Sizeof(tcpTestItem{}))
	b := tickbatch.New[tcpTestItem](tickbatch.Config{
		Sink:         sink,
		QueueSize:    64,
		MaxBatchSize: 4096,
		MaxItemSize:  itemSize,
		TickRate:     1000,
		OnFlushError: func(error) {},
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := b.Start(ctx)

	// Push items across several drain cycles so write failures are visible even
	// if the first write races through before the RST arrives.
	for i := range 10 {
		b.Push(tcpTestItem{Seq: uint32(i), Value: float64(i)})
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done

	if b.FlushErrorCount() == 0 {
		t.Error("expected FlushErrorCount > 0 after broken connection")
	}
}

func TestTCPSinkUnixDomainSocket(t *testing.T) {
	// Verify that NewTCPSink("unix", path) works end-to-end.
	// macOS caps Unix socket paths at 104 bytes; use a short /tmp path.
	path := fmt.Sprintf("/tmp/tb%d.sock", os.Getpid())
	t.Cleanup(func() {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			t.Logf("socket remove: %v", err)
		}
	})

	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := ln.Close(); err != nil {
			t.Logf("listener close: %v", err)
		}
	})

	frames := make(chan []byte, 4)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() {
			if err := conn.Close(); err != nil {
				t.Logf("conn close: %v", err)
			}
		}()
		for {
			frame, err := readFrame(conn)
			if err != nil {
				return
			}
			frames <- frame
		}
	}()

	sink, err := tickbatch.NewTCPSink("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sink.Close(); err != nil {
			t.Logf("sink close: %v", err)
		}
	})

	itemSize := int(unsafe.Sizeof(tcpTestItem{}))
	b := tickbatch.New[tcpTestItem](tickbatch.Config{
		Sink:         sink,
		QueueSize:    16,
		MaxBatchSize: 1024,
		MaxItemSize:  itemSize,
		TickRate:     1000,
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := b.Start(ctx)

	b.Push(tcpTestItem{Seq: 99, Value: 9.9})
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	select {
	case frame := <-frames:
		if len(frame) < 8 {
			t.Fatalf("UDS frame too short: %d bytes", len(frame))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for UDS frame")
	}
}
