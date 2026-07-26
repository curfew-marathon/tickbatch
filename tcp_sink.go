package tickbatch

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"time"
)

// TCPSink is a [ReliableSink] that transmits each flushed batch as a length-prefixed
// frame over a TCP or Unix Domain Socket connection.
//
// Wire format: 4-byte little-endian payload length followed by the raw payload.
// Receivers read 4 bytes to obtain the frame length, then read exactly that many
// bytes for the payload.
//
// Construct via [NewTCPSink]. Call [TCPSink.Close] after the [Batcher] has stopped
// to release the underlying OS connection.
//
// Because net.Conn.Write blocks until the kernel accepts the bytes into the socket
// send buffer, TCPSink implements [ReliableSink] and is safe to pair with
// [Config.DeltaEncoding] = true.
//
// To use Unix Domain Sockets instead of TCP, pass "unix" as the network argument
// and a socket path as addr. The framing and delivery semantics are identical.
type TCPSink struct {
	conn net.Conn
	// lbuf is the length-prefix scratch buffer. At most one Flush call is active
	// at a time: either the drain goroutine calls directly (FlushTimeout == 0) or
	// flushInFlight serializes concurrent goroutine-based calls (FlushTimeout > 0).
	// No mutex is needed.
	lbuf [4]byte
}

// NewTCPSink dials a connection using network and addr and returns a ready-to-use
// TCPSink. The network argument must be "tcp", "tcp4", "tcp6", or "unix". The addr
// argument must be a valid host:port string for TCP or an absolute socket path for
// Unix Domain Sockets.
func NewTCPSink(network, addr string) (*TCPSink, error) {
	conn, err := net.Dial(network, addr)
	if err != nil {
		return nil, err
	}
	return &TCPSink{conn: conn}, nil
}

// Flush transmits a 4-byte little-endian length prefix followed by payload using
// a single vectorized write. On POSIX systems net.Buffers.WriteTo issues one
// writev(2) syscall, sending both buffers without memory concatenation.
// Flush must not retain payload beyond the duration of the call. If ctx carries a
// deadline it is applied to the connection write and cleared afterwards, so a
// stalled socket cannot park the drain goroutine past [Config.FlushTimeout].
func (t *TCPSink) Flush(ctx context.Context, payload []byte) error {
	if dl, ok := ctx.Deadline(); ok {
		_ = t.conn.SetWriteDeadline(dl)
		defer func() { _ = t.conn.SetWriteDeadline(time.Time{}) }()
	}
	// The length prefix is a uint32; a payload above 4 GiB would silently truncate
	// modulo 2^32 and corrupt the framing of every subsequent frame on the stream.
	// Reject it explicitly rather than emit an unparseable prefix.
	if uint64(len(payload)) > math.MaxUint32 {
		return fmt.Errorf("tickbatch: TCPSink payload %d bytes exceeds the 4 GiB length-prefix limit", len(payload))
	}
	binary.LittleEndian.PutUint32(t.lbuf[:], uint32(len(payload)))
	bufs := net.Buffers{t.lbuf[:], payload}
	n, err := bufs.WriteTo(t.conn)
	if err != nil {
		return err
	}
	if want := int64(4 + len(payload)); n != want {
		return fmt.Errorf("tickbatch: TCPSink short write: wrote %d of %d bytes: %w", n, want, io.ErrShortWrite)
	}
	return nil
}

// Reliable marks TCPSink as a [ReliableSink]: net.Conn.Write blocks until the
// kernel accepts the bytes, so a nil error means the payload was handed to the
// transport.
func (t *TCPSink) Reliable() {}

// Close releases the underlying network connection. It must be called once the
// associated [Batcher] has stopped to avoid leaking OS resources.
func (t *TCPSink) Close() error {
	return t.conn.Close()
}
