package tickbatch

import (
	"encoding/binary"
	"net"
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
	// lbuf is the length-prefix scratch buffer. It is mutated without a lock
	// because Flush is only ever called from the single drain goroutine.
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
// Flush must not retain payload beyond the duration of the call.
func (t *TCPSink) Flush(payload []byte) error {
	binary.LittleEndian.PutUint32(t.lbuf[:], uint32(len(payload)))
	bufs := net.Buffers{t.lbuf[:], payload}
	_, err := bufs.WriteTo(t.conn)
	return err
}

func (t *TCPSink) reliable() {}

// Close releases the underlying network connection. It must be called once the
// associated [Batcher] has stopped to avoid leaking OS resources.
func (t *TCPSink) Close() error {
	return t.conn.Close()
}
