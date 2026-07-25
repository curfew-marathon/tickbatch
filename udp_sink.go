package tickbatch

import "net"

// UDPSink is a fire-and-forget [Sink] that transmits each flushed batch as a
// single UDP datagram using the standard net package.
//
// Construct via [NewUDPSink]. Call [UDPSink.Close] after the [Batcher] has
// stopped to release the underlying OS socket.
type UDPSink struct {
	conn *net.UDPConn
}

// NewUDPSink dials a UDP connection to addr and returns a ready-to-use UDPSink.
// The addr parameter must be a valid host:port string (e.g. "127.0.0.1:9000").
func NewUDPSink(addr string) (*UDPSink, error) {
	raddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		return nil, err
	}
	return &UDPSink{conn: conn}, nil
}

// Flush transmits payload as a single UDP datagram and returns any write error.
// Flush must not retain payload beyond the duration of the call.
func (u *UDPSink) Flush(payload []byte) error {
	_, err := u.conn.Write(payload)
	return err
}

// Close releases the underlying UDP socket. It must be called once the
// associated [Batcher] has stopped to avoid leaking OS resources.
func (u *UDPSink) Close() error {
	return u.conn.Close()
}
