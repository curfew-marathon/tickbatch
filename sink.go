package tickbatch

import "fmt"

// Sink is the pluggable transport interface for flushed batch payloads.
//
// Implementations receive a fully-serialized byte payload and are responsible
// for delivering it to the desired downstream system. Flush must not retain
// the payload slice beyond the call — callers reuse the underlying buffer.
type Sink interface {
	// Flush delivers a serialized batch payload to the downstream transport.
	// It must not retain payload beyond the duration of the call.
	Flush(payload []byte) error
}

// ReliableSink is an optional marker interface that a Sink may embed to declare
// at-least-once delivery semantics: a nil error from Flush guarantees the payload
// was durably accepted by the downstream system. Sinks that do not implement this
// interface (such as [UDPSink], which is fire-and-forget) must not be paired with
// [Config.DeltaEncoding] = true, because a lost datagram advances the sender's
// delta baseline while the receiver never sees the frame, causing permanent desync.
type ReliableSink interface {
	Sink
	reliable()
}

// StdoutSink is a diagnostic Sink that prints batch metadata to standard output.
//
// It is intended for development and testing only and must not be used in
// production hot paths.
type StdoutSink struct{}

// Flush prints the byte length of the payload to standard output and returns nil.
func (s StdoutSink) Flush(payload []byte) error {
	fmt.Printf("StdoutSink: flushed batch of %d bytes\n", len(payload))
	return nil
}

func (StdoutSink) reliable() {}
