package tickbatch

import (
	"context"
	"fmt"
)

// Sink is the pluggable transport interface for flushed batch payloads.
//
// Implementations receive a fully-serialized byte payload and are responsible
// for delivering it to the desired downstream system. Flush must not retain
// the payload slice beyond the call; callers reuse the underlying buffer.
//
// The ctx carries the deadline derived from [Config.FlushTimeout] (per flush)
// or [Config.ShutdownTimeout] (final drain). Implementations that perform
// cancelable I/O should honor ctx - for net.Conn this means setting a write
// deadline from ctx.Deadline. Sinks that cannot observe ctx still work: the
// engine keeps a goroutine backstop for [Config.FlushTimeout], but honoring ctx
// is what makes cancellation clean and leak-free (see [Config.ShutdownTimeout]).
//
// Async broker clients (Kafka producers, gRPC streams, and any driver that
// enqueues the slice and returns before transmitting) must copy the payload
// before returning. Use [CopyingSink] to do this once rather than in every
// custom adapter.
type Sink interface {
	// Flush delivers a serialized batch payload to the downstream transport.
	// It must not retain payload beyond the duration of the call. It should
	// honor ctx cancellation and deadlines where the transport allows.
	Flush(ctx context.Context, payload []byte) error
}

// ReliableSink is an optional marker interface that a Sink may implement to declare
// at-least-once delivery semantics: a nil error from Flush guarantees the payload
// was accepted by the downstream transport for delivery. Sinks that do not implement this
// interface (such as [UDPSink], which is fire-and-forget) must not be paired with
// [Config.DeltaEncoding] = true, because a lost datagram advances the sender's
// delta baseline while the receiver never sees the frame, causing permanent desync.
//
// The marker method [ReliableSink.Reliable] is exported so third-party packages can
// implement ReliableSink for their own transports (Kafka, gRPC, custom brokers) and
// pair them with [Config.DeltaEncoding]. It is a no-op; implementing it is a
// behavioral promise, not a code contract.
type ReliableSink interface {
	Sink
	// Reliable is a no-op marker declaring at-least-once delivery semantics.
	Reliable()
}

// StdoutSink is a diagnostic Sink that prints batch metadata to standard output.
//
// It is intended for development and testing only and must not be used in
// production hot paths.
type StdoutSink struct{}

// Flush prints the byte length of the payload to standard output and returns nil.
func (s StdoutSink) Flush(_ context.Context, payload []byte) error {
	fmt.Printf("StdoutSink: flushed batch of %d bytes\n", len(payload))
	return nil
}

// Reliable marks StdoutSink as a [ReliableSink]; its synchronous write completes
// before Flush returns.
func (StdoutSink) Reliable() {}
