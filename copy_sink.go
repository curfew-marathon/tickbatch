package tickbatch

// CopyingSink wraps any [Sink] and copies the payload into a fresh owned buffer
// before calling the inner Flush. Use this when the inner Sink enqueues the slice
// asynchronously — Kafka producers, gRPC streams, or any broker client that returns
// before the bytes are transmitted — so the engine's buffer-reuse contract is satisfied.
//
// The allocation cost of make([]byte, len(payload)) is incurred once per batch on
// the drain goroutine, never on the producer's hot path.
type CopyingSink struct {
	Inner Sink
}

// Flush copies payload into a fresh buffer and forwards it to the inner Sink.
// The copy is performed before the inner call so the engine may freely reuse
// the underlying buffer as soon as this method returns.
func (c CopyingSink) Flush(payload []byte) error {
	cp := make([]byte, len(payload))
	copy(cp, payload)
	return c.Inner.Flush(cp)
}

// ReliableCopyingSink is a [CopyingSink] variant that additionally implements
// [ReliableSink]. Use it when the wrapped inner Sink guarantees at-least-once
// delivery semantics, enabling [Config.DeltaEncoding] = true.
type ReliableCopyingSink struct {
	CopyingSink
}

func (ReliableCopyingSink) reliable() {}
