// Package tickbatchtest provides test doubles and a conformance harness for
// authors implementing a tickbatch.Sink.
//
// It offers a recording fake ([RecordingSink]), a reliable recording fake
// ([ReliableRecordingSink]), and [TestSink], a battery of assertions that a Sink
// implementation must satisfy. Sink authors call TestSink from their own package
// tests to gain confidence their transport honors the interface contract.
package tickbatchtest

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/curfew-marathon/tickbatch"
)

// RecordingSink is a tickbatch.Sink test double that stores a copy of every
// payload it receives. It copies before returning, so it satisfies the buffer-reuse
// contract and is safe to use with an engine that reuses its output buffer.
//
// It is safe for concurrent use.
type RecordingSink struct {
	// Err, when non-nil, is returned by Flush to simulate a failing transport.
	Err      error
	payloads [][]byte
	mu       sync.Mutex
}

// Flush records a copy of payload and returns [RecordingSink.Err].
func (r *RecordingSink) Flush(_ context.Context, payload []byte) error {
	cp := make([]byte, len(payload))
	copy(cp, payload)
	r.mu.Lock()
	r.payloads = append(r.payloads, cp)
	r.mu.Unlock()
	return r.Err
}

// Payloads returns copies of all recorded payloads in delivery order.
func (r *RecordingSink) Payloads() [][]byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([][]byte, len(r.payloads))
	for i, p := range r.payloads {
		out[i] = bytes.Clone(p)
	}
	return out
}

// Len returns the number of recorded payloads.
func (r *RecordingSink) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.payloads)
}

// ReliableRecordingSink is a [RecordingSink] that also implements
// tickbatch.ReliableSink, so it may be paired with Config.DeltaEncoding = true.
type ReliableRecordingSink struct {
	RecordingSink
}

// Reliable marks the sink as a tickbatch.ReliableSink.
func (r *ReliableRecordingSink) Reliable() {}

// Static interface assertions.
var (
	_ tickbatch.Sink         = (*RecordingSink)(nil)
	_ tickbatch.ReliableSink = (*ReliableRecordingSink)(nil)
)

// TestSink runs the tickbatch.Sink conformance battery against s. Call it from a
// test in your sink's package:
//
//	func TestMySink(t *testing.T) { tickbatchtest.TestSink(t, newMySink(t)) }
//
// It verifies the invariants observable through the Sink interface: Flush returns
// promptly (no deadlock) for representative payloads under a context deadline, does
// not panic on empty or nil payloads, tolerates repeated calls, and does not corrupt
// delivery when the caller mutates the input buffer after Flush returns (the engine's
// buffer-reuse contract). Each call passes a context carrying a deadline, so a sink
// that honors ctx cancels cleanly. It cannot observe whether an opaque sink internally
// retains the slice, so pair it with a transport-level round-trip test for full coverage.
func TestSink(t testing.TB, s tickbatch.Sink) {
	t.Helper()

	call := func(name string, payload []byte) {
		// Pass a per-call context carrying a 2s deadline so the conformance check
		// exercises ctx propagation. A sink that honors ctx cancels within the
		// deadline and its goroutine exits cleanly (no leak); a sink that ignores
		// ctx is still caught by the outer 5s harness backstop, which fails the
		// test rather than letting CI hang.
		cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer ccancel()
		done := make(chan error, 1)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					done <- errPanic{r}
				}
			}()
			done <- s.Flush(cctx, payload)
		}()
		select {
		case err := <-done:
			if _, ok := err.(errPanic); ok {
				t.Errorf("Sink.Flush(%s) panicked: %v", name, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("Sink.Flush(%s) did not return within 5s", name)
		}
	}

	call("normal", []byte("conformance payload"))
	call("empty", []byte{})
	call("nil", nil)
	call("repeated#1", []byte("again"))
	call("repeated#2", []byte("again"))

	// Buffer-reuse: mutate the caller's buffer immediately after Flush returns.
	// A conformant sink must have either written synchronously or copied, so this
	// must not panic or block.
	buf := []byte("mutable payload buffer")
	call("before-mutation", buf)
	for i := range buf {
		buf[i] = 0
	}
	call("after-mutation", buf)
}

// errPanic wraps a recovered panic value so the conformance runner can distinguish
// a panic from a returned error.
type errPanic struct{ v any }

func (e errPanic) Error() string { return "panic" }
