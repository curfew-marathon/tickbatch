package tickbatchtest_test

import (
	"context"
	"testing"

	"github.com/curfew-marathon/tickbatch"
	"github.com/curfew-marathon/tickbatch/tickbatchtest"
)

// TestRecordingSinkConformance runs the conformance battery against the provided
// fakes to prove both the fakes and the harness are self-consistent.
func TestRecordingSinkConformance(t *testing.T) {
	tickbatchtest.TestSink(t, &tickbatchtest.RecordingSink{})
	tickbatchtest.TestSink(t, &tickbatchtest.ReliableRecordingSink{})
	tickbatchtest.TestSink(t, tickbatch.StdoutSink{})
}

// TestRecordingSinkRecordsCopies verifies RecordingSink stores independent copies
// that survive mutation of the caller's buffer.
func TestRecordingSinkRecordsCopies(t *testing.T) {
	s := &tickbatchtest.RecordingSink{}
	buf := []byte{1, 2, 3, 4}
	if err := s.Flush(context.Background(), buf); err != nil {
		t.Fatal(err)
	}
	for i := range buf {
		buf[i] = 0
	}
	got := s.Payloads()
	if len(got) != 1 {
		t.Fatalf("Payloads len = %d, want 1", len(got))
	}
	want := []byte{1, 2, 3, 4}
	for i := range want {
		if got[0][i] != want[i] {
			t.Fatalf("payload byte %d = %d, want %d (copy not isolated)", i, got[0][i], want[i])
		}
	}
}

// TestReliableRecordingSinkEnablesDelta proves the reliable fake satisfies the
// ReliableSink check so DeltaEncoding is accepted.
func TestReliableRecordingSinkEnablesDelta(t *testing.T) {
	_, err := tickbatch.New[marker](tickbatch.Config{
		Sink:          &tickbatchtest.ReliableRecordingSink{},
		QueueSize:     16,
		MaxBatchSize:  1024,
		MaxItemSize:   8,
		TickRate:      100,
		DeltaEncoding: true,
	})
	if err != nil {
		t.Fatalf("New with ReliableRecordingSink + DeltaEncoding: %v", err)
	}
}

// marker is a minimal Serializable for the delta-acceptance test.
type marker struct{ V uint64 }

func (m marker) Marshal(buf []byte) int {
	if len(buf) < 8 {
		return 0
	}
	buf[0] = byte(m.V)
	return 8
}
