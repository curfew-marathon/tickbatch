package tickbatch_test

import (
	"bytes"
	"testing"
	"unsafe"

	"github.com/curfew-marathon/tickbatch"
)

// refSink captures the raw slice reference passed to Flush without copying it.
// This simulates an async broker that enqueues the pointer and returns immediately.
type refSink struct{ got []byte }

func (r *refSink) Flush(payload []byte) error {
	r.got = payload
	return nil
}

// copyTestItem is a minimal Serializable for use in this file's Batcher tests.
type copyTestItem struct {
	A uint64
	B uint64
}

func (c copyTestItem) Marshal(buf []byte) int {
	const size = int(unsafe.Sizeof(copyTestItem{}))
	if len(buf) < size {
		return 0
	}
	*(*copyTestItem)(unsafe.Pointer(&buf[0])) = c
	return size
}

func TestCopyingSinkIsolatesBuffer(t *testing.T) {
	// CopyingSink must give the inner Sink an independent copy of the payload.
	// Zeroing the original source buffer after Flush returns must not corrupt
	// the bytes the inner sink received.
	inner := &refSink{}
	cs := tickbatch.CopyingSink{Inner: inner}

	original := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	if err := cs.Flush(original); err != nil {
		t.Fatal(err)
	}

	want := bytes.Clone(inner.got)

	// Overwrite the source; would corrupt inner.got if CopyingSink passed the
	// original slice reference instead of a copy.
	for i := range original {
		original[i] = 0
	}

	if !bytes.Equal(inner.got, want) {
		t.Errorf("CopyingSink did not isolate payload: inner sink sees mutated bytes")
	}
}

func TestReliableCopyingSinkEnablesDeltaEncoding(t *testing.T) {
	// ReliableCopyingSink must satisfy the ReliableSink interface check in New
	// so that DeltaEncoding = true does not panic.
	inner := &refSink{}
	cs := tickbatch.ReliableCopyingSink{CopyingSink: tickbatch.CopyingSink{Inner: inner}}

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("ReliableCopyingSink failed ReliableSink check: %v", r)
		}
	}()

	_ = tickbatch.New[copyTestItem](tickbatch.Config{
		Sink:          cs,
		QueueSize:     16,
		MaxBatchSize:  1024,
		MaxItemSize:   int(unsafe.Sizeof(copyTestItem{})),
		TickRate:      1000,
		DeltaEncoding: true,
	})
}
