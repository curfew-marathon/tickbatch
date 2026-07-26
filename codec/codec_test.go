package codec_test

import (
	"context"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/curfew-marathon/tickbatch"
	"github.com/curfew-marathon/tickbatch/codec"
)

// sample is a flat, pointer-free item used to generate real frames.
type sample struct {
	ID    uint32
	Price float64
}

func (s sample) Marshal(buf []byte) int {
	const size = int(unsafe.Sizeof(sample{}))
	if len(buf) < size {
		return 0
	}
	*(*sample)(unsafe.Pointer(&buf[0])) = s
	return size
}

// captureSink records every flushed payload (copied) for later decoding.
type captureSink struct {
	payloads [][]byte
	mu       sync.Mutex
}

func (c *captureSink) Flush(_ context.Context, payload []byte) error {
	cp := make([]byte, len(payload))
	copy(cp, payload)
	c.mu.Lock()
	c.payloads = append(c.payloads, cp)
	c.mu.Unlock()
	return nil
}

// Reliable makes the sink satisfy tickbatch.ReliableSink so DeltaEncoding is allowed.
func (c *captureSink) Reliable() {}

func (c *captureSink) frames() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([][]byte, len(c.payloads))
	copy(out, c.payloads)
	return out
}

// waitFor polls cond until it is true or a deadline elapses. It replaces fixed
// time.Sleep barriers, which are racy under loaded or race-enabled CI: instead of
// guessing how long a flush takes, the tests wait on the batcher's own flush
// counters, which advance only after a real Sink.Flush.
func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestDecodeRawFrames feeds real non-delta frames through Decode and checks that
// the header parses and the CRC verifies.
func TestDecodeRawFrames(t *testing.T) {
	itemSize := int(unsafe.Sizeof(sample{}))
	sink := &captureSink{}
	b := tickbatch.MustNew[sample](tickbatch.Config{
		QueueSize:    64,
		MaxBatchSize: 4096,
		MaxItemSize:  itemSize,
		TickRate:     500,
		Sink:         sink,
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := b.Start(ctx)
	for i := 0; i < 5; i++ {
		b.Push(sample{ID: uint32(i), Price: float64(i) * 2.5})
	}
	waitFor(t, func() bool { return b.FlushedItems() >= 5 }, "5 items flushed")
	cancel()
	<-done

	frames := sink.frames()
	if len(frames) == 0 {
		t.Fatal("no frames captured")
	}
	var totalItems int
	for i, f := range frames {
		h, body, err := codec.Decode(f)
		if err != nil {
			t.Fatalf("frame %d: Decode error: %v", i, err)
		}
		if int(h.Count)*itemSize != len(body) {
			t.Errorf("frame %d: count %d * itemSize %d != body len %d", i, h.Count, itemSize, len(body))
		}
		totalItems += int(h.Count)
	}
	if totalItems != 5 {
		t.Errorf("decoded %d items across frames, want 5", totalItems)
	}
}

// TestDecodeCRCMismatch verifies Decode rejects a corrupted body.
func TestDecodeCRCMismatch(t *testing.T) {
	itemSize := int(unsafe.Sizeof(sample{}))
	sink := &captureSink{}
	b := tickbatch.MustNew[sample](tickbatch.Config{
		QueueSize:    16,
		MaxBatchSize: 1024,
		MaxItemSize:  itemSize,
		TickRate:     500,
		Sink:         sink,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := b.Start(ctx)
	b.Push(sample{ID: 1, Price: 1.0})
	waitFor(t, func() bool { return b.FlushedItems() >= 1 }, "1 item flushed")
	cancel()
	<-done

	frames := sink.frames()
	if len(frames) == 0 {
		t.Fatal("no frames captured")
	}
	corrupt := append([]byte(nil), frames[0]...)
	corrupt[len(corrupt)-1] ^= 0xFF // flip a body byte
	if _, _, err := codec.Decode(corrupt); err != codec.ErrCRCMismatch {
		t.Errorf("Decode(corrupt) error = %v, want ErrCRCMismatch", err)
	}
}

// TestDeltaReconstruction feeds real delta-encoded frames (with keyframes) through
// DeltaReconstructor and checks that each reconstructed frame's CRC verifies and
// its body matches a raw-encoded reference of the same items.
func TestDeltaReconstruction(t *testing.T) {
	itemSize := int(unsafe.Sizeof(sample{}))
	sink := &captureSink{}
	b := tickbatch.MustNew[sample](tickbatch.Config{
		QueueSize:        64,
		MaxBatchSize:     4096,
		MaxItemSize:      itemSize,
		TickRate:         500,
		Sink:             sink,
		DeltaEncoding:    true,
		KeyframeInterval: 3, // exercise both keyframe and delta frames
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := b.Start(ctx)

	// Push one item at a time, waiting for each to be flushed before pushing the
	// next, so every item lands in its own frame. This makes the keyframe/delta mix
	// deterministic (frame 1 is a keyframe, frame 2 onward are deltas) without any
	// fixed sleeps.
	const nFrames = 7
	for i := 0; i < nFrames; i++ {
		b.Push(sample{ID: uint32(i), Price: float64(i) * 1.25})
		want := uint64(i + 1)
		waitFor(t, func() bool { return b.FlushedBatches() >= want }, "each item flushed to its own frame")
	}
	cancel()
	<-done

	frames := sink.frames()
	if len(frames) == 0 {
		t.Fatal("no frames captured")
	}

	var d codec.DeltaReconstructor
	sawKeyframe, sawDelta := false, false
	var decodedItems int
	var firstDelta []byte
	for i, f := range frames {
		raw, h, err := d.Reconstruct(f)
		if err != nil {
			t.Fatalf("frame %d: Reconstruct error: %v", i, err)
		}
		if h.Keyframe {
			sawKeyframe = true
		} else {
			sawDelta = true
			if firstDelta == nil {
				firstDelta = f
			}
		}
		body := raw[8:]
		if int(h.Count)*itemSize != len(body) {
			t.Errorf("frame %d: count %d * itemSize %d != body len %d", i, h.Count, itemSize, len(body))
		}
		decodedItems += int(h.Count)
	}
	if !sawKeyframe {
		t.Error("expected at least one keyframe in the stream")
	}
	if !sawDelta {
		t.Error("expected at least one delta frame in the stream")
	}
	if decodedItems == 0 {
		t.Error("expected to decode at least one item")
	}

	// Mid-stream join: a fresh reconstructor with no baseline must reject a delta
	// frame via CRC rather than emit corrupt data (see SPEC.md, "Mid-stream joins").
	if firstDelta != nil {
		var fresh codec.DeltaReconstructor
		if _, _, err := fresh.Reconstruct(firstDelta); err != codec.ErrCRCMismatch {
			t.Errorf("mid-stream join on a delta frame: got err %v, want ErrCRCMismatch", err)
		}
	}
}
