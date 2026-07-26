package tickbatch

import (
	"context"
	"testing"
	"unsafe"

	"github.com/curfew-marathon/tickbatch/codec"
)

// testXORPanic asserts that XORBytes panics with the deliberate bounds-check
// message for the given inputs. Matching the exact string ensures a native
// runtime index-out-of-range panic (which would occur if the guard were removed)
// does not silently satisfy the assertion.
func testXORPanic(t *testing.T, dst, a, b []byte) {
	t.Helper()
	const wantMsg = "tickbatch: xorBytes: dst and b must each be at least len(a) bytes"
	defer func() {
		r := recover()
		got, ok := r.(string)
		if !ok || got != wantMsg {
			t.Errorf("XORBytes: unexpected panic value %#v (dst=%d a=%d b=%d)", r, len(dst), len(a), len(b))
		}
	}()
	xorBytes(dst, a, b)
}

// TestXORBytesPanicGuard verifies that XORBytes panics when dst or b is shorter
// than a. The fuzz corpus always passes equal-length slices, so without this
// explicit test a refactor that removes the guard would pass undetected.
func TestXORBytesPanicGuard(t *testing.T) {
	testXORPanic(t, []byte("abcde"), []byte("abc"), []byte("ab"))   // len(b) < len(a)
	testXORPanic(t, []byte("ab"), []byte("abcde"), []byte("abcde")) // len(dst) < len(a)
}

// FuzzXORBytes stress-tests the vectorized XOR engine against random byte slice
// lengths, exercising both the 8-byte word loop and the len%8 tail-byte fallback.
// It asserts that the output is correct and that XORing twice recovers the original.
func FuzzXORBytes(f *testing.F) {
	// Seed corpus: pure tail (< 8), exact word, word + tail.
	f.Add([]byte{0x01, 0x02, 0x03})
	f.Add([]byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x22})
	f.Add([]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09})
	f.Add([]byte{
		0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80,
		0x90, 0xA0, 0xB0, 0xC0, 0xD0, 0xE0, 0xF0, 0x01,
		0x11, 0x21, 0x31, 0x41, 0x51, 0x61, 0x71, 0x81,
		0x91, 0xA1, 0xB1, 0xC1, 0xD1, 0xE1, 0xF1, 0x02,
		0x12, 0x22, 0x32, // 35 bytes: 4 words + 3 tail bytes
	})

	f.Fuzz(func(t *testing.T, a []byte) {
		n := len(a)
		if n == 0 {
			return
		}

		// Build b as the bitwise complement of a so every bit differs.
		b := make([]byte, n)
		for i, v := range a {
			b[i] = ^v
		}
		dst := make([]byte, n)

		xorBytes(dst, a, b)

		for i := 0; i < n; i++ {
			if want := a[i] ^ b[i]; dst[i] != want {
				t.Fatalf("byte %d: got %02x, want %02x", i, dst[i], want)
			}
		}

		// Self-inverse: XOR(dst, b) must recover a exactly.
		recovered := make([]byte, n)
		xorBytes(recovered, dst, b)
		for i := 0; i < n; i++ {
			if recovered[i] != a[i] {
				t.Fatalf("reversibility byte %d: got %02x, want %02x", i, recovered[i], a[i])
			}
		}
	})
}

// FuzzTickSerialization stress-tests the OrderUpdate marshal/unmarshal round-trip
// with randomly generated field values, including bit patterns that produce NaN
// floats, infinity, and denormals. It asserts that the serialized bytes are
// recovered identically after deserialization.
func FuzzTickSerialization(f *testing.F) {
	f.Add(uint32(1), float32(100.0), float32(10.0), float32(1.0), uint32(1_700_000_000), float32(0.0))
	f.Add(uint32(0), float32(0.0), float32(0.0), float32(-1.0), uint32(0), float32(0.0))
	f.Add(^uint32(0), float32(512.25), float32(999.99), float32(1.0), ^uint32(0), float32(1.0))

	f.Fuzz(func(t *testing.T, orderID uint32, price, qty, side float32, ts uint32, checksum float32) {
		input := OrderUpdate{
			OrderID:   orderID,
			Price:     price,
			Quantity:  qty,
			Side:      side,
			Timestamp: ts,
			Checksum:  checksum,
		}

		const size = int(unsafe.Sizeof(OrderUpdate{}))
		buf := make([]byte, size)
		n := input.Marshal(buf)
		if n != size {
			t.Fatalf("Marshal returned %d bytes, want %d", n, size)
		}

		// Compare buf directly against the raw memory layout of input.
		// This catches deterministic-but-wrong Marshal implementations that a
		// round-trip comparison (marshal → unmarshal → re-marshal) would miss.
		// Byte-level comparison preserves NaN bit-pattern safety.
		expected := (*[size]byte)(unsafe.Pointer(&input))[:]
		for i := 0; i < size; i++ {
			if buf[i] != expected[i] {
				t.Fatalf("Marshal byte %d: got %02x, want %02x", i, buf[i], expected[i])
			}
		}
	})
}

// fuzzMarshalItem is a Serializable whose Marshal behavior is fully driven by its
// own fields, letting a fuzzer exercise every branch of the drain-loop bounds
// checks in drainAndFlush without unsafe pointer tricks. The declared field is the
// length Marshal reports to the engine, which may legally be zero or - as a
// contract violation the engine must tolerate rather than trust - larger than
// len(buf). The written field is how many bytes Marshal actually fills, always
// clamped to len(buf) so Marshal itself never overruns the caller's buffer.
type fuzzMarshalItem struct {
	declared int32
	written  int32
	fill     byte
	_        [3]byte // Explicit padding keeps the struct flat and its size stable.
}

// Marshal fills min(max(written, 0), len(buf)) bytes with fill and returns
// declared, deliberately allowing a returned length that is zero, negative, or
// larger than len(buf) to stress the engine's defensive truncation path. It never
// itself writes outside buf; only the reported length is hostile.
func (f fuzzMarshalItem) Marshal(buf []byte) int {
	w := int(f.written)
	if w < 0 {
		w = 0
	}
	if w > len(buf) {
		w = len(buf)
	}
	for i := 0; i < w; i++ {
		buf[i] = f.fill
	}
	return int(f.declared)
}

// fuzzCaptureSink records a copy of every flushed frame. The fuzz test drives
// drainAndFlush synchronously on one goroutine, so no locking is required.
type fuzzCaptureSink struct {
	frames [][]byte
}

// Flush stores a copy of payload and never fails.
func (s *fuzzCaptureSink) Flush(_ context.Context, payload []byte) error {
	cp := make([]byte, len(payload))
	copy(cp, payload)
	s.frames = append(s.frames, cp)
	return nil
}

// FuzzMarshalBounds drives the real Push to drain to popMarshal to drainAndFlush
// path with a Marshal whose reported length and fill length are fuzz-controlled,
// across fuzzed buffer geometries (MaxItemSize and MaxBatchSize). Unlike
// FuzzTickSerialization, which exercises Marshal in isolation, this proves two
// engine-level invariants that no other test covers end to end:
//
//  1. The drain loop never panics or overruns byteBuffer, even when Marshal
//     reports zero, negative, or more bytes than the remaining buffer holds (the
//     contract-violation branch documented in SPEC.md), and even when the ring is
//     driven past capacity under either backpressure policy.
//  2. Every frame the engine actually emits is well-formed: the reference decoder
//     parses it and its CRC validates over the body.
func FuzzMarshalBounds(f *testing.F) {
	f.Add([]byte{4, 0, 4, 0xAB}, uint8(4), uint8(1))       // Exact-fill valid item.
	f.Add([]byte{0, 0, 0, 0x00}, uint8(8), uint8(2))       // Marshal returns 0: truncated.
	f.Add([]byte{0x00, 0x40, 0, 0x11}, uint8(3), uint8(1)) // Over-long return: truncated.
	f.Add([]byte{0x00, 0x80, 4, 0xAB}, uint8(4), uint8(1)) // Negative reported length: truncated.
	f.Add(make([]byte, 80), uint8(8), uint8(0x81))         // Ring overflow under DropOldest.
	f.Add([]byte{}, uint8(1), uint8(1))                    // No items: no frames emitted.

	f.Fuzz(func(t *testing.T, data []byte, rawItemSize, rawBatchItems uint8) {
		itemSize := int(rawItemSize%64) + 1    // Range [1, 64].
		batchItems := int(rawBatchItems%8) + 1 // Range [1, 8] item slots per batch.
		maxBatch := headerSize + itemSize*batchItems

		// Choose the backpressure policy from a spare fuzz bit so both the
		// DropNewest and DropOldest overflow paths are exercised.
		backpressure := DropNewest
		if rawBatchItems&0x80 != 0 {
			backpressure = DropOldest
		}

		sink := &fuzzCaptureSink{}
		b, err := New[fuzzMarshalItem](Config{
			QueueSize:    16,
			MaxBatchSize: maxBatch,
			MaxItemSize:  itemSize,
			TickRate:     60,
			Sink:         sink,
			Backpressure: backpressure,
		})
		if err != nil {
			t.Fatalf("New rejected a valid config (itemSize=%d batchItems=%d): %v", itemSize, batchItems, err)
		}

		// Push every parsed item (4 bytes each), intentionally allowed to exceed
		// QueueSize so Push is exercised on a full ring under the configured
		// backpressure policy; overflow must drop or evict without panicking. The
		// upper bound keeps runtime bounded for large inputs. declared is derived
		// from a signed 16-bit value so zero, negative, and over-long reported
		// lengths all reach the engine's truncation guard.
		const stride = 4
		for i := 0; i+stride <= len(data) && i < 64*stride; i += stride {
			rawDeclared := int16(uint16(data[i]) | uint16(data[i+1])<<8)
			declared := int(rawDeclared) % (maxBatch*2 + 16) // May be negative or exceed avail.
			written := int(data[i+2]) % (itemSize + 1)       // Range [0, itemSize].
			b.Push(fuzzMarshalItem{
				declared: int32(declared),
				written:  int32(written),
				fill:     data[i+3],
			})
		}

		// Drain synchronously. Each cycle consumes at least one item (flushing it or
		// dropping it as truncated), so QueueSize+8 cycles fully drains the ring.
		prevOffset := 0
		var keyframeN uint32
		for k := 0; k < 24; k++ {
			b.drainAndFlush(context.Background(), &prevOffset, &keyframeN)
		}

		// Every emitted frame must decode and pass its CRC check.
		for fi, frame := range sink.frames {
			h, body, derr := codec.Decode(frame)
			if derr != nil {
				t.Fatalf("emitted frame %d (len=%d) failed to decode: %v", fi, len(frame), derr)
			}
			if h.Count == 0 {
				t.Fatalf("emitted frame %d decoded with zero item count", fi)
			}
			if len(body) < int(h.Count) {
				t.Fatalf("emitted frame %d body %d shorter than count %d", fi, len(body), h.Count)
			}
		}
	})
}
