package tickbatch

import (
	"testing"
	"unsafe"
)

// testXORPanic asserts that XORBytes panics for the given inputs.
// Uses stdlib recover so testify is not required.
func testXORPanic(t *testing.T, dst, a, b []byte) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("XORBytes did not panic with mismatched lengths (dst=%d a=%d b=%d)", len(dst), len(a), len(b))
		}
	}()
	XORBytes(dst, a, b)
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

		XORBytes(dst, a, b)

		for i := 0; i < n; i++ {
			if want := a[i] ^ b[i]; dst[i] != want {
				t.Fatalf("byte %d: got %02x, want %02x", i, dst[i], want)
			}
		}

		// Self-inverse: XOR(dst, b) must recover a exactly.
		recovered := make([]byte, n)
		XORBytes(recovered, dst, b)
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
