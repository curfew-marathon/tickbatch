package tickbatch

import "unsafe"

// xorBytes computes the bitwise XOR of a and b, writing each result byte into dst.
//
// This is an internal delta-encoding primitive and is intentionally unexported: it
// carries an unsafe uint64 word-cast with an alignment contract that must not become
// part of the stable public API. The engine only ever calls it with buffers that
// start at offset 0 of heap allocations (8-byte aligned), so the word-cast is safe.
//
// The length of dst and b must each be at least len(a); extra trailing bytes are ignored.
// Passing shorter dst or b panics. The hot path reinterprets the backing arrays as
// []uint64 via unsafe.Slice and processes 32 bytes per iteration using a 4-wide
// unrolled loop. Four independent XOR operations per iteration have no data dependency,
// so an out-of-order core issues them in parallel, amortizing the loop-branch cost
// across 32 bytes rather than 8. A cleanup loop handles the remaining 1-3 words when
// len(a)/8 is not a multiple of 4, followed by a byte-wise tail for the final
// len(a)%8 bytes.
func xorBytes(dst, a, b []byte) {
	n := len(a)
	if n == 0 {
		return
	}
	if len(dst) < n || len(b) < n {
		panic("tickbatch: xorBytes: dst and b must each be at least len(a) bytes")
	}
	words := n / 8
	if words > 0 {
		dw := unsafe.Slice((*uint64)(unsafe.Pointer(&dst[0])), words)
		aw := unsafe.Slice((*uint64)(unsafe.Pointer(&a[0])), words)
		bw := unsafe.Slice((*uint64)(unsafe.Pointer(&b[0])), words)
		i := 0
		// 4-wide unroll: four independent XORs per iteration have no data
		// dependency, so a wide out-of-order core issues them in parallel and
		// the loop branch is amortized across 32 bytes instead of 8.
		for ; i+4 <= words; i += 4 {
			dw[i] = aw[i] ^ bw[i]
			dw[i+1] = aw[i+1] ^ bw[i+1]
			dw[i+2] = aw[i+2] ^ bw[i+2]
			dw[i+3] = aw[i+3] ^ bw[i+3]
		}
		for ; i < words; i++ {
			dw[i] = aw[i] ^ bw[i]
		}
	}
	// Speculative loads hoist the bounds checks for all three tail slices out
	// of the loop body; the compiler eliminates per-iteration BCE after seeing
	// that da[tail-1], aa[tail-1], and ba[tail-1] are provably in-bounds.
	wordEnd := words * 8
	tail := n - wordEnd
	if tail > 0 {
		da, aa, ba := dst[wordEnd:], a[wordEnd:], b[wordEnd:]
		_ = da[tail-1]
		_ = aa[tail-1]
		_ = ba[tail-1]
		for i := 0; i < tail; i++ {
			da[i] = aa[i] ^ ba[i]
		}
	}
}
