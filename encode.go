package tickbatch

import "unsafe"

// XORBytes computes the bitwise XOR of a and b, writing each result byte into dst.
//
// All three slices must have equal length: len(dst) >= len(a) and len(b) >= len(a).
// Passing shorter dst or b panics. The hot path reinterprets the backing arrays as
// []uint64 via unsafe.Slice and processes eight bytes per loop iteration, maximizing
// CPU throughput on 64-bit word boundaries. A byte-wise tail loop handles the
// remaining len(a)%8 bytes to prevent out-of-bounds access on payloads whose length
// is not a multiple of 8.
//
// Alignment note: on AArch64, plain LDR/STR instructions tolerate unaligned
// addresses, so a subslice starting at a non-zero offset does not fault.
// The length-equality requirement above is the only hard contract callers must satisfy.
func XORBytes(dst, a, b []byte) {
	n := len(a)
	if n == 0 {
		return
	}
	if len(dst) < n || len(b) < n {
		panic("tickbatch: XORBytes: dst and b must each be at least len(a) bytes")
	}
	words := n / 8
	if words > 0 {
		dw := unsafe.Slice((*uint64)(unsafe.Pointer(&dst[0])), words)
		aw := unsafe.Slice((*uint64)(unsafe.Pointer(&a[0])), words)
		bw := unsafe.Slice((*uint64)(unsafe.Pointer(&b[0])), words)
		for i := 0; i < words; i++ {
			dw[i] = aw[i] ^ bw[i]
		}
	}
	for i := words * 8; i < n; i++ {
		dst[i] = a[i] ^ b[i]
	}
}
