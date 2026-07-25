package tickbatch

import "unsafe"

// XORBytes computes the bitwise XOR of a and b, writing each result byte into dst.
//
// All three slices must have equal length. The backing arrays of dst, a, and b
// must each begin at an 8-byte-aligned address (guaranteed when passing slices
// returned by make). The hot path reinterprets the backing arrays as []uint64
// via unsafe.Slice and processes eight bytes per loop iteration, maximizing CPU
// throughput on 64-bit word boundaries. A byte-wise tail loop handles the
// remaining len(a)%8 bytes to prevent out-of-bounds access on payloads whose
// length is not a multiple of 8.
func XORBytes(dst, a, b []byte) {
	n := len(a)
	if n == 0 {
		return
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
