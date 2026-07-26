// Package tickbatch is a lock-free, zero-allocation telemetry batching engine.
package tickbatch

// Serializable is the contract for all payload types ingested by a [Batcher].
//
// Implementors must use flat, pointer-free structs. This is both a performance
// and a security requirement:
//
//   - Performance: pointer fields cause the GC to scan every slot in the ring
//     buffer on every collection cycle, degrading throughput from O(1) to O(n).
//     A flat struct guarantees O(1) GC scan time regardless of ring buffer capacity.
//
//   - Security: a Marshal that performs a raw memory copy
//     (e.g. *(*T)(unsafe.Pointer(&buf[0])) = e) serializes the complete in-memory
//     layout of the struct, including:
//     (a) inter-field padding bytes, which may contain stale stack or heap data
//     (an information-leak analogous to the kernel copy_to_user padding class);
//     (b) pointer and slice header words (address, len, cap), which transmit live
//     heap addresses on the wire and can defeat ASLR.
//
// To reduce padding leak risk, prefer var-initialization before setting fields:
//
//	var e MyEvent     // gc zeroes the backing store in practice, including padding
//	e.Field = value   // but the Go spec does not guarantee padding bytes are zero
//	batcher.Push(e)
//
// The only portable, spec-guaranteed approach is to include explicit padding fields
// in the struct definition (e.g. _ [6]byte) so the padding is a named zero-value
// field rather than invisible compiler-inserted bytes:
//
//	type MyEvent struct {
//	    Field  uint32
//	    _      [4]byte // explicit padding: guaranteed zero by the struct's zero value
//	}
//
// Composite literals (MyEvent{Field: value}) set named fields to their zero values
// but do not guarantee compiler-inserted padding bytes between fields are zeroed.
//
// This is a caller contract, not a compile-time or runtime enforcement.
//
// Marshal encodes the receiver into buf and returns the number of bytes written.
// It must never allocate.
type Serializable interface {
	Marshal(buf []byte) int
}
