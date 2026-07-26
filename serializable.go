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
//   - Security: a Marshal that performs a raw memory copy (e.g. *(*T)(&buf[0]) = e)
//     serializes the complete in-memory layout of the struct, including:
//     (a) inter-field padding bytes, which may contain stale stack or heap data
//     (an information-leak analogous to the kernel copy_to_user padding class);
//     (b) pointer and slice header words (address, len, cap), which transmit live
//     heap addresses on the wire and can defeat ASLR.
//
// To prevent padding leaks, zero-initialize the struct before setting fields:
//
//	var e MyEvent          // all bytes including padding are zeroed
//	e.Field = value
//	batcher.Push(e)
//
// Assigning a composite literal (MyEvent{Field: value}) does not guarantee that
// padding bytes between fields are zero.
//
// This is a caller contract, not a compile-time or runtime enforcement.
//
// Marshal encodes the receiver into buf and returns the number of bytes written.
// It must never allocate.
type Serializable interface {
	Marshal(buf []byte) int
}
