// Package tickbatch is a lock-free, zero-allocation telemetry batching engine.
package tickbatch

// Serializable is the contract for all payload types ingested by a [Batcher].
//
// Implementors MUST use flat, pointer-free structs. Pointer fields force the GC
// to scan every slot in the ring buffer on every collection cycle, degrading
// throughput from O(1) to O(n). A flat struct guarantees O(1) GC scan time
// regardless of ring buffer capacity.
//
// Marshal encodes the receiver into buf and returns the number of bytes written.
// It must never allocate.
type Serializable interface {
	Marshal(buf []byte) int
}
