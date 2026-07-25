package tickbatch

// Compressor is the pluggable compression interface for batch payloads.
//
// Compress writes the compressed form of src into the pre-allocated dst slice
// and returns the number of bytes written. The caller guarantees that dst has
// sufficient capacity; implementations must not grow or replace it. Neither
// src nor dst may be retained beyond the duration of the call.
type Compressor interface {
	// Compress encodes src into dst and returns the number of bytes written.
	Compress(dst, src []byte) (int, error)
}
