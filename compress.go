package tickbatch

// Compressor is the pluggable compression interface for batch payloads.
//
// Compress writes the compressed form of src into the pre-allocated dst slice
// and returns the number of bytes written. Implementations must not write past
// len(dst); the returned n must be within [0, len(dst)]. Returning a value
// outside that range is a contract violation and will be treated as an error
// by the engine. If dst is too small to hold the compressed output,
// implementations must return an error rather than panicking or truncating
// silently. Neither src nor dst may be retained beyond the duration of the call.
type Compressor interface {
	// Compress encodes src into dst and returns the number of bytes written.
	Compress(dst, src []byte) (int, error)
}
