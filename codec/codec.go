// Package codec is the reference decoder for the tickbatch wire format.
//
// It reads frames produced by tickbatch.Batcher and passed to a Sink: it parses
// the 8-byte header, verifies the CRC, and - for delta-encoded streams - reverses
// the XOR delta against the previous frame, honoring keyframes. See ../SPEC.md for
// the full wire-format contract this package implements.
//
// The package is stdlib-only and has no dependency on the tickbatch core, so
// consumers (which may be separate services in another language's absence, or Go
// receivers) can vendor just this decoder.
package codec

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// headerSize is the fixed wire header length. It mirrors the producer's header and
// is frozen for the v1 format.
const headerSize = 8

// keyframeBit is bit 15 of the integrity tag, signaling a delta baseline reset.
const keyframeBit = 0x8000

// crcMask selects the low 15 bits of the integrity tag (the truncated CRC).
const crcMask = 0x7fff

var (
	// ErrShortFrame is returned when a frame is smaller than the fixed header.
	ErrShortFrame = errors.New("tickbatch/codec: frame shorter than header")
	// ErrCRCMismatch is returned when the body CRC does not match the header tag.
	ErrCRCMismatch = errors.New("tickbatch/codec: CRC mismatch")
)

// Header is the decoded fixed header of a frame.
type Header struct {
	// Seq is the frame sequence ID. It increments on every attempted flush, so
	// gaps do not imply a delta desync (see ../SPEC.md).
	Seq uint32
	// Count is the number of items in the body.
	Count uint16
	// CRC is the low 15 bits of CRC-32/IEEE over the raw body.
	CRC uint16
	// Keyframe reports whether bit 15 of the integrity tag is set, marking a
	// full-frame delta baseline reset.
	Keyframe bool
}

// ParseHeader decodes the 8-byte header at the start of frame without verifying
// the CRC or interpreting the body.
func ParseHeader(frame []byte) (Header, error) {
	if len(frame) < headerSize {
		return Header{}, ErrShortFrame
	}
	tag := binary.LittleEndian.Uint16(frame[6:8])
	return Header{
		Seq:      binary.LittleEndian.Uint32(frame[0:4]),
		Count:    binary.LittleEndian.Uint16(frame[4:6]),
		CRC:      tag & crcMask,
		Keyframe: tag&keyframeBit != 0,
	}, nil
}

// crcOK reports whether the CRC of body matches the truncated CRC in want.
func crcOK(body []byte, want uint16) bool {
	return crc32.ChecksumIEEE(body)&crcMask == uint32(want)
}

// Decode parses a single raw (non-delta) frame, verifies its CRC, and returns the
// header and the body slice (aliasing frame, not copied). Use it for streams where
// the producer's Config.DeltaEncoding is false. For delta-encoded streams use a
// [DeltaReconstructor] instead.
func Decode(frame []byte) (Header, []byte, error) {
	h, err := ParseHeader(frame)
	if err != nil {
		return Header{}, nil, err
	}
	body := frame[headerSize:]
	if !crcOK(body, h.CRC) {
		return Header{}, nil, ErrCRCMismatch
	}
	return h, body, nil
}

// DeltaReconstructor reverses tickbatch's XOR delta encoding, reconstructing each
// raw frame from the transmitted (possibly XOR-encoded) frame. It maintains the
// previous raw frame as the delta baseline. The zero value is ready to use.
//
// It is not safe for concurrent use; drive it from a single consumer goroutine.
type DeltaReconstructor struct {
	prev []byte
}

// Reconstruct recovers the raw frame from a transmitted frame and returns the raw
// frame (a fresh slice), its parsed header, and any error.
//
// A frame is treated as a keyframe when its header reads as a keyframe and its CRC
// validates directly; such frames are sent raw and reset the baseline. Otherwise
// the frame is delta-decoded by XORing against the previous raw frame (zero-extended
// to the current length), and the recovered frame's CRC is verified. The CRC is the
// keyframe/delta disambiguator (see ../SPEC.md).
func (d *DeltaReconstructor) Reconstruct(frame []byte) ([]byte, Header, error) {
	if len(frame) < headerSize {
		return nil, Header{}, ErrShortFrame
	}

	// Keyframe path: header and body are readable as-is.
	if hk, err := ParseHeader(frame); err == nil && hk.Keyframe && crcOK(frame[headerSize:], hk.CRC) {
		raw := make([]byte, len(frame))
		copy(raw, frame)
		d.prev = raw
		return raw, hk, nil
	}

	// Delta path: XOR against the previous raw frame, zero-extended.
	raw := make([]byte, len(frame))
	for i := range frame {
		var p byte
		if i < len(d.prev) {
			p = d.prev[i]
		}
		raw[i] = frame[i] ^ p
	}
	h, err := ParseHeader(raw)
	if err != nil {
		return nil, Header{}, err
	}
	if !crcOK(raw[headerSize:], h.CRC) {
		return nil, Header{}, ErrCRCMismatch
	}
	d.prev = raw
	return raw, h, nil
}
