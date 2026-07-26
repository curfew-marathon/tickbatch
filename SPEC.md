# tickbatch wire format v1

This document specifies the on-wire frame format produced by `Batcher` and passed
to `Sink.Flush`. It is the contract third-party decoders implement. The reference
decoder in [`codec/`](./codec) implements exactly this spec.

The format is frozen for the v1.x series. Fields are added only via the flags
mechanism described below; existing field positions and meanings never change.

## Frame layout

Every non-empty flush produces one frame:

```
byte offset   field
[0:4]         sequence ID   uint32, little-endian
[4:6]         item count    uint16, little-endian
[6:8]         integrity tag uint16, little-endian:
                bit 15      keyframe flag (1 = keyframe / delta baseline reset)
                bits 0-14   low 15 bits of CRC-32/IEEE over the raw body [8:N]
[8:N]         body          concatenated item encodings (see below)
```

The 8-byte header (`[0:8]`) is **always little-endian on every platform**.

An empty batch (zero items) produces no frame at all - no header is emitted - so a
receiver never observes a frame with `count == 0`.

## Body encoding

The body `[8:N]` is the concatenation of each item's `Marshal` output, back to
back, with no per-item length prefix or delimiter. Item boundaries are recovered
out-of-band: both ends agree on the fixed item wire size (the `Marshal` contract
below), and `count` gives the number of items.

Body bytes are written in the producer's **native byte order** (`Marshal` copies
raw in-memory struct representations). A big-endian consumer of a frame produced on
a little-endian host must byte-swap fields itself. The 8-byte header is unaffected -
it is always little-endian.

### Marshal contract

`Marshal(buf []byte) int` must:
- write **at least 1 byte** and return the count written;
- never return more than `len(buf)`.

A return of `0` means "nothing written": the engine drops that item and increments
`TruncatedCount()`. A return greater than `len(buf)` is a contract violation; the
engine defensively drops the item and increments `TruncatedCount()` rather than
overrunning its buffer. Neither case ever panics.

## Codec configuration is out-of-band

Whether the body is **delta-encoded** and/or **compressed** is **not** self-described
in the frame. It is established out-of-band by shared `Config` between producer and
consumer. This is a deliberate v1 design choice for a telemetry pipe where both ends
are configured together; it keeps the header at 8 bytes and the hot path at two byte
stores. A receiver must know the producer's `DeltaEncoding` and `Compressor` settings.

When compression is enabled, it is applied **last** (after any delta transform), so
the decode order is: decompress → reverse delta → parse header/body.

## Delta encoding

When `Config.DeltaEncoding` is true, each frame (header included) is XORed against
the previous **raw** frame before transmission, except keyframes which are sent raw.

Sender rules (see `drainAndFlush`):
- **Keyframe** (first flush, and every `KeyframeInterval` successful flushes when
  `KeyframeInterval > 0`): the raw frame is sent unchanged. Its header is directly
  readable and bit 15 of the integrity tag is set.
- **Delta frame**: the transmitted bytes are `XOR(rawFrame, previousRawFrame)` over
  the current frame length, where `previousRawFrame` is zero-extended past its length.
  Because the header is XORed too, a delta frame's header bytes are **not** directly
  readable.

Receiver rules (see [`codec.DeltaReconstructor`](./codec)):
- Maintain the previous reconstructed raw frame, zero-extended to the current length.
- A frame is a keyframe iff its header (read directly) has bit 15 set **and** its
  CRC validates over its body as-is. Otherwise it is a delta frame: recover
  `rawFrame = XOR(received, previousRawFrame)`, then validate the CRC from the
  recovered header. The CRC - computed by the sender over the raw body *before* XOR -
  is the disambiguator; a delta frame whose header bit happens to read as 1 will fail
  the direct CRC check and fall through to the delta path.

### Sequence gaps do NOT imply desync

`sequenceID` increments on **every** attempted flush, but the delta baseline
(`previousState`) advances **only on a successful flush**. Therefore, when a flush
fails or times out, the receiver observes a gap in sequence IDs that does **not**
correspond to a baseline advance. A decoder must **not** treat a sequence gap as
"resync required". The delta chain remains intact across the gap; only frames that
were actually delivered advance the baseline. Wait for a keyframe (or a CRC failure)
to trigger a resync - never a bare gap.

Fire-and-forget transports (e.g. `UDPSink`) are incompatible with delta encoding:
a genuinely lost frame advances the sender baseline while the receiver never sees it,
producing permanent desync. Only pair `DeltaEncoding` with a `ReliableSink`.

## Sequence wrap

`sequenceID` is a `uint32` and wraps to zero after 2^32 − 1 batches (~2.2 years at
60 Hz). There is no epoch counter or MAC. Receivers that care about total ordering
across a wrap must track wraps themselves.
