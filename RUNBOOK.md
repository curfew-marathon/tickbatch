# tickbatch - On-Call Runbook

This runbook covers the two failure modes that produce data loss in a tickbatch deployment and explains how to distinguish them using the engine's exported metrics.

---

## Metric quick-reference

| Metric | What it tells you |
|---|---|
| `DroppedCount()` | Items silently discarded under `DropNewest` (ring full, new item rejected) |
| `EvictedCount()` | Items silently discarded under `DropOldest` (ring full, oldest item ejected) |
| `FlushErrorCount()` | Cumulative `Sink.Flush` failures, including `FlushTimeout` expirations |
| `LastFlushAt()` | Timestamp of last successful delivery - primary MTTR clock |
| `QueueDepth()` | Current items waiting in ring - leading saturation indicator |
| `QueueCap()` | Ring capacity (`Config.QueueSize`) |
| `FlushedItems()` | Total items delivered; compare against ingest counter to quantify loss |
| `BytesFlushed()` | Total bytes delivered |
| `CoalescedTicks()` | Drain cycles skipped because a flush overran the tick interval |

---

## Failure mode A - Sink stalled (tarpit / network partition)

**Symptom:** `EvictedCount` or `DroppedCount` is climbing **and**, once at least one flush has succeeded (`LastFlushAt` is non-zero), `time.Since(b.LastFlushAt())` exceeds your threshold. `FlushErrorCount` may be rising too (if `Config.FlushTimeout` is set). Note the zero `LastFlushAt` case separately (step 1): before the first successful flush it is the zero time, so `time.Since` reads as "infinitely stale" and must not by itself be taken as a mid-run stall.

**What is happening:** The drain goroutine is blocked inside `Sink.Flush`. Meanwhile, producers continue running and can keep incrementing `DroppedCount` and `EvictedCount` independently - these counters are not paused by a stalled drain. Without a `FlushTimeout`, a partitioned sink can park the sole drain goroutine indefinitely.

**Steps:**

1. Check `b.LastFlushAt()`. If it is zero, no successful flush has ever occurred - this may be a startup misconfiguration (wrong address, authentication failure) rather than a mid-run partition. If it is non-zero but stale beyond your SLO, the sink stopped delivering at that timestamp.
2. Check `FlushErrorCount()`. If it is rising, flushes are timing out (`FlushTimeout` is set and expiring). If it is zero and `b.LastFlushAt()` is stale, the sink is blocking without returning an error - no `FlushTimeout` is configured.
3. Check `CoalescedTicks()`. A large value confirms the drain goroutine has been running behind the tick rate for an extended period.
4. Remediate the downstream: fail over the endpoint, restart the stuck connection, or route to a fallback sink.
5. If `Config.FlushTimeout` is not set, set it. Without it, a hung sink parks the drain goroutine for the duration of the OS-level I/O timeout (potentially minutes), which maximizes ring loss.
6. After sink recovery, verify `LastFlushAt()` advances and `FlushErrorCount()` stops rising.
7. **Delta-encoding recovery:** If `Config.DeltaEncoding` is true, any missed flush advances the sender's XOR baseline while the receiver did not process that frame. After reconnection, every subsequent delta frame will decode incorrectly until the receiver receives a full (non-delta) reference frame. The engine has no built-in mechanism to force a reference frame - the receiver must detect and request a reset, or the sender must be restarted to re-establish a clean baseline.

---

## Failure mode B - Producer outrunning drain (capacity / tuning)

**Symptom:** `EvictedCount` or `DroppedCount` is climbing **but** `LastFlushAt()` is recent and `FlushErrorCount()` is stable. `CoalescedTicks()` is low or zero.

**What is happening:** The sink is healthy, but items arrive faster than the drain loop can flush them. The ring is saturating on the producer side, not the consumer side.

**Steps:**

1. Confirm `time.Since(b.LastFlushAt())` is small - delivery is happening, just not fast enough.
2. Check `QueueDepth() / QueueCap()`. Sustained values above 0.8 confirm saturation before loss begins; if you are already seeing drops, utilization is at 1.0.
3. Check `FlushedItems() / FlushedBatches()` - if batches are small and `TickRate` is low, raising `TickRate` will flush more frequently and reduce ring accumulation.
4. If `QueueCap()` is small relative to the burst rate, raise `Config.QueueSize` (must remain a power of two).
5. Review `Config.Backpressure` for the data semantics:
   - `DropNewest`: preserves oldest items (audit-trail / sequenced event streams). New arrivals are discarded on overflow.
   - `DropOldest`: preserves newest items (market data / sensor readings where recency matters most). Oldest items are evicted on overflow.
6. Quantify loss via `FlushedItems` versus your ingest counter. File the gap for compliance or backfill pipelines.

---

## Alert thresholds (suggested starting points)

```text
# Leading indicator - page before any loss occurs
QueueDepth() / QueueCap() > 0.8  for 30s

# MTTR clock - sink stalled (guard zero: LastFlushAt is zero until first success)
!b.LastFlushAt().IsZero() && time.Since(b.LastFlushAt()) > 5s

# Loss already occurring
DroppedCount() + EvictedCount()  rate > 0  for 10s

# Drain rate collapsing
CoalescedTicks()  rate > 0  for 60s
```

Tune these thresholds to your deployment. The `QueueSize / TickRate` ratio is an illustrative one-item-per-tick estimate only - in practice each drain cycle flushes multiple items and `Sink.Flush` latency can reduce throughput below the configured rate. Use measured net arrival-minus-drain rates from `FlushedItems` vs. your ingest counter to size `QueueSize` for real burst profiles.
