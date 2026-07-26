# tickbatch/contrib

Optional integration adapters for [tickbatch](../). This is a **separate Go module**
so that pulling in Prometheus (or any future adapter dependency) never breaches the
core library's zero-dependency guarantee. The core `tickbatch` module remains
stdlib-only; only code that imports `contrib` takes on these dependencies.

All adapters are pull-based: they sample `Batcher.Stats()` (a handful of atomic
loads) at scrape/read time and never touch the `Push` hot path.

## Packages

- **tickbatchprom** - a `prometheus.Collector` that exports tickbatch counters and
  gauges. Register `tickbatchprom.NewCollector("myapp", b.Stats)` with your registry.
- **tickbatchexpvar** - publishes the same snapshot as JSON through the standard
  library `expvar` package (no third-party dependency). Call
  `tickbatchexpvar.Publish("tickbatch", b.Stats)`.

## OpenTelemetry Collector terminology mapping

There is no OTel dependency here; instead, this table documents how to map tickbatch
counters onto the OTel Collector's `otelcol_*` self-observability vocabulary when you
forward these metrics into an OTel pipeline:

| tickbatch `Stats` field | OTel Collector analogue           | Notes                                             |
| ----------------------- | --------------------------------- | ------------------------------------------------- |
| `Dropped`               | `refused` / dropped (queue full)  | DropNewest: incoming item rejected.               |
| `Evicted`               | dropped (overflow)                | DropOldest: oldest queued item discarded.         |
| `Truncated`             | dropped (bad item)                | Marshal returned an invalid size; item discarded. |
| `FlushErrors`           | send failures                     | Failed or timed-out sink flushes.                 |
| `FlushedBatches`        | sent (batches)                    | Batches accepted by the sink.                     |
| `FlushedItems`          | sent (spans/points)               | Items accepted by the sink.                       |
| `QueueDepth`            | queue size                        | Best-effort gauge; leading saturation indicator.  |
| `QueueCap`              | queue capacity                    | Ring capacity.                                     |

Total data loss is `Dropped + Evicted + Truncated`. A rising `FlushErrors` while
`LastFlushAt` is stale is the primary signal of a partitioned downstream sink.
