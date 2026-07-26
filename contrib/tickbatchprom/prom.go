// Package tickbatchprom bridges a tickbatch.Batcher's operational counters to
// Prometheus via a pull-based prometheus.Collector.
//
// It follows the client_golang model: the cost of observation is paid by the
// scraper, not the producer. The collector samples tickbatch.Stats once per scrape
// (each Stats() call is a handful of atomic loads), so it never touches the Push
// hot path. This package lives in the tickbatch/contrib module so the core library
// keeps its zero-dependency guarantee.
package tickbatchprom

import (
	"github.com/curfew-marathon/tickbatch"
	"github.com/prometheus/client_golang/prometheus"
)

// StatsFunc supplies a Stats snapshot on demand. Pass a Batcher's Stats method
// value: tickbatchprom.NewCollector("myapp", b.Stats).
type StatsFunc func() tickbatch.Stats

// Collector is a prometheus.Collector that exports tickbatch counters and gauges.
type Collector struct {
	stats StatsFunc

	dropped        *prometheus.Desc
	evicted        *prometheus.Desc
	truncated      *prometheus.Desc
	flushedBatches *prometheus.Desc
	flushedItems   *prometheus.Desc
	flushErrors    *prometheus.Desc
	bytesFlushed   *prometheus.Desc
	coalescedTicks *prometheus.Desc
	queueDepth     *prometheus.Desc
	queueCap       *prometheus.Desc
	lastFlush      *prometheus.Desc
}

// NewCollector builds a Collector that reports metrics under the given namespace
// (for example "myapp" yields metrics like myapp_tickbatch_dropped_total). Register
// it with prometheus.MustRegister.
func NewCollector(namespace string, stats StatsFunc) *Collector {
	fq := func(name string) string {
		return prometheus.BuildFQName(namespace, "tickbatch", name)
	}
	return &Collector{
		stats:          stats,
		dropped:        prometheus.NewDesc(fq("dropped_total"), "Items dropped on a full queue (DropNewest).", nil, nil),
		evicted:        prometheus.NewDesc(fq("evicted_total"), "Items evicted from the queue head (DropOldest).", nil, nil),
		truncated:      prometheus.NewDesc(fq("truncated_total"), "Items discarded because Marshal returned an invalid size.", nil, nil),
		flushedBatches: prometheus.NewDesc(fq("flushed_batches_total"), "Batches delivered to the sink.", nil, nil),
		flushedItems:   prometheus.NewDesc(fq("flushed_items_total"), "Items delivered to the sink.", nil, nil),
		flushErrors:    prometheus.NewDesc(fq("flush_errors_total"), "Failed or timed-out sink flushes.", nil, nil),
		bytesFlushed:   prometheus.NewDesc(fq("bytes_flushed_total"), "Payload bytes delivered to the sink.", nil, nil),
		coalescedTicks: prometheus.NewDesc(fq("coalesced_ticks_total"), "Drain cycles skipped because a flush overran the tick interval.", nil, nil),
		queueDepth:     prometheus.NewDesc(fq("queue_depth"), "Best-effort count of items currently queued.", nil, nil),
		queueCap:       prometheus.NewDesc(fq("queue_capacity"), "Ring buffer capacity.", nil, nil),
		lastFlush:      prometheus.NewDesc(fq("last_flush_timestamp_seconds"), "Unix time of the last successful flush (0 if never).", nil, nil),
	}
}

// Describe implements prometheus.Collector.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.dropped
	ch <- c.evicted
	ch <- c.truncated
	ch <- c.flushedBatches
	ch <- c.flushedItems
	ch <- c.flushErrors
	ch <- c.bytesFlushed
	ch <- c.coalescedTicks
	ch <- c.queueDepth
	ch <- c.queueCap
	ch <- c.lastFlush
}

// Collect implements prometheus.Collector. It samples Stats once and emits every
// metric, so all values come from a single consistent-enough snapshot.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	s := c.stats()
	counter := func(d *prometheus.Desc, v uint64) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, float64(v))
	}
	gauge := func(d *prometheus.Desc, v float64) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v)
	}
	counter(c.dropped, s.Dropped)
	counter(c.evicted, s.Evicted)
	counter(c.truncated, s.Truncated)
	counter(c.flushedBatches, s.FlushedBatches)
	counter(c.flushedItems, s.FlushedItems)
	counter(c.flushErrors, s.FlushErrors)
	counter(c.bytesFlushed, s.BytesFlushed)
	counter(c.coalescedTicks, s.CoalescedTicks)
	gauge(c.queueDepth, float64(s.QueueDepth))
	gauge(c.queueCap, float64(s.QueueCap))
	var lastFlush float64
	if !s.LastFlushAt.IsZero() {
		lastFlush = float64(s.LastFlushAt.Unix())
	}
	gauge(c.lastFlush, lastFlush)
}

// Static assertion that Collector satisfies the interface.
var _ prometheus.Collector = (*Collector)(nil)
