// Package tickbatchexpvar publishes a tickbatch.Batcher's operational counters
// through the standard library expvar package.
//
// Unlike the Prometheus adapter this package has no third-party dependency; it
// exists in the tickbatch/contrib module only to keep the adapter surface grouped.
// Values are sampled lazily on each expvar read via an expvar.Func, so publishing
// never touches the Push hot path.
package tickbatchexpvar

import (
	"expvar"

	"github.com/curfew-marathon/tickbatch"
)

// StatsFunc supplies a Stats snapshot on demand. Pass a Batcher's Stats method
// value: tickbatchexpvar.Publish("tickbatch", b.Stats).
type StatsFunc func() tickbatch.Stats

// snapshot is the JSON shape exposed at the expvar endpoint.
type snapshot struct {
	Dropped        uint64 `json:"dropped"`
	Evicted        uint64 `json:"evicted"`
	Truncated      uint64 `json:"truncated"`
	FlushedBatches uint64 `json:"flushed_batches"`
	FlushedItems   uint64 `json:"flushed_items"`
	FlushErrors    uint64 `json:"flush_errors"`
	BytesFlushed   uint64 `json:"bytes_flushed"`
	CoalescedTicks uint64 `json:"coalesced_ticks"`
	QueueDepth     uint64 `json:"queue_depth"`
	QueueCap       uint64 `json:"queue_capacity"`
	LastFlushUnix  int64  `json:"last_flush_unix"`
}

// Var returns an expvar.Var that renders the current Stats as JSON each time it is
// read. Use it when you want to register under a custom key or nest it inside a map.
func Var(stats StatsFunc) expvar.Var {
	return expvar.Func(func() any {
		s := stats()
		var lastFlush int64
		if !s.LastFlushAt.IsZero() {
			lastFlush = s.LastFlushAt.Unix()
		}
		return snapshot{
			Dropped:        s.Dropped,
			Evicted:        s.Evicted,
			Truncated:      s.Truncated,
			FlushedBatches: s.FlushedBatches,
			FlushedItems:   s.FlushedItems,
			FlushErrors:    s.FlushErrors,
			BytesFlushed:   s.BytesFlushed,
			CoalescedTicks: s.CoalescedTicks,
			QueueDepth:     s.QueueDepth,
			QueueCap:       s.QueueCap,
			LastFlushUnix:  lastFlush,
		}
	})
}

// Publish registers the Stats snapshot under name via expvar.Publish. It panics if
// name is already registered, matching expvar.Publish semantics.
func Publish(name string, stats StatsFunc) {
	expvar.Publish(name, Var(stats))
}
