package tickbatchprom_test

import (
	"strings"
	"testing"

	"github.com/curfew-marathon/tickbatch"
	"github.com/curfew-marathon/tickbatch/contrib/tickbatchprom"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestCollectorExportsStats(t *testing.T) {
	stats := func() tickbatch.Stats {
		return tickbatch.Stats{
			Dropped:        3,
			FlushedBatches: 10,
			FlushedItems:   40,
			QueueDepth:     5,
			QueueCap:       1024,
		}
	}
	c := tickbatchprom.NewCollector("myapp", stats)

	reg := prometheus.NewRegistry()
	if err := reg.Register(c); err != nil {
		t.Fatalf("register: %v", err)
	}

	expected := `
# HELP myapp_tickbatch_dropped_total Items dropped on a full queue (DropNewest).
# TYPE myapp_tickbatch_dropped_total counter
myapp_tickbatch_dropped_total 3
# HELP myapp_tickbatch_queue_depth Best-effort count of items currently queued.
# TYPE myapp_tickbatch_queue_depth gauge
myapp_tickbatch_queue_depth 5
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(expected),
		"myapp_tickbatch_dropped_total", "myapp_tickbatch_queue_depth"); err != nil {
		t.Errorf("unexpected metrics: %v", err)
	}
}
