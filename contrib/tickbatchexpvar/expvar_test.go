package tickbatchexpvar_test

import (
	"strings"
	"testing"
	"time"

	"github.com/curfew-marathon/tickbatch"
	"github.com/curfew-marathon/tickbatch/contrib/tickbatchexpvar"
)

func TestVarRendersJSON(t *testing.T) {
	stats := func() tickbatch.Stats {
		return tickbatch.Stats{
			Dropped:      7,
			FlushedItems: 21,
			QueueCap:     512,
			LastFlushAt:  time.Unix(1_700_000_000, 0),
		}
	}
	v := tickbatchexpvar.Var(stats)
	out := v.String()

	for _, want := range []string{`"dropped":7`, `"flushed_items":21`, `"queue_capacity":512`, `"last_flush_unix":1700000000`} {
		if !strings.Contains(out, want) {
			t.Errorf("expvar JSON %q missing %q", out, want)
		}
	}
}
