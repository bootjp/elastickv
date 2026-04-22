package monitoring

import (
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// fakeWriteConflictSource implements WriteConflictCounterSource with
// canned snapshots so collector tests need not open a real store.
type fakeWriteConflictSource struct {
	mu    sync.Mutex
	snap  map[string]uint64
	nilOK bool
}

func (f *fakeWriteConflictSource) set(s map[string]uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.snap = s
}

func (f *fakeWriteConflictSource) WriteConflictCountsByPrefix() map[string]uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.snap == nil && !f.nilOK {
		return map[string]uint64{}
	}
	out := make(map[string]uint64, len(f.snap))
	for k, v := range f.snap {
		out[k] = v
	}
	return out
}

func TestWriteConflictCollectorEmitsPositiveDeltas(t *testing.T) {
	registry := NewRegistry("n1", "10.0.0.1:50051")
	collector := registry.WriteConflictCollector()
	require.NotNil(t, collector)

	src := &fakeWriteConflictSource{}
	sources := []WriteConflictSource{{GroupID: 1, GroupIDStr: "1", Source: src}}

	// Baseline: establishes the delta reference. No series emitted
	// yet because every count equals its previous zero, so the
	// collector skips strict-greater checks — but once the snapshot
	// grows on the next tick, those buckets appear.
	src.set(map[string]uint64{
		"write|txn_rollback": 3,
		"read|redis_string":  1,
	})
	collector.ObserveOnce(sources)

	// Advance: +2 rollback write conflicts, +4 redis_string reads.
	src.set(map[string]uint64{
		"write|txn_rollback": 5,
		"read|redis_string":  5,
		"write|zset":         7, // brand-new bucket; whole count emits.
	})
	collector.ObserveOnce(sources)

	// Idempotent: same snapshot → zero delta → no double-counting.
	collector.ObserveOnce(sources)

	err := testutil.GatherAndCompare(
		registry.Gatherer(),
		strings.NewReader(`
# HELP elastickv_store_write_conflict_total OCC write conflicts detected at the MVCC store layer, bucketed by conflicting key-prefix class and conflict kind (read=read-write, write=write-write). Rate shows cluster-wide conflict pressure; sharp increases on specific prefixes point at hot keys or lock-resolver races (e.g. txn_rollback for the PR #581 incident pattern).
# TYPE elastickv_store_write_conflict_total counter
elastickv_store_write_conflict_total{group="1",key_prefix="redis_string",kind="read",node_address="10.0.0.1:50051",node_id="n1"} 5
elastickv_store_write_conflict_total{group="1",key_prefix="txn_rollback",kind="write",node_address="10.0.0.1:50051",node_id="n1"} 5
elastickv_store_write_conflict_total{group="1",key_prefix="zset",kind="write",node_address="10.0.0.1:50051",node_id="n1"} 7
`),
		"elastickv_store_write_conflict_total",
	)
	require.NoError(t, err)
}

func TestWriteConflictCollectorHandlesSourceReset(t *testing.T) {
	// A store reopen resets the underlying counters. The collector
	// must not emit a negative delta; it rebases silently.
	registry := NewRegistry("n2", "10.0.0.2:50051")
	collector := registry.WriteConflictCollector()
	require.NotNil(t, collector)

	src := &fakeWriteConflictSource{}
	sources := []WriteConflictSource{{GroupID: 7, GroupIDStr: "7", Source: src}}

	src.set(map[string]uint64{"write|txn_lock": 10})
	collector.ObserveOnce(sources)

	src.set(map[string]uint64{"write|txn_lock": 2}) // reopen / reset
	collector.ObserveOnce(sources)

	src.set(map[string]uint64{"write|txn_lock": 5}) // +3 from the post-reset baseline
	collector.ObserveOnce(sources)

	err := testutil.GatherAndCompare(
		registry.Gatherer(),
		strings.NewReader(`
# HELP elastickv_store_write_conflict_total OCC write conflicts detected at the MVCC store layer, bucketed by conflicting key-prefix class and conflict kind (read=read-write, write=write-write). Rate shows cluster-wide conflict pressure; sharp increases on specific prefixes point at hot keys or lock-resolver races (e.g. txn_rollback for the PR #581 incident pattern).
# TYPE elastickv_store_write_conflict_total counter
elastickv_store_write_conflict_total{group="7",key_prefix="txn_lock",kind="write",node_address="10.0.0.2:50051",node_id="n2"} 13
`),
		"elastickv_store_write_conflict_total",
	)
	require.NoError(t, err)
}

func TestWriteConflictCollectorSkipsNilSourceAndMalformedLabels(t *testing.T) {
	registry := NewRegistry("n3", "10.0.0.3:50051")
	collector := registry.WriteConflictCollector()
	require.NotNil(t, collector)

	// Nil source must be skipped without panic.
	require.NotPanics(t, func() {
		collector.ObserveOnce([]WriteConflictSource{{GroupID: 1, GroupIDStr: "1", Source: nil}})
	})

	// Malformed label (missing the "|" separator) must be skipped;
	// the well-formed neighbour must still be emitted.
	src := &fakeWriteConflictSource{}
	src.set(map[string]uint64{
		"malformed_no_pipe": 5,
		"write|hash":        2,
	})
	collector.ObserveOnce([]WriteConflictSource{{GroupID: 42, GroupIDStr: "42", Source: src}})

	err := testutil.GatherAndCompare(
		registry.Gatherer(),
		strings.NewReader(`
# HELP elastickv_store_write_conflict_total OCC write conflicts detected at the MVCC store layer, bucketed by conflicting key-prefix class and conflict kind (read=read-write, write=write-write). Rate shows cluster-wide conflict pressure; sharp increases on specific prefixes point at hot keys or lock-resolver races (e.g. txn_rollback for the PR #581 incident pattern).
# TYPE elastickv_store_write_conflict_total counter
elastickv_store_write_conflict_total{group="42",key_prefix="hash",kind="write",node_address="10.0.0.3:50051",node_id="n3"} 2
`),
		"elastickv_store_write_conflict_total",
	)
	require.NoError(t, err)
}

func TestWriteConflictCollectorZeroRegistryIsSafe(t *testing.T) {
	var c *WriteConflictCollector
	require.NotPanics(t, func() { c.ObserveOnce(nil) })

	registry := NewRegistry("n1", "10.0.0.1:50051")
	collector := registry.WriteConflictCollector()
	require.NotPanics(t, func() { collector.ObserveOnce(nil) })
}
