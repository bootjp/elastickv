package monitoring

import (
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// fakePebbleSource implements PebbleMetricsSource with canned values
// so tests can exercise the collector without opening a real Pebble
// DB. A nil stored value makes Metrics() return nil (exercising the
// "store closed / mid-swap" skip path).
type fakePebbleSource struct {
	mu      sync.Mutex
	metrics *pebble.Metrics
}

func (f *fakePebbleSource) set(m *pebble.Metrics) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.metrics = m
}

func (f *fakePebbleSource) Metrics() *pebble.Metrics {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.metrics
}

// fakePebbleCapacitySource additionally implements PebbleCacheCapacitySource
// so the collector emits elastickv_pebble_block_cache_capacity_bytes.
type fakePebbleCapacitySource struct {
	fakePebbleSource
	capacity int64
}

func (f *fakePebbleCapacitySource) BlockCacheCapacityBytes() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.capacity
}

// newFakeMetrics builds a *pebble.Metrics populated only with the
// fields the collector reads. Other fields stay at their zero value.
func newFakeMetrics(l0Sub int32, l0Files int64, debt uint64, inProg int64, compactCount int64,
	memCount int64, memSize uint64, memZombie int64,
	cacheSize int64, hits int64, misses int64,
) *pebble.Metrics {
	m := &pebble.Metrics{}
	m.Levels[0].Sublevels = l0Sub
	m.Levels[0].TablesCount = l0Files
	m.Compact.EstimatedDebt = debt
	m.Compact.NumInProgress = inProg
	m.Compact.Count = compactCount
	m.MemTable.Count = memCount
	m.MemTable.Size = memSize
	m.MemTable.ZombieCount = memZombie
	m.BlockCache.Size = cacheSize
	m.BlockCache.Hits = hits
	m.BlockCache.Misses = misses
	return m
}

func TestPebbleCollectorMirrorsGaugesAndCounters(t *testing.T) {
	registry := NewRegistry("n1", "10.0.0.1:50051")
	collector := registry.PebbleCollector()
	require.NotNil(t, collector)

	src := &fakePebbleSource{}
	sources := []PebbleSource{{GroupID: 1, GroupIDStr: "1", Source: src}}

	// Baseline tick: initial counter values establish the delta
	// baseline, gauges reflect the snapshot immediately.
	src.set(newFakeMetrics(
		2, 7, 1024, 1, 10,
		3, 2048, 1,
		8192, 100, 20,
	))
	collector.ObserveOnce(sources)

	// Advance: gauges change, monotonic counters grow.
	src.set(newFakeMetrics(
		5, 12, 4096, 2, 15,
		4, 8192, 2,
		16384, 150, 25,
	))
	collector.ObserveOnce(sources)

	// Idempotent second pass with the same snapshot must not
	// double-count the counters.
	collector.ObserveOnce(sources)

	err := testutil.GatherAndCompare(
		registry.Gatherer(),
		strings.NewReader(`
# HELP elastickv_pebble_block_cache_hits_total Cumulative block cache hits reported by Pebble.
# TYPE elastickv_pebble_block_cache_hits_total counter
elastickv_pebble_block_cache_hits_total{group="1",node_address="10.0.0.1:50051",node_id="n1"} 150
# HELP elastickv_pebble_block_cache_misses_total Cumulative block cache misses reported by Pebble.
# TYPE elastickv_pebble_block_cache_misses_total counter
elastickv_pebble_block_cache_misses_total{group="1",node_address="10.0.0.1:50051",node_id="n1"} 25

# HELP elastickv_pebble_block_cache_size_bytes Current bytes in use by Pebble's block cache.
# TYPE elastickv_pebble_block_cache_size_bytes gauge
elastickv_pebble_block_cache_size_bytes{group="1",node_address="10.0.0.1:50051",node_id="n1"} 16384
# HELP elastickv_pebble_compact_count_total Cumulative number of compactions completed by Pebble since the process started.
# TYPE elastickv_pebble_compact_count_total counter
elastickv_pebble_compact_count_total{group="1",node_address="10.0.0.1:50051",node_id="n1"} 15
# HELP elastickv_pebble_compact_estimated_debt_bytes Estimated number of bytes Pebble still needs to compact for the LSM to reach a stable state. Growth indicates compactions are falling behind ingest.
# TYPE elastickv_pebble_compact_estimated_debt_bytes gauge
elastickv_pebble_compact_estimated_debt_bytes{group="1",node_address="10.0.0.1:50051",node_id="n1"} 4096
# HELP elastickv_pebble_compact_in_progress Number of compactions currently in progress.
# TYPE elastickv_pebble_compact_in_progress gauge
elastickv_pebble_compact_in_progress{group="1",node_address="10.0.0.1:50051",node_id="n1"} 2
# HELP elastickv_pebble_l0_num_files Current number of sstables in L0 reported by Pebble. Paired with elastickv_pebble_l0_sublevels to diagnose L0 pressure.
# TYPE elastickv_pebble_l0_num_files gauge
elastickv_pebble_l0_num_files{group="1",node_address="10.0.0.1:50051",node_id="n1"} 12
# HELP elastickv_pebble_l0_sublevels Current L0 sublevel count reported by Pebble. Climbing sublevels are the canonical precursor to a write stall; alert when this exceeds the L0CompactionThreshold for a sustained period.
# TYPE elastickv_pebble_l0_sublevels gauge
elastickv_pebble_l0_sublevels{group="1",node_address="10.0.0.1:50051",node_id="n1"} 5
# HELP elastickv_pebble_memtable_count Current count of memtables (active + queued for flush).
# TYPE elastickv_pebble_memtable_count gauge
elastickv_pebble_memtable_count{group="1",node_address="10.0.0.1:50051",node_id="n1"} 4
# HELP elastickv_pebble_memtable_size_bytes Current bytes allocated by memtables and large flushable batches.
# TYPE elastickv_pebble_memtable_size_bytes gauge
elastickv_pebble_memtable_size_bytes{group="1",node_address="10.0.0.1:50051",node_id="n1"} 8192
# HELP elastickv_pebble_memtable_zombie_count Current count of zombie memtables (no longer referenced by the DB but pinned by open iterators).
# TYPE elastickv_pebble_memtable_zombie_count gauge
elastickv_pebble_memtable_zombie_count{group="1",node_address="10.0.0.1:50051",node_id="n1"} 2
`),
		"elastickv_pebble_l0_sublevels",
		"elastickv_pebble_l0_num_files",
		"elastickv_pebble_compact_estimated_debt_bytes",
		"elastickv_pebble_compact_in_progress",
		"elastickv_pebble_compact_count_total",
		"elastickv_pebble_memtable_count",
		"elastickv_pebble_memtable_size_bytes",
		"elastickv_pebble_memtable_zombie_count",
		"elastickv_pebble_block_cache_size_bytes",
		"elastickv_pebble_block_cache_hits_total",
		"elastickv_pebble_block_cache_misses_total",
	)
	require.NoError(t, err)
}

func TestPebbleCollectorHandlesSourceReset(t *testing.T) {
	// If the underlying DB is replaced (Restore reopens it) the
	// monotonic counters may go DOWN. The collector must not emit
	// negative deltas; instead, it rebases silently.
	registry := NewRegistry("n1", "10.0.0.1:50051")
	collector := registry.PebbleCollector()
	require.NotNil(t, collector)

	src := &fakePebbleSource{}
	sources := []PebbleSource{{GroupID: 7, GroupIDStr: "7", Source: src}}

	src.set(newFakeMetrics(0, 0, 0, 0, 10, 0, 0, 0, 0, 100, 5))
	collector.ObserveOnce(sources) // baseline: 10 compactions, 100 hits

	src.set(newFakeMetrics(0, 0, 0, 0, 3, 0, 0, 0, 0, 20, 5)) // simulated reset
	collector.ObserveOnce(sources)

	src.set(newFakeMetrics(0, 0, 0, 0, 5, 0, 0, 0, 0, 30, 5)) // +2 compactions, +10 hits from post-reset baseline
	collector.ObserveOnce(sources)

	// Expected: baseline (10 / 100) + 0 + post-reset delta (2 / 10).
	err := testutil.GatherAndCompare(
		registry.Gatherer(),
		strings.NewReader(`
# HELP elastickv_pebble_block_cache_hits_total Cumulative block cache hits reported by Pebble.
# TYPE elastickv_pebble_block_cache_hits_total counter
elastickv_pebble_block_cache_hits_total{group="7",node_address="10.0.0.1:50051",node_id="n1"} 110
# HELP elastickv_pebble_compact_count_total Cumulative number of compactions completed by Pebble since the process started.
# TYPE elastickv_pebble_compact_count_total counter
elastickv_pebble_compact_count_total{group="7",node_address="10.0.0.1:50051",node_id="n1"} 12
`),
		"elastickv_pebble_compact_count_total",
		"elastickv_pebble_block_cache_hits_total",
	)
	require.NoError(t, err)
}

func TestPebbleCollectorSkipsNilSnapshot(t *testing.T) {
	// A source that returns nil (store closed mid-restore) must not
	// panic and must not populate any series for that group.
	registry := NewRegistry("n1", "10.0.0.1:50051")
	collector := registry.PebbleCollector()
	require.NotNil(t, collector)

	src := &fakePebbleSource{}
	sources := []PebbleSource{{GroupID: 9, GroupIDStr: "9", Source: src}}

	// Both nil-source and nil-snapshot should be safe.
	require.NotPanics(t, func() { collector.ObserveOnce(sources) })
	require.NotPanics(t, func() { collector.ObserveOnce([]PebbleSource{{GroupID: 1, GroupIDStr: "1", Source: nil}}) })

	// No gauges or counters should exist yet.
	require.Equal(t, 0, testutil.CollectAndCount(registry.pebble.l0Sublevels))
	require.Equal(t, 0, testutil.CollectAndCount(registry.pebble.compactCountTotal))
}

func TestPebbleCollectorEmitsBlockCacheCapacity(t *testing.T) {
	// When the source implements PebbleCacheCapacitySource, the
	// collector mirrors the configured capacity into
	// elastickv_pebble_block_cache_capacity_bytes alongside the current
	// usage gauge. This lets operators see a low hit rate and
	// immediately tell whether the cache is undersized vs simply cold.
	registry := NewRegistry("n1", "10.0.0.1:50051")
	collector := registry.PebbleCollector()
	require.NotNil(t, collector)

	src := &fakePebbleCapacitySource{capacity: 256 << 20}
	src.set(newFakeMetrics(
		0, 0, 0, 0, 0,
		0, 0, 0,
		4096, 0, 0,
	))
	sources := []PebbleSource{{GroupID: 3, GroupIDStr: "3", Source: src}}
	collector.ObserveOnce(sources)

	err := testutil.GatherAndCompare(
		registry.Gatherer(),
		strings.NewReader(`
# HELP elastickv_pebble_block_cache_capacity_bytes Configured maximum size of Pebble's block cache in bytes. Paired with elastickv_pebble_block_cache_size_bytes so operators can see usage relative to capacity and with the hit/miss counters so they can reason about whether a low hit rate reflects a cold cache or an undersized one.
# TYPE elastickv_pebble_block_cache_capacity_bytes gauge
elastickv_pebble_block_cache_capacity_bytes{group="3",node_address="10.0.0.1:50051",node_id="n1"} 2.68435456e+08
# HELP elastickv_pebble_block_cache_size_bytes Current bytes in use by Pebble's block cache.
# TYPE elastickv_pebble_block_cache_size_bytes gauge
elastickv_pebble_block_cache_size_bytes{group="3",node_address="10.0.0.1:50051",node_id="n1"} 4096
`),
		"elastickv_pebble_block_cache_capacity_bytes",
		"elastickv_pebble_block_cache_size_bytes",
	)
	require.NoError(t, err)
}

func TestPebbleCollectorSkipsCapacityWhenUnsupported(t *testing.T) {
	// A PebbleMetricsSource that does NOT additionally implement
	// PebbleCacheCapacitySource must not cause the capacity gauge to be
	// populated for its group. This preserves backward compatibility
	// with sources that pre-date the capacity interface.
	registry := NewRegistry("n1", "10.0.0.1:50051")
	collector := registry.PebbleCollector()
	require.NotNil(t, collector)

	src := &fakePebbleSource{}
	src.set(newFakeMetrics(0, 0, 0, 0, 0, 0, 0, 0, 1024, 0, 0))
	collector.ObserveOnce([]PebbleSource{{GroupID: 4, GroupIDStr: "4", Source: src}})

	// The capacity vector should remain empty (no label sets observed).
	require.Equal(t, 0, testutil.CollectAndCount(registry.pebble.blockCacheCapacityBytes))
}

func TestPebbleCollectorZeroRegistryIsSafe(t *testing.T) {
	// Code paths that bypass the registry (tests, bootstrap helpers)
	// must tolerate a nil collector / empty sources without panicking.
	var c *PebbleCollector
	require.NotPanics(t, func() { c.ObserveOnce(nil) })

	registry := NewRegistry("n1", "10.0.0.1:50051")
	collector := registry.PebbleCollector()
	require.NotPanics(t, func() { collector.ObserveOnce(nil) })
}
