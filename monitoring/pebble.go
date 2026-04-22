package monitoring

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/prometheus/client_golang/prometheus"
)

// Pebble LSM metrics. These mirror the most operationally useful
// fields from *pebble.DB.Metrics() so operators can graph/alert on
// write-stall signals (L0 sublevels, compaction debt) and capacity
// trends (memtable, block cache) without importing Pebble from every
// dashboard.
//
// The point-in-time fields (Sublevels, NumFiles, EstimatedDebt,
// MemTable.*, NumInProgress, BlockCache.Size/Count) are exposed as
// Prometheus GAUGES — each poll overwrites the previous value.
// Monotonic fields (Compact.Count, BlockCache.Hits/Misses) are exposed
// as COUNTERS; the collector emits only the positive delta against the
// last snapshot so a store reset (Restore/swap) does not produce
// negative values.
//
// Name convention: elastickv_pebble_* to keep a consistent node_id /
// node_address label prefix with the rest of the registry.

const defaultPebblePollInterval = 5 * time.Second

// PebbleMetrics owns the Prometheus vectors for Pebble LSM internals.
// One instance per registry; shared by all groups (labelled by group
// ID + level where relevant).
type PebbleMetrics struct {
	// L0 pressure: incident signals.
	l0Sublevels *prometheus.GaugeVec
	l0NumFiles  *prometheus.GaugeVec

	// Compaction queue depth / debt.
	compactEstimatedDebt *prometheus.GaugeVec
	compactInProgress    *prometheus.GaugeVec
	compactCountTotal    *prometheus.CounterVec

	// Memtable footprint.
	memtableCount       *prometheus.GaugeVec
	memtableSizeBytes   *prometheus.GaugeVec
	memtableZombieCount *prometheus.GaugeVec

	// Block cache.
	blockCacheSizeBytes *prometheus.GaugeVec
	blockCacheHitsTotal *prometheus.CounterVec
	blockCacheMissTotal *prometheus.CounterVec
}

func newPebbleMetrics(registerer prometheus.Registerer) *PebbleMetrics {
	m := &PebbleMetrics{
		l0Sublevels: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "elastickv_pebble_l0_sublevels",
				Help: "Current L0 sublevel count reported by Pebble. Climbing sublevels are the canonical precursor to a write stall; alert when this exceeds the L0CompactionThreshold for a sustained period.",
			},
			[]string{"group"},
		),
		l0NumFiles: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "elastickv_pebble_l0_num_files",
				Help: "Current number of sstables in L0 reported by Pebble. Paired with elastickv_pebble_l0_sublevels to diagnose L0 pressure.",
			},
			[]string{"group"},
		),
		compactEstimatedDebt: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "elastickv_pebble_compact_estimated_debt_bytes",
				Help: "Estimated number of bytes Pebble still needs to compact for the LSM to reach a stable state. Growth indicates compactions are falling behind ingest.",
			},
			[]string{"group"},
		),
		compactInProgress: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "elastickv_pebble_compact_in_progress",
				Help: "Number of compactions currently in progress.",
			},
			[]string{"group"},
		),
		compactCountTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_pebble_compact_count_total",
				Help: "Cumulative number of compactions completed by Pebble since the process started.",
			},
			[]string{"group"},
		),
		memtableCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "elastickv_pebble_memtable_count",
				Help: "Current count of memtables (active + queued for flush).",
			},
			[]string{"group"},
		),
		memtableSizeBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "elastickv_pebble_memtable_size_bytes",
				Help: "Current bytes allocated by memtables and large flushable batches.",
			},
			[]string{"group"},
		),
		memtableZombieCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "elastickv_pebble_memtable_zombie_count",
				Help: "Current count of zombie memtables (no longer referenced by the DB but pinned by open iterators).",
			},
			[]string{"group"},
		),
		blockCacheSizeBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "elastickv_pebble_block_cache_size_bytes",
				Help: "Current bytes in use by Pebble's block cache.",
			},
			[]string{"group"},
		),
		blockCacheHitsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_pebble_block_cache_hits_total",
				Help: "Cumulative block cache hits reported by Pebble.",
			},
			[]string{"group"},
		),
		blockCacheMissTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_pebble_block_cache_misses_total",
				Help: "Cumulative block cache misses reported by Pebble.",
			},
			[]string{"group"},
		),
	}

	registerer.MustRegister(
		m.l0Sublevels,
		m.l0NumFiles,
		m.compactEstimatedDebt,
		m.compactInProgress,
		m.compactCountTotal,
		m.memtableCount,
		m.memtableSizeBytes,
		m.memtableZombieCount,
		m.blockCacheSizeBytes,
		m.blockCacheHitsTotal,
		m.blockCacheMissTotal,
	)
	return m
}

// PebbleMetricsSource abstracts the per-group access to a Pebble DB's
// Metrics(). The concrete *store pebbleStore satisfies this via its
// Metrics() accessor. Returning nil (e.g. store closed mid-restore) is
// allowed; the collector will skip that group for the tick.
type PebbleMetricsSource interface {
	Metrics() *pebble.Metrics
}

// PebbleSource binds a raft group ID to its Pebble store. Multiple
// groups can be polled by a single collector on a sharded node.
// GroupIDStr is the pre-formatted decimal form of GroupID used as the
// "group" Prometheus label; pre-computing it avoids a per-tick
// strconv.FormatUint allocation in observeOnce.
type PebbleSource struct {
	GroupID    uint64
	GroupIDStr string
	Source     PebbleMetricsSource
}

// PebbleCollector polls each registered Pebble store on a fixed
// interval and mirrors the snapshot into the Prometheus vectors.
// Gauges are overwritten; counters advance by the positive delta
// against the previous snapshot.
type PebbleCollector struct {
	metrics *PebbleMetrics

	mu       sync.Mutex
	previous map[uint64]pebbleSnapshot
}

type pebbleSnapshot struct {
	compactCount     int64
	blockCacheHits   int64
	blockCacheMisses int64
}

func newPebbleCollector(metrics *PebbleMetrics) *PebbleCollector {
	return &PebbleCollector{
		metrics:  metrics,
		previous: map[uint64]pebbleSnapshot{},
	}
}

// Start begins polling sources on interval until ctx is canceled.
// Passing interval <= 0 uses defaultPebblePollInterval (5 s), matching
// the DispatchCollector cadence so operators see consistent refresh
// rates across dashboards. Pebble.Metrics() acquires internal mutexes
// but is not expensive; 5 s gives ample headroom.
func (c *PebbleCollector) Start(ctx context.Context, sources []PebbleSource, interval time.Duration) {
	if c == nil || c.metrics == nil || len(sources) == 0 {
		return
	}
	if interval <= 0 {
		interval = defaultPebblePollInterval
	}
	c.observeOnce(sources)
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.observeOnce(sources)
			}
		}
	}()
}

// ObserveOnce is exposed for tests and single-shot callers.
func (c *PebbleCollector) ObserveOnce(sources []PebbleSource) {
	c.observeOnce(sources)
}

func (c *PebbleCollector) observeOnce(sources []PebbleSource) {
	if c == nil || c.metrics == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, src := range sources {
		if src.Source == nil {
			continue
		}
		snap := src.Source.Metrics()
		if snap == nil {
			continue
		}
		group := src.GroupIDStr

		// L0 pressure: gauges, overwritten each tick.
		c.metrics.l0Sublevels.WithLabelValues(group).Set(float64(snap.Levels[0].Sublevels))
		c.metrics.l0NumFiles.WithLabelValues(group).Set(float64(snap.Levels[0].TablesCount))

		// Compaction.
		c.metrics.compactEstimatedDebt.WithLabelValues(group).Set(float64(snap.Compact.EstimatedDebt))
		c.metrics.compactInProgress.WithLabelValues(group).Set(float64(snap.Compact.NumInProgress))

		// Memtable.
		c.metrics.memtableCount.WithLabelValues(group).Set(float64(snap.MemTable.Count))
		c.metrics.memtableSizeBytes.WithLabelValues(group).Set(float64(snap.MemTable.Size))
		c.metrics.memtableZombieCount.WithLabelValues(group).Set(float64(snap.MemTable.ZombieCount))

		// Block cache gauge.
		c.metrics.blockCacheSizeBytes.WithLabelValues(group).Set(float64(snap.BlockCache.Size))

		// Monotonic counters: emit only the positive delta. A smaller
		// value means the source was reset (store reopened); rebase
		// silently without emitting negative.
		prev := c.previous[src.GroupID]
		curr := pebbleSnapshot{
			compactCount:     snap.Compact.Count,
			blockCacheHits:   snap.BlockCache.Hits,
			blockCacheMisses: snap.BlockCache.Misses,
		}
		if curr.compactCount > prev.compactCount {
			c.metrics.compactCountTotal.WithLabelValues(group).Add(float64(curr.compactCount - prev.compactCount))
		}
		if curr.blockCacheHits > prev.blockCacheHits {
			c.metrics.blockCacheHitsTotal.WithLabelValues(group).Add(float64(curr.blockCacheHits - prev.blockCacheHits))
		}
		if curr.blockCacheMisses > prev.blockCacheMisses {
			c.metrics.blockCacheMissTotal.WithLabelValues(group).Add(float64(curr.blockCacheMisses - prev.blockCacheMisses))
		}
		c.previous[src.GroupID] = curr
	}
}
