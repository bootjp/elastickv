package monitoring

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Write-conflict metrics expose the MVCC store layer's OCC conflict
// signal. Conflicts are detected inside the store's ApplyMutations —
// both write-write (mutation vs newer committed version) and
// read-write (read set vs newer committed version) — and attributed
// by a bounded key-prefix classification (txn_lock, redis_string,
// zset, ...). The metric is protocol-independent: whether the
// consumer is Redis, DynamoDB, or raw KV, a conflict landing here is
// the same underlying event.
//
// This complements the proxy-side view added in #585: the proxy
// counter tells you how many client requests observed a conflict,
// but a single client request can map to many Raft proposals (e.g.
// a DynamoDB TransactWriteItems across several items). The store
// counter is the authoritative per-proposal count and the right
// signal for capacity/alerting.

const defaultWriteConflictPollInterval = 5 * time.Second

// WriteConflictMetrics owns the per-(group, kind, key_prefix) counter
// vector used by the write-conflict dashboard. Registered once per
// Registry.
type WriteConflictMetrics struct {
	writeConflictTotal *prometheus.CounterVec
}

func newWriteConflictMetrics(registerer prometheus.Registerer) *WriteConflictMetrics {
	m := &WriteConflictMetrics{
		writeConflictTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_store_write_conflict_total",
				Help: "OCC write conflicts detected at the MVCC store layer, bucketed by conflicting key-prefix class and conflict kind (read=read-write, write=write-write). Rate shows cluster-wide conflict pressure; sharp increases on specific prefixes point at hot keys or lock-resolver races (e.g. txn_rollback for the PR #581 incident pattern).",
			},
			[]string{"group", "kind", "key_prefix"},
		),
	}
	registerer.MustRegister(m.writeConflictTotal)
	return m
}

// WriteConflictCounterSource abstracts per-group access to the MVCC
// store's OCC conflict counters. The concrete store implementations
// (pebbleStore, mvccStore, ShardStore, LeaderRoutedStore) satisfy
// this via WriteConflictCountsByPrefix(); keys in the returned map
// follow the "<kind>|<key_prefix>" encoding from the store package.
type WriteConflictCounterSource interface {
	WriteConflictCountsByPrefix() map[string]uint64
}

// WriteConflictSource binds a raft group ID to a counter source.
// Multiple groups can be polled by a single collector on a sharded
// node. GroupIDStr is the pre-formatted decimal form of GroupID used
// as the "group" Prometheus label; pre-computing it avoids a
// per-tick strconv allocation.
type WriteConflictSource struct {
	GroupID    uint64
	GroupIDStr string
	Source     WriteConflictCounterSource
}

// WriteConflictCollector polls each registered store on a fixed
// interval and mirrors the snapshot into the Prometheus counter
// vector. Store-side counts are monotonic for the lifetime of a
// single store instance; counters advance by the positive delta
// against the last snapshot so a store reopen (Restore swap) does
// not produce negative values.
type WriteConflictCollector struct {
	metrics *WriteConflictMetrics

	mu       sync.Mutex
	previous map[uint64]map[string]uint64
}

func newWriteConflictCollector(metrics *WriteConflictMetrics) *WriteConflictCollector {
	return &WriteConflictCollector{
		metrics:  metrics,
		previous: map[uint64]map[string]uint64{},
	}
}

// Start polls sources on the given interval until ctx is canceled.
// Passing interval <= 0 uses defaultWriteConflictPollInterval (5 s),
// matching the DispatchCollector / PebbleCollector cadence.
func (c *WriteConflictCollector) Start(ctx context.Context, sources []WriteConflictSource, interval time.Duration) {
	if c == nil || c.metrics == nil || len(sources) == 0 {
		return
	}
	if interval <= 0 {
		interval = defaultWriteConflictPollInterval
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
func (c *WriteConflictCollector) ObserveOnce(sources []WriteConflictSource) {
	c.observeOnce(sources)
}

func (c *WriteConflictCollector) observeOnce(sources []WriteConflictSource) {
	if c == nil || c.metrics == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, src := range sources {
		if src.Source == nil {
			continue
		}
		curr := src.Source.WriteConflictCountsByPrefix()
		prev := c.previous[src.GroupID]
		for label, count := range curr {
			prevCount := prev[label]
			if count <= prevCount {
				// Counter reset (store reopen) or no change:
				// do not emit. prev is replaced below, which
				// rebases the delta baseline silently.
				continue
			}
			kind, keyPrefix, ok := splitWriteConflictLabel(label)
			if !ok {
				continue
			}
			c.metrics.writeConflictTotal.
				WithLabelValues(src.GroupIDStr, kind, keyPrefix).
				Add(float64(count - prevCount))
		}
		// Copy curr into previous so future ticks use the latest
		// snapshot as baseline even if the source happens to reset.
		snap := make(map[string]uint64, len(curr))
		for k, v := range curr {
			snap[k] = v
		}
		c.previous[src.GroupID] = snap
	}
}

// splitWriteConflictLabel mirrors store.DecodeWriteConflictLabel
// without importing the store package (which would pull pebble into
// monitoring). The encoding is stable: "<kind>|<key_prefix>".
func splitWriteConflictLabel(label string) (kind, keyPrefix string, ok bool) {
	idx := strings.IndexByte(label, '|')
	if idx < 0 {
		return "", "", false
	}
	return label[:idx], label[idx+1:], true
}
