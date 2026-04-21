package monitoring

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Hot-path metrics support the "Redis Hot Path" dashboard
// (monitoring/grafana/dashboards/elastickv-redis-hotpath.json). They
// were added to confirm PR #560 (GET fast-path) landed in production:
// the LinearizableRead call rate should drop sharply on a
// string-dominated workload while GET p99 stays flat or improves, and
// the lease-hit ratio approaches 1.0 once leases are steady.
//
// Names follow the existing elastickv_* prefix convention. The
// metrics defined in this file are all monotonic counters.

const (
	leaseReadOutcomeHit  = "hit"
	leaseReadOutcomeMiss = "miss"

	defaultDispatchPollInterval = 5 * time.Second
)

// HotPathMetrics owns the Prometheus vectors introduced for the Redis
// GET hot-path dashboard. Kept in its own type so the Registry can hold
// a single instance and hand out scoped observer/collector objects.
type HotPathMetrics struct {
	leaseReadsTotal      *prometheus.CounterVec
	dispatchDroppedTotal *prometheus.CounterVec
	dispatchErrorsTotal  *prometheus.CounterVec
	stepQueueFullTotal   *prometheus.CounterVec
	luaFastPathTotal     *prometheus.CounterVec
}

// LuaFastPathOutcome labels tag each Lua-side read fast-path decision
// so operators can see how often a given command (ZRANGEBYSCORE,
// ZSCORE, HGET, etc.) actually takes the fast path vs falls back.
const (
	LuaFastPathOutcomeHit             = "hit"
	LuaFastPathOutcomeSkipLoaded = "skip_loaded"
	LuaFastPathOutcomeSkipCachedType  = "skip_cached_type"
	LuaFastPathOutcomeFallback        = "fallback"
)

func newHotPathMetrics(registerer prometheus.Registerer) *HotPathMetrics {
	m := &HotPathMetrics{
		leaseReadsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_lease_read_total",
				Help: "Lease-read outcomes from the kv Coordinator (hit = served from local AppliedIndex, miss = fell back to LinearizableRead).",
			},
			[]string{"outcome"},
		),
		dispatchDroppedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_raft_dispatch_dropped_total",
				Help: "Outbound raft messages dropped before transport because the per-peer channel was full. Mirrors etcd raft Engine.dispatchDropCount.",
			},
			[]string{"group"},
		),
		dispatchErrorsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_raft_dispatch_errors_total",
				Help: "Outbound raft dispatches that reached the transport but failed. Mirrors etcd raft Engine.dispatchErrorCount.",
			},
			[]string{"group"},
		),
		stepQueueFullTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_raft_step_queue_full_total",
				Help: "Inbound raft messages that could not be enqueued because stepCh was full; indicates the raft loop is starved (classic pre-#560 seek-storm symptom).",
			},
			[]string{"group"},
		),
		luaFastPathTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_lua_cmd_fastpath_total",
				Help: "Per-redis.call() fast-path outcome inside Lua scripts. cmd identifies the command (zrangebyscore, zscore, ...); outcome is hit, skip_loaded, skip_cached_type, or fallback.",
			},
			[]string{"cmd", "outcome"},
		),
	}

	registerer.MustRegister(
		m.leaseReadsTotal,
		m.dispatchDroppedTotal,
		m.dispatchErrorsTotal,
		m.stepQueueFullTotal,
		m.luaFastPathTotal,
	)
	return m
}

// LuaFastPathObserver records fast-path outcomes for redis.call()
// inside Lua scripts. The zero value is safe and silently drops
// samples so tests can pass LuaFastPathObserver{} as a stub.
//
// Hot-path shape: each Observe* call on a LuaFastPathCmd handle is a
// single non-blocking atomic increment on a pre-resolved
// prometheus.Counter (client_golang's default Counter uses
// sync/atomic internally). Callers resolve one LuaFastPathCmd per
// command at server construction to avoid
// CounterVec.WithLabelValues (mutex-guarded map lookup) on the hot
// path.
type LuaFastPathObserver struct {
	metrics *HotPathMetrics
}

// LuaFastPathCmd is a pre-resolved bundle of fast-path outcome
// counters for a single Lua command. Construct once via
// LuaFastPathObserver.ForCommand(cmd) at server startup; call the
// Observe* methods per redis.call(). Safe to copy.
type LuaFastPathCmd struct {
	hit             prometheus.Counter
	skipLoaded prometheus.Counter
	skipCachedType  prometheus.Counter
	fallback        prometheus.Counter
}

// ForCommand pre-resolves the counter handles for cmd. Returns a
// zero-value LuaFastPathCmd when the observer is empty (tests),
// which silently drops all Observe* calls.
func (o LuaFastPathObserver) ForCommand(cmd string) LuaFastPathCmd {
	if o.metrics == nil {
		return LuaFastPathCmd{}
	}
	vec := o.metrics.luaFastPathTotal
	return LuaFastPathCmd{
		hit:             vec.WithLabelValues(cmd, LuaFastPathOutcomeHit),
		skipLoaded: vec.WithLabelValues(cmd, LuaFastPathOutcomeSkipLoaded),
		skipCachedType:  vec.WithLabelValues(cmd, LuaFastPathOutcomeSkipCachedType),
		fallback:        vec.WithLabelValues(cmd, LuaFastPathOutcomeFallback),
	}
}

// ObserveHit / ObserveSkipLoaded / ObserveSkipCachedType /
// ObserveFallback record one outcome. Each is a single atomic
// increment when the counter is wired; a no-op on the zero value.
func (c LuaFastPathCmd) ObserveHit() {
	if c.hit != nil {
		c.hit.Inc()
	}
}

func (c LuaFastPathCmd) ObserveSkipLoaded() {
	if c.skipLoaded != nil {
		c.skipLoaded.Inc()
	}
}

func (c LuaFastPathCmd) ObserveSkipCachedType() {
	if c.skipCachedType != nil {
		c.skipCachedType.Inc()
	}
}

func (c LuaFastPathCmd) ObserveFallback() {
	if c.fallback != nil {
		c.fallback.Inc()
	}
}

// LeaseReadObserver implements kv.LeaseReadObserver by incrementing the
// elastickv_lease_read_total counter vector. Callers grab an instance
// via Registry.LeaseReadObserver(); the zero value is safe and silently
// drops samples, so tests can pass LeaseReadObserver{} as a stub.
type LeaseReadObserver struct {
	metrics *HotPathMetrics
}

// ObserveLeaseRead records a single lease-read outcome.
func (o LeaseReadObserver) ObserveLeaseRead(hit bool) {
	if o.metrics == nil {
		return
	}
	outcome := leaseReadOutcomeMiss
	if hit {
		outcome = leaseReadOutcomeHit
	}
	o.metrics.leaseReadsTotal.WithLabelValues(outcome).Inc()
}

// DispatchCounterSource abstracts the etcd raft Engine's monotonic
// dispatch counters so monitoring can scrape them without importing
// the etcd package. The concrete etcd Engine satisfies this interface
// via its DispatchDropCount / DispatchErrorCount / StepQueueFullCount
// accessors.
type DispatchCounterSource interface {
	DispatchDropCount() uint64
	DispatchErrorCount() uint64
	StepQueueFullCount() uint64
}

// DispatchSource binds a raft group ID to its counter source. Multiple
// groups can be polled by a single collector on a sharded node.
type DispatchSource struct {
	GroupID uint64
	Source  DispatchCounterSource
}

// DispatchCollector polls the etcd raft Engine's atomic dispatch
// counters on a fixed interval and mirrors them into monotonic
// Prometheus counters. We poll rather than calling Add() inline in the
// raft path because those code paths are already hot and must not take
// any additional interface call; the counters are atomic.Uint64 in the
// engine and polling is cheap (O(groups) reads every 5s).
type DispatchCollector struct {
	metrics *HotPathMetrics

	mu       sync.Mutex
	previous map[uint64]dispatchSnapshot
}

type dispatchSnapshot struct {
	drops     uint64
	errors    uint64
	stepFulls uint64
}

func newDispatchCollector(metrics *HotPathMetrics) *DispatchCollector {
	return &DispatchCollector{
		metrics:  metrics,
		previous: map[uint64]dispatchSnapshot{},
	}
}

// Start polls sources on the given interval until ctx is canceled.
// Passing interval <= 0 uses defaultDispatchPollInterval (5 s), which
// matches the cadence of RaftObserver so operators see consistent
// refresh rates across dashboards.
func (c *DispatchCollector) Start(ctx context.Context, sources []DispatchSource, interval time.Duration) {
	if c == nil || c.metrics == nil || len(sources) == 0 {
		return
	}
	if interval <= 0 {
		interval = defaultDispatchPollInterval
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
func (c *DispatchCollector) ObserveOnce(sources []DispatchSource) {
	c.observeOnce(sources)
}

func (c *DispatchCollector) observeOnce(sources []DispatchSource) {
	if c == nil || c.metrics == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, src := range sources {
		if src.Source == nil {
			continue
		}
		curr := dispatchSnapshot{
			drops:     src.Source.DispatchDropCount(),
			errors:    src.Source.DispatchErrorCount(),
			stepFulls: src.Source.StepQueueFullCount(),
		}
		prev := c.previous[src.GroupID]
		group := strconv.FormatUint(src.GroupID, 10)
		// The engine's counters are monotonic; still, guard against
		// wraparound / replacement of the underlying engine (e.g. a
		// test reopens it) by only advancing the Prometheus counter
		// when the current value is strictly greater than the last
		// snapshot. A smaller value means the source was reset and
		// we restart the delta baseline without emitting negative.
		if curr.drops > prev.drops {
			c.metrics.dispatchDroppedTotal.WithLabelValues(group).Add(float64(curr.drops - prev.drops))
		}
		if curr.errors > prev.errors {
			c.metrics.dispatchErrorsTotal.WithLabelValues(group).Add(float64(curr.errors - prev.errors))
		}
		if curr.stepFulls > prev.stepFulls {
			c.metrics.stepQueueFullTotal.WithLabelValues(group).Add(float64(curr.stepFulls - prev.stepFulls))
		}
		c.previous[src.GroupID] = curr
	}
}
