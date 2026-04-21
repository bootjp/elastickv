package monitoring

import (
	"strings"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestLeaseReadObserverHitsAndMisses(t *testing.T) {
	registry := NewRegistry("n1", "10.0.0.1:50051")
	observer := registry.LeaseReadObserver()

	observer.ObserveLeaseRead(true)
	observer.ObserveLeaseRead(true)
	observer.ObserveLeaseRead(false)

	err := testutil.GatherAndCompare(
		registry.Gatherer(),
		strings.NewReader(`
# HELP elastickv_lease_read_total Lease-read outcomes from the kv Coordinator (hit = served from local AppliedIndex, miss = fell back to LinearizableRead).
# TYPE elastickv_lease_read_total counter
elastickv_lease_read_total{node_address="10.0.0.1:50051",node_id="n1",outcome="hit"} 2
elastickv_lease_read_total{node_address="10.0.0.1:50051",node_id="n1",outcome="miss"} 1
`),
		"elastickv_lease_read_total",
	)
	require.NoError(t, err)
}

func TestLuaFastPathObserverCountsByCmdAndOutcome(t *testing.T) {
	registry := NewRegistry("n1", "10.0.0.1:50051")
	cmd := registry.LuaFastPathObserver().ForCommand("zrangebyscore")

	cmd.ObserveHit()
	cmd.ObserveHit()
	cmd.ObserveSkipLoaded()
	cmd.ObserveFallback(LuaFastPathFallbackMissingKey)
	cmd.ObserveFallback(LuaFastPathFallbackTruncated)
	cmd.ObserveFallback("bogus-reason") // routes to fallback_other

	err := testutil.GatherAndCompare(
		registry.Gatherer(),
		strings.NewReader(`
# HELP elastickv_lua_cmd_fastpath_total Per-redis.call() fast-path outcome inside Lua scripts. cmd identifies the command (zrangebyscore, zscore, ...); outcome is hit, skip_loaded, skip_cached_type, or fallback_* (subdivided by reason: ineligible, missing_key, wrong_type, truncated, large_offset, other).
# TYPE elastickv_lua_cmd_fastpath_total counter
elastickv_lua_cmd_fastpath_total{cmd="zrangebyscore",node_address="10.0.0.1:50051",node_id="n1",outcome="fallback_ineligible"} 0
elastickv_lua_cmd_fastpath_total{cmd="zrangebyscore",node_address="10.0.0.1:50051",node_id="n1",outcome="fallback_large_offset"} 0
elastickv_lua_cmd_fastpath_total{cmd="zrangebyscore",node_address="10.0.0.1:50051",node_id="n1",outcome="fallback_missing_key"} 1
elastickv_lua_cmd_fastpath_total{cmd="zrangebyscore",node_address="10.0.0.1:50051",node_id="n1",outcome="fallback_other"} 1
elastickv_lua_cmd_fastpath_total{cmd="zrangebyscore",node_address="10.0.0.1:50051",node_id="n1",outcome="fallback_truncated"} 1
elastickv_lua_cmd_fastpath_total{cmd="zrangebyscore",node_address="10.0.0.1:50051",node_id="n1",outcome="fallback_wrong_type"} 0
elastickv_lua_cmd_fastpath_total{cmd="zrangebyscore",node_address="10.0.0.1:50051",node_id="n1",outcome="hit"} 2
elastickv_lua_cmd_fastpath_total{cmd="zrangebyscore",node_address="10.0.0.1:50051",node_id="n1",outcome="skip_cached_type"} 0
elastickv_lua_cmd_fastpath_total{cmd="zrangebyscore",node_address="10.0.0.1:50051",node_id="n1",outcome="skip_loaded"} 1
`),
		"elastickv_lua_cmd_fastpath_total",
	)
	require.NoError(t, err)
}

func TestLuaFastPathObserverZeroValueIsNoop(t *testing.T) {
	var observer LuaFastPathObserver
	cmd := observer.ForCommand("zrangebyscore")
	require.NotPanics(t, func() {
		cmd.ObserveHit()
		cmd.ObserveSkipLoaded()
		cmd.ObserveSkipCachedType()
		cmd.ObserveFallback(LuaFastPathFallbackMissingKey)
		cmd.ObserveFallback("")
	})
}

func TestLeaseReadObserverZeroValueIsNoop(t *testing.T) {
	// LeaseReadObserver{} is documented as safe; the Coordinator
	// falls back to this when monitoring is disabled. Calling
	// ObserveLeaseRead must not panic.
	var observer LeaseReadObserver
	require.NotPanics(t, func() {
		observer.ObserveLeaseRead(true)
		observer.ObserveLeaseRead(false)
	})
}

// fakeDispatchSource implements DispatchCounterSource on atomic
// uint64s so tests can advance counters without touching the etcd
// engine directly.
type fakeDispatchSource struct {
	drops     atomic.Uint64
	errors    atomic.Uint64
	stepFulls atomic.Uint64
}

func (f *fakeDispatchSource) DispatchDropCount() uint64  { return f.drops.Load() }
func (f *fakeDispatchSource) DispatchErrorCount() uint64 { return f.errors.Load() }
func (f *fakeDispatchSource) StepQueueFullCount() uint64 { return f.stepFulls.Load() }

func TestDispatchCollectorMirrorsDeltas(t *testing.T) {
	registry := NewRegistry("n1", "10.0.0.1:50051")
	collector := registry.DispatchCollector()
	require.NotNil(t, collector)

	src := &fakeDispatchSource{}
	sources := []DispatchSource{{GroupID: 1, Source: src}}

	// First pass initialises the delta baseline.
	collector.ObserveOnce(sources)

	src.drops.Store(3)
	src.errors.Store(2)
	src.stepFulls.Store(1)
	collector.ObserveOnce(sources)

	// A second pass with no change must NOT double-count.
	collector.ObserveOnce(sources)

	err := testutil.GatherAndCompare(
		registry.Gatherer(),
		strings.NewReader(`
# HELP elastickv_raft_dispatch_dropped_total Outbound raft messages dropped before transport because the per-peer channel was full. Mirrors etcd raft Engine.dispatchDropCount.
# TYPE elastickv_raft_dispatch_dropped_total counter
elastickv_raft_dispatch_dropped_total{group="1",node_address="10.0.0.1:50051",node_id="n1"} 3
# HELP elastickv_raft_dispatch_errors_total Outbound raft dispatches that reached the transport but failed. Mirrors etcd raft Engine.dispatchErrorCount.
# TYPE elastickv_raft_dispatch_errors_total counter
elastickv_raft_dispatch_errors_total{group="1",node_address="10.0.0.1:50051",node_id="n1"} 2
# HELP elastickv_raft_step_queue_full_total Inbound raft messages that could not be enqueued because stepCh was full; indicates the raft loop is starved (classic pre-#560 seek-storm symptom).
# TYPE elastickv_raft_step_queue_full_total counter
elastickv_raft_step_queue_full_total{group="1",node_address="10.0.0.1:50051",node_id="n1"} 1
`),
		"elastickv_raft_dispatch_dropped_total",
		"elastickv_raft_dispatch_errors_total",
		"elastickv_raft_step_queue_full_total",
	)
	require.NoError(t, err)
}

func TestDispatchCollectorHandlesSourceReset(t *testing.T) {
	// If the engine's counter is replaced (e.g. a test reopens it)
	// the snapshot may go DOWN. The collector must not emit negative
	// deltas; instead, it rebases silently.
	registry := NewRegistry("n1", "10.0.0.1:50051")
	collector := registry.DispatchCollector()

	src := &fakeDispatchSource{}
	sources := []DispatchSource{{GroupID: 7, Source: src}}

	src.drops.Store(10)
	collector.ObserveOnce(sources) // mirrors initial 10

	src.drops.Store(4) // simulated reset: MUST NOT emit -6
	collector.ObserveOnce(sources)

	src.drops.Store(6) // +2 from the post-reset baseline
	collector.ObserveOnce(sources)

	// Expected: 10 (initial) + 0 (no negative) + 2 (post-reset delta) = 12.
	err := testutil.GatherAndCompare(
		registry.Gatherer(),
		strings.NewReader(`
# HELP elastickv_raft_dispatch_dropped_total Outbound raft messages dropped before transport because the per-peer channel was full. Mirrors etcd raft Engine.dispatchDropCount.
# TYPE elastickv_raft_dispatch_dropped_total counter
elastickv_raft_dispatch_dropped_total{group="7",node_address="10.0.0.1:50051",node_id="n1"} 12
`),
		"elastickv_raft_dispatch_dropped_total",
	)
	require.NoError(t, err)
}
