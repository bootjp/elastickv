package monitoring

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// fakeHLCSource lets the HLCObserver tests fix PhysicalCeiling and
// NextFencedRejections to specific values without spinning up a real
// HLC or wiring it to a Raft group.  Mirrors the test-shim pattern
// in monitoring/raft_test.go.
type fakeHLCSource struct {
	ceiling    int64
	rejections uint64
}

func (f *fakeHLCSource) PhysicalCeiling() int64       { return f.ceiling }
func (f *fakeHLCSource) NextFencedRejections() uint64 { return f.rejections }

func TestHLCObserveOnce_NegativeSkewIsHealthy(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	metrics := newHLCMetrics(reg)
	obs := newHLCObserver(metrics)

	// Ceiling 5 s in the future → skew is negative.
	future := time.Now().Add(5 * time.Second).UnixMilli()
	obs.ObserveOnce(&fakeHLCSource{ceiling: future})

	require.Less(t, testutil.ToFloat64(metrics.wallSkewSeconds), 0.0,
		"a future ceiling must produce a negative wallSkewSeconds gauge — that's the healthy case")
	require.Greater(t, testutil.ToFloat64(metrics.physicalCeilingSeconds), 0.0)
}

func TestHLCObserveOnce_PositiveSkewMeansExpired(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	metrics := newHLCMetrics(reg)
	obs := newHLCObserver(metrics)

	// Ceiling 5 s in the past → skew is positive, meaning the fence
	// would refuse to issue.
	past := time.Now().Add(-5 * time.Second).UnixMilli()
	obs.ObserveOnce(&fakeHLCSource{ceiling: past})

	require.Greater(t, testutil.ToFloat64(metrics.wallSkewSeconds), 0.0,
		"a past ceiling must produce a positive wallSkewSeconds gauge — that's the alerting case")
}

func TestHLCObserveOnce_RejectionDeltasReportAsCounterIncrements(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	metrics := newHLCMetrics(reg)
	obs := newHLCObserver(metrics)

	src := &fakeHLCSource{ceiling: time.Now().UnixMilli() + 1000}

	// First tick: source reports 5 rejections; counter goes from 0 → 5.
	src.rejections = 5
	obs.ObserveOnce(src)
	require.InDelta(t, 5.0, testutil.ToFloat64(metrics.nextFencedRejectionsTotal), 0.0001)

	// Second tick: source reports 8; counter should be 8 total
	// (delta +3 added to the existing 5), not double-counted as 13.
	src.rejections = 8
	obs.ObserveOnce(src)
	require.InDelta(t, 8.0, testutil.ToFloat64(metrics.nextFencedRejectionsTotal), 0.0001)

	// Source went backwards (e.g. process restart with shared registry —
	// not expected in production but defensive). Counter must NOT
	// decrement; it just freezes until the source catches up.
	src.rejections = 2
	obs.ObserveOnce(src)
	require.InDelta(t, 8.0, testutil.ToFloat64(metrics.nextFencedRejectionsTotal), 0.0001)
}

func TestHLCObserver_NilReceiverIsNoop(t *testing.T) {
	t.Parallel()

	var obs *HLCObserver
	require.NotPanics(t, func() {
		obs.ObserveOnce(&fakeHLCSource{})
	})
}

func TestHLCObserver_NilSourceIsNoop(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	obs := newHLCObserver(newHLCMetrics(reg))
	require.NotPanics(t, func() {
		obs.ObserveOnce(nil)
	})
}

// TestHLCObserveOnce_PreBootstrapHoldsSkewAtZero verifies that the
// skew gauge stays at 0 when PhysicalCeiling() == 0 (pre-bootstrap,
// before the first HLC lease lands).  Without this guard the
// observer would publish wallSkewSeconds ≈ 1.75e9 (~55 years) and
// trip the recommended `wallSkewSeconds > -0.5` alert on every cold
// start until the first lease propagates.  Regression for the medium
// finding on PR #879.
func TestHLCObserveOnce_PreBootstrapHoldsSkewAtZero(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	metrics := newHLCMetrics(reg)
	obs := newHLCObserver(metrics)

	obs.ObserveOnce(&fakeHLCSource{ceiling: 0})

	require.InDelta(t, 0.0, testutil.ToFloat64(metrics.wallSkewSeconds), 0.0001,
		"pre-bootstrap (ceiling == 0) must hold wallSkewSeconds at 0; otherwise the gauge reports a 55-year skew and trips the recommended alert on every cold start")
	require.InDelta(t, 0.0, testutil.ToFloat64(metrics.physicalCeilingSeconds), 0.0001)
}

// TestRegistryHLCObserverReturnsSameInstance is the regression for
// the low finding on PR #879: HLCObserver() previously returned a
// fresh observer on every call, which would reset lastRejections and
// double-count rejections against the cumulative Prometheus counter
// if a caller pulled the observer twice.  After the fix the observer
// is constructed in NewRegistry and returned by reference.
func TestRegistryHLCObserverReturnsSameInstance(t *testing.T) {
	t.Parallel()

	r := NewRegistry("test-node", "127.0.0.1:0")
	first := r.HLCObserver()
	second := r.HLCObserver()
	require.NotNil(t, first)
	require.Same(t, first, second,
		"HLCObserver() must return the same observer instance per registry so lastRejections delta state survives multiple pulls")
}
