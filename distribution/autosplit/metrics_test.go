package autosplit

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestPrometheusObserverRegistersBoundedMetrics(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	observer, ok := NewPrometheusObserver(registry).(*prometheusObserver)
	require.True(t, ok)

	observer.ObserveCandidatesPromoted(2)
	observer.ObserveSplitScheduled()
	observer.ObserveSplitFailed("unexpected_detail")
	observer.ObserveSkipped(SkipReasonRouteCap)
	observer.ObserveSkipped(SkipReasonCooldown)
	observer.ObserveIsolationDeclined(IsolationDeclineTopKErrorBound)
	observer.ObserveCompoundPartial()
	observer.ObserveState(true, 7, 2, 250*time.Millisecond)

	require.Equal(t, float64(2), testutil.ToFloat64(observer.candidatesPromoted))
	require.Equal(t, float64(1), testutil.ToFloat64(observer.splitsScheduled))
	require.Equal(t, float64(1), testutil.ToFloat64(observer.splitsFailed.WithLabelValues("rpc_error")))
	require.Equal(t, float64(1), testutil.ToFloat64(observer.skipped.WithLabelValues(string(SkipReasonRouteCap))))
	require.Equal(t, float64(0), testutil.ToFloat64(observer.skipped.WithLabelValues(string(SkipReasonCooldown))))
	require.Equal(t, float64(1), testutil.ToFloat64(observer.isolationDeclined.WithLabelValues(string(IsolationDeclineTopKErrorBound))))
	require.Equal(t, float64(1), testutil.ToFloat64(observer.compoundPartial))
	require.Equal(t, float64(1), testutil.ToFloat64(observer.enabled))
	require.Equal(t, float64(7), testutil.ToFloat64(observer.trackedRoutes))
	require.Equal(t, float64(2), testutil.ToFloat64(observer.cooldownActive))
	require.InDelta(t, 0.25, testutil.ToFloat64(observer.evalDuration), 0.0001)

	families, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, families, 10)
}
