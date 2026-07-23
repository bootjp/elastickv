package monitoring

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestTSOObserverExportsOperationalMetrics(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	metrics := newTSOMetrics(reg)
	observer := newTSOObserver(metrics)

	observer.ObserveTSORequest("reserve", "remote", "success", 3*time.Millisecond)
	observer.ObserveTSORequest("reserve", "remote", "error", 20*time.Millisecond)
	observer.ObserveTSOShadowComparison("legacy_ahead", 7)
	observer.ObserveTSOShadowComparison("legacy_overlap", 3)
	observer.ObserveTSOMode("shadow")
	observer.ObserveTSOModeReload("applied")
	observer.ObserveTSODurableState(true, false)

	require.Equal(t, 1.0, testutil.ToFloat64(metrics.shadowComparison.WithLabelValues("legacy_ahead")))
	require.Equal(t, 1.0, testutil.ToFloat64(metrics.shadowComparison.WithLabelValues("legacy_overlap")))
	require.Equal(t, 1.0, testutil.ToFloat64(metrics.mode.WithLabelValues("shadow")))
	require.Equal(t, 0.0, testutil.ToFloat64(metrics.mode.WithLabelValues("legacy")))
	require.Equal(t, 1.0, testutil.ToFloat64(metrics.reloads.WithLabelValues("applied")))
	require.Equal(t, 1.0, testutil.ToFloat64(metrics.durableState.WithLabelValues("cutover")))
	require.Equal(t, 0.0, testutil.ToFloat64(metrics.durableState.WithLabelValues("phase_d")))

	families, err := reg.Gather()
	require.NoError(t, err)
	require.True(t, hasMetricFamily(families, "elastickv_tso_request_duration_seconds"))
	require.True(t, hasMetricFamily(families, "elastickv_tso_shadow_divergence_timestamps"))
}

func hasMetricFamily(families []*dto.MetricFamily, name string) bool {
	for _, family := range families {
		if family.GetName() == name {
			return true
		}
	}
	return false
}
