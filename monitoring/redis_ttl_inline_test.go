package monitoring

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestRedisTTLInlineMigrationMetricsExportPasses(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	metrics := newRedisTTLInlineMigrationMetrics(reg)

	before := float64(time.Now().Unix())
	metrics.ObserveRedisTTLInlineMigrationPass("ttl-inline-string", 2, false)
	metrics.ObserveRedisTTLInlineMigrationPass("ttl-inline-string", 2, true)
	metrics.ObserveRedisTTLInlineMigrationPass("ttl-inline-string", 2, true)
	metrics.ObserveRedisTTLInlineMigrationPass("ttl-inline-hash", 3, false)

	require.Equal(t, 1.0, testutil.ToFloat64(metrics.passes.WithLabelValues("ttl-inline-string", "2", "not_clean")))
	require.Equal(t, 2.0, testutil.ToFloat64(metrics.passes.WithLabelValues("ttl-inline-string", "2", "clean")))
	require.Equal(t, 1.0, testutil.ToFloat64(metrics.passes.WithLabelValues("ttl-inline-hash", "3", "not_clean")))
	require.GreaterOrEqual(t, testutil.ToFloat64(metrics.lastCleanPass.WithLabelValues("ttl-inline-string", "2")), before)
	require.Equal(t, 1, testutil.CollectAndCount(metrics.lastCleanPass),
		"only a clean pass stamps the last clean pass time")

	families, err := reg.Gather()
	require.NoError(t, err)
	require.True(t, hasMetricFamily(families, "elastickv_redis_ttl_inline_migration_passes_total"))
	require.True(t, hasMetricFamily(families, "elastickv_redis_ttl_inline_migration_last_clean_pass_timestamp_seconds"))
}

func TestRedisTTLInlineMigrationObserverIsNilSafe(t *testing.T) {
	t.Parallel()
	var metrics *RedisTTLInlineMigrationMetrics
	require.NotPanics(t, func() { metrics.ObserveRedisTTLInlineMigrationPass("ttl-inline-string", 1, true) })
	var r *Registry
	require.Nil(t, r.RedisTTLInlineMigrationObserver())
}
