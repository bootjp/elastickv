package monitoring

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// Each outcome keeps its own last pass time, so a not_clean pass after a clean
// one stays visible next to it: a gate that compares the two can tell that the
// clean pass is no longer the latest word on the plan.
func TestRedisTTLInlineMigrationMetricsExportPasses(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	metrics := newRedisTTLInlineMigrationMetrics(reg)

	before := float64(time.Now().Unix())
	metrics.ObserveRedisTTLInlineMigrationPass("ttl-inline-string", 2, RedisTTLInlinePassNotClean)
	metrics.ObserveRedisTTLInlineMigrationPass("ttl-inline-string", 2, RedisTTLInlinePassClean)
	metrics.ObserveRedisTTLInlineMigrationPass("ttl-inline-string", 2, RedisTTLInlinePassClean)
	metrics.ObserveRedisTTLInlineMigrationPass("ttl-inline-hash", 3, RedisTTLInlinePassPartial)

	require.Equal(t, 1.0, testutil.ToFloat64(metrics.passes.WithLabelValues("ttl-inline-string", "2", "not_clean")))
	require.Equal(t, 2.0, testutil.ToFloat64(metrics.passes.WithLabelValues("ttl-inline-string", "2", "clean")))
	require.Equal(t, 1.0, testutil.ToFloat64(metrics.passes.WithLabelValues("ttl-inline-hash", "3", "partial")))
	for _, labels := range [][]string{
		{"ttl-inline-string", "2", "not_clean"},
		{"ttl-inline-string", "2", "clean"},
		{"ttl-inline-hash", "3", "partial"},
	} {
		require.GreaterOrEqual(t, testutil.ToFloat64(metrics.lastPass.WithLabelValues(labels...)), before, labels)
	}
	require.Equal(t, 3, testutil.CollectAndCount(metrics.lastPass))

	families, err := reg.Gather()
	require.NoError(t, err)
	require.True(t, hasMetricFamily(families, "elastickv_redis_ttl_inline_migration_passes_total"))
	require.True(t, hasMetricFamily(families, "elastickv_redis_ttl_inline_migration_last_pass_timestamp_seconds"))
	require.False(t, hasMetricFamily(families, "elastickv_redis_ttl_inline_migration_last_clean_pass_timestamp_seconds"))
}

func TestRedisTTLInlineMigrationObserverIsNilSafe(t *testing.T) {
	t.Parallel()
	var metrics *RedisTTLInlineMigrationMetrics
	require.NotPanics(t, func() {
		metrics.ObserveRedisTTLInlineMigrationPass("ttl-inline-string", 1, RedisTTLInlinePassClean)
	})
	var r *Registry
	require.Nil(t, r.RedisTTLInlineMigrationObserver())
}
