package monitoring

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// Each outcome keeps the start time of its latest pass, so a not_clean pass
// after a clean one stays visible next to it, and a gate can require a clean
// pass that started, not just ended, after an event.
func TestRedisTTLInlineMigrationMetricsExportPasses(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	metrics := newRedisTTLInlineMigrationMetrics(reg)

	start := time.Unix(1_700_000_000, 0)
	metrics.ObserveRedisTTLInlineMigrationPass("ttl-inline-string", 2, RedisTTLInlinePassNotClean, start)
	metrics.ObserveRedisTTLInlineMigrationPass("ttl-inline-string", 2, RedisTTLInlinePassClean, start.Add(time.Minute))
	metrics.ObserveRedisTTLInlineMigrationPass("ttl-inline-string", 2, RedisTTLInlinePassClean, start.Add(2*time.Minute))
	metrics.ObserveRedisTTLInlineMigrationPass("ttl-inline-hash", 3, RedisTTLInlinePassPartial, start)

	require.Equal(t, 1.0, testutil.ToFloat64(metrics.passes.WithLabelValues("ttl-inline-string", "2", "not_clean")))
	require.Equal(t, 2.0, testutil.ToFloat64(metrics.passes.WithLabelValues("ttl-inline-string", "2", "clean")))
	require.Equal(t, 1.0, testutil.ToFloat64(metrics.passes.WithLabelValues("ttl-inline-hash", "3", "partial")))
	for labels, want := range map[[3]string]time.Time{
		{"ttl-inline-string", "2", "not_clean"}: start,
		{"ttl-inline-string", "2", "clean"}:     start.Add(2 * time.Minute),
		{"ttl-inline-hash", "3", "partial"}:     start,
	} {
		got := testutil.ToFloat64(metrics.lastPassStart.WithLabelValues(labels[0], labels[1], labels[2]))
		require.InDelta(t, float64(want.Unix()), got, 0, labels)
	}
	require.Equal(t, 3, testutil.CollectAndCount(metrics.lastPassStart))

	families, err := reg.Gather()
	require.NoError(t, err)
	require.True(t, hasMetricFamily(families, "elastickv_redis_ttl_inline_migration_passes_total"))
	require.True(t, hasMetricFamily(families, "elastickv_redis_ttl_inline_migration_last_pass_start_timestamp_seconds"))
	require.False(t, hasMetricFamily(families, "elastickv_redis_ttl_inline_migration_last_pass_timestamp_seconds"))
}

func TestRedisTTLInlineMigrationObserverIsNilSafe(t *testing.T) {
	t.Parallel()
	var metrics *RedisTTLInlineMigrationMetrics
	require.NotPanics(t, func() {
		metrics.ObserveRedisTTLInlineMigrationPass("ttl-inline-string", 1, RedisTTLInlinePassClean, time.Now())
	})
	var r *Registry
	require.Nil(t, r.RedisTTLInlineMigrationObserver())
}
