package monitoring

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestSnapshotOffloadMetricsRecordOutcomes(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := newSnapshotOffloadMetrics(reg)

	m.ObserveSnapshotOffloadPublished(7, 4211, 5<<20, 12*time.Second)
	m.ObserveSnapshotOffloadSkipped(7, snapshotOffloadSkipNotLeader)
	m.ObserveSnapshotOffloadSkipped(7, snapshotOffloadSkipAlreadyPublished)
	m.ObserveSnapshotOffloadFailed(9, errors.New("object store unavailable"))

	require.NoError(t, testutil.GatherAndCompare(
		reg,
		strings.NewReader(`
# HELP elastickv_snapshot_offload_published_total Total physical snapshots published to the object store, by Raft group.
# TYPE elastickv_snapshot_offload_published_total counter
elastickv_snapshot_offload_published_total{group_id="7"} 1
# HELP elastickv_snapshot_offload_last_published_index Raft index of the most recent snapshot this process published, by group. Staleness here is the backup-freshness signal.
# TYPE elastickv_snapshot_offload_last_published_index gauge
elastickv_snapshot_offload_last_published_index{group_id="7"} 4211
# HELP elastickv_snapshot_offload_failed_total Total offload attempts that failed, by Raft group. A sustained rate means backups are not being taken.
# TYPE elastickv_snapshot_offload_failed_total counter
elastickv_snapshot_offload_failed_total{group_id="9"} 1
# HELP elastickv_snapshot_offload_skipped_total Total offload scans that published nothing, by Raft group and reason. Routine on a follower or an unchanged snapshot.
# TYPE elastickv_snapshot_offload_skipped_total counter
elastickv_snapshot_offload_skipped_total{group_id="7",reason="already_published"} 1
elastickv_snapshot_offload_skipped_total{group_id="7",reason="not_leader"} 1
`),
		"elastickv_snapshot_offload_published_total",
		"elastickv_snapshot_offload_last_published_index",
		"elastickv_snapshot_offload_failed_total",
		"elastickv_snapshot_offload_skipped_total",
	))
}

// TestSnapshotOffloadMetricsBoundTheSkipReasonLabel is the cardinality
// guard: an unrecognised reason must collapse rather than mint a
// series per distinct string.
func TestSnapshotOffloadMetricsBoundTheSkipReasonLabel(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := newSnapshotOffloadMetrics(reg)

	m.ObserveSnapshotOffloadSkipped(1, "something-new")
	m.ObserveSnapshotOffloadSkipped(1, "something-else")
	m.ObserveSnapshotOffloadSkipped(1, "")

	require.Equal(t, 1, testutil.CollectAndCount(m.skipped))
	require.InDelta(t, 3.0,
		testutil.ToFloat64(m.skipped.WithLabelValues("1", snapshotOffloadSkipUnknown)), 0.0001)
}

// TestSnapshotOffloadMetricsDoNotLabelByError pins that the failure
// counter carries no error text: messages are unbounded, and one
// recurring failure would otherwise explode the metric's cardinality.
func TestSnapshotOffloadMetricsDoNotLabelByError(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := newSnapshotOffloadMetrics(reg)

	for i := range 20 {
		m.ObserveSnapshotOffloadFailed(1, errors.New(strings.Repeat("x", i+1)))
	}
	require.Equal(t, 1, testutil.CollectAndCount(m.failed),
		"distinct error texts must not create distinct series")
}

func TestSnapshotOffloadMetricsNilReceiverIsInert(t *testing.T) {
	t.Parallel()

	var m *SnapshotOffloadMetrics
	require.NotPanics(t, func() {
		m.ObserveSnapshotOffloadPublished(1, 2, 3, time.Second)
		m.ObserveSnapshotOffloadSkipped(1, "x")
		m.ObserveSnapshotOffloadFailed(1, errors.New("boom"))
	})
	require.NotNil(t, NewRegistry("n1", "127.0.0.1:1").SnapshotOffloadObserver())

	var nilRegistry *Registry
	require.Nil(t, nilRegistry.SnapshotOffloadObserver())
}
