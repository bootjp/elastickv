package monitoring

import (
	"strings"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/filesystem"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestFileSystemMetricsObservePinnedHotspot(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	metrics := newFileSystemMetrics(reg)

	metrics.ObserveFilePinnedHotspot(fsPinnedHotspotReasonSplitBoundary)
	metrics.ObserveFilePinnedHotspot("unexpected")

	err := testutil.GatherAndCompare(
		reg,
		strings.NewReader(`
# HELP elastickv_fs_file_pinned_hotspot_total Total filesystem file-pinned hotspot split planner skips by reason.
# TYPE elastickv_fs_file_pinned_hotspot_total counter
elastickv_fs_file_pinned_hotspot_total{reason="split_boundary"} 1
elastickv_fs_file_pinned_hotspot_total{reason="unknown"} 1
`),
		"elastickv_fs_file_pinned_hotspot_total",
	)
	require.NoError(t, err)
}

func TestFileSystemMetricsObserveOperationsAndPlacement(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	metrics := newFileSystemMetrics(reg)
	metrics.ObserveChunkRead(1500 * time.Microsecond)
	metrics.ObserveChunkWrite(2500 * time.Microsecond)
	metrics.ObserveHomeEpochConflict()
	metrics.ObserveInodeIDCollisionRetry()
	metrics.ObserveMoveJob([]byte("job-a"), true)
	metrics.ObservePlacementStats(filesystem.PlacementStats{
		FilesByGroup:     map[uint64]uint64{1: 2, 2: 3},
		MultiShardFiles:  2,
		MultiShardInodes: []uint64{10, 20},
		MoveInflight:     1,
		OpenHandleLeases: 4,
		OrphanedInodes:   5,
	})

	require.Equal(t, float64(1), testutil.ToFloat64(metrics.chunkReadOpsTotal))
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.chunkWriteOpsTotal))
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.homeEpochConflictTotal))
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.inodeCollisionTotal))
	require.Equal(t, float64(2), testutil.ToFloat64(metrics.fileMultiShardTotal))
	require.Equal(t, float64(2), testutil.ToFloat64(metrics.fileMultiShardCurrent))
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.fileMoveInflight))
	require.Equal(t, float64(4), testutil.ToFloat64(metrics.openHandleLeaseActive))
	require.Equal(t, float64(5), testutil.ToFloat64(metrics.orphanInodeGCPending))
	require.Equal(t, float64(2), testutil.ToFloat64(metrics.filesByGroup.WithLabelValues("1")))
	require.Equal(t, float64(3), testutil.ToFloat64(metrics.filesByGroup.WithLabelValues("2")))

	metrics.ObservePlacementStats(filesystem.PlacementStats{
		FilesByGroup:     map[uint64]uint64{2: 1},
		MultiShardFiles:  2,
		MultiShardInodes: []uint64{20, 30},
		MoveInflight:     1,
	})
	require.Equal(t, float64(3), testutil.ToFloat64(metrics.fileMultiShardTotal), "only newly detected inodes increment the counter")
	require.Equal(t, float64(2), testutil.ToFloat64(metrics.fileMultiShardCurrent))
	metrics.ObserveMoveJob([]byte("job-a"), false)
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.fileMoveInflight), "the durable scan remains authoritative")
	metrics.ObservePlacementStats(filesystem.PlacementStats{FilesByGroup: map[uint64]uint64{2: 1}})
	require.Equal(t, float64(0), testutil.ToFloat64(metrics.fileMoveInflight))
	err := testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP elastickv_fs_files_by_group Current active filesystem files by owning shard group.
# TYPE elastickv_fs_files_by_group gauge
elastickv_fs_files_by_group{group_id="2"} 1
`), "elastickv_fs_files_by_group")
	require.NoError(t, err)
}

func TestRegistryReturnsFileSystemObserver(t *testing.T) {
	t.Parallel()

	registry := NewRegistry("n1", "127.0.0.1:0")
	require.NotNil(t, registry.FileSystemObserver())
}
