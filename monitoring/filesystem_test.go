package monitoring

import (
	"strings"
	"testing"

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

func TestRegistryReturnsFileSystemObserver(t *testing.T) {
	t.Parallel()

	registry := NewRegistry("n1", "127.0.0.1:0")
	require.NotNil(t, registry.FileSystemObserver())
}
