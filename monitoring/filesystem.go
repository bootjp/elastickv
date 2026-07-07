package monitoring

import "github.com/prometheus/client_golang/prometheus"

const (
	fsPinnedHotspotReasonSplitBoundary = "split_boundary"
	fsPinnedHotspotReasonUnknown       = "unknown"
)

type FileSystemObserver interface {
	ObserveFilePinnedHotspot(reason string)
}

type FileSystemMetrics struct {
	filePinnedHotspotTotal *prometheus.CounterVec
}

func newFileSystemMetrics(registerer prometheus.Registerer) *FileSystemMetrics {
	m := &FileSystemMetrics{
		filePinnedHotspotTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_fs_file_pinned_hotspot_total",
				Help: "Total filesystem file-pinned hotspot split planner skips by reason.",
			},
			[]string{"reason"},
		),
	}
	registerer.MustRegister(m.filePinnedHotspotTotal)
	return m
}

func (m *FileSystemMetrics) ObserveFilePinnedHotspot(reason string) {
	if m == nil {
		return
	}
	m.filePinnedHotspotTotal.WithLabelValues(normalizeFilePinnedHotspotReason(reason)).Inc()
}

func normalizeFilePinnedHotspotReason(reason string) string {
	switch reason {
	case fsPinnedHotspotReasonSplitBoundary:
		return reason
	default:
		return fsPinnedHotspotReasonUnknown
	}
}
