package monitoring

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// snapshotPayloadBucketBase is the smallest payload-size histogram
// bucket (1 MiB). Snapshots below it are rounding error next to the
// multi-gigabyte cases the histogram exists to show.
const (
	snapshotPayloadBucketBase   = 1 << 20 // 1 MiB
	snapshotPayloadBucketFactor = 4
	snapshotPayloadBucketCount  = 8 // 1 MiB through ~16 GiB
)

// SnapshotOffloadMetrics exposes the physical snapshot offload
// scheduler's outcomes (design doc §4).
//
// group_id is a label on every series: its cardinality is the number
// of Raft groups this process hosts, which is bounded by deployment
// topology rather than by traffic. skip reason is a closed set owned
// by the scheduler.
type SnapshotOffloadMetrics struct {
	published        *prometheus.CounterVec
	skipped          *prometheus.CounterVec
	failed           *prometheus.CounterVec
	lastPublishIndex *prometheus.GaugeVec
	publishSeconds   *prometheus.HistogramVec
	payloadBytes     *prometheus.HistogramVec
}

func newSnapshotOffloadMetrics(registerer prometheus.Registerer) *SnapshotOffloadMetrics {
	m := &SnapshotOffloadMetrics{
		published: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_snapshot_offload_published_total",
				Help: "Total physical snapshots published to the object store, by Raft group.",
			},
			[]string{"group_id"},
		),
		skipped: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_snapshot_offload_skipped_total",
				Help: "Total offload scans that published nothing, by Raft group and reason. Routine on a follower or an unchanged snapshot.",
			},
			[]string{"group_id", "reason"},
		),
		failed: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_snapshot_offload_failed_total",
				Help: "Total offload attempts that failed, by Raft group. A sustained rate means backups are not being taken.",
			},
			[]string{"group_id"},
		),
		lastPublishIndex: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "elastickv_snapshot_offload_last_published_index",
				Help: "Raft index of the most recent snapshot this process published, by group. Staleness here is the backup-freshness signal.",
			},
			[]string{"group_id"},
		),
		publishSeconds: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "elastickv_snapshot_offload_publish_seconds",
				Help:    "Wall time to spool, upload and commit one snapshot.",
				Buckets: []float64{0.5, 1, 5, 15, 30, 60, 300, 900, 1800, 3600},
			},
			[]string{"group_id"},
		),
		payloadBytes: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "elastickv_snapshot_offload_payload_bytes",
				Help: "Size of each published snapshot payload.",
				Buckets: prometheus.ExponentialBuckets(
					snapshotPayloadBucketBase,
					snapshotPayloadBucketFactor,
					snapshotPayloadBucketCount,
				),
			},
			[]string{"group_id"},
		),
	}
	registerer.MustRegister(
		m.published,
		m.skipped,
		m.failed,
		m.lastPublishIndex,
		m.publishSeconds,
		m.payloadBytes,
	)
	return m
}

// ObserveSnapshotOffloadPublished records one successful publication.
func (m *SnapshotOffloadMetrics) ObserveSnapshotOffloadPublished(
	groupID, index uint64, payloadBytes int64, elapsed time.Duration,
) {
	if m == nil {
		return
	}
	label := snapshotOffloadGroupLabel(groupID)
	m.published.WithLabelValues(label).Inc()
	m.lastPublishIndex.WithLabelValues(label).Set(float64(index))
	m.publishSeconds.WithLabelValues(label).Observe(max(0, elapsed).Seconds())
	m.payloadBytes.WithLabelValues(label).Observe(float64(max(int64(0), payloadBytes)))
}

// ObserveSnapshotOffloadSkipped records a scan that published nothing.
func (m *SnapshotOffloadMetrics) ObserveSnapshotOffloadSkipped(groupID uint64, reason string) {
	if m == nil {
		return
	}
	m.skipped.WithLabelValues(snapshotOffloadGroupLabel(groupID), normalizeSnapshotOffloadSkip(reason)).Inc()
}

// ObserveSnapshotOffloadFailed records a failed attempt. The error is
// deliberately not a label: its text is unbounded, and a per-message
// series would let one recurring failure explode the metric's
// cardinality. Diagnosis comes from the scheduler's log line.
func (m *SnapshotOffloadMetrics) ObserveSnapshotOffloadFailed(groupID uint64, _ error) {
	if m == nil {
		return
	}
	m.failed.WithLabelValues(snapshotOffloadGroupLabel(groupID)).Inc()
}

func snapshotOffloadGroupLabel(groupID uint64) string {
	return strconv.FormatUint(groupID, 10)
}

// Skip reasons emitted by the scheduler.
const (
	snapshotOffloadSkipNotLeader        = "not_leader"
	snapshotOffloadSkipAlreadyPublished = "already_published"
	snapshotOffloadSkipNoSnapshot       = "no_persisted_snapshot"
	snapshotOffloadSkipInFlight         = "already_in_flight"
	snapshotOffloadSkipUnknownLeader    = "leadership_unknown"
	snapshotOffloadSkipUnknown          = "unknown"
)

// normalizeSnapshotOffloadSkip keeps the reason label inside the
// scheduler's closed set.
func normalizeSnapshotOffloadSkip(reason string) string {
	switch reason {
	case snapshotOffloadSkipNotLeader,
		snapshotOffloadSkipAlreadyPublished,
		snapshotOffloadSkipNoSnapshot,
		snapshotOffloadSkipInFlight,
		snapshotOffloadSkipUnknownLeader:
		return reason
	default:
		return snapshotOffloadSkipUnknown
	}
}
