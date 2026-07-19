package monitoring

import (
	"strconv"
	"sync"
	"time"

	"github.com/bootjp/elastickv/internal/filesystem"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	fsPinnedHotspotReasonSplitBoundary = "split_boundary"
	fsPinnedHotspotReasonUnknown       = "unknown"
	fsLatencyBucketStartMS             = 0.25
	fsLatencyBucketFactor              = 2
	fsLatencyBucketCount               = 12
)

type FileSystemObserver interface {
	ObserveFilePinnedHotspot(reason string)
	ObserveChunkRead(time.Duration)
	ObserveChunkWrite(time.Duration)
	ObserveHomeEpochConflict()
	ObserveInodeIDCollisionRetry()
	ObserveMoveJob(jobID []byte, active bool)
	ObservePlacementStats(filesystem.PlacementStats)
}

type FileSystemMetrics struct {
	mu                     sync.Mutex
	filePinnedHotspotTotal *prometheus.CounterVec
	fileMultiShardTotal    prometheus.Counter
	fileMultiShardCurrent  prometheus.Gauge
	fileMoveInflight       prometheus.Gauge
	chunkReadOpsTotal      prometheus.Counter
	chunkWriteOpsTotal     prometheus.Counter
	chunkReadLatencyMS     prometheus.Histogram
	chunkWriteLatencyMS    prometheus.Histogram
	homeEpochConflictTotal prometheus.Counter
	openHandleLeaseActive  prometheus.Gauge
	orphanInodeGCPending   prometheus.Gauge
	inodeCollisionTotal    prometheus.Counter
	filesByGroup           *prometheus.GaugeVec
	activeMoveJobs         map[string]struct{}
	lastFilesByGroup       map[string]struct{}
	lastMultiShardInodes   map[uint64]struct{}
	lastMoveInflight       uint64
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
		fileMultiShardTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "elastickv_fs_file_multi_shard_detected_total",
			Help: "Total newly observed active files with chunks on more than one shard group.",
		}),
		fileMultiShardCurrent: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "elastickv_fs_file_multi_shard_current",
			Help: "Current active files with chunks observed on more than one shard group.",
		}),
		fileMoveInflight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "elastickv_fs_file_move_inflight",
			Help: "Current durable whole-file migration jobs that have not completed.",
		}),
		chunkReadOpsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "elastickv_fs_chunk_read_ops_total",
			Help: "Total filesystem read operations that may access file chunks.",
		}),
		chunkWriteOpsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "elastickv_fs_chunk_write_ops_total",
			Help: "Total filesystem write operations that may mutate file chunks.",
		}),
		chunkReadLatencyMS: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "elastickv_fs_chunk_read_latency_ms",
			Help:    "Filesystem read operation latency in milliseconds.",
			Buckets: prometheus.ExponentialBuckets(fsLatencyBucketStartMS, fsLatencyBucketFactor, fsLatencyBucketCount),
		}),
		chunkWriteLatencyMS: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "elastickv_fs_chunk_write_latency_ms",
			Help:    "Filesystem write operation latency in milliseconds.",
			Buckets: prometheus.ExponentialBuckets(fsLatencyBucketStartMS, fsLatencyBucketFactor, fsLatencyBucketCount),
		}),
		homeEpochConflictTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "elastickv_fs_home_epoch_conflict_total",
			Help: "Total filesystem mutations rejected because the file home was migrating or changed epoch.",
		}),
		openHandleLeaseActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "elastickv_fs_open_handle_lease_active",
			Help: "Current durable filesystem open-handle leases.",
		}),
		orphanInodeGCPending: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "elastickv_fs_orphan_inode_gc_pending",
			Help: "Current orphaned filesystem inodes awaiting final garbage collection.",
		}),
		inodeCollisionTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "elastickv_fs_inode_id_collision_retry_total",
			Help: "Total inode allocator retries caused by reserved or already-used IDs.",
		}),
		filesByGroup: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "elastickv_fs_files_by_group",
			Help: "Current active filesystem files by owning shard group.",
		}, []string{"group_id"}),
		activeMoveJobs:       make(map[string]struct{}),
		lastFilesByGroup:     make(map[string]struct{}),
		lastMultiShardInodes: make(map[uint64]struct{}),
	}
	registerer.MustRegister(
		m.filePinnedHotspotTotal,
		m.fileMultiShardTotal,
		m.fileMultiShardCurrent,
		m.fileMoveInflight,
		m.chunkReadOpsTotal,
		m.chunkWriteOpsTotal,
		m.chunkReadLatencyMS,
		m.chunkWriteLatencyMS,
		m.homeEpochConflictTotal,
		m.openHandleLeaseActive,
		m.orphanInodeGCPending,
		m.inodeCollisionTotal,
		m.filesByGroup,
	)
	return m
}

func (m *FileSystemMetrics) ObserveChunkRead(elapsed time.Duration) {
	if m == nil {
		return
	}
	m.chunkReadOpsTotal.Inc()
	m.chunkReadLatencyMS.Observe(float64(elapsed) / float64(time.Millisecond))
}

func (m *FileSystemMetrics) ObserveChunkWrite(elapsed time.Duration) {
	if m == nil {
		return
	}
	m.chunkWriteOpsTotal.Inc()
	m.chunkWriteLatencyMS.Observe(float64(elapsed) / float64(time.Millisecond))
}

func (m *FileSystemMetrics) ObserveHomeEpochConflict() {
	if m != nil {
		m.homeEpochConflictTotal.Inc()
	}
}

func (m *FileSystemMetrics) ObserveInodeIDCollisionRetry() {
	if m != nil {
		m.inodeCollisionTotal.Inc()
	}
}

func (m *FileSystemMetrics) ObserveMoveJob(jobID []byte, active bool) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := string(jobID)
	if active {
		m.activeMoveJobs[key] = struct{}{}
	} else {
		delete(m.activeMoveJobs, key)
	}
	m.updateMoveInflightLocked()
}

func (m *FileSystemMetrics) ObservePlacementStats(stats filesystem.PlacementStats) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastMoveInflight = stats.MoveInflight
	m.updateMoveInflightLocked()
	m.openHandleLeaseActive.Set(float64(stats.OpenHandleLeases))
	m.orphanInodeGCPending.Set(float64(stats.OrphanedInodes))
	m.fileMultiShardCurrent.Set(float64(stats.MultiShardFiles))
	nextMultiShard := make(map[uint64]struct{}, len(stats.MultiShardInodes))
	for _, inode := range stats.MultiShardInodes {
		nextMultiShard[inode] = struct{}{}
		if _, found := m.lastMultiShardInodes[inode]; !found {
			m.fileMultiShardTotal.Inc()
		}
	}
	m.lastMultiShardInodes = nextMultiShard

	nextGroups := make(map[string]struct{}, len(stats.FilesByGroup))
	for groupID, files := range stats.FilesByGroup {
		label := strconv.FormatUint(groupID, 10)
		nextGroups[label] = struct{}{}
		m.filesByGroup.WithLabelValues(label).Set(float64(files))
	}
	for label := range m.lastFilesByGroup {
		if _, found := nextGroups[label]; !found {
			m.filesByGroup.DeleteLabelValues(label)
		}
	}
	m.lastFilesByGroup = nextGroups
}

func (m *FileSystemMetrics) updateMoveInflightLocked() {
	inflight := m.lastMoveInflight
	if active := uint64(len(m.activeMoveJobs)); active > inflight {
		inflight = active
	}
	m.fileMoveInflight.Set(float64(inflight))
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
