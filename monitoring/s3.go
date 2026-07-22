package monitoring

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	s3PutAdmissionStagePrereserve = "prereserve"
	s3PutAdmissionStagePerBatch   = "perbatch"
	s3PutAdmissionStageUnknown    = "unknown"

	s3PutAdmissionProtocolFixed   = "fixed-length"
	s3PutAdmissionProtocolChunked = "chunked"
	s3PutAdmissionProtocolUnknown = "unknown"

	s3BlobOffloadModeLegacy  = "legacy"
	s3BlobOffloadModeOffload = "offload"
	s3BlobOffloadModeUnknown = "unknown"

	s3BlobOffloadReasonFlagDisabled      = "flag_disabled"
	s3BlobOffloadReasonCapabilityMissing = "capability_missing"
	s3BlobOffloadReasonDataPathDisabled  = "data_path_disabled"
	s3BlobOffloadReasonEnabled           = "enabled"
	s3BlobOffloadReasonUnknown           = "unknown"
)

type S3PutAdmissionObserver interface {
	ObserveS3PutAdmissionInflight(bytes int64)
	ObserveS3PutAdmissionRejection(stage, protocol string)
	ObserveS3PutAdmissionWait(stage, protocol string, duration time.Duration)
}

type S3BlobOffloadObserver interface {
	ObserveS3BlobOffloadDecision(mode, reason string)
	ObserveS3ChunkBlobReplicationDegraded()
	ObserveS3ChunkBlobSHAMismatch()
	ObserveS3ChunkBlobUnrecoverable()
}

type S3Metrics struct {
	putAdmissionInflightBytes    prometheus.Gauge
	putAdmissionRejections       *prometheus.CounterVec
	putAdmissionWait             *prometheus.HistogramVec
	blobOffloadWriteDecisions    *prometheus.CounterVec
	chunkBlobReplicationDegraded prometheus.Counter
	chunkBlobSHAMismatches       prometheus.Counter
	chunkBlobUnrecoverableReads  prometheus.Counter
	chunkBlobBackfillQueueDepth  prometheus.Gauge
	chunkBlobBackfillResults     *prometheus.CounterVec
}

func newS3Metrics(registerer prometheus.Registerer) *S3Metrics {
	m := &S3Metrics{
		putAdmissionInflightBytes: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "elastickv_s3_put_admission_inflight_bytes",
				Help: "Current S3 PUT body bytes admitted by this node and not yet released after Raft dispatch.",
			},
		),
		putAdmissionRejections: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_s3_put_admission_rejections_total",
				Help: "Total S3 PUT admission rejections by admission stage and request protocol.",
			},
			[]string{"stage", "protocol"},
		),
		putAdmissionWait: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "elastickv_s3_put_admission_wait_seconds",
				Help:    "Time spent waiting for an S3 PUT per-batch admission slot.",
				Buckets: []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30},
			},
			[]string{"stage", "protocol"},
		),
		blobOffloadWriteDecisions: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_s3_blob_offload_write_decisions_total",
				Help: "Total S3 write-path blob offload decisions by selected mode and fallback reason.",
			},
			[]string{"mode", "reason"},
		),
		chunkBlobReplicationDegraded: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "elastickv_s3_chunkblob_replication_degraded_total",
				Help: "Total S3 chunkblob writes that could not reach the configured replication target but remained recoverable.",
			},
		),
		chunkBlobSHAMismatches: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "elastickv_s3_chunkblob_sha_mismatch_total",
				Help: "Total S3 chunkblob reads or writes rejected because the stored payload did not match its content SHA-256.",
			},
		),
		chunkBlobUnrecoverableReads: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "elastickv_s3_chunkblob_unrecoverable_total",
				Help: "Total S3 chunkblob reads that could not recover the referenced content from any available peer.",
			},
		),
		chunkBlobBackfillQueueDepth: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "elastickv_s3_chunkblob_backfill_queue_depth",
				Help: "Current number of S3 chunkrefs queued for asynchronous local blob backfill.",
			},
		),
		chunkBlobBackfillResults: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_s3_chunkblob_backfill_results_total",
				Help: "Total S3 chunkblob backfill outcomes.",
			},
			[]string{"result"},
		),
	}
	registerer.MustRegister(
		m.putAdmissionInflightBytes,
		m.putAdmissionRejections,
		m.putAdmissionWait,
		m.blobOffloadWriteDecisions,
		m.chunkBlobReplicationDegraded,
		m.chunkBlobSHAMismatches,
		m.chunkBlobUnrecoverableReads,
		m.chunkBlobBackfillQueueDepth,
		m.chunkBlobBackfillResults,
	)
	return m
}

func (m *S3Metrics) ObserveS3PutAdmissionInflight(bytes int64) {
	if m == nil {
		return
	}
	m.putAdmissionInflightBytes.Set(float64(max(int64(0), bytes)))
}

func (m *S3Metrics) ObserveS3PutAdmissionRejection(stage, protocol string) {
	if m == nil {
		return
	}
	m.putAdmissionRejections.WithLabelValues(
		normalizeS3PutAdmissionStage(stage),
		normalizeS3PutAdmissionProtocol(protocol),
	).Inc()
}

func (m *S3Metrics) ObserveS3PutAdmissionWait(stage, protocol string, duration time.Duration) {
	if m == nil {
		return
	}
	if duration < 0 {
		duration = 0
	}
	m.putAdmissionWait.WithLabelValues(
		normalizeS3PutAdmissionStage(stage),
		normalizeS3PutAdmissionProtocol(protocol),
	).Observe(duration.Seconds())
}

func (m *S3Metrics) ObserveS3BlobOffloadDecision(mode, reason string) {
	if m == nil {
		return
	}
	m.blobOffloadWriteDecisions.WithLabelValues(
		normalizeS3BlobOffloadMode(mode),
		normalizeS3BlobOffloadReason(reason),
	).Inc()
}

func (m *S3Metrics) ObserveS3ChunkBlobReplicationDegraded() {
	if m == nil {
		return
	}
	m.chunkBlobReplicationDegraded.Inc()
}

func (m *S3Metrics) ObserveS3ChunkBlobSHAMismatch() {
	if m == nil {
		return
	}
	m.chunkBlobSHAMismatches.Inc()
}

func (m *S3Metrics) ObserveS3ChunkBlobUnrecoverable() {
	if m == nil {
		return
	}
	m.chunkBlobUnrecoverableReads.Inc()
}

func (m *S3Metrics) ObserveS3ChunkBlobBackfillQueueDepth(depth int) {
	if m == nil {
		return
	}
	m.chunkBlobBackfillQueueDepth.Set(float64(max(0, depth)))
}

func (m *S3Metrics) ObserveS3ChunkBlobBackfillResult(result string) {
	if m == nil {
		return
	}
	m.chunkBlobBackfillResults.WithLabelValues(normalizeS3BlobBackfillResult(result)).Inc()
}

func normalizeS3BlobBackfillResult(result string) string {
	switch result {
	case "fetched", "local_hit", "gone", "failed", "queue_drop":
		return result
	default:
		return s3BlobOffloadModeUnknown
	}
}

func normalizeS3PutAdmissionStage(stage string) string {
	switch stage {
	case s3PutAdmissionStagePrereserve, s3PutAdmissionStagePerBatch:
		return stage
	default:
		return s3PutAdmissionStageUnknown
	}
}

func normalizeS3BlobOffloadMode(mode string) string {
	switch mode {
	case s3BlobOffloadModeLegacy, s3BlobOffloadModeOffload:
		return mode
	default:
		return s3BlobOffloadModeUnknown
	}
}

func normalizeS3BlobOffloadReason(reason string) string {
	switch reason {
	case s3BlobOffloadReasonFlagDisabled,
		s3BlobOffloadReasonCapabilityMissing,
		s3BlobOffloadReasonDataPathDisabled,
		s3BlobOffloadReasonEnabled:
		return reason
	default:
		return s3BlobOffloadReasonUnknown
	}
}

func normalizeS3PutAdmissionProtocol(protocol string) string {
	switch protocol {
	case s3PutAdmissionProtocolFixed, s3PutAdmissionProtocolChunked:
		return protocol
	default:
		return s3PutAdmissionProtocolUnknown
	}
}
