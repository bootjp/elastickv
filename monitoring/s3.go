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
)

type S3PutAdmissionObserver interface {
	ObserveS3PutAdmissionInflight(bytes int64)
	ObserveS3PutAdmissionRejection(stage, protocol string)
	ObserveS3PutAdmissionWait(stage, protocol string, duration time.Duration)
}

type S3Metrics struct {
	putAdmissionInflightBytes prometheus.Gauge
	putAdmissionRejections    *prometheus.CounterVec
	putAdmissionWait          *prometheus.HistogramVec
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
	}
	registerer.MustRegister(
		m.putAdmissionInflightBytes,
		m.putAdmissionRejections,
		m.putAdmissionWait,
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

func normalizeS3PutAdmissionStage(stage string) string {
	switch stage {
	case s3PutAdmissionStagePrereserve, s3PutAdmissionStagePerBatch:
		return stage
	default:
		return s3PutAdmissionStageUnknown
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
