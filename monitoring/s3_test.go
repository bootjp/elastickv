package monitoring

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestS3PutAdmissionMetricsObserve(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	metrics := newS3Metrics(reg)

	metrics.ObserveS3PutAdmissionInflight(1024)
	metrics.ObserveS3PutAdmissionRejection(s3PutAdmissionStagePrereserve, s3PutAdmissionProtocolFixed)
	metrics.ObserveS3PutAdmissionRejection(s3PutAdmissionStagePerBatch, s3PutAdmissionProtocolChunked)
	metrics.ObserveS3PutAdmissionWait(s3PutAdmissionStagePerBatch, s3PutAdmissionProtocolChunked, 12*time.Millisecond)

	err := testutil.GatherAndCompare(
		reg,
		strings.NewReader(`
# HELP elastickv_s3_put_admission_inflight_bytes Current S3 PUT body bytes admitted by this node and not yet released after Raft dispatch.
# TYPE elastickv_s3_put_admission_inflight_bytes gauge
elastickv_s3_put_admission_inflight_bytes 1024
# HELP elastickv_s3_put_admission_rejections_total Total S3 PUT admission rejections by admission stage and request protocol.
# TYPE elastickv_s3_put_admission_rejections_total counter
elastickv_s3_put_admission_rejections_total{protocol="chunked",stage="perbatch"} 1
elastickv_s3_put_admission_rejections_total{protocol="fixed-length",stage="prereserve"} 1
`),
		"elastickv_s3_put_admission_inflight_bytes",
		"elastickv_s3_put_admission_rejections_total",
	)
	require.NoError(t, err)
	require.Equal(t, 1, testutil.CollectAndCount(metrics.putAdmissionWait))
}

func TestRegistryReturnsS3PutAdmissionObserver(t *testing.T) {
	t.Parallel()

	registry := NewRegistry("n1", "127.0.0.1:0")
	require.NotNil(t, registry.S3PutAdmissionObserver())
}

func TestRegistryS3PutAdmissionObserverNilWhenMetricsMissing(t *testing.T) {
	t.Parallel()

	registry := &Registry{}
	require.Nil(t, registry.S3PutAdmissionObserver())
}
