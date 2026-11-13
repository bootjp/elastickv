package adapter

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type s3ObjectDurationSample struct {
	operation string
	mode      string
	outcome   string
	duration  time.Duration
}

type recordingS3ObjectLatencyObserver struct {
	recordingS3BlobOffloadObserver
	samples []s3ObjectDurationSample
}

func (o *recordingS3ObjectLatencyObserver) ObserveS3ObjectDuration(operation, mode, outcome string, duration time.Duration) {
	o.samples = append(o.samples, s3ObjectDurationSample{operation, mode, outcome, duration})
}

func TestS3ObjectDurationTracksLegacyFallbackAndFailures(t *testing.T) {
	t.Parallel()
	st := store.NewMVCCStore()
	observer := &recordingS3ObjectLatencyObserver{}
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil,
		WithS3BlobOffloadEnabled(true), WithS3BlobOffloadObserver(observer))

	rec := httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-latency", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-latency/object", bytes.NewReader([]byte("data"))))
	require.Equal(t, http.StatusOK, rec.Code)
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodGet, "/bucket-latency/object", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "data", rec.Body.String())
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodGet, "/bucket-latency/missing", nil))
	require.Equal(t, http.StatusNotFound, rec.Code)
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/missing/object", bytes.NewReader([]byte("data"))))
	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Len(t, observer.samples, 4)
	require.Equal(t, s3ObjectDurationSample{operation: "put", mode: "legacy", outcome: "success"}, withoutS3SampleDuration(observer.samples[0]))
	require.Equal(t, s3ObjectDurationSample{operation: "get", mode: "legacy", outcome: "success"}, withoutS3SampleDuration(observer.samples[1]))
	require.Equal(t, s3ObjectDurationSample{operation: "get", mode: "unknown", outcome: "failure"}, withoutS3SampleDuration(observer.samples[2]))
	require.Equal(t, s3ObjectDurationSample{operation: "put", mode: "unknown", outcome: "failure"}, withoutS3SampleDuration(observer.samples[3]))
	for _, sample := range observer.samples {
		require.GreaterOrEqual(t, sample.duration, time.Duration(0))
	}
}

func TestS3ObjectDurationTracksProxyOnMissAndFailedGet(t *testing.T) {
	t.Parallel()
	observer := &recordingS3ObjectLatencyObserver{}
	server, cluster, localResolver := newS3BlobM1TestServer(t, observer)
	putS3BlobM1Object(t, server, "/bucket-latency/object", []byte("offloaded"))
	require.Len(t, observer.samples, 1)
	require.Equal(t, s3ObjectDurationSample{operation: "put", mode: "offload", outcome: "success"}, withoutS3SampleDuration(observer.samples[0]))

	localResolver.set(store.NewMVCCStore())
	rec := httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodGet, "/bucket-latency/object", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "offloaded", rec.Body.String())
	require.Greater(t, cluster.fetchCount(), 0)
	require.Len(t, observer.samples, 2)
	require.Equal(t, s3ObjectDurationSample{operation: "get", mode: "offload", outcome: "success"}, withoutS3SampleDuration(observer.samples[1]))

	cluster.clearBlobs()
	localResolver.set(store.NewMVCCStore())
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodGet, "/bucket-latency/object", nil))
	require.Equal(t, http.StatusInternalServerError, rec.Code)
	require.Len(t, observer.samples, 3)
	require.Equal(t, s3ObjectDurationSample{operation: "get", mode: "offload", outcome: "failure"}, withoutS3SampleDuration(observer.samples[2]))
}

func TestS3ManifestDataMode(t *testing.T) {
	t.Parallel()
	require.Equal(t, "unknown", s3ManifestDataMode(nil))
	require.Equal(t, "legacy", s3ManifestDataMode(&s3ObjectManifest{}))
	require.Equal(t, "offload", s3ManifestDataMode(&s3ObjectManifest{Parts: []s3ObjectPart{{Offloaded: true}}}))
	require.Equal(t, "mixed", s3ManifestDataMode(&s3ObjectManifest{Parts: []s3ObjectPart{{Offloaded: true}, {}}}))
	manifest := &s3ObjectManifest{Parts: []s3ObjectPart{
		{Offloaded: true, ChunkSizes: []uint64{4}},
		{ChunkSizes: []uint64{4}},
	}}
	require.Equal(t, "offload", s3ManifestRangeDataMode(manifest, 0, 4))
	require.Equal(t, "mixed", s3ManifestRangeDataMode(manifest, 3, 2))
	require.Equal(t, "legacy", s3ManifestRangeDataMode(manifest, 4, 4))
}

func withoutS3SampleDuration(sample s3ObjectDurationSample) s3ObjectDurationSample {
	sample.duration = 0
	return sample
}
