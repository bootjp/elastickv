package adapter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestS3BlobOffloadEnvDefaultsDisabled(t *testing.T) {
	t.Setenv(s3BlobOffloadEnvVar, "")

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	require.False(t, server.blobOffloadEnabled)
}

func TestS3BlobOffloadEnvCanEnableGate(t *testing.T) {
	t.Setenv(s3BlobOffloadEnvVar, "true")

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	require.True(t, server.blobOffloadEnabled)
}

func TestS3BlobOffloadDecisionFailsClosed(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(
		nil,
		"",
		st,
		newLocalAdapterCoordinator(st),
		nil,
		WithS3BlobOffloadEnabled(true),
	)

	decision := server.s3BlobOffloadDecision(context.Background())
	require.Equal(t, s3BlobOffloadModeLegacy, decision.mode)
	require.Equal(t, s3BlobOffloadReasonCapabilityMissing, decision.reason)
}

func TestS3BlobOffloadDecisionEnablesImplementedDataPath(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(
		nil,
		"",
		st,
		newLocalAdapterCoordinator(st),
		nil,
		WithS3BlobOffloadEnabled(true),
		WithS3BlobOffloadCapabilityChecker(alwaysS3BlobOffloadCapable{}),
	)

	decision := server.s3BlobOffloadDecision(context.Background())
	require.Equal(t, s3BlobOffloadModeOffload, decision.mode)
	require.Equal(t, s3BlobOffloadReasonEnabled, decision.reason)
}

func TestS3Server_PutObjectBlobOffloadFlagFallsBackToLegacyWithoutCapability(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	coord := newLocalAdapterCoordinator(st)
	observer := &recordingS3BlobOffloadObserver{}
	server := NewS3Server(
		nil,
		"",
		st,
		coord,
		nil,
		WithS3BlobOffloadEnabled(true),
		WithS3BlobOffloadObserver(observer),
	)

	rec := httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-a", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-a/object.txt", strings.NewReader("payload")))
	require.Equal(t, http.StatusOK, rec.Code)

	readTS := server.readTS()
	legacy, err := st.ScanAt(context.Background(), []byte(s3keys.BlobPrefix), prefixScanEnd([]byte(s3keys.BlobPrefix)), 10, readTS)
	require.NoError(t, err)
	require.NotEmpty(t, legacy)

	chunkRefs, err := st.ScanAt(context.Background(), []byte(s3keys.ChunkRefPrefix), prefixScanEnd([]byte(s3keys.ChunkRefPrefix)), 10, readTS)
	require.NoError(t, err)
	require.Empty(t, chunkRefs)

	require.Equal(t, []s3BlobOffloadDecision{
		{mode: s3BlobOffloadModeLegacy, reason: s3BlobOffloadReasonCapabilityMissing},
	}, observer.decisions)
}

type alwaysS3BlobOffloadCapable struct{}

func (alwaysS3BlobOffloadCapable) AllPeersSupportS3BlobOffload(context.Context) bool {
	return true
}

type recordingS3BlobOffloadObserver struct {
	decisions           []s3BlobOffloadDecision
	replicationDegraded int
	shaMismatch         int
	unrecoverable       int
}

func (o *recordingS3BlobOffloadObserver) ObserveS3BlobOffloadDecision(mode, reason string) {
	o.decisions = append(o.decisions, s3BlobOffloadDecision{mode: mode, reason: reason})
}

func (o *recordingS3BlobOffloadObserver) ObserveS3ChunkBlobReplicationDegraded() {
	o.replicationDegraded++
}

func (o *recordingS3BlobOffloadObserver) ObserveS3ChunkBlobSHAMismatch() {
	o.shaMismatch++
}

func (o *recordingS3BlobOffloadObserver) ObserveS3ChunkBlobUnrecoverable() {
	o.unrecoverable++
}
