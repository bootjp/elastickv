package adapter

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestS3ReadObservationMarksServedReads(t *testing.T) {
	coord := &dynamoReadMarkerCoordinator{}
	coord.leaderSet = true
	coord.leader = true
	server := NewS3Server(nil, "", store.NewMVCCStore(), coord, nil)
	t.Cleanup(server.cancelCleanups)
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		req := httptest.NewRequest(method, "/", nil)
		server.handle(httptest.NewRecorder(), req)
	}
	require.Equal(t, []uint64{0}, coord.marks)
}

func TestS3ReadObservationMarksDeniedAnonymousACLProbe(t *testing.T) {
	coord := &redisReadObservationCoordinator{}
	coord.leaderSet = true
	coord.leader = true
	server := NewS3Server(nil, "", store.NewMVCCStore(), coord, nil)
	server.staticCreds = map[string]string{"access": "secret"}
	t.Cleanup(server.cancelCleanups)

	recorder := httptest.NewRecorder()
	server.handle(recorder, httptest.NewRequest(http.MethodGet, "/private/object", nil))
	require.Equal(t, http.StatusForbidden, recorder.Code)
	require.Equal(t, []uint64{0}, coord.marks)
	require.NotEmpty(t, coord.keys)
	for _, key := range coord.keys {
		require.True(t, bytes.Equal(s3keys.BucketMetaKey("private"), key))
	}
}
