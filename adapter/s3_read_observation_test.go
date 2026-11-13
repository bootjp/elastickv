package adapter

import (
	"bytes"
	"context"
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

func TestS3ReadObservationAttributesConditionalMutationRoute(t *testing.T) {
	coord := &redisReadObservationCoordinator{}
	coord.leaderSet = true
	coord.leader = true
	st := store.NewMVCCStore()
	meta, err := encodeS3BucketMeta(&s3BucketMeta{BucketName: "bucket", Generation: 3})
	require.NoError(t, err)
	require.NoError(t, st.PutAt(context.Background(), s3keys.BucketMetaKey("bucket"), meta, 1, 0))
	server := NewS3Server(nil, "", st, coord, nil)
	t.Cleanup(server.cancelCleanups)

	for _, method := range []string{http.MethodPut, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			coord.keys = nil
			req := httptest.NewRequest(method, "/bucket/object", nil)
			req.Header.Set("If-Match", "stale")
			require.False(t, server.maybeProxyToLeader(httptest.NewRecorder(), req))
			require.Contains(t, coord.keys, s3keys.RouteKey("bucket", 3, "object"))
		})
	}
}
