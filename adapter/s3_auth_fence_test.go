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

type s3AuthFenceStore struct {
	store.MVCCStore
	globalCalls int
	keyedCalls  [][][]byte
}

func (s *s3AuthFenceStore) GlobalCommittedTimestampFloor(context.Context) (uint64, error) {
	s.globalCalls++
	return s.LastCommitTS(), nil
}

func (s *s3AuthFenceStore) CommittedTimestampFloorForKeys(_ context.Context, keys [][]byte) (uint64, error) {
	s.keyedCalls = append(s.keyedCalls, keys)
	return s.LastCommitTS(), nil
}

func TestS3InvalidAuthDoesNotFenceOrProxy(t *testing.T) {
	for _, tc := range []struct{ name, method, path, auth string }{
		{"malformed-signature", http.MethodGet, "/bucket/object", "invalid"},
		{"malformed-presign", http.MethodHead, "/bucket/object?X-Amz-Algorithm=invalid", ""},
		{"unsigned-write", http.MethodPut, "/bucket/object", ""},
		{"unsigned-root", http.MethodGet, "/", ""},
		{"unsigned-acl", http.MethodGet, "/bucket?acl", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := &s3AuthFenceStore{MVCCStore: store.NewMVCCStore()}
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			srv := NewS3Server(nil, "", st, &followerS3Coordinator{}, nil,
				WithS3StaticCredentials(map[string]string{testS3AccessKey: testS3SecretKey}))
			req := newS3TestRequest(tc.method, tc.path, nil)
			req.Header.Set("Authorization", tc.auth)
			rec := httptest.NewRecorder()
			srv.handle(rec, req)
			require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
			require.Zero(t, st.globalCalls)
			require.Empty(t, st.keyedCalls)
		})
	}
}

func TestS3AnonymousACLOnlyFencesBucket(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		t.Run(method, func(t *testing.T) {
			st := &s3AuthFenceStore{MVCCStore: store.NewMVCCStore()}
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			srv := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil,
				WithS3StaticCredentials(map[string]string{testS3AccessKey: testS3SecretKey}))
			rec := httptest.NewRecorder()
			srv.handle(rec, newS3TestRequest(method, "/private/object", nil))
			require.Equal(t, http.StatusForbidden, rec.Code)
			require.Zero(t, st.globalCalls)
			require.Equal(t, [][][]byte{{s3keys.BucketMetaKey("private")}}, st.keyedCalls)
		})
	}
}

func TestS3PublicObjectReadDoesNotFenceGlobally(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		t.Run(method, func(t *testing.T) {
			base := store.NewMVCCStore()
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			srv := NewS3Server(nil, "", base, &s3ReadSnapshotTestCoordinator{Coordinator: newLocalAdapterCoordinator(base)}, nil,
				WithS3StaticCredentials(map[string]string{testS3AccessKey: testS3SecretKey}))
			stamp := currentS3SigningTime()
			req := newSignedS3Request(t, "/public", "", stamp)
			req.Header.Set("x-amz-acl", "public-read")
			req = resignS3Request(t, req, stamp)
			rec := httptest.NewRecorder()
			srv.handle(rec, req)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			rec = httptest.NewRecorder()
			srv.handle(rec, newSignedS3Request(t, "/public/object", "data", stamp))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			st := &s3AuthFenceStore{MVCCStore: base}
			srv.store = st
			rec = httptest.NewRecorder()
			srv.handle(rec, newS3TestRequest(method, "/public/object", nil))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			require.Zero(t, st.globalCalls)
			require.NotEmpty(t, st.keyedCalls)
			require.Equal(t, [][]byte{s3keys.BucketMetaKey("public")}, st.keyedCalls[0])
			for _, keys := range st.keyedCalls {
				for _, key := range keys {
					require.True(t, strings.Contains(string(key), "public"))
				}
			}
			if method == http.MethodGet {
				require.Equal(t, "data", rec.Body.String())
			}
		})
	}
}
