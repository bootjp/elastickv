package adapter

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type s3StaleSnapshotStore struct {
	store.MVCCStore
	localWatermark uint64
	floorError     error
	fencedKeys     [][][]byte
}

func (s *s3StaleSnapshotStore) LastCommitTS() uint64 { return s.localWatermark }

func (s *s3StaleSnapshotStore) CommittedTimestampFloorForKeys(_ context.Context, keys [][]byte) (uint64, error) {
	s.fencedKeys = append(s.fencedKeys, keys)
	return s.MVCCStore.LastCommitTS(), s.floorError
}

func TestS3GetObjectFencesBeforeChoosingItsSnapshot(t *testing.T) {
	for _, head := range []bool{false, true} {
		t.Run(map[bool]string{false: "GET", true: "HEAD"}[head], func(t *testing.T) {
			st := store.NewMVCCStore()
			srv := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)
			put := func(path, body string) {
				rec := httptest.NewRecorder()
				srv.handle(rec, newS3TestRequest(http.MethodPut, path, strings.NewReader(body)))
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			}
			put("/bucket", "")
			put("/bucket/key", "0")
			old := st.LastCommitTS()
			put("/bucket/key", "1")
			lagging := &s3StaleSnapshotStore{MVCCStore: st, localWatermark: old}
			srv.store = lagging
			rec := httptest.NewRecorder()
			method := http.MethodGet
			if head {
				method = http.MethodHead
			}
			srv.handle(rec, newS3TestRequest(method, "/bucket/key", nil))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			require.Equal(t, `"`+md5Hex("1")+`"`, rec.Header().Get("ETag"))
			if !head {
				require.Equal(t, "1", rec.Body.String())
			}
			require.NotEmpty(t, lagging.fencedKeys)
			require.Contains(t, lagging.fencedKeys[0], s3keys.BucketMetaKey("bucket"))
			require.Equal(t, [][]byte{s3keys.BucketMetaKey("bucket"), s3keys.ObjectManifestKey("bucket", 1, "key")}, lagging.fencedKeys[1])
			lagging.floorError = errors.New("read fence unavailable")
			rec = httptest.NewRecorder()
			srv.handle(rec, newS3TestRequest(method, "/bucket/key", nil))
			require.GreaterOrEqual(t, rec.Code, http.StatusInternalServerError)
		})
	}
}

func (s *s3StaleSnapshotStore) GlobalCommittedTimestampFloor(context.Context) (uint64, error) {
	return s.MVCCStore.LastCommitTS(), s.floorError
}

func TestS3MetadataReadsFenceLaggingLocalWatermarks(t *testing.T) {
	st := store.NewMVCCStore()
	t.Cleanup(func() { _ = st.Close() })
	srv := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)
	for _, path := range []string{"/bucket", "/bucket/key"} {
		rec := httptest.NewRecorder()
		srv.handle(rec, newS3TestRequest(http.MethodPut, path, strings.NewReader("1")))
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	}
	srv.store = &s3StaleSnapshotStore{MVCCStore: st, localWatermark: 1}
	for _, tc := range []struct{ method, path, body string }{
		{http.MethodHead, "/bucket", ""},
		{http.MethodGet, "/", "<Name>bucket</Name>"},
		{http.MethodGet, "/bucket?list-type=2", "<Key>key</Key>"},
		{http.MethodGet, "/bucket?versioning", "VersioningConfiguration"},
		{http.MethodGet, "/bucket?acl", "AccessControlPolicy"},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			srv.handle(rec, newS3TestRequest(tc.method, tc.path, nil))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			require.Contains(t, rec.Body.String(), tc.body)
		})
	}
}
