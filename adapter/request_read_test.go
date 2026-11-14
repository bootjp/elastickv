package adapter

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type requestScopeStore struct {
	store.MVCCStore
	observed []*kv.ReadRequest
}

func (s *requestScopeStore) GetAt(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	request, ok := kv.ReadRequestFromContext(ctx)
	if !ok {
		panic("unscoped adapter read")
	}
	s.observed = append(s.observed, request)
	return s.MVCCStore.GetAt(ctx, key, ts)
}

func (s *requestScopeStore) ScanAt(ctx context.Context, start, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	request, ok := kv.ReadRequestFromContext(ctx)
	if !ok {
		panic("unscoped adapter scan")
	}
	s.observed = append(s.observed, request)
	return s.MVCCStore.ScanAt(ctx, start, end, limit, ts)
}

func TestRequestReadBridgeRejectsUnissuedAndRetiredTimestamps(t *testing.T) {
	base := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	require.NoError(t, base.PutAt(t.Context(), []byte("key"), []byte("new"), 42, 0))
	st := &requestScopeStore{MVCCStore: base}
	request := kv.NewReadRequest(t.Context(), newLocalAdapterCoordinator(st), st, nil)
	defer request.Close()
	snapshot, err := request.Latest(request.Context(), "test")
	require.NoError(t, err)
	_, err = requestGetAt(request.Context(), st, []byte("key"), 2)
	require.ErrorIs(t, err, kv.ErrReadSnapshotUnissued)
	require.Empty(t, st.observed)
	value, err := requestGetAt(request.Context(), st, []byte("key"), snapshot.Timestamp())
	require.NoError(t, err)
	require.Equal(t, "new", string(value))
	_, err = requestGetAt(request.Context(), base, []byte("key"), snapshot.Timestamp())
	require.ErrorIs(t, err, kv.ErrReadSnapshotScope)
	request.Close()
	_, err = requestGetAt(request.Context(), st, []byte("key"), snapshot.Timestamp())
	require.ErrorIs(t, err, kv.ErrReadRequestClosed)
	require.Len(t, st.observed, 1)
}

func TestGRPCReadIngressCreatesAndClosesDistinctScopes(t *testing.T) {
	base := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	require.NoError(t, base.PutAt(t.Context(), []byte("key"), []byte("value"), 1, 0))
	st := &requestScopeStore{MVCCStore: base}
	tracker := kv.NewActiveTimestampTracker()
	srv := NewGRPCServer(st, newLocalAdapterCoordinator(st), WithGRPCActiveTimestampTracker(tracker))
	parent := kv.NewReadRequest(t.Context(), nil, st, nil)
	defer parent.Close()
	for range 2 {
		_, err := srv.Get(parent.Context(), &pb.GetRequest{Key: []byte("key")})
		require.NoError(t, err)
		_, err = srv.Scan(parent.Context(), &pb.ScanRequest{Limit: 10})
		require.NoError(t, err)
	}
	require.Len(t, st.observed, 4)
	seen := map[*kv.ReadRequest]bool{}
	for _, scope := range st.observed {
		require.NotSame(t, parent, scope)
		require.False(t, seen[scope])
		seen[scope] = true
		_, err := scope.Issued(scope.Context(), 1)
		require.ErrorIs(t, err, kv.ErrReadRequestClosed)
	}
	require.NoError(t, parent.Context().Err())
	require.Zero(t, tracker.Oldest())
}

func TestS3HTTPReadIngressDoesNotReuseCallerScope(t *testing.T) {
	base := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	st := &requestScopeStore{MVCCStore: base}
	srv := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)
	t.Cleanup(srv.cancelCleanups)
	parent := kv.NewReadRequest(t.Context(), nil, st, nil)
	defer parent.Close()
	var previous *kv.ReadRequest
	for range 2 {
		rec := httptest.NewRecorder()
		req := newS3TestRequest(http.MethodHead, "/missing", nil).WithContext(parent.Context())
		srv.handle(rec, req)
		require.Equal(t, http.StatusNotFound, rec.Code)
		require.NotEmpty(t, st.observed)
		scope := st.observed[len(st.observed)-1]
		require.NotSame(t, parent, scope)
		if previous != nil {
			require.NotSame(t, previous, scope)
		}
		previous = scope
		_, err := scope.Issued(scope.Context(), 1)
		require.ErrorIs(t, err, kv.ErrReadRequestClosed)
	}
	require.NoError(t, parent.Context().Err())
}

func TestS3AdminStreamRetainsRequestUntilClose(t *testing.T) {
	base := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	tracker := kv.NewActiveTimestampTracker()
	srv := NewS3Server(nil, "", base, newLocalAdapterCoordinator(base), nil, WithS3ActiveTimestampTracker(tracker))
	t.Cleanup(srv.cancelCleanups)
	createBucketForAdminTest(t, srv, "stream-bucket")
	require.NoError(t, srv.AdminPutObject(t.Context(), fullAdminBucketsPrincipal(), "stream-bucket", "object", strings.NewReader("streamed"), "text/plain"))
	body, _, err := srv.AdminGetObject(t.Context(), fullAdminBucketsPrincipal(), "stream-bucket", "object")
	require.NoError(t, err)
	defer body.Close()
	reader, ok := body.(*s3AdminObjectReader)
	require.True(t, ok)
	require.NoError(t, reader.readRequest.Context().Err())
	require.NotZero(t, tracker.Oldest())
	content, err := io.ReadAll(body)
	require.NoError(t, err)
	require.Equal(t, "streamed", string(content))
	require.NoError(t, reader.readRequest.Context().Err())
	require.NoError(t, body.Close())
	require.ErrorIs(t, reader.readRequest.Context().Err(), context.Canceled)
	require.Zero(t, tracker.Oldest())
	_, err = body.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.ErrClosedPipe)
}

func TestManifestVersionReadDoesNotExpandMetadataCapability(t *testing.T) {
	base := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	require.NoError(t, base.PutAt(t.Context(), []byte("metadata"), []byte("manifest"), 42, 0))
	require.NoError(t, base.PutAt(t.Context(), []byte("body-version"), []byte("body"), 42, 0))
	st := &keyedReadScopeStore{MVCCStore: base}
	request := kv.NewReadRequest(t.Context(), nil, st, nil)
	defer request.Close()
	snapshot, err := request.Latest(request.Context(), "metadata", []byte("metadata"))
	require.NoError(t, err)
	body, err := manifestVersionGetAt(request.Context(), st, []byte("body-version"), snapshot.Timestamp())
	require.NoError(t, err)
	require.Equal(t, "body", string(body))
	_, err = request.Get(request.Context(), snapshot, []byte("body-version"))
	require.ErrorIs(t, err, kv.ErrReadSnapshotKeyScope)
	_, err = manifestVersionGetAt(request.Context(), st, []byte("body-version"), 41)
	require.ErrorIs(t, err, kv.ErrReadSnapshotUnissued)
}
