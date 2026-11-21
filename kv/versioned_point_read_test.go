package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestVersionedPointReadForwarding(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		resp *pb.RawGetResponse
		want store.VersionedValue
		err  error
	}{
		{
			name: "empty live value",
			resp: &pb.RawGetResponse{Exists: true, Version: &pb.RawGetResponse_CommitTs{CommitTs: 7}},
			want: store.VersionedValue{TS: 7},
		},
		{
			name: "explicit missing",
			resp: &pb.RawGetResponse{Version: &pb.RawGetResponse_Missing{Missing: true}},
			err:  store.ErrKeyNotFound,
		},
		{
			name: "old server reports live value",
			resp: &pb.RawGetResponse{Value: []byte("legacy"), Exists: true},
			err:  store.ErrNotSupported,
		},
		{
			name: "old server reports missing",
			resp: &pb.RawGetResponse{},
			err:  store.ErrNotSupported,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeRawKVServer{getResp: tc.resp}
			addr := startRawKVServer(t, fake)
			key := []byte("key")
			const snapshot = uint64(10)
			const routeVersion = uint64(100)
			routed := NewLeaderRoutedStore(store.NewMVCCStore(), &stubLeaderCoordinator{leader: addr})
			t.Cleanup(func() { require.NoError(t, routed.Close()) })
			got, err := routed.GetVersionAtWithReadFence(t.Context(), key, snapshot, 0, routeVersion)
			require.ErrorIs(t, err, tc.err)
			require.Equal(t, tc.want, got)
			checkForwardedVersionRequest(t, fake, 0, snapshot, routeVersion, key)

			engine := distribution.NewEngine()
			require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
				Version: routeVersion,
				Routes:  []distribution.RouteDescriptor{{RouteID: 1, GroupID: 1, State: distribution.RouteStateActive}},
			}))
			group := &ShardGroup{Store: store.NewMVCCStore(), Engine: newStubFollowerEngine(addr)}
			t.Cleanup(func() { require.NoError(t, group.Store.Close()) })
			group.ConfigureVersionReadAdmission(func(context.Context) error { return nil })
			shards := NewShardStore(engine, map[uint64]*ShardGroup{1: group})
			t.Cleanup(func() { require.NoError(t, shards.Close()) })
			got, err = shards.GetVersionAt(t.Context(), key, snapshot)
			require.ErrorIs(t, err, tc.err)
			require.Equal(t, tc.want, got)
			checkForwardedVersionRequest(t, fake, 1, snapshot, routeVersion, key)

			fake.mu.Lock()
			defer fake.mu.Unlock()
			require.Equal(t, 2, fake.getCalls)
			require.Zero(t, fake.latestCalls)
		})
	}
}

func checkForwardedVersionRequest(t *testing.T, fake *fakeRawKVServer, groupID, snapshot, routeVersion uint64, key []byte) {
	t.Helper()
	fake.mu.Lock()
	defer fake.mu.Unlock()
	req := fake.lastGetReq
	require.True(t, req.GetNeedCommitTs())
	require.Equal(t, groupID, req.GetGroupId())
	require.Equal(t, snapshot, req.GetTs())
	require.Equal(t, routeVersion, req.GetReadRouteVersion())
	require.Equal(t, key, req.GetKey())
}

func TestVersionedPointReadWithStagedVisibilityIsUnsupported(t *testing.T) {
	t.Parallel()
	st, group := newStagedVisibilityShardStore(t)
	group.ConfigureVersionReadAdmission(func(context.Context) error { return nil })
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	key := []byte("k")
	require.NoError(t, group.Store.PutAt(t.Context(), distribution.MigrationStagedDataKey(9, key), []byte("staged"), 20, 0))

	value, err := st.GetAt(t.Context(), key, 25)
	require.NoError(t, err)
	require.Equal(t, []byte("staged"), value)
	_, err = st.GetVersionAt(t.Context(), key, 25)
	require.ErrorIs(t, err, store.ErrNotSupported)
}
