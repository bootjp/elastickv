package kv

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/s3keys"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type followerProxyEngine struct {
	leader string
}

func (e *followerProxyEngine) Propose(context.Context, []byte) (*raftengine.ProposalResult, error) {
	return nil, ErrLeaderNotFound
}

func (e *followerProxyEngine) ProposeAdmin(context.Context, []byte) (*raftengine.ProposalResult, error) {
	return nil, ErrLeaderNotFound
}

func (e *followerProxyEngine) State() raftengine.State {
	return raftengine.StateFollower
}

func (e *followerProxyEngine) Leader() raftengine.LeaderInfo {
	return raftengine.LeaderInfo{Address: e.leader}
}

func (e *followerProxyEngine) VerifyLeader(context.Context) error {
	return ErrLeaderNotFound
}

func (e *followerProxyEngine) LinearizableRead(context.Context) (uint64, error) {
	return 0, ErrLeaderNotFound
}

func (e *followerProxyEngine) Status() raftengine.Status {
	return raftengine.Status{
		State:  raftengine.StateFollower,
		Leader: raftengine.LeaderInfo{Address: e.leader},
	}
}

func (e *followerProxyEngine) Configuration(context.Context) (raftengine.Configuration, error) {
	return raftengine.Configuration{}, nil
}

func (e *followerProxyEngine) Close() error {
	return nil
}

func TestShardStoreScanAt_IncludesListKeysAcrossShards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 1, 0))
	require.NoError(t, st.PutAt(ctx, []byte("b"), []byte("vb"), 2, 0))

	userKey := []byte("x")
	itemKey := store.ListItemKey(userKey, 0)
	require.NoError(t, st.PutAt(ctx, itemKey, []byte("v0"), 3, 0))

	// A full scan should surface internal list keys that may live on any shard.
	kvs, err := st.ScanAt(ctx, []byte(""), nil, 1, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, itemKey, kvs[0].Key)
}

// SplitRange normalizes every boundary through kv.RouteKey before storing it
// (adapter/distribution_server.go), so a boundary governing a Redis wide-column
// family is always a decoded user key -- never a raw !hs|fld|-prefixed byte
// string. The fence must therefore cover the group owning the logical user key,
// plus the legacy raw-prefix group when rows may still be routed that way
// (redisWideColumnLegacyScanRouteRange contributes that second query).
//
// The earlier form of this test split at prefix+'m', a raw-prefixed boundary
// SplitRange cannot produce, and asserted a raw range intersection. That
// comparison puts raw storage bytes against decoded user-key boundaries and
// selects the wrong owning route, so it did not describe a reachable state.
func TestShardStoreReadFenceGroupKeysForRangeCoversOwningAndLegacyGroups(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		userKey    string
		wantOwner  uint64
		wantGroups []uint64
	}{
		// "alpha" sorts below the boundary, so the owning group and the legacy
		// raw-prefix group are the same one and collapse to a single fence key.
		{name: "owner below boundary", userKey: "alpha", wantOwner: 1, wantGroups: []uint64{1}},
		// "zebra" sorts above it while the raw !hs|fld| prefix sorts below, so
		// the two queries land on different groups and both must be fenced.
		{name: "owner above boundary", userKey: "zebra", wantOwner: 2, wantGroups: []uint64{2, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			prefix := store.HashFieldScanPrefix([]byte(tc.userKey))
			engine := distribution.NewEngine()
			engine.UpdateRoute([]byte(""), []byte("m"), 1)
			engine.UpdateRoute([]byte("m"), nil, 2)
			st := NewShardStore(engine, map[uint64]*ShardGroup{
				1: {},
				2: {},
			})

			routes, _ := st.routesForForwardScan(prefix, store.PrefixScanEnd(prefix))
			gotGroups := make([]uint64, 0, len(routes))
			for _, route := range routes {
				gotGroups = append(gotGroups, route.GroupID)
			}
			require.Equal(t, tc.wantGroups, gotGroups)
			require.Equal(t, tc.wantOwner, routes[0].GroupID,
				"the logical user key's owner must be resolved first")

			got := st.ReadFenceGroupKeysForRange(prefix, store.PrefixScanEnd(prefix))
			require.Len(t, got, len(tc.wantGroups))
			require.Contains(t, got, prefix,
				"the queried prefix must be fenced so groupForKey re-derives the owner")
		})
	}
}

func TestShardStoreReadFenceGroupKeysForListRangeUsesStorageRepresentative(t *testing.T) {
	t.Parallel()

	for _, userKey := range [][]byte{
		[]byte("!sqs|foo"),
		[]byte("!redis|str|foo"),
	} {
		t.Run(string(userKey), func(t *testing.T) {
			t.Parallel()
			prefix := store.ListMetaDeltaScanPrefix(userKey)
			engine := distribution.NewEngine()
			engine.UpdateRoute([]byte(""), userKey, 1)
			engine.UpdateRoute(userKey, nil, 2)
			st := NewShardStore(engine, map[uint64]*ShardGroup{
				1: {},
				2: {},
			})

			got := st.ReadFenceGroupKeysForRange(prefix, store.PrefixScanEnd(prefix))

			require.Equal(t, [][]byte{prefix}, got)
			require.Equal(t, userKey, routeKey(got[0]))
		})
	}
}

func TestShardStoreScanAt_RoutesListItemScansByUserKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	userKey := []byte("x") // routes to group 2
	k0 := store.ListItemKey(userKey, 0)
	k1 := store.ListItemKey(userKey, 1)
	k2 := store.ListItemKey(userKey, 2)
	require.NoError(t, st.PutAt(ctx, k0, []byte("v0"), 1, 0))
	require.NoError(t, st.PutAt(ctx, k1, []byte("v1"), 2, 0))
	require.NoError(t, st.PutAt(ctx, k2, []byte("v2"), 3, 0))

	end := store.ListItemKey(userKey, 3) // exclusive upper bound
	kvs, err := st.ScanAt(ctx, k0, end, 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 3)
	require.Equal(t, k0, kvs[0].Key)
	require.Equal(t, k1, kvs[1].Key)
	require.Equal(t, k2, kvs[2].Key)
}

func TestShardStoreScanAtWithReadFence_RoutesListAuxiliaryScansByUserKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	t.Cleanup(func() {
		_ = groups[1].Store.Close()
		_ = groups[2].Store.Close()
	})
	shardStore := NewShardStore(engine, groups)
	userKey := []byte("x")
	deltaKey := store.ListMetaDeltaKey(userKey, 10, 0)
	claimKey := store.ListClaimKey(userKey, 1)
	require.NoError(t, groups[2].Store.PutAt(ctx, deltaKey, []byte("delta"), 10, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, claimKey, []byte("claim"), 11, 0))

	for _, tc := range []struct {
		name   string
		prefix []byte
		key    []byte
	}{
		{name: "delta", prefix: store.ListMetaDeltaScanPrefix(userKey), key: deltaKey},
		{name: "claim", prefix: store.ListClaimScanPrefix(userKey), key: claimKey},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kvs, err := shardStore.ScanAtWithReadFence(
				ctx, tc.prefix, prefixScanEnd(tc.prefix), 10, ^uint64(0), false, 0, engine.Version(), nil, nil,
			)
			require.NoError(t, err)
			require.Len(t, kvs, 1)
			require.Equal(t, tc.key, kvs[0].Key)
		})
	}
}

func TestShardStoreScanAt_RoutesBareListAuxiliaryScansAcrossShards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	t.Cleanup(func() {
		_ = groups[1].Store.Close()
		_ = groups[2].Store.Close()
	})
	st := NewShardStore(engine, groups)

	left := store.ListMetaDeltaKey([]byte("anna"), 10, 0)
	right := store.ListMetaDeltaKey([]byte("zoey"), 11, 0)
	require.NoError(t, groups[1].Store.PutAt(ctx, left, []byte("left"), 10, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, right, []byte("right"), 11, 0))

	prefix := []byte(store.ListMetaDeltaPrefix)
	kvs, err := st.ScanAt(ctx, prefix, prefixScanEnd(prefix), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, [][]byte{left, right}, [][]byte{kvs[0].Key, kvs[1].Key})
}

func TestShardStoreScanGroupAt_UsesExplicitGroup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)
	groups := map[uint64]*ShardGroup{
		1:  {Store: store.NewMVCCStore()},
		42: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	key := []byte("!sqs|msg|vis|p|orders|partition-2")
	require.NoError(t, groups[42].Store.PutAt(ctx, key, []byte("msg-2"), 7, 0))

	kvs, err := st.ScanGroupAt(ctx, 42, []byte("!sqs|msg|vis|p|"), prefixScanEnd([]byte("!sqs|msg|vis|p|")), 10, 7)
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, key, kvs[0].Key)
	require.Equal(t, []byte("msg-2"), kvs[0].Value)
}

func TestShardStoreGetGroupAt_UsesExplicitGroup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)
	groups := map[uint64]*ShardGroup{
		1:  {Store: store.NewMVCCStore()},
		42: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	key := []byte("!sqs|msg|data|p|orders|partition-2|msg-2")
	require.NoError(t, groups[42].Store.PutAt(ctx, key, []byte("payload"), 7, 0))

	val, err := st.GetGroupAt(ctx, 42, key, 7)
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), val)

	_, err = st.GetAt(ctx, key, 7)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestShardStoreWritePathsRejectRouteWriteTimestampFloor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{
				RouteID:             1,
				Start:               []byte(""),
				End:                 nil,
				GroupID:             1,
				State:               distribution.RouteStateActive,
				MinWriteTSExclusive: 10,
			},
		},
	}))
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
	}
	t.Cleanup(func() {
		_ = groups[1].Store.Close()
	})
	st := NewShardStore(engine, groups)

	require.ErrorIs(t, st.PutAt(ctx, []byte("put-stale"), []byte("v"), 10, 0), store.ErrWriteConflict)
	require.NoError(t, st.PutAt(ctx, []byte("put-fresh"), []byte("v"), 11, 0))

	require.ErrorIs(t, st.DeleteAt(ctx, []byte("delete-stale"), 10), store.ErrWriteConflict)
	require.NoError(t, st.DeleteAt(ctx, []byte("delete-fresh"), 11))

	require.ErrorIs(t, st.PutWithTTLAt(ctx, []byte("ttl-stale"), []byte("v"), 10, 99), store.ErrWriteConflict)
	require.NoError(t, st.PutWithTTLAt(ctx, []byte("ttl-fresh"), []byte("v"), 11, 99))

	require.ErrorIs(t, st.ExpireAt(ctx, []byte("expire-stale"), 99, 10), store.ErrWriteConflict)
	require.NoError(t, st.PutAt(ctx, []byte("expire-fresh"), []byte("v"), 11, 0))
	require.NoError(t, st.ExpireAt(ctx, []byte("expire-fresh"), 99, 12))

	require.ErrorIs(t, st.ApplyMutations(ctx, []*store.KVPairMutation{
		{Op: store.OpTypePut, Key: []byte("apply-stale"), Value: []byte("v")},
	}, nil, 0, 10), store.ErrWriteConflict)
	require.NoError(t, st.ApplyMutations(ctx, []*store.KVPairMutation{
		{Op: store.OpTypePut, Key: []byte("apply-fresh"), Value: []byte("v")},
	}, nil, 0, 11))

	require.ErrorIs(t, st.ApplyMutationsRaft(ctx, []*store.KVPairMutation{
		{Op: store.OpTypePut, Key: []byte("raft-stale"), Value: []byte("v")},
	}, nil, 0, 10), store.ErrWriteConflict)
	require.NoError(t, st.ApplyMutationsRaft(ctx, []*store.KVPairMutation{
		{Op: store.OpTypePut, Key: []byte("raft-fresh"), Value: []byte("v")},
	}, nil, 0, 11))

	require.ErrorIs(t, st.ApplyMutationsRaftAt(ctx, []*store.KVPairMutation{
		{Op: store.OpTypePut, Key: []byte("raft-at-stale"), Value: []byte("v")},
	}, nil, 0, 10, 1), store.ErrWriteConflict)
	require.NoError(t, st.ApplyMutationsRaftAt(ctx, []*store.KVPairMutation{
		{Op: store.OpTypePut, Key: []byte("raft-at-fresh"), Value: []byte("v")},
	}, nil, 0, 11, 2))

	require.ErrorIs(t, st.DeletePrefixAt(ctx, []byte("prefix-stale"), nil, 10), store.ErrWriteConflict)
	require.NoError(t, st.DeletePrefixAt(ctx, []byte("prefix-fresh"), nil, 11))

	require.ErrorIs(t, st.DeletePrefixAtRaft(ctx, []byte("raft-prefix-stale"), nil, 10), store.ErrWriteConflict)
	require.NoError(t, st.DeletePrefixAtRaft(ctx, []byte("raft-prefix-fresh"), nil, 11))

	require.ErrorIs(t, st.DeletePrefixAtRaftAt(ctx, []byte("raft-at-prefix-stale"), nil, 10, 3), store.ErrWriteConflict)
	require.NoError(t, st.DeletePrefixAtRaftAt(ctx, []byte("raft-at-prefix-fresh"), nil, 11, 4))
}

func TestShardStoreDeletePrefixChecksRedisLogicalRouteFloors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte(""), End: []byte("m"), GroupID: 1, State: distribution.RouteStateActive},
			{RouteID: 2, Start: []byte("m"), GroupID: 2, State: distribution.RouteStateActive, MinWriteTSExclusive: 100},
		},
	}))
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	t.Cleanup(func() {
		_ = groups[1].Store.Close()
		_ = groups[2].Store.Close()
	})
	st := NewShardStore(engine, groups)

	require.ErrorIs(t, st.DeletePrefixAt(ctx, []byte(store.HashFieldPrefix), nil, 100), store.ErrWriteConflict)
	require.ErrorIs(t, st.DeletePrefixAt(ctx, []byte("!lst|"), nil, 100), store.ErrWriteConflict)
	require.ErrorIs(t, st.DeletePrefixAt(ctx, []byte("!redis|hash|"), nil, 100), store.ErrWriteConflict)
	require.NoError(t, st.DeletePrefixAt(ctx, store.HashFieldScanPrefix([]byte("alpha")), nil, 100))
	require.ErrorIs(t, st.DeletePrefixAt(ctx, store.HashFieldScanPrefix([]byte("zulu")), nil, 100), store.ErrWriteConflict)
	require.NoError(t, st.DeletePrefixAt(ctx, store.HashFieldScanPrefix([]byte("zulu")), nil, 101))
}

func TestShardStore_ForwardsReadFenceStamps(t *testing.T) {
	t.Parallel()

	fake := &fakeRawKVServer{
		getResp: &pb.RawGetResponse{
			Exists: true,
			Value:  []byte("remote-v"),
		},
		scanResp: &pb.RawScanAtResponse{},
		latestResp: &pb.RawLatestCommitTSResponse{
			Ts:     42,
			Exists: true,
		},
	}
	addr, stop := startRawKVServer(t, fake)
	t.Cleanup(stop)

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 100,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte(""), GroupID: 1, State: distribution.RouteStateActive},
		},
	}))
	st := NewShardStore(engine, map[uint64]*ShardGroup{
		1: {
			Store:  store.NewMVCCStore(),
			Engine: &stubFollowerEngine{leaderAddr: addr},
		},
	})
	t.Cleanup(func() { _ = st.Close() })

	ctx := context.Background()
	_, err := st.GetAt(ctx, []byte("k"), 10)
	require.NoError(t, err)
	_, _, err = st.LatestCommitTS(ctx, []byte("k"))
	require.NoError(t, err)
	_, err = st.ScanAtWithReadFence(ctx, []byte("a"), []byte("z"), 10, 11, false, 0, 79, []byte("a"), []byte("m"))
	require.NoError(t, err)

	fake.mu.Lock()
	require.Equal(t, uint64(100), fake.lastGetReq.GetReadRouteVersion())
	require.Equal(t, uint64(100), fake.lastLatestReq.GetReadRouteVersion())
	require.Equal(t, uint64(1), fake.lastLatestReq.GetGroupId())
	require.Equal(t, uint64(100), fake.lastScanReq.GetReadRouteVersion())
	require.Equal(t, uint64(1), fake.lastScanReq.GetGroupId())
	require.Equal(t, []byte("a"), fake.lastScanReq.GetRouteStart())
	require.Equal(t, []byte("m"), fake.lastScanReq.GetRouteEnd())
	require.True(t, fake.lastScanReq.GetRouteBoundsPresent())
	fake.mu.Unlock()

	_, err = st.ScanAt(ctx, []byte("a"), []byte("z"), 10, 11)
	require.NoError(t, err)

	fake.mu.Lock()
	require.Equal(t, uint64(1), fake.lastScanReq.GetGroupId())
	require.Equal(t, uint64(100), fake.lastScanReq.GetReadRouteVersion())
	fake.mu.Unlock()

	_, err = st.ScanKeysAtWithReadFence(ctx, []byte("a"), []byte("z"), 10, 11, 0, 82)
	require.NoError(t, err)

	fake.mu.Lock()
	require.Equal(t, uint64(1), fake.lastScanReq.GetGroupId())
	require.Equal(t, uint64(100), fake.lastScanReq.GetReadRouteVersion())
	require.True(t, fake.lastScanReq.GetKeysOnly())
	fake.mu.Unlock()

	_, err = st.ScanAtWithReadFence(ctx, []byte("a"), []byte("z"), 10, 11, false, 0, 80, []byte{}, []byte{})
	require.NoError(t, err)

	fake.mu.Lock()
	require.Equal(t, uint64(1), fake.lastScanReq.GetGroupId())
	require.Equal(t, uint64(100), fake.lastScanReq.GetReadRouteVersion())
	require.True(t, fake.lastScanReq.GetRouteBoundsPresent())
	fake.mu.Unlock()

	_, err = st.ScanAtWithReadFence(ctx, []byte("a"), []byte("z"), 10, 11, false, 0, 81, nil, nil)
	require.NoError(t, err)

	fake.mu.Lock()
	require.Equal(t, uint64(1), fake.lastScanReq.GetGroupId())
	require.Equal(t, uint64(100), fake.lastScanReq.GetReadRouteVersion())
	require.False(t, fake.lastScanReq.GetRouteBoundsPresent())
	fake.mu.Unlock()

	_, err = st.ScanAt(ctx, []byte(""), nil, 10, 11)
	require.NoError(t, err)

	fake.mu.Lock()
	defer fake.mu.Unlock()
	require.Equal(t, uint64(1), fake.lastScanReq.GetGroupId())
	require.Equal(t, uint64(100), fake.lastScanReq.GetReadRouteVersion())
}

func TestShardStoreRoutesForScanUsesWideColumnUserKey(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)
	st := NewShardStore(engine, nil)
	userKey := []byte("z-user")

	for _, tc := range []struct {
		name   string
		prefix []byte
	}{
		{name: "hash fields", prefix: store.HashFieldScanPrefix(userKey)},
		{name: "hash deltas", prefix: store.HashMetaDeltaScanPrefix(userKey)},
		{name: "set members", prefix: store.SetMemberScanPrefix(userKey)},
		{name: "set deltas", prefix: store.SetMetaDeltaScanPrefix(userKey)},
		{name: "zset members", prefix: store.ZSetMemberScanPrefix(userKey)},
		{name: "zset scores", prefix: store.ZSetScoreScanPrefix(userKey)},
		{name: "zset deltas", prefix: store.ZSetMetaDeltaScanPrefix(userKey)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			routes, clamp := st.routesForScan(tc.prefix, prefixScanEnd(tc.prefix))
			require.False(t, clamp)
			require.Len(t, routes, 2)
			require.Equal(t, uint64(2), routes[0].GroupID)
			require.Equal(t, uint64(1), routes[1].GroupID)
		})
	}
}

func TestShardStoreScanAtRoutesWideColumnPrefixesByUserKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	userKey := []byte("z-user")

	for _, tc := range []struct {
		name   string
		key    []byte
		prefix []byte
	}{
		{name: "hash field", key: store.HashFieldKey(userKey, []byte("field")), prefix: store.HashFieldScanPrefix(userKey)},
		{name: "hash delta", key: store.HashMetaDeltaKey(userKey, 10, 1), prefix: store.HashMetaDeltaScanPrefix(userKey)},
		{name: "set member", key: store.SetMemberKey(userKey, []byte("member")), prefix: store.SetMemberScanPrefix(userKey)},
		{name: "set delta", key: store.SetMetaDeltaKey(userKey, 11, 1), prefix: store.SetMetaDeltaScanPrefix(userKey)},
		{name: "zset member", key: store.ZSetMemberKey(userKey, []byte("member")), prefix: store.ZSetMemberScanPrefix(userKey)},
		{name: "zset score", key: store.ZSetScoreKey(userKey, 1.5, []byte("member")), prefix: store.ZSetScoreScanPrefix(userKey)},
		{name: "zset delta", key: store.ZSetMetaDeltaKey(userKey, 12, 1), prefix: store.ZSetMetaDeltaScanPrefix(userKey)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, st.PutAt(ctx, tc.key, []byte("value"), 20, 0))
			kvs, err := st.ScanAt(ctx, tc.prefix, prefixScanEnd(tc.prefix), 10, 20)
			require.NoError(t, err)
			require.Equal(t, []*store.KVPair{{Key: tc.key, Value: []byte("value")}}, kvs)
			_, err = groups[1].Store.GetAt(ctx, tc.key, 20)
			require.ErrorIs(t, err, store.ErrKeyNotFound)
		})
	}
}

func TestShardStoreReadFenceFailsClosedWhileCatalogVersionIsBehind(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte(""), GroupID: 1, State: distribution.RouteStateActive},
		},
	}))
	groupStore := store.NewMVCCStore()
	require.NoError(t, groupStore.PutAt(context.Background(), []byte("k"), []byte("stale"), 1, 0))
	st := NewShardStore(engine, map[uint64]*ShardGroup{1: {Store: groupStore}})

	tests := []struct {
		name string
		read func(context.Context) error
	}{
		{
			name: "point read",
			read: func(ctx context.Context) error {
				_, err := st.GetAtWithReadFence(ctx, []byte("k"), 1, 0, 2)
				return err
			},
		},
		{
			name: "latest commit timestamp",
			read: func(ctx context.Context) error {
				_, _, err := st.LatestCommitTSWithReadFence(ctx, []byte("k"), 2)
				return err
			},
		},
		{
			name: "range scan",
			read: func(ctx context.Context) error {
				_, err := st.ScanAtWithReadFence(ctx, []byte("a"), []byte("z"), 1, 1, false, 0, 2, nil, nil)
				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			err := tc.read(ctx)
			require.ErrorIs(t, err, ErrReadRouteVersionUnavailable)
		})
	}
}

func TestShardStoreReadFenceWaitsForCatalogAndReroutesPointRead(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte(""), GroupID: 1, State: distribution.RouteStateActive},
		},
	}))
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	require.NoError(t, groups[1].Store.PutAt(context.Background(), []byte("k"), []byte("old-owner"), 1, 0))
	require.NoError(t, groups[2].Store.PutAt(context.Background(), []byte("k"), []byte("new-owner"), 1, 0))
	st := NewShardStore(engine, groups)

	applyErr := make(chan error, 1)
	go func() {
		time.Sleep(20 * time.Millisecond)
		applyErr <- engine.ApplySnapshot(distribution.CatalogSnapshot{
			Version: 2,
			Routes: []distribution.RouteDescriptor{
				{RouteID: 1, Start: []byte(""), GroupID: 2, State: distribution.RouteStateActive},
			},
		})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	value, err := st.GetAtWithReadFence(ctx, []byte("k"), 1, 0, 2)
	require.NoError(t, err)
	require.Equal(t, []byte("new-owner"), value)
	require.NoError(t, <-applyErr)
}

func TestShardStoreScanAtWithReadFence_RoutesUsingSuppliedBounds(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	rawPrefix := []byte("!redis|meta|")
	first := []byte("!redis|meta|x")
	second := []byte("!redis|meta|y")
	require.NoError(t, groups[2].Store.PutAt(ctx, first, []byte("v1"), 1, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, second, []byte("v2"), 2, 0))

	kvs, err := st.ScanAtWithReadFence(ctx, rawPrefix, prefixScanEnd(rawPrefix), 10, 2, false, 0, st.ReadRouteVersion(), []byte("m"), nil)
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, first, kvs[0].Key)
	require.Equal(t, second, kvs[1].Key)

	kvs, err = st.ScanAtWithReadFence(ctx, rawPrefix, prefixScanEnd(rawPrefix), 10, 2, true, 0, st.ReadRouteVersion(), []byte("m"), nil)
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, second, kvs[0].Key)
	require.Equal(t, first, kvs[1].Key)
}

func TestShardStoreScanAtWithReadFence_ScansSameGroupSuppliedBoundsAcrossRouteIntervals(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 1)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	rawPrefix := []byte("!redis|meta|")
	first := []byte("!redis|meta|a")
	second := []byte("!redis|meta|z")
	require.NoError(t, groups[1].Store.PutAt(ctx, first, []byte("v1"), 1, 0))
	require.NoError(t, groups[1].Store.PutAt(ctx, second, []byte("v2"), 2, 0))

	for _, tc := range []struct {
		name       string
		reverse    bool
		routeStart []byte
		routeEnd   []byte
		want       [][]byte
	}{
		{
			name:       "left interval only",
			routeStart: []byte("a"),
			routeEnd:   []byte("z"),
			want:       [][]byte{first},
		},
		{
			name:       "forward across intervals",
			routeStart: []byte("a"),
			want:       [][]byte{first, second},
		},
		{
			name:       "reverse across intervals",
			reverse:    true,
			routeStart: []byte("a"),
			want:       [][]byte{second, first},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kvs, err := st.ScanAtWithReadFence(ctx, rawPrefix, prefixScanEnd(rawPrefix), 10, 2, tc.reverse, 0, st.ReadRouteVersion(), tc.routeStart, tc.routeEnd)
			require.NoError(t, err)
			require.Len(t, kvs, len(tc.want))
			for i, want := range tc.want {
				require.Equal(t, want, kvs[i].Key)
			}
		})
	}
}

func TestShardStoreScanAtWithReadFence_FiltersWideRedisKeysByUserKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 1)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	rawPrefix := []byte("!hs|")
	left := store.HashFieldKey([]byte("alpha"), []byte("f"))
	right := store.HashFieldKey([]byte("zulu"), []byte("f"))
	require.NoError(t, groups[1].Store.PutAt(ctx, left, []byte("left"), 1, 0))
	require.NoError(t, groups[1].Store.PutAt(ctx, right, []byte("right"), 2, 0))

	kvs, err := st.ScanAtWithReadFence(ctx, rawPrefix, prefixScanEnd(rawPrefix), 1, 2, false, 0, st.ReadRouteVersion(), []byte("m"), nil)
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, right, kvs[0].Key)

	kvs, err = st.ScanAtWithReadFence(ctx, rawPrefix, prefixScanEnd(rawPrefix), 1, 2, true, 0, st.ReadRouteVersion(), []byte{}, []byte("m"))
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, left, kvs[0].Key)
}

func TestShardStoreScanAtWithReadFence_FiltersSuppliedBoundsByRouteKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 1)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	rawPrefix := []byte("!redis|meta|")
	left := []byte("!redis|meta|a")
	right := []byte("!redis|meta|z")
	require.NoError(t, groups[1].Store.PutAt(ctx, left, []byte("left"), 1, 0))
	require.NoError(t, groups[1].Store.PutAt(ctx, right, []byte("right"), 2, 0))

	kvs, err := st.ScanAtWithReadFence(ctx, rawPrefix, prefixScanEnd(rawPrefix), 1, 2, false, 0, st.ReadRouteVersion(), []byte("m"), nil)
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, right, kvs[0].Key)

	kvs, err = st.ScanAtWithReadFence(ctx, rawPrefix, prefixScanEnd(rawPrefix), 1, 2, true, 0, st.ReadRouteVersion(), []byte{}, []byte("m"))
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, left, kvs[0].Key)
}

func TestShardStoreScanAtWithReadFence_FiltersRedisAuxiliaryBoundsByRouteKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 1)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
	}
	t.Cleanup(func() { _ = groups[1].Store.Close() })
	st := NewShardStore(engine, groups)

	for _, tc := range []struct {
		name   string
		prefix []byte
		left   []byte
		right  []byte
	}{
		{
			name:   "list delta",
			prefix: []byte(store.ListMetaDeltaPrefix),
			left:   store.ListMetaDeltaKey([]byte("alpha"), 10, 0),
			right:  store.ListMetaDeltaKey([]byte("zulu"), 11, 0),
		},
		{
			name:   "list claim",
			prefix: []byte(store.ListClaimPrefix),
			left:   store.ListClaimKey([]byte("alpha"), 1),
			right:  store.ListClaimKey([]byte("zulu"), 1),
		},
		{
			name:   "stream meta",
			prefix: []byte(store.StreamMetaPrefix),
			left:   store.StreamMetaKey([]byte("alpha")),
			right:  store.StreamMetaKey([]byte("zulu")),
		},
		{
			name:   "stream entry",
			prefix: []byte(store.StreamEntryPrefix),
			left:   store.StreamEntryKey([]byte("alpha"), 1, 0),
			right:  store.StreamEntryKey([]byte("zulu"), 1, 0),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, groups[1].Store.PutAt(ctx, tc.left, []byte("left"), 1, 0))
			require.NoError(t, groups[1].Store.PutAt(ctx, tc.right, []byte("right"), 2, 0))

			kvs, err := st.ScanAtWithReadFence(ctx, tc.prefix, prefixScanEnd(tc.prefix), 1, 2, false, 0, st.ReadRouteVersion(), []byte("m"), nil)
			require.NoError(t, err)
			require.Len(t, kvs, 1)
			require.Equal(t, tc.right, kvs[0].Key)
		})
	}
}

func TestShardStoreScanAtWithReadFence_FiltersByEachRouteBounds(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), []byte("z"), 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	rawPrefix := []byte("!redis|meta|")
	left := []byte("!redis|meta|b")
	staleRightOnLeftGroup := []byte("!redis|meta|x")
	right := []byte("!redis|meta|y")
	require.NoError(t, groups[1].Store.PutAt(ctx, left, []byte("left"), 1, 0))
	require.NoError(t, groups[1].Store.PutAt(ctx, staleRightOnLeftGroup, []byte("stale"), 2, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, right, []byte("right"), 3, 0))

	kvs, err := st.ScanAtWithReadFence(ctx, rawPrefix, prefixScanEnd(rawPrefix), 10, 3, false, 0, st.ReadRouteVersion(), []byte("a"), []byte("z"))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, left, kvs[0].Key)
	require.Equal(t, right, kvs[1].Key)
}

func TestShardStoreScanAtWithReadFence_ServesExplicitGroupReverse(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	rawPrefix := []byte("!redis|meta|")
	left := []byte("!redis|meta|a")
	right := []byte("!redis|meta|z")
	require.NoError(t, groups[1].Store.PutAt(ctx, left, []byte("left"), 1, 0))
	require.NoError(t, groups[1].Store.PutAt(ctx, right, []byte("right"), 2, 0))

	// An unbounded explicit-group reverse scan is served through the fenced
	// route path rather than rejected. Rejecting it here only pushed callers
	// back onto the unfenced ReverseScanGroupAt shortcut in the gRPC server.
	unbounded, err := st.ScanAtWithReadFence(ctx, rawPrefix, prefixScanEnd(rawPrefix), 1, 2, true, 1, st.ReadRouteVersion(), nil, nil)
	require.NoError(t, err)
	require.Len(t, unbounded, 1)
	require.Equal(t, right, unbounded[0].Key)

	kvs, err := st.ScanAtWithReadFence(ctx, rawPrefix, prefixScanEnd(rawPrefix), -1, 2, true, 1, st.ReadRouteVersion(), []byte("m"), nil)
	require.NoError(t, err)
	require.Empty(t, kvs)

	kvs, err = st.ScanAtWithReadFence(ctx, rawPrefix, prefixScanEnd(rawPrefix), 1, 2, true, 1, st.ReadRouteVersion(), []byte("m"), nil)
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, right, kvs[0].Key)
	require.Equal(t, []byte("right"), kvs[0].Value)
}

func TestShardStoreScanAt_IncludesS3ManifestKeysAcrossShards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	k1 := s3keys.ObjectManifestKey("bucket-a", 1, "alpha")
	k2 := s3keys.ObjectManifestKey("bucket-a", 1, "zeta")
	require.NoError(t, st.PutAt(ctx, k1, []byte("m1"), 1, 0))
	require.NoError(t, st.PutAt(ctx, k2, []byte("m2"), 2, 0))

	start := s3keys.ObjectManifestPrefixForBucket("bucket-a", 1)
	kvs, err := st.ScanAt(ctx, start, prefixScanEnd(start), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, k1, kvs[0].Key)
	require.Equal(t, k2, kvs[1].Key)
}

func TestShardStoreScanKeysAt_IncludesKeysAcrossShards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 1, 0))
	require.NoError(t, st.PutAt(ctx, []byte("b"), []byte("vb"), 2, 0))
	require.NoError(t, st.DeleteAt(ctx, []byte("b"), 3))
	require.NoError(t, st.PutAt(ctx, []byte("x"), []byte("vx"), 4, 0))
	require.NoError(t, st.PutAt(ctx, []byte("z"), []byte("vz"), 5, 0))

	keys, err := st.ScanKeysAt(ctx, []byte(""), nil, 2, ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("a"), []byte("x")}, keys)
}

func TestShardStoreScanKeysAt_DeduplicatesUnclampedGroups(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 1)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 1, 0))
	require.NoError(t, st.PutAt(ctx, []byte("z"), []byte("vz"), 2, 0))

	keys, err := st.ScanKeysAt(ctx, []byte(""), nil, 10, ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("a"), []byte("z")}, keys)
}

func TestShardStoreScanAt_DeduplicatesUnclampedGroups(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 1)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 1, 0))
	require.NoError(t, st.PutAt(ctx, []byte("z"), []byte("vz"), 2, 0))

	kvs, err := st.ScanAt(ctx, []byte(""), nil, 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, []byte("a"), kvs[0].Key)
	require.Equal(t, []byte("z"), kvs[1].Key)
}

func TestBackupScannerDeduplicatesUnclampedGroups(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 1)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 1, 0))
	require.NoError(t, st.PutAt(ctx, []byte("z"), []byte("vz"), 2, 0))

	sc := st.NewBackupScanner([]byte(""), nil, ^uint64(0), 10)
	defer sc.Close()

	var got [][]byte
	for {
		kvp, ok, err := sc.Next(ctx)
		require.NoError(t, err)
		if !ok {
			break
		}
		got = append(got, kvp.Key)
	}
	require.Equal(t, [][]byte{[]byte("a"), []byte("z")}, got)
}

func TestBackupScannerRetainsAllCapturedRangesForUnclampedGroups(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 1)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	require.NoError(t, groups[1].Store.PutAt(ctx, []byte("a"), []byte("va"), 1, 0))
	require.NoError(t, groups[1].Store.PutAt(ctx, []byte("z"), []byte("vz"), 2, 0))

	sc := st.NewBackupScanner([]byte(""), nil, ^uint64(0), 10)
	defer sc.Close()

	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 10,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte(""), End: []byte("m"), GroupID: 1, State: distribution.RouteStateActive},
			{RouteID: 2, Start: []byte("m"), GroupID: 2, State: distribution.RouteStateActive},
		},
	}))

	var got [][]byte
	for {
		kvp, ok, err := sc.Next(ctx)
		require.NoError(t, err)
		if !ok {
			break
		}
		got = append(got, kvp.Key)
	}
	require.Equal(t, [][]byte{[]byte("a"), []byte("z")}, got)
}

func TestBackupScannerRetainsAllCapturedChunkRanges(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	home := uint64(11)
	inodeA := uint64(22)
	inodeB := uint64(23)
	routeA := fskeys.ChunkRouteKey(home, inodeA)
	routeB := fskeys.ChunkRouteKey(home, inodeB)
	routeEnd := prefixScanEnd(routeB)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), routeA, 1)
	engine.UpdateRoute(routeA, routeB, 2)
	engine.UpdateRoute(routeB, routeEnd, 2)
	engine.UpdateRoute(routeEnd, nil, 3)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
		3: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	chunkA := fskeys.ChunkKey(home, inodeA, 0)
	chunkB := fskeys.ChunkKey(home, inodeB, 0)
	require.NoError(t, st.PutAt(ctx, chunkA, []byte("a"), 1, 0))
	require.NoError(t, st.PutAt(ctx, chunkB, []byte("b"), 2, 0))

	start := fskeys.ChunkPrefix(home, inodeA)
	end := prefixScanEnd(fskeys.ChunkPrefix(home, inodeB))
	sc := st.NewBackupScanner(start, end, ^uint64(0), 10)
	defer sc.Close()

	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 10,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte(""), End: routeA, GroupID: 1, State: distribution.RouteStateActive},
			{RouteID: 2, Start: routeA, End: routeB, GroupID: 2, State: distribution.RouteStateActive},
			{RouteID: 3, Start: routeB, End: routeEnd, GroupID: 1, State: distribution.RouteStateActive},
			{RouteID: 4, Start: routeEnd, GroupID: 3, State: distribution.RouteStateActive},
		},
	}))

	var got [][]byte
	for {
		kvp, ok, err := sc.Next(ctx)
		require.NoError(t, err)
		if !ok {
			break
		}
		got = append(got, kvp.Key)
	}
	require.Equal(t, [][]byte{chunkA, chunkB}, got)
}

func TestBackupScannerDoesNotUseLiveCatalogForMaterialization(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	require.NoError(t, groups[1].Store.PutAt(ctx, []byte("x"), []byte("stale-old-owner"), 1, 0))

	sc := st.NewBackupScanner([]byte(""), nil, ^uint64(0), 10)
	defer sc.Close()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 10,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte(""), GroupID: 1, State: distribution.RouteStateActive},
		},
	}))

	kvp, ok, err := sc.Next(ctx)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, kvp)
}

func TestShardStoreScanKeysRouteAtLeaderRefillsAfterTxnInternalKeys(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	g := &ShardGroup{Store: store.NewMVCCStore()}
	st := NewShardStore(distribution.NewEngine(), map[uint64]*ShardGroup{1: g})

	require.NoError(t, g.Store.PutAt(ctx, txnCommitKey([]byte("primary"), 10), []byte("commit"), 1, 0))
	require.NoError(t, g.Store.PutAt(ctx, []byte("a"), []byte("va"), 2, 0))

	keys, err := st.scanKeysRouteAtLeader(ctx, g, []byte(""), nil, 1, ^uint64(0), 0)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("a")}, keys)
}

func TestShardStoreScanKeysRouteAtLeaderPreservesEmptyKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	g := &ShardGroup{Store: store.NewMVCCStore()}
	st := NewShardStore(distribution.NewEngine(), map[uint64]*ShardGroup{1: g})

	require.NoError(t, g.Store.PutAt(ctx, []byte(""), []byte("empty"), 1, 0))
	require.NoError(t, g.Store.PutAt(ctx, []byte("a"), []byte("va"), 2, 0))

	keys, err := st.scanKeysRouteAtLeader(ctx, g, nil, nil, 2, ^uint64(0), 0)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte(""), []byte("a")}, keys)
}

func TestShardStoreProxyScanKeysAtUsesSelectedGroup(t *testing.T) {
	t.Parallel()

	fake := &fakeRawKVServer{
		scanResp: &pb.RawScanAtResponse{
			Kv: []*pb.RawKVPair{{Key: []byte("k"), Value: []byte("v")}},
		},
	}
	addr, stop := startRawKVServer(t, fake)
	t.Cleanup(stop)

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 42)
	g := &ShardGroup{
		Engine: &followerProxyEngine{leader: addr},
		Store:  store.NewMVCCStore(),
	}
	st := NewShardStore(engine, map[uint64]*ShardGroup{42: g})

	keys, err := st.scanKeyRouteAt(ctx, distribution.Route{GroupID: 42}, []byte(""), nil, 10, ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("k")}, keys)

	fake.mu.Lock()
	defer fake.mu.Unlock()
	require.Equal(t, 1, fake.scanCalls)
	require.Equal(t, uint64(42), fake.lastScanGroupID)
	require.True(t, fake.lastScanKeysOnly)
}

func TestShardStoreProxyScanAtUsesSelectedGroup(t *testing.T) {
	t.Parallel()

	fake := &fakeRawKVServer{
		scanResp: &pb.RawScanAtResponse{
			Kv: []*pb.RawKVPair{{Key: []byte("k"), Value: []byte("v")}},
		},
	}
	addr, stop := startRawKVServer(t, fake)
	t.Cleanup(stop)

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 42)
	g := &ShardGroup{
		Engine: &followerProxyEngine{leader: addr},
		Store:  store.NewMVCCStore(),
	}
	st := NewShardStore(engine, map[uint64]*ShardGroup{42: g})

	kvs, err := st.ScanAt(ctx, []byte(""), nil, 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, []byte("k"), kvs[0].Key)

	fake.mu.Lock()
	defer fake.mu.Unlock()
	require.Equal(t, 1, fake.scanCalls)
	require.Equal(t, uint64(42), fake.lastScanGroupID)
}

func TestShardStoreProxyForwardPageAdvancesFromRawPage(t *testing.T) {
	t.Parallel()

	internalKey := txnCommitKey([]byte("primary"), 10)
	fake := &fakeRawKVServer{
		scanResp: &pb.RawScanAtResponse{
			Kv: []*pb.RawKVPair{
				{Key: []byte("a"), Value: []byte("va")},
				{Key: internalKey, Value: []byte("commit")},
			},
		},
	}
	addr, stop := startRawKVServer(t, fake)
	t.Cleanup(stop)

	ctx := context.Background()
	g := &ShardGroup{
		Engine: &followerProxyEngine{leader: addr},
		Store:  store.NewMVCCStore(),
	}
	st := NewShardStore(distribution.NewEngine(), map[uint64]*ShardGroup{42: g})

	page, err := st.scanRouteAtForwardPage(ctx, distribution.Route{GroupID: 42}, g, []byte(""), nil, 2, ^uint64(0), 0, nil, nil)
	require.NoError(t, err)
	require.True(t, page.full)
	require.Equal(t, internalKey, page.advanceKey)
	require.Len(t, page.kvs, 1)
	require.Equal(t, []byte("a"), page.kvs[0].Key)
}

func TestKeyOnlyScanHelpersPreserveEmptyKey(t *testing.T) {
	t.Parallel()

	keys := keysFromKVs(kvPairsFromKeys([][]byte{nil, []byte(""), []byte("a")}))
	require.Equal(t, [][]byte{[]byte(""), []byte("a")}, keys)
}

func TestMergeAndTrimScanKeysSortsBeforeTruncating(t *testing.T) {
	t.Parallel()

	keys := mergeAndTrimScanKeys(nil, [][]byte{[]byte("c"), []byte("d"), []byte("b")}, 2)
	require.Equal(t, [][]byte{[]byte("b"), []byte("c")}, keys)
}

func TestBackupScannerPaging(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	var commitTS uint64 = 1
	for _, key := range [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("x"), []byte("z")} {
		require.NoError(t, st.PutAt(ctx, key, []byte("v"), commitTS, 0))
		commitTS++
	}

	sc := st.NewBackupScanner([]byte(""), nil, ^uint64(0), 2)
	defer sc.Close()

	var got [][]byte
	for {
		kvp, ok, err := sc.Next(ctx)
		require.NoError(t, err)
		if !ok {
			break
		}
		got = append(got, kvp.Key)
	}
	require.Equal(t, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("x"), []byte("z")}, got)
}

func TestBackupScannerMaterializesFromCapturedRoute(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	require.NoError(t, groups[1].Store.PutAt(ctx, []byte("a"), []byte("old-owner"), 1, 0))

	sc := st.NewBackupScanner([]byte(""), nil, ^uint64(0), 1)
	defer sc.Close()

	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte(""), GroupID: 2, State: distribution.RouteStateActive},
		},
	}))

	kvp, ok, err := sc.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("a"), kvp.Key)
	require.Equal(t, []byte("old-owner"), kvp.Value)

	kvp, ok, err = sc.Next(ctx)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, kvp)
}

func TestBackupScannerMaterializesFromEnumeratedRoute(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	require.NoError(t, groups[1].Store.PutAt(ctx, []byte("a"), []byte("stale-first-route"), 1, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, []byte("a"), []byte("enumerated-route"), 2, 0))

	sc := &backupScanner{
		store:         st,
		routes:        []distribution.Route{{RouteID: 1, Start: []byte(""), GroupID: 1}, {RouteID: 2, Start: []byte(""), GroupID: 2}},
		clampToRoutes: false,
		cursor:        []byte(""),
		ts:            ^uint64(0),
		pageSize:      1,
	}
	defer sc.Close()

	kvp, ok, err := sc.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("a"), kvp.Key)
	require.Equal(t, []byte("enumerated-route"), kvp.Value)
}

func TestBackupScannerPreservesFullRoutingAfterListKeyCursor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	listKey := store.ListItemKey([]byte("x"), 0)
	require.NoError(t, st.PutAt(ctx, listKey, []byte("list"), 1, 0))
	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("plain"), 2, 0))

	sc := st.NewBackupScanner([]byte(""), nil, ^uint64(0), 1)
	defer sc.Close()

	var got [][]byte
	for {
		kvp, ok, err := sc.Next(ctx)
		require.NoError(t, err)
		if !ok {
			break
		}
		got = append(got, kvp.Key)
	}
	require.Equal(t, [][]byte{listKey, []byte("a")}, got)
}

func TestBackupScannerContinuesPastTxnInternalOnlyPage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	require.NoError(t, groups[1].Store.PutAt(ctx, txnCommitKey([]byte("primary"), 10), []byte("commit"), 1, 0))
	require.NoError(t, groups[1].Store.PutAt(ctx, []byte("a"), []byte("visible"), 2, 0))

	sc := st.NewBackupScanner([]byte(""), nil, ^uint64(0), 1)
	defer sc.Close()

	kvp, ok, err := sc.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("a"), kvp.Key)

	kvp, ok, err = sc.Next(ctx)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, kvp)
}

func TestBackupScannerContinuesPastStaleOffRoutePage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	require.NoError(t, groups[1].Store.PutAt(ctx, []byte("x"), []byte("stale-old-owner"), 1, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, []byte("z"), []byte("visible-new-owner"), 2, 0))

	sc := st.NewBackupScanner([]byte(""), nil, ^uint64(0), 1)
	defer sc.Close()

	kvp, ok, err := sc.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("z"), kvp.Key)
	require.Equal(t, []byte("visible-new-owner"), kvp.Value)

	kvp, ok, err = sc.Next(ctx)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, kvp)
}

func TestBackupScannerEmptyKeyContinuesAfterPage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	require.NoError(t, st.PutAt(ctx, []byte(""), []byte("empty"), 1, 0))
	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("later"), 2, 0))

	sc := st.NewBackupScanner(nil, nil, ^uint64(0), 1)
	defer sc.Close()

	kvp, ok, err := sc.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte(""), kvp.Key)
	require.Equal(t, []byte("empty"), kvp.Value)

	kvp, ok, err = sc.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("a"), kvp.Key)
	require.Equal(t, []byte("later"), kvp.Value)

	kvp, ok, err = sc.Next(ctx)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, kvp)
}

func TestBackupScannerPebblePagingDoesNotRepeatLastKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)
	pebbleStore, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pebbleStore.Close()) })

	groups := map[uint64]*ShardGroup{
		1: {Store: pebbleStore},
	}
	st := NewShardStore(engine, groups)
	require.NoError(t, pebbleStore.PutAt(ctx, []byte("a"), []byte("va"), 1, 0))
	require.NoError(t, pebbleStore.PutAt(ctx, []byte("b"), []byte("vb"), 2, 0))

	sc := st.NewBackupScanner(nil, nil, ^uint64(0), 1)
	defer sc.Close()

	kvp, ok, err := sc.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("a"), kvp.Key)

	kvp, ok, err = sc.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("b"), kvp.Key)

	kvp, ok, err = sc.Next(ctx)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, kvp)
}

func TestBackupScannerPagesPebbleKeysInLogicalOrder(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)
	pebbleStore, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	defer pebbleStore.Close()

	groups := map[uint64]*ShardGroup{
		1: {Store: pebbleStore},
	}
	st := NewShardStore(engine, groups)
	require.NoError(t, pebbleStore.PutAt(ctx, []byte("a"), []byte("va"), 10, 0))
	require.NoError(t, pebbleStore.PutAt(ctx, []byte("a\x80"), []byte("vax"), 20, 0))

	sc := st.NewBackupScanner(nil, nil, 20, 1)
	defer sc.Close()

	kvp, ok, err := sc.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("a"), kvp.Key)
	require.Equal(t, []byte("va"), kvp.Value)

	kvp, ok, err = sc.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("a\x80"), kvp.Key)
	require.Equal(t, []byte("vax"), kvp.Value)

	kvp, ok, err = sc.Next(ctx)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, kvp)
}

func TestShardStoreScanAt_RoutesS3ManifestScansByLogicalObjectKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	k0 := s3keys.ObjectManifestKey("bucket-a", 1, "z/object-0")
	k1 := s3keys.ObjectManifestKey("bucket-a", 1, "z/object-1")
	require.NoError(t, st.PutAt(ctx, k0, []byte("m0"), 1, 0))
	require.NoError(t, st.PutAt(ctx, k1, []byte("m1"), 2, 0))

	start := s3keys.ObjectManifestScanStart("bucket-a", 1, "z/")
	end := prefixScanEnd(start)
	kvs, err := st.ScanAt(ctx, start, end, 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, k0, kvs[0].Key)
	require.Equal(t, k1, kvs[1].Key)
}

func TestShardStoreScanAt_RoutesRedisWideColumnPrefixAcrossShards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("am"), 1)
	engine.UpdateRoute([]byte("am"), nil, 2)
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	t.Cleanup(func() {
		_ = groups[1].Store.Close()
		_ = groups[2].Store.Close()
	})
	st := NewShardStore(engine, groups)

	left := store.HashFieldKey([]byte("alice"), []byte("field"))
	right := store.HashFieldKey([]byte("amy"), []byte("field"))
	require.NoError(t, st.PutAt(ctx, left, []byte("left"), 1, 0))
	require.NoError(t, st.PutAt(ctx, right, []byte("right"), 2, 0))

	start := store.HashFieldScanPrefix([]byte("a"))
	end := prefixScanEnd([]byte(store.HashFieldPrefix))
	kvs, err := st.ScanAt(ctx, start, end, 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.ElementsMatch(t, [][]byte{left, right}, [][]byte{kvs[0].Key, kvs[1].Key})
}

func TestShardStoreScanAt_RoutesBareRedisWideColumnFamilyAcrossShards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	t.Cleanup(func() {
		_ = groups[1].Store.Close()
		_ = groups[2].Store.Close()
	})
	st := NewShardStore(engine, groups)

	left := store.HashFieldKey([]byte("anna"), []byte("field"))
	right := store.HashFieldKey([]byte("zoey"), []byte("field"))
	require.NoError(t, st.PutAt(ctx, left, []byte("left"), 1, 0))
	require.NoError(t, st.PutAt(ctx, right, []byte("right"), 2, 0))

	prefix := []byte(store.HashFieldPrefix)
	kvs, err := st.ScanAt(ctx, prefix, prefixScanEnd(prefix), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, [][]byte{left, right}, [][]byte{kvs[0].Key, kvs[1].Key})
}

func TestShardStoreScanAt_RoutesRedisWideColumnCursorAcrossRemainingShards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	t.Cleanup(func() {
		_ = groups[1].Store.Close()
		_ = groups[2].Store.Close()
	})
	st := NewShardStore(engine, groups)

	left := store.HashFieldKey([]byte("anna"), []byte("field"))
	right := store.HashFieldKey([]byte("zoey"), []byte("field"))
	require.NoError(t, st.PutAt(ctx, left, []byte("left"), 1, 0))
	require.NoError(t, st.PutAt(ctx, right, []byte("right"), 2, 0))

	prefix := []byte(store.HashFieldPrefix)
	kvs, err := st.ScanAt(ctx, nextScanCursor(left), prefixScanEnd(prefix), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, [][]byte{right}, [][]byte{kvs[0].Key})
}

func TestShardStoreScanAt_RoutesExactRedisWideColumnScanToOneShard(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("am"), 1)
	engine.UpdateRoute([]byte("am"), nil, 2)
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	t.Cleanup(func() {
		_ = groups[1].Store.Close()
		_ = groups[2].Store.Close()
	})
	st := NewShardStore(engine, groups)

	start := store.HashFieldScanPrefix([]byte("alice"))
	routes, clamp, _ := st.routesForScanWithVersion(start, prefixScanEnd(start))
	require.False(t, clamp)
	require.Len(t, routes, 1)
	require.Equal(t, uint64(1), routes[0].GroupID)
}

func TestShardStoreRoutesForWideColumnBoundedPatternIncludesLegacyRawRoute(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	t.Cleanup(func() {
		_ = groups[1].Store.Close()
		_ = groups[2].Store.Close()
	})
	st := NewShardStore(engine, groups)

	start := store.HashFieldScanPrefix([]byte("m"))
	routes, clamp, _ := st.routesForScanWithVersion(start, prefixScanEnd([]byte(store.HashFieldPrefix)))
	require.False(t, clamp)
	require.Len(t, routes, 2)
	require.Equal(t, uint64(2), routes[0].GroupID)
	require.Equal(t, uint64(1), routes[1].GroupID)
}

func TestShardStoreRedisWideColumnReadsLegacyRawRoute(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	t.Cleanup(func() {
		_ = groups[1].Store.Close()
		_ = groups[2].Store.Close()
	})
	st := NewShardStore(engine, groups)

	key := store.HashFieldKey([]byte("zulu"), []byte("field"))
	require.NoError(t, groups[1].Store.PutAt(ctx, key, []byte("legacy"), 5, 0))

	value, err := st.GetAt(ctx, key, 5)
	require.NoError(t, err)
	require.Equal(t, []byte("legacy"), value)

	ts, exists, err := st.LatestCommitTS(ctx, key)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, uint64(5), ts)

	prefix := store.HashFieldScanPrefix([]byte("zulu"))
	kvs, err := st.ScanAt(ctx, prefix, prefixScanEnd(prefix), 10, 5)
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, []byte("legacy"), kvs[0].Value)

	require.NoError(t, st.PutAt(ctx, key, []byte("current"), 6, 0))
	value, err = st.GetAt(ctx, key, 6)
	require.NoError(t, err)
	require.Equal(t, []byte("current"), value)

	ts, exists, err = st.LatestCommitTS(ctx, key)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, uint64(6), ts)

	kvs, err = st.ScanAt(ctx, prefix, prefixScanEnd(prefix), 10, 6)
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, []byte("current"), kvs[0].Value)

	kvs, err = st.ReverseScanAt(ctx, prefix, prefixScanEnd(prefix), 10, 6)
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, []byte("current"), kvs[0].Value)

	require.NoError(t, st.DeleteAt(ctx, key, 7))
	_, err = st.GetAt(ctx, key, 7)
	require.ErrorIs(t, err, store.ErrKeyNotFound)

	kvs, err = st.ScanAt(ctx, prefix, prefixScanEnd(prefix), 10, 7)
	require.NoError(t, err)
	require.Empty(t, kvs)

	kvs, err = st.ReverseScanAt(ctx, prefix, prefixScanEnd(prefix), 10, 7)
	require.NoError(t, err)
	require.Empty(t, kvs)

	require.NoError(t, st.PutAt(ctx, key, []byte("future"), 9, 0))
	_, err = st.GetAt(ctx, key, 8)
	require.ErrorIs(t, err, store.ErrKeyNotFound)

	kvs, err = st.ScanAt(ctx, prefix, prefixScanEnd(prefix), 10, 8)
	require.NoError(t, err)
	require.Empty(t, kvs)

	value, err = st.GetAt(ctx, key, 9)
	require.NoError(t, err)
	require.Equal(t, []byte("future"), value)
}

func TestShardStoreRedisWideColumnScanRefillsAfterLogicalTombstones(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	t.Cleanup(func() {
		_ = groups[1].Store.Close()
		_ = groups[2].Store.Close()
	})
	st := NewShardStore(engine, groups)

	userKey := []byte("zulu")
	a := store.HashFieldKey(userKey, []byte("a"))
	b := store.HashFieldKey(userKey, []byte("b"))
	c := store.HashFieldKey(userKey, []byte("c"))
	d := store.HashFieldKey(userKey, []byte("d"))
	for _, item := range []struct {
		key   []byte
		value []byte
	}{
		{key: a, value: []byte("legacy-a")},
		{key: b, value: []byte("legacy-b")},
		{key: c, value: []byte("legacy-c")},
		{key: d, value: []byte("legacy-d")},
	} {
		require.NoError(t, groups[1].Store.PutAt(ctx, item.key, item.value, 5, 0))
	}
	require.NoError(t, st.DeleteAt(ctx, a, 7))
	require.NoError(t, st.DeleteAt(ctx, b, 7))

	prefix := store.HashFieldScanPrefix(userKey)
	kvs, err := st.ScanAt(ctx, prefix, prefixScanEnd(prefix), 2, 7)
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, c, kvs[0].Key)
	require.Equal(t, []byte("legacy-c"), kvs[0].Value)
	require.Equal(t, d, kvs[1].Key)
	require.Equal(t, []byte("legacy-d"), kvs[1].Value)

	keys, err := st.ScanKeysAt(ctx, prefix, prefixScanEnd(prefix), 2, 7)
	require.NoError(t, err)
	require.Equal(t, [][]byte{c, d}, keys)
}

func TestShardStoreReverseRedisWideColumnScanRefillsAfterLogicalTombstones(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	t.Cleanup(func() {
		_ = groups[1].Store.Close()
		_ = groups[2].Store.Close()
	})
	st := NewShardStore(engine, groups)

	userKey := []byte("zulu")
	a := store.HashFieldKey(userKey, []byte("a"))
	b := store.HashFieldKey(userKey, []byte("b"))
	c := store.HashFieldKey(userKey, []byte("c"))
	d := store.HashFieldKey(userKey, []byte("d"))
	for _, item := range []struct {
		key   []byte
		value []byte
	}{
		{key: a, value: []byte("legacy-a")},
		{key: b, value: []byte("legacy-b")},
		{key: c, value: []byte("legacy-c")},
		{key: d, value: []byte("legacy-d")},
	} {
		require.NoError(t, groups[1].Store.PutAt(ctx, item.key, item.value, 5, 0))
	}
	require.NoError(t, st.DeleteAt(ctx, d, 7))
	require.NoError(t, st.DeleteAt(ctx, c, 7))

	prefix := store.HashFieldScanPrefix(userKey)
	kvs, err := st.ReverseScanAt(ctx, prefix, prefixScanEnd(prefix), 2, 7)
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, b, kvs[0].Key)
	require.Equal(t, []byte("legacy-b"), kvs[0].Value)
	require.Equal(t, a, kvs[1].Key)
	require.Equal(t, []byte("legacy-a"), kvs[1].Value)
}

func TestShardStoreReverseRedisWideColumnScanPrefersLogicalRoute(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	t.Cleanup(func() {
		_ = groups[1].Store.Close()
		_ = groups[2].Store.Close()
	})
	st := NewShardStore(engine, groups)

	key := store.HashFieldKey([]byte("zulu"), []byte("field"))
	require.NoError(t, groups[1].Store.PutAt(ctx, key, []byte("legacy"), 5, 0))
	require.NoError(t, st.PutAt(ctx, key, []byte("current"), 6, 0))

	prefix := store.HashFieldScanPrefix([]byte("zulu"))
	kvs, err := st.ReverseScanAt(ctx, prefix, prefixScanEnd(prefix), 10, 6)
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, []byte("current"), kvs[0].Value)
}

func TestShardStoreScanAt_RoutesFilesystemChunkScansByChunkRouteKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	home := uint64(11)
	inode := uint64(22)
	routeKey := fskeys.ChunkRouteKey(home, inode)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), routeKey, 1)
	engine.UpdateRoute(routeKey, nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	k0 := fskeys.ChunkKey(home, inode, 0)
	k1 := fskeys.ChunkKey(home, inode, 1)
	require.NoError(t, st.PutAt(ctx, k0, []byte("c0"), 1, 0))
	require.NoError(t, st.PutAt(ctx, k1, []byte("c1"), 2, 0))

	start := fskeys.ChunkPrefix(home, inode)
	kvs, err := st.ScanAt(ctx, start, prefixScanEnd(start), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, k0, kvs[0].Key)
	require.Equal(t, k1, kvs[1].Key)
}

func TestShardStoreResolveFilesystemHomeSlot(t *testing.T) {
	t.Parallel()

	boundary := fskeys.ChunkRouteKey(100, 0)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), boundary, 1)
	engine.UpdateRoute(boundary, nil, 2)
	st := NewShardStore(engine, map[uint64]*ShardGroup{})

	home, err := st.ResolveFilesystemHomeSlot(1, 77)
	require.NoError(t, err)
	groupID, ok := st.FilesystemGroupForHome(home, 77)
	require.True(t, ok)
	require.EqualValues(t, 1, groupID)
	require.Less(t, home, uint64(100))

	home, err = st.ResolveFilesystemHomeSlot(2, 77)
	require.NoError(t, err)
	groupID, ok = st.FilesystemGroupForHome(home, 77)
	require.True(t, ok)
	require.EqualValues(t, 2, groupID)
	require.GreaterOrEqual(t, home, uint64(100))

	_, err = st.ResolveFilesystemHomeSlot(3, 77)
	require.ErrorIs(t, err, ErrFilesystemPlacementTargetNotFound)
}

func TestShardStoreFilesystemGroupIDsReturnsPhysicalGroupsSorted(t *testing.T) {
	t.Parallel()

	st := NewShardStore(distribution.NewEngine(), map[uint64]*ShardGroup{
		9: {},
		2: {},
		5: {},
	})
	require.Equal(t, []uint64{2, 5, 9}, st.FilesystemGroupIDs())
}

func TestShardStoreScanAt_RoutesFilesystemUsageCountersAcrossRouteGroups(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	usagePrefix := fskeys.UsageRouteAllPrefix()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), usagePrefix, 1)
	engine.UpdateRoute(usagePrefix, nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	key := fskeys.UsageRouteKey(fskeys.InodeKey(22))
	staleOnlyKey := fskeys.UsageRouteKey(fskeys.InodeKey(23))
	require.NoError(t, st.PutAt(ctx, key, []byte("usage"), 1, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, key, []byte("stale"), 2, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, staleOnlyKey, []byte("stale-only"), 2, 0))

	kvs, err := st.ScanAt(ctx, usagePrefix, prefixScanEnd(usagePrefix), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, key, kvs[0].Key)
	require.Equal(t, []byte("usage"), kvs[0].Value)

	keys, err := st.ScanKeysAt(ctx, usagePrefix, prefixScanEnd(usagePrefix), 10, ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, [][]byte{key}, keys)

	kvs, err = st.ReverseScanAt(ctx, usagePrefix, prefixScanEnd(usagePrefix), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, key, kvs[0].Key)
	require.Equal(t, []byte("usage"), kvs[0].Value)
}

func TestShardStoreScanAt_RefillsAfterStaleFilesystemUsageCounters(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	usagePrefix := fskeys.UsageRouteAllPrefix()
	ownerBoundary := fskeys.InodeKey(50)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), ownerBoundary, 1)
	engine.UpdateRoute(ownerBoundary, nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	staleOne := fskeys.UsageRouteKey(fskeys.InodeKey(1))
	staleTwo := fskeys.UsageRouteKey(fskeys.InodeKey(2))
	owned := fskeys.UsageRouteKey(fskeys.InodeKey(99))
	require.NoError(t, groups[2].Store.PutAt(ctx, staleOne, []byte("stale-1"), 1, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, staleTwo, []byte("stale-2"), 2, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, owned, []byte("owned"), 3, 0))

	kvs, err := st.ScanAt(ctx, usagePrefix, prefixScanEnd(usagePrefix), 1, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, owned, kvs[0].Key)
	require.Equal(t, []byte("owned"), kvs[0].Value)

	keys, err := st.ScanKeysAt(ctx, usagePrefix, prefixScanEnd(usagePrefix), 1, ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, [][]byte{owned}, keys)
}

func TestBackupScannerPrefersFilesystemUsageOwnerCopy(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	usagePrefix := fskeys.UsageRouteAllPrefix()
	ownerBoundary := fskeys.InodeKey(50)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), ownerBoundary, 1)
	engine.UpdateRoute(ownerBoundary, nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	key := fskeys.UsageRouteKey(fskeys.InodeKey(22))
	require.NoError(t, groups[1].Store.PutAt(ctx, key, []byte("owner"), 1, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, key, []byte("stale"), 2, 0))

	scanner := st.NewBackupScanner(usagePrefix, prefixScanEnd(usagePrefix), ^uint64(0), 1)
	defer scanner.Close()

	pair, ok, err := scanner.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, key, pair.Key)
	require.Equal(t, []byte("owner"), pair.Value)
	pair, ok, err = scanner.Next(ctx)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, pair)
}

func TestShardStoreScanAt_RoutesFilesystemChunkSubrangeByChunkRouteKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	home := uint64(11)
	inode := uint64(22)
	routeKey := fskeys.ChunkRouteKey(home, inode)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), routeKey, 1)
	engine.UpdateRoute(routeKey, nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	k0 := fskeys.ChunkKey(home, inode, 0)
	k1 := fskeys.ChunkKey(home, inode, 1)
	require.NoError(t, st.PutAt(ctx, k0, []byte("c0"), 1, 0))
	require.NoError(t, st.PutAt(ctx, k1, []byte("c1"), 2, 0))

	kvs, err := st.ScanAt(ctx, k0, nextScanCursor(k0), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, k0, kvs[0].Key)
}

func TestShardStoreScanAt_RoutesFilesystemChunkCrossFileSubrangeEndRoute(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	home := uint64(11)
	inodeA := uint64(22)
	inodeB := uint64(23)
	routeA := fskeys.ChunkRouteKey(home, inodeA)
	routeB := fskeys.ChunkRouteKey(home, inodeB)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), routeA, 1)
	engine.UpdateRoute(routeA, routeB, 2)
	engine.UpdateRoute(routeB, nil, 3)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
		3: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	kA := fskeys.ChunkKey(home, inodeA, 7)
	kB0 := fskeys.ChunkKey(home, inodeB, 0)
	kB5 := fskeys.ChunkKey(home, inodeB, 5)
	require.NoError(t, st.PutAt(ctx, kA, []byte("a7"), 1, 0))
	require.NoError(t, st.PutAt(ctx, kB0, []byte("b0"), 2, 0))
	require.NoError(t, st.PutAt(ctx, kB5, []byte("b5"), 3, 0))

	kvs, err := st.ScanAt(ctx, kA, kB5, 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, kA, kvs[0].Key)
	require.Equal(t, kB0, kvs[1].Key)
}

func TestShardStoreScanAt_RoutesFilesystemChunkCrossFileCarriedPrefixEnd(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	home := uint64(11)
	inodeA := uint64(0xfe)
	inodeB := uint64(0xff)
	routeA := fskeys.ChunkRouteKey(home, inodeA)
	routeB := fskeys.ChunkRouteKey(home, inodeB)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), routeA, 1)
	engine.UpdateRoute(routeA, routeB, 2)
	engine.UpdateRoute(routeB, nil, 3)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
		3: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	kA := fskeys.ChunkKey(home, inodeA, 7)
	kB := fskeys.ChunkKey(home, inodeB, 0)
	require.NoError(t, st.PutAt(ctx, kA, []byte("a7"), 1, 0))
	require.NoError(t, st.PutAt(ctx, kB, []byte("b0"), 2, 0))

	kvs, err := st.ScanAt(ctx, kA, prefixScanEnd(fskeys.ChunkPrefix(home, inodeB)), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, kA, kvs[0].Key)
	require.Equal(t, kB, kvs[1].Key)
}

func TestShardStoreScanAt_UnboundedFilesystemChunkScanIncludesRawKeys(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	home := uint64(11)
	inode := uint64(22)
	routeKey := fskeys.ChunkRouteKey(home, inode)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), routeKey, 1)
	engine.UpdateRoute(routeKey, nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	k0 := fskeys.ChunkKey(home, inode, 0)
	k1 := fskeys.ChunkKey(home, inode, 1)
	inodeKey := fskeys.InodeKey(99)
	require.NoError(t, st.PutAt(ctx, k0, []byte("c0"), 1, 0))
	require.NoError(t, st.PutAt(ctx, k1, []byte("c1"), 2, 0))
	require.NoError(t, st.PutAt(ctx, inodeKey, []byte("inode"), 3, 0))

	kvs, err := st.ScanAt(ctx, nextScanCursor(k0), nil, 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, k1, kvs[0].Key)
	require.Equal(t, inodeKey, kvs[1].Key)
}

func TestShardStoreScanAt_BoundedFilesystemChunkScanIncludesRawKeys(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	home := uint64(11)
	inode := uint64(22)
	routeKey := fskeys.ChunkRouteKey(home, inode)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), routeKey, 1)
	engine.UpdateRoute(routeKey, nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	k0 := fskeys.ChunkKey(home, inode, 0)
	k1 := fskeys.ChunkKey(home, inode, 1)
	inodeKey := fskeys.InodeKey(99)
	require.NoError(t, st.PutAt(ctx, k0, []byte("c0"), 1, 0))
	require.NoError(t, st.PutAt(ctx, k1, []byte("c1"), 2, 0))
	require.NoError(t, st.PutAt(ctx, inodeKey, []byte("inode"), 3, 0))

	kvs, err := st.ScanAt(ctx, nextScanCursor(k0), prefixScanEnd(fskeys.InodeAllPrefix()), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, k1, kvs[0].Key)
	require.Equal(t, inodeKey, kvs[1].Key)
}

func TestShardStoreScanAt_UpperBoundedFilesystemChunkScanIncludesChunkRoutes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	home := uint64(11)
	inode := uint64(22)
	routeKey := fskeys.ChunkRouteKey(home, inode)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), routeKey, 1)
	engine.UpdateRoute(routeKey, nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	k0 := fskeys.ChunkKey(home, inode, 0)
	k1 := fskeys.ChunkKey(home, inode, 1)
	require.NoError(t, st.PutAt(ctx, k0, []byte("c0"), 1, 0))
	require.NoError(t, st.PutAt(ctx, k1, []byte("c1"), 2, 0))

	kvs, err := st.ScanAt(ctx, nil, prefixScanEnd(fskeys.ChunkAllPrefix()), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, k0, kvs[0].Key)
	require.Equal(t, k1, kvs[1].Key)
}

func TestShardStoreScanAt_NilStartFanoutUsesExplicitGroupForProxy(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	recorder := &recordingRawScanKVServer{}
	addr, stop := startRawKVServer(t, recorder)
	t.Cleanup(stop)

	rawRouteEnd := fskeys.ChunkRouteKey(11, 22)
	chunkRouteEnd := prefixScanEnd(rawRouteEnd)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), rawRouteEnd, 1)
	engine.UpdateRoute(rawRouteEnd, chunkRouteEnd, 2)
	engine.UpdateRoute(chunkRouteEnd, nil, 3)

	groups := map[uint64]*ShardGroup{
		1: {Engine: followerEngineForTest(addr), Store: store.NewMVCCStore()},
		2: {Engine: followerEngineForTest(addr), Store: store.NewMVCCStore()},
		3: {Engine: followerEngineForTest(addr), Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	t.Cleanup(func() { require.NoError(t, st.Close()) })

	_, err := st.ScanAt(ctx, nil, prefixScanEnd(fskeys.ChunkAllPrefix()), 10, ^uint64(0))
	require.NoError(t, err)

	groupIDs := recorder.rawScanGroupIDs()
	require.NotContains(t, groupIDs, uint64(0))
	require.Contains(t, groupIDs, uint64(1))
	require.Contains(t, groupIDs, uint64(2))
}

func TestShardStoreReverseScanAt_UpperBoundedFilesystemChunkScanIncludesChunkRoutes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	home := uint64(11)
	inode := uint64(22)
	routeKey := fskeys.ChunkRouteKey(home, inode)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), routeKey, 1)
	engine.UpdateRoute(routeKey, nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	k0 := fskeys.ChunkKey(home, inode, 0)
	k1 := fskeys.ChunkKey(home, inode, 1)
	require.NoError(t, st.PutAt(ctx, k0, []byte("c0"), 1, 0))
	require.NoError(t, st.PutAt(ctx, k1, []byte("c1"), 2, 0))

	kvs, err := st.ReverseScanAt(ctx, nil, prefixScanEnd(fskeys.ChunkAllPrefix()), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, k1, kvs[0].Key)
	require.Equal(t, k0, kvs[1].Key)
}

type recordingRawScanKVServer struct {
	pb.UnimplementedRawKVServer

	mu       sync.Mutex
	groupIDs []uint64
}

func (s *recordingRawScanKVServer) RawScanAt(_ context.Context, req *pb.RawScanAtRequest) (*pb.RawScanAtResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.groupIDs = append(s.groupIDs, req.GetGroupId())
	return &pb.RawScanAtResponse{}, nil
}

func (s *recordingRawScanKVServer) rawScanGroupIDs() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]uint64(nil), s.groupIDs...)
}

type followerLeaderAddrEngine struct {
	*fakeLeaseEngine
	addr string
}

func followerEngineForTest(addr string) *followerLeaderAddrEngine {
	inner := &fakeLeaseEngine{}
	inner.state.Store(raftengine.StateFollower)
	return &followerLeaderAddrEngine{fakeLeaseEngine: inner, addr: addr}
}

func (e *followerLeaderAddrEngine) Leader() raftengine.LeaderInfo {
	return raftengine.LeaderInfo{ID: "n1", Address: e.addr}
}

func TestShardStoreScanAt_DeduplicatesFilesystemChunkRoutesByGroup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	home := uint64(11)
	inode := uint64(22)
	routeStart := fskeys.ChunkRouteKey(home, inode)
	routeEnd := prefixScanEnd(routeStart)
	routeSplit := append(append([]byte(nil), routeStart...), 0x80)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), routeStart, 1)
	engine.UpdateRoute(routeStart, routeSplit, 2)
	engine.UpdateRoute(routeSplit, routeEnd, 2)
	engine.UpdateRoute(routeEnd, nil, 3)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
		3: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	k0 := fskeys.ChunkKey(home, inode, 0)
	k1 := fskeys.ChunkKey(home, inode, 1)
	require.NoError(t, st.PutAt(ctx, k0, []byte("c0"), 1, 0))
	require.NoError(t, st.PutAt(ctx, k1, []byte("c1"), 2, 0))

	start := fskeys.ChunkPrefix(home, inode)
	kvs, err := st.ScanAt(ctx, start, prefixScanEnd(start), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, k0, kvs[0].Key)
	require.Equal(t, k1, kvs[1].Key)
}

func TestShardStoreReverseScanAt_RoutesFilesystemChunkScansByChunkRouteKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	home := uint64(11)
	inode := uint64(22)
	routeKey := fskeys.ChunkRouteKey(home, inode)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), routeKey, 1)
	engine.UpdateRoute(routeKey, nil, 2)
	st := NewShardStore(engine, map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	})

	k0 := fskeys.ChunkKey(home, inode, 0)
	k1 := fskeys.ChunkKey(home, inode, 1)
	require.NoError(t, st.PutAt(ctx, k0, []byte("c0"), 1, 0))
	require.NoError(t, st.PutAt(ctx, k1, []byte("c1"), 2, 0))

	routes, clampToRoutes := st.routesForReverseScan(fskeys.ChunkPrefix(home, inode), prefixScanEnd(fskeys.ChunkPrefix(home, inode)))
	require.False(t, clampToRoutes)
	require.Len(t, routes, 1)
	require.Equal(t, uint64(2), routes[0].GroupID)

	kvs, err := st.ReverseScanAt(ctx, fskeys.ChunkPrefix(home, inode), prefixScanEnd(fskeys.ChunkPrefix(home, inode)), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, k1, kvs[0].Key)
	require.Equal(t, k0, kvs[1].Key)
}

// TestShardStoreReverseScanAt_DescendingOrderAcrossShards verifies that
// ReverseScanAt with a nil start (clampToRoutes=false) merges results from all
// shards and returns them in descending key order.
func TestShardStoreReverseScanAt_DescendingOrderAcrossShards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	// Shard 1 (keys < "m")
	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 1, 0))
	require.NoError(t, st.PutAt(ctx, []byte("c"), []byte("vc"), 2, 0))
	// Shard 2 (keys >= "m")
	require.NoError(t, st.PutAt(ctx, []byte("x"), []byte("vx"), 3, 0))
	require.NoError(t, st.PutAt(ctx, []byte("z"), []byte("vz"), 4, 0))

	// nil start → clampToRoutes=false; both shards must be merged in descending order.
	kvs, err := st.ReverseScanAt(ctx, nil, nil, 4, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 4)
	require.Equal(t, []byte("z"), kvs[0].Key)
	require.Equal(t, []byte("x"), kvs[1].Key)
	require.Equal(t, []byte("c"), kvs[2].Key)
	require.Equal(t, []byte("a"), kvs[3].Key)
}

// TestShardStoreReverseScanAt_LimitAcrossShards verifies that the limit is
// correctly applied when results from multiple shards are merged in descending
// order. The top-N keys across all shards must be returned.
func TestShardStoreReverseScanAt_LimitAcrossShards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	// Shard 1 (keys < "m")
	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 1, 0))
	require.NoError(t, st.PutAt(ctx, []byte("b"), []byte("vb"), 2, 0))
	require.NoError(t, st.PutAt(ctx, []byte("c"), []byte("vc"), 3, 0))
	// Shard 2 (keys >= "m")
	require.NoError(t, st.PutAt(ctx, []byte("x"), []byte("vx"), 4, 0))
	require.NoError(t, st.PutAt(ctx, []byte("y"), []byte("vy"), 5, 0))
	require.NoError(t, st.PutAt(ctx, []byte("z"), []byte("vz"), 6, 0))

	// limit=4: top-4 in descending order are z, y, x, c.
	kvs, err := st.ReverseScanAt(ctx, nil, nil, 4, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 4)
	require.Equal(t, []byte("z"), kvs[0].Key)
	require.Equal(t, []byte("y"), kvs[1].Key)
	require.Equal(t, []byte("x"), kvs[2].Key)
	require.Equal(t, []byte("c"), kvs[3].Key)
}

// TestShardStoreReverseScanAt_SingleShard verifies that ReverseScanAt on a
// single shard returns results in descending key order.
func TestShardStoreReverseScanAt_SingleShard(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 1, 0))
	require.NoError(t, st.PutAt(ctx, []byte("b"), []byte("vb"), 2, 0))
	require.NoError(t, st.PutAt(ctx, []byte("c"), []byte("vc"), 3, 0))

	kvs, err := st.ReverseScanAt(ctx, nil, nil, 2, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, []byte("c"), kvs[0].Key)
	require.Equal(t, []byte("b"), kvs[1].Key)
}

// TestShardStoreReverseScanAt_IncludesS3ManifestKeysDescending mirrors
// TestShardStoreScanAt_IncludesS3ManifestKeysAcrossShards but for
// ReverseScanAt — results must be returned in descending key order.
func TestShardStoreReverseScanAt_IncludesS3ManifestKeysDescending(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	k1 := s3keys.ObjectManifestKey("bucket-a", 1, "alpha")
	k2 := s3keys.ObjectManifestKey("bucket-a", 1, "zeta")
	require.NoError(t, st.PutAt(ctx, k1, []byte("m1"), 1, 0))
	require.NoError(t, st.PutAt(ctx, k2, []byte("m2"), 2, 0))

	start := s3keys.ObjectManifestPrefixForBucket("bucket-a", 1)
	kvs, err := st.ReverseScanAt(ctx, start, prefixScanEnd(start), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	// "zeta" > "alpha" → descending order puts k2 first.
	require.Equal(t, k2, kvs[0].Key)
	require.Equal(t, k1, kvs[1].Key)
}

// TestMergeAndTrimReverseScanResults verifies that the helper merges two
// slices, sorts them in descending key order, and trims to the given limit.
func TestMergeAndTrimReverseScanResults(t *testing.T) {
	t.Parallel()

	out := []*store.KVPair{
		{Key: []byte("z"), Value: []byte("vz")},
		{Key: []byte("m"), Value: []byte("vm")},
	}
	kvs := []*store.KVPair{
		{Key: []byte("y"), Value: []byte("vy")},
		{Key: []byte("a"), Value: []byte("va")},
	}

	result := mergeAndTrimReverseScanResults(out, kvs, 3)
	require.Len(t, result, 3)
	require.Equal(t, []byte("z"), result[0].Key)
	require.Equal(t, []byte("y"), result[1].Key)
	require.Equal(t, []byte("m"), result[2].Key)
}

func TestMergeAndTrimReverseScanResults_EmptyInput(t *testing.T) {
	t.Parallel()

	out := []*store.KVPair{{Key: []byte("z"), Value: []byte("vz")}}
	result := mergeAndTrimReverseScanResults(out, nil, 10)
	require.Len(t, result, 1)
	require.Equal(t, []byte("z"), result[0].Key)
}

func TestMergeAndTrimReverseScanResults_WithinLimit(t *testing.T) {
	t.Parallel()

	out := []*store.KVPair{{Key: []byte("z"), Value: []byte("vz")}}
	kvs := []*store.KVPair{{Key: []byte("a"), Value: []byte("va")}}

	result := mergeAndTrimReverseScanResults(out, kvs, 10)
	require.Len(t, result, 2)
	require.Equal(t, []byte("z"), result[0].Key)
	require.Equal(t, []byte("a"), result[1].Key)
}

func TestMergeAndTrimReverseScanResults_ExactLimit(t *testing.T) {
	t.Parallel()

	out := []*store.KVPair{
		{Key: []byte("z"), Value: []byte("vz")},
		{Key: []byte("c"), Value: []byte("vc")},
	}
	kvs := []*store.KVPair{
		{Key: []byte("y"), Value: []byte("vy")},
		{Key: []byte("a"), Value: []byte("va")},
	}

	// limit=2: top-2 in descending order are "z", "y".
	result := mergeAndTrimReverseScanResults(out, kvs, 2)
	require.Len(t, result, 2)
	require.Equal(t, []byte("z"), result[0].Key)
	require.Equal(t, []byte("y"), result[1].Key)
}

func TestScanLockBoundsForKVs_ReverseOrder(t *testing.T) {
	t.Parallel()

	kvs := []*store.KVPair{
		{Key: []byte("c"), Value: []byte("vc")},
		{Key: []byte("b"), Value: []byte("vb")},
	}

	lockStart, lockEnd := scanLockBoundsForKVs(kvs, []byte("a"), []byte("d"), 2)
	require.Equal(t, []byte("a"), lockStart)
	require.Equal(t, nextScanCursor([]byte("c")), lockEnd)
}

func TestScanLockBoundsForKVsDirection_ReverseUsesReturnedWindow(t *testing.T) {
	t.Parallel()

	kvs := []*store.KVPair{
		{Key: []byte("z"), Value: []byte("vz")},
		{Key: []byte("y"), Value: []byte("vy")},
	}

	lockStart, lockEnd := scanLockBoundsForKVsDirection(kvs, []byte("a"), []byte("zz"), 2, true)
	require.Equal(t, []byte("y"), lockStart)
	require.Equal(t, []byte("zz"), lockEnd)
}

func TestScanLockBoundsForKVs_PreservesOriginalStart(t *testing.T) {
	t.Parallel()

	kvs := []*store.KVPair{
		{Key: []byte("c"), Value: []byte("vc")},
		{Key: []byte("e"), Value: []byte("ve")},
	}

	lockStart, lockEnd := scanLockBoundsForKVs(kvs, []byte("a"), []byte("z"), 2)
	require.Equal(t, []byte("a"), lockStart)
	require.Equal(t, nextScanCursor([]byte("e")), lockEnd)
}

func TestScanLockBoundsForKVs_IncompleteScanUsesOriginalRange(t *testing.T) {
	t.Parallel()

	kvs := []*store.KVPair{
		{Key: []byte("c"), Value: []byte("vc")},
	}

	lockStart, lockEnd := scanLockBoundsForKVs(kvs, []byte("a"), []byte("z"), 2)
	require.Equal(t, []byte("a"), lockStart)
	require.Equal(t, []byte("z"), lockEnd)
}

func TestScanLockBoundsForKVs_EmptyUsesOriginalRange(t *testing.T) {
	t.Parallel()

	lockStart, lockEnd := scanLockBoundsForKVs(nil, []byte("a"), []byte("z"), 10)
	require.Equal(t, []byte("a"), lockStart)
	require.Equal(t, []byte("z"), lockEnd)
}

func TestScanLockBoundsForKVs_FullInternalPageUsesRawPageBound(t *testing.T) {
	t.Parallel()

	internalKey := txnCommitKey([]byte("primary"), 10)
	kvs := []*store.KVPair{
		{Key: internalKey, Value: []byte("commit")},
	}

	lockStart, lockEnd := scanLockBoundsForKVs(kvs, []byte(""), nil, 1)
	require.Equal(t, []byte(""), lockStart)
	require.Equal(t, nextScanCursor(internalKey), lockEnd)
}

func TestScanLockBoundsForKVs_ReverseInternalOnlyPageUsesOriginalRange(t *testing.T) {
	t.Parallel()

	internalKey := txnCommitKey([]byte("primary"), 10)
	kvs := []*store.KVPair{
		{Key: internalKey, Value: []byte("commit")},
	}

	lockStart, lockEnd := scanLockBoundsForKVsDirection(kvs, []byte(""), []byte("z"), 1, true)
	require.Equal(t, []byte(""), lockStart)
	require.Equal(t, []byte("z"), lockEnd)
}

// Redis list-delta/claim and stream rows are placed by their raw key but are
// treated as owned by the logical user key by route-bound scans and by
// prefix-write floors. Point writes checked only the raw-key route, so a fenced
// user key still accepted its auxiliary rows.
func TestShardStorePointWriteChecksRedisAuxiliaryLogicalRouteFloor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			// Raw "!..." keys sort below "m" and land on the unfenced route.
			{RouteID: 1, Start: []byte(""), End: []byte("m"), GroupID: 1, State: distribution.RouteStateActive},
			// The logical user key "zulu" lands on the fenced route.
			{RouteID: 2, Start: []byte("m"), GroupID: 2, State: distribution.RouteStateActive, MinWriteTSExclusive: 100},
		},
	}))
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	t.Cleanup(func() {
		_ = groups[1].Store.Close()
		_ = groups[2].Store.Close()
	})
	st := NewShardStore(engine, groups)

	fenced := []byte("zulu")
	unfenced := []byte("alpha")

	tests := []struct {
		name       string
		key        []byte
		commitTS   uint64
		wantReject bool
	}{
		{
			name:       "list delta under a fenced user key",
			key:        store.ListMetaDeltaKey(fenced, 10, 0),
			commitTS:   100,
			wantReject: true,
		},
		{
			name:       "list claim under a fenced user key",
			key:        store.ListClaimKey(fenced, 1),
			commitTS:   100,
			wantReject: true,
		},
		{
			name:       "stream meta under a fenced user key",
			key:        store.StreamMetaKey(fenced),
			commitTS:   100,
			wantReject: true,
		},
		{
			name:       "stream entry under a fenced user key",
			key:        store.StreamEntryKey(fenced, 123, 4),
			commitTS:   100,
			wantReject: true,
		},
		{
			name:       "stream entry above the fenced floor is admitted",
			key:        store.StreamEntryKey(fenced, 123, 5),
			commitTS:   101,
			wantReject: false,
		},
		{
			name:       "auxiliary row under an unfenced user key is admitted",
			key:        store.ListMetaDeltaKey(unfenced, 10, 0),
			commitTS:   100,
			wantReject: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := st.PutAt(ctx, tt.key, []byte("v"), tt.commitTS, 0)
			if tt.wantReject {
				require.ErrorIs(t, err, store.ErrWriteConflict)
				return
			}
			require.NoError(t, err)
		})
	}
}

// The coordinator's admission check must reject the same rows, so a fenced user
// key's auxiliary writes never reach Raft in the first place.
func TestShardedCoordinatorRejectsRedisAuxiliaryWriteUnderLogicalFloor(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte(""), End: []byte("m"), GroupID: 1, State: distribution.RouteStateActive},
			{RouteID: 2, Start: []byte("m"), GroupID: 2, State: distribution.RouteStateActive, MinWriteTSExclusive: 100},
		},
	}))
	c := &ShardedCoordinator{engine: engine}

	fenced := store.StreamEntryKey([]byte("zulu"), 123, 4)
	require.ErrorIs(t,
		c.ensureMutationsWriteAllowed([]*pb.Mutation{{Op: pb.Op_PUT, Key: fenced, Value: []byte("v")}}, 100),
		store.ErrWriteConflict)
	require.NoError(t,
		c.ensureMutationsWriteAllowed([]*pb.Mutation{{Op: pb.Op_PUT, Key: fenced, Value: []byte("v")}}, 101))
}

// A tombstone on the primary route must hide the legacy wide-column value even
// when a newer version sits above the read timestamp. The remote fallback probe
// used to compare only the newest commit timestamp, so latest > ts read as "not
// visible here", the point read fell through to the legacy route, and the
// snapshot read between the tombstone and the newer write resurrected the old
// value.
func TestShardStorePointReadStopsLegacyFallbackOnRemoteTombstone(t *testing.T) {
	t.Parallel()

	const readTS = uint64(100)

	tests := []struct {
		name             string
		versionVisible   bool
		versionSupported bool
		wantLegacyValue  bool
	}{
		{
			name:             "leader reports a version visible at the read ts",
			versionVisible:   true,
			versionSupported: true,
			wantLegacyValue:  false,
		},
		{
			name:             "leader reports no version at or before the read ts",
			versionVisible:   false,
			versionSupported: true,
			wantLegacyValue:  true,
		},
		{
			// Pre-upgrade peer: the probe is unanswered, so the caller keeps
			// the old latest-commit heuristic rather than treating the silence
			// as "no version".
			name:             "peer predating the probe falls back to the heuristic",
			versionVisible:   false,
			versionSupported: false,
			wantLegacyValue:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fake := &fakeRawKVServer{
				getResp: &pb.RawGetResponse{Exists: false},
				latestResp: &pb.RawLatestCommitTSResponse{
					// Newer than readTS, which is what defeats the heuristic.
					Ts:                      readTS + 100,
					Exists:                  true,
					VersionVisible:          tt.versionVisible,
					VersionVisibleSupported: tt.versionSupported,
				},
			}
			addr, stop := startRawKVServer(t, fake)
			t.Cleanup(stop)

			engine := distribution.NewEngine()
			require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
				Version: 5,
				Routes: []distribution.RouteDescriptor{
					// Raw "!hs|fld|..." keys sort below "m" and stay on group 2.
					{RouteID: 1, Start: []byte(""), End: []byte("m"), GroupID: 2, State: distribution.RouteStateActive},
					// The logical user key "zulu" lives on group 1.
					{RouteID: 2, Start: []byte("m"), GroupID: 1, State: distribution.RouteStateActive},
				},
			}))

			legacyStore := store.NewMVCCStore()
			groups := map[uint64]*ShardGroup{
				1: {Store: store.NewMVCCStore(), Engine: &stubFollowerEngine{leaderAddr: addr}},
				2: {Store: legacyStore},
			}
			st := NewShardStore(engine, groups)
			t.Cleanup(func() { _ = st.Close() })

			ctx := context.Background()
			fieldKey := store.HashFieldKey([]byte("zulu"), []byte("f"))
			require.NoError(t, legacyStore.PutAt(ctx, fieldKey, []byte("legacy"), 1, 0))

			got, err := st.GetAt(ctx, fieldKey, readTS)
			if tt.wantLegacyValue {
				require.NoError(t, err)
				require.Equal(t, []byte("legacy"), got)
				return
			}
			require.ErrorIs(t, err, store.ErrKeyNotFound)
		})
	}
}

// The server half: a group-scoped presence probe is answered from the group's
// own store, and requests that do not ask leave both response fields unset.
func TestShardStoreVersionExistsAtOrBeforeGroupWithReadFence(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte(""), GroupID: 1, State: distribution.RouteStateActive},
		},
	}))
	st := NewShardStore(engine, map[uint64]*ShardGroup{1: {Store: store.NewMVCCStore()}})
	t.Cleanup(func() { _ = st.Close() })

	key := []byte("k")
	require.NoError(t, st.PutAt(ctx, key, []byte("v"), 50, 0))

	visible, ok, err := st.VersionExistsAtOrBeforeGroupWithReadFence(ctx, key, 1, 100, 0)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, visible)

	visible, ok, err = st.VersionExistsAtOrBeforeGroupWithReadFence(ctx, key, 1, 10, 0)
	require.NoError(t, err)
	require.True(t, ok)
	require.False(t, visible)

	// Unknown group: authoritative "no version" rather than an unanswered probe.
	visible, ok, err = st.VersionExistsAtOrBeforeGroupWithReadFence(ctx, key, 99, 100, 0)
	require.NoError(t, err)
	require.True(t, ok)
	require.False(t, visible)
}

// fenceGroupRouter is a Coordinator that only implements the group routing the
// fence dedup path uses, resolving exactly the way production does: normalize
// the key first, then look the route up.
type fenceGroupRouter struct {
	Coordinator
	engine *distribution.Engine
}

func (f *fenceGroupRouter) EngineGroupIDForKey(key []byte) uint64 {
	route, ok := f.engine.GetRoute(routeKey(key))
	if !ok {
		return 0
	}
	return route.GroupID
}

// The fence's representative keys are re-normalized by every downstream
// consumer (LeaseReadGroupKeys -> EngineGroupIDForKey -> ResolveGroup ->
// routeKey). For a Redis wide-column range the owner key and the legacy
// raw-prefix key both normalize to the same user key, so resolving groups from
// bytes collapses the two into one and leaves the legacy group unfenced. The
// group id has to survive on the target instead.
func TestReadFenceTargetsSurviveGroupDedup(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)
	st := NewShardStore(engine, map[uint64]*ShardGroup{1: {}, 2: {}})

	prefix := store.HashFieldScanPrefix([]byte("zebra"))
	targets := st.ReadFenceTargetsForRange(prefix, store.PrefixScanEnd(prefix))
	require.Len(t, targets, 2, "owner group plus legacy raw-prefix group")

	carried := make([]uint64, 0, len(targets))
	reResolved := make([]uint64, 0, len(targets))
	for _, target := range targets {
		carried = append(carried, target.GroupID)
		route, ok := engine.GetRoute(routeKey(target.Key))
		require.True(t, ok)
		reResolved = append(reResolved, route.GroupID)
	}

	require.ElementsMatch(t, []uint64{2, 1}, carried,
		"the fence must name both groups")
	// Re-deriving from bytes is exactly what loses the legacy group; asserting
	// it here pins why GroupID is carried rather than recomputed.
	require.Equal(t, []uint64{2, 2}, reResolved,
		"both representative keys normalize to the owner, so bytes alone are lossy")
}

// LeaseReadGroupTargets must keep both groups, where the key-only
// LeaseReadGroupKeys collapses them.
func TestLeaseReadGroupTargetsKeepsLegacyGroup(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)
	st := NewShardStore(engine, map[uint64]*ShardGroup{1: {}, 2: {}})
	router := &fenceGroupRouter{engine: engine}

	prefix := store.HashFieldScanPrefix([]byte("zebra"))
	targets := st.ReadFenceTargetsForRange(prefix, store.PrefixScanEnd(prefix))

	kept := LeaseReadGroupTargets(router, targets)
	keptGroups := make([]uint64, 0, len(kept))
	for _, target := range kept {
		keptGroups = append(keptGroups, target.GroupID)
	}
	require.ElementsMatch(t, []uint64{2, 1}, keptGroups,
		"both fenced groups must survive dedup")

	collapsed := LeaseReadGroupKeys(router, st.ReadFenceGroupKeysForRange(prefix, store.PrefixScanEnd(prefix)))
	require.Len(t, collapsed, 1,
		"the key-only path collapses to one group; this is the gap targets close")
}

// syntheticGroupRouter mimics Coordinate: EngineGroupIDForKey returns a constant
// that exists only to collapse single-group deployments to one lease. It is not
// a real group id and no group map contains it.
type syntheticGroupRouter struct {
	Coordinator
}

func (syntheticGroupRouter) EngineGroupIDForKey([]byte) uint64 { return 1 }

// A point key carries GroupID 0, meaning "resolve from Key". Dedup must not
// stamp the resolved id onto it: on a single-group Coordinate that id is
// synthetic, and a later group-routed lease read or leader check would look it
// up in a group map that has never heard of it and fail closed with
// ErrLeaderNotFound.
func TestLeaseReadGroupTargetsKeepsKeyResolutionForPointKeys(t *testing.T) {
	t.Parallel()

	targets := []ReadFenceTarget{{Key: []byte("!redis|str|k")}}
	got := LeaseReadGroupTargets(syntheticGroupRouter{}, targets)

	require.Len(t, got, 1)
	require.Zero(t, got[0].GroupID,
		"a key-resolved target must stay key-resolved through dedup")
	require.Equal(t, []byte("!redis|str|k"), got[0].Key)
}
