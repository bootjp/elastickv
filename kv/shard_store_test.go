package kv

import (
	"context"
	"sync"
	"testing"

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

func (e *followerProxyEngine) SnapshotEvery() uint64 { return 0 }

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

	keys, err := st.scanKeysRouteAtLeader(ctx, g, []byte(""), nil, 1, ^uint64(0))
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

	keys, err := st.scanKeysRouteAtLeader(ctx, g, nil, nil, 2, ^uint64(0))
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

	page, err := st.scanRouteAtForwardPage(ctx, distribution.Route{GroupID: 42}, g, []byte(""), nil, 2, ^uint64(0))
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

func TestBackupKeyScannerPaging(t *testing.T) {
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
	commitTS := uint64(1)
	for _, key := range [][]byte{[]byte("a"), []byte("b"), []byte("x"), []byte("z")} {
		require.NoError(t, st.PutAt(ctx, key, []byte("value-not-materialized"), commitTS, 0))
		commitTS++
	}

	sc := st.NewBackupKeyScanner([]byte(""), nil, ^uint64(0), 2)
	defer sc.Close()
	var got [][]byte
	for {
		key, ok, err := sc.Next(ctx)
		require.NoError(t, err)
		if !ok {
			break
		}
		got = append(got, key)
	}
	require.Equal(t, [][]byte{[]byte("a"), []byte("b"), []byte("x"), []byte("z")}, got)
}

func TestBackupKeyScannerFiltersStaleOffRouteKeys(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	groups := map[uint64]*ShardGroup{1: {Store: store.NewMVCCStore()}}
	st := NewShardStore(distribution.NewEngine(), groups)
	require.NoError(t, groups[1].Store.PutAt(ctx, []byte("a"), []byte("stale"), 1, 0))
	require.NoError(t, groups[1].Store.PutAt(ctx, []byte("x"), []byte("owned"), 2, 0))
	scanner := &backupKeyScanner{
		store: st, routes: []distribution.Route{{Start: []byte("m"), GroupID: 1}},
		cursor: []byte(""), ts: ^uint64(0), pageSize: 1,
	}
	defer scanner.Close()

	key, ok, err := scanner.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("x"), key)
	_, ok, err = scanner.Next(ctx)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestBackupScannerReusesCapturedRouteSnapshot(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	require.NoError(t, groups[1].Store.PutAt(ctx, []byte("a"), []byte("pinned-owner"), 1, 0))
	snapshot := st.CaptureBackupRouteSnapshot(nil, nil)
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte(""), GroupID: 2, State: distribution.RouteStateActive},
		},
	}))
	require.NoError(t, groups[2].Store.PutAt(ctx, []byte("a"), []byte("live-owner"), 2, 0))

	scanner := st.NewBackupScannerAtSnapshot(snapshot, ^uint64(0), 1)
	defer scanner.Close()
	pair, ok, err := scanner.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("pinned-owner"), pair.Value)
}

func TestCaptureBackupRouteSnapshotAtUsesDurableCatalogTimestamp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	engine := distribution.NewEngine()
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	catalog := distribution.NewCatalogStore(groups[1].Store)
	old, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{
		{RouteID: 1, Start: []byte(""), End: []byte("m"), GroupID: 1, State: distribution.RouteStateActive},
		{RouteID: 2, Start: []byte("m"), GroupID: 2, State: distribution.RouteStateActive},
	})
	require.NoError(t, err)
	require.NoError(t, engine.ApplySnapshot(old))
	oldReadTS := groups[1].Store.LastCommitTS()
	current, err := catalog.Save(ctx, old.Version, []distribution.RouteDescriptor{
		{RouteID: 1, Start: []byte(""), GroupID: 1, State: distribution.RouteStateActive},
	})
	require.NoError(t, err)
	require.NoError(t, engine.ApplySnapshot(current))

	snapshot, err := CaptureBackupRouteSnapshotAt(ctx, catalog, oldReadTS)
	require.NoError(t, err)
	require.Len(t, snapshot.routes, 2)
	require.Equal(t, uint64(1), snapshot.routes[0].GroupID)
	require.Equal(t, uint64(2), snapshot.routes[1].GroupID)
}

func TestCaptureBackupRouteSnapshotAtReadsRowsFromCatalogOwner(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	owner := store.NewMVCCStore()
	wronglyRouted := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(owner)
	saved, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{
		{RouteID: 1, Start: []byte(""), End: []byte("m"), GroupID: 1, State: distribution.RouteStateActive},
		{RouteID: 2, Start: []byte("m"), GroupID: 2, State: distribution.RouteStateActive},
	})
	require.NoError(t, err)

	// A live shard router could send the reserved route-row prefix to group 2.
	// The backup reader must stay on the durable catalog owner instead.
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 2)
	_ = NewShardStore(engine, map[uint64]*ShardGroup{
		1: {Store: owner},
		2: {Store: wronglyRouted},
	})

	snapshot, err := CaptureBackupRouteSnapshotAt(ctx, catalog, owner.LastCommitTS())
	require.NoError(t, err)
	require.Len(t, snapshot.routes, len(saved.Routes))
	require.Equal(t, uint64(1), snapshot.routes[0].GroupID)
	require.Equal(t, uint64(2), snapshot.routes[1].GroupID)
}

func TestValidateBackupSnapshotAtRejectsPreparedInsert(t *testing.T) {
	ctx := context.Background()
	st := store.NewMVCCStore()
	fsm, ok := NewKvFSMWithHLCAndTracker(
		st, NewHLC(), NewActiveTimestampTracker(WithActiveTimestampTrackerSweepInterval(0)),
	).(*kvFSM)
	require.True(t, ok)
	primary := []byte("insert-only")
	prepare := &pb.Request{IsTxn: true, Phase: pb.Phase_PREPARE, Ts: 30, Mutations: []*pb.Mutation{
		{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{
			PrimaryKey: primary, LockTTLms: defaultTxnLockTTLms,
		})},
		{Op: pb.Op_PUT, Key: primary, Value: []byte("pending")},
	}}
	require.Nil(t, applyBackupTestRequest(t, fsm, prepare))

	engine := distribution.NewEngineWithDefaultRoute()
	shards := NewShardStore(engine, map[uint64]*ShardGroup{1: {Store: st}})
	snapshot := shards.CaptureBackupRouteSnapshot(nil, nil)
	err := shards.ValidateBackupSnapshotAt(ctx, snapshot, 50, 16)
	require.ErrorIs(t, err, ErrTxnLocked)
}

func TestValidateBackupSnapshotAtRejectsPostCutTxnDecision(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name  string
		phase pb.Phase
	}{
		{name: "commit", phase: pb.Phase_COMMIT},
		{name: "rollback", phase: pb.Phase_ABORT},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st := store.NewMVCCStore()
			fsm, ok := NewKvFSMWithHLCAndTracker(
				st, NewHLC(), NewActiveTimestampTracker(WithActiveTimestampTrackerSweepInterval(0)),
			).(*kvFSM)
			require.True(t, ok)
			primary := []byte("post-cut-" + tc.name)
			prepare := &pb.Request{IsTxn: true, Phase: pb.Phase_PREPARE, Ts: 30, Mutations: []*pb.Mutation{
				{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{
					PrimaryKey: primary, LockTTLms: defaultTxnLockTTLms,
				})},
				{Op: pb.Op_PUT, Key: primary, Value: []byte("pending")},
			}}
			require.Nil(t, applyBackupTestRequest(t, fsm, prepare))
			decision := &pb.Request{IsTxn: true, Phase: tc.phase, Ts: 30, Mutations: []*pb.Mutation{
				{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{
					PrimaryKey: primary, CommitTS: 60,
				})},
				{Op: pb.Op_PUT, Key: primary},
			}}
			require.Nil(t, applyBackupTestRequest(t, fsm, decision))

			engine := distribution.NewEngineWithDefaultRoute()
			shards := NewShardStore(engine, map[uint64]*ShardGroup{1: {Store: st}})
			snapshot := shards.CaptureBackupRouteSnapshot(nil, nil)
			err := shards.ValidateBackupSnapshotAt(ctx, snapshot, 50, 16)
			require.ErrorIs(t, err, ErrTxnLocked)
		})
	}
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
	require.NoError(t, groups[1].Store.PutAt(ctx, []byte("a"), []byte("first-enumerated-route"), 1, 0))
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
	require.Equal(t, []byte("first-enumerated-route"), kvp.Value)
}

func TestBackupScannersPreferCapturedOwnerForDuplicateKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(distribution.NewEngine(), groups)
	require.NoError(t, groups[2].Store.PutAt(ctx, []byte("a"), []byte("captured-owner"), 1, 0))
	require.NoError(t, groups[1].Store.PutAt(ctx, []byte("a"), []byte("stale-owner"), 1, 0))
	routes := []distribution.Route{
		{RouteID: 2, Start: []byte(""), End: []byte("m"), GroupID: 2},
		{RouteID: 1, Start: []byte("m"), GroupID: 1},
	}

	keyScanner := &backupKeyScanner{
		store: st, routes: routes, clampToRoutes: false,
		cursor: []byte(""), ts: ^uint64(0), pageSize: 1,
	}
	defer keyScanner.Close()
	key, ok, err := keyScanner.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("a"), key)
	_, ok, err = keyScanner.Next(ctx)
	require.NoError(t, err)
	require.False(t, ok)

	valueScanner := &backupScanner{
		store: st, routes: routes, clampToRoutes: false,
		cursor: []byte(""), ts: ^uint64(0), pageSize: 1,
	}
	defer valueScanner.Close()
	pair, ok, err := valueScanner.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("a"), pair.Key)
	require.Equal(t, []byte("captured-owner"), pair.Value)
}

func TestBackupScannerKeepsPartitionResolvedOwnerForSQSKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	key := []byte("!sqs|msg|data|p|orders|partition-2|msg-2")
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(distribution.NewEngine(), groups).
		WithPartitionResolver(&fakePartitionResolver{
			routes:           map[string]uint64{string(key): 2},
			recognisedPrefix: []byte("!sqs|msg|data|p|"),
		})
	require.NoError(t, groups[1].Store.PutAt(ctx, key, []byte("stale-engine-owner"), 1, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, key, []byte("partition-owner"), 2, 0))
	routes := []distribution.Route{
		{RouteID: 1, Start: []byte(""), End: []byte("m"), GroupID: 1},
		{RouteID: 2, Start: []byte("m"), GroupID: 2},
	}

	keyScanner := &backupKeyScanner{
		store: st, routes: routes, clampToRoutes: false,
		cursor: []byte(""), ts: ^uint64(0), pageSize: 1,
	}
	defer keyScanner.Close()
	gotKey, ok, err := keyScanner.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, key, gotKey)
	_, ok, err = keyScanner.Next(ctx)
	require.NoError(t, err)
	require.False(t, ok)

	valueScanner := &backupScanner{
		store: st, routes: routes, clampToRoutes: false,
		cursor: []byte(""), ts: ^uint64(0), pageSize: 1,
	}
	defer valueScanner.Close()
	pair, ok, err := valueScanner.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, key, pair.Key)
	require.Equal(t, []byte("partition-owner"), pair.Value)
}

func TestBackupScannersIncludePartitionOnlyGroups(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	key := []byte("!sqs|msg|data|p|orders|partition-2|msg-2")
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(distribution.NewEngine(), groups).
		WithPartitionResolver(&fakePartitionResolver{
			routes:           map[string]uint64{string(key): 2},
			recognisedPrefix: []byte("!sqs|msg|data|p|"),
		})
	require.NoError(t, groups[2].Store.PutAt(ctx, key, []byte("partition-only"), 1, 0))
	snapshot := BackupRouteSnapshotWithScanGroups(BackupRouteSnapshot{
		routes: []distribution.Route{{RouteID: 1, Start: []byte(""), End: []byte("m"), GroupID: 1}},
	}, []uint64{1, 2})

	keyScanner := st.NewBackupKeyScannerAtSnapshot(snapshot, ^uint64(0), 1)
	defer keyScanner.Close()
	gotKey, ok, err := keyScanner.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, key, gotKey)

	valueScanner := st.NewBackupScannerAtSnapshot(snapshot, ^uint64(0), 1)
	defer valueScanner.Close()
	pair, ok, err := valueScanner.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, key, pair.Key)
	require.Equal(t, []byte("partition-only"), pair.Value)
}

func TestFilteredBackupScannerAppliesFilterAfterOwnership(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(distribution.NewEngine(), groups)
	require.NoError(t, groups[1].Store.PutAt(ctx, []byte("bad"), []byte("stale-off-route"), 1, 0))
	require.NoError(t, groups[1].Store.PutAt(ctx, []byte("x"), []byte("owned"), 2, 0))
	scanner := &backupScanner{
		store: st,
		routes: []distribution.Route{
			{RouteID: 1, Start: []byte("m"), GroupID: 1},
		},
		cursor:   []byte(""),
		ts:       ^uint64(0),
		pageSize: 1,
		keyFilter: func(key []byte) (bool, error) {
			if string(key) == "bad" {
				return false, ErrInvalidRequest
			}
			return true, nil
		},
	}
	defer scanner.Close()

	pair, ok, err := scanner.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("x"), pair.Key)
	require.Equal(t, []byte("owned"), pair.Value)
}

func TestBackupScannerFailsClosedOnUnresolvedPartitionedSQSKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	key := []byte("!sqs|msg|data|p|orders|partition-9|msg-9")
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(distribution.NewEngine(), groups).
		WithPartitionResolver(&fakePartitionResolver{
			routes:           map[string]uint64{},
			recognisedPrefix: []byte("!sqs|msg|data|p|"),
		})
	require.NoError(t, groups[1].Store.PutAt(ctx, key, []byte("unresolved"), 1, 0))
	scanner := &backupScanner{
		store: st,
		routes: []distribution.Route{
			{RouteID: 1, Start: []byte(""), End: []byte("m"), GroupID: 1},
		},
		clampToRoutes: false,
		cursor:        []byte(""),
		ts:            ^uint64(0),
		pageSize:      1,
	}
	defer scanner.Close()

	_, _, err := scanner.Next(ctx)
	require.ErrorIs(t, err, ErrInvalidRequest)
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
