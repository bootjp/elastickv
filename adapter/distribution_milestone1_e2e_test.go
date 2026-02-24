package adapter

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	cerrs "github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestMilestone1SplitRange_EndToEndRefreshAndDataPath(t *testing.T) {
	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	group1Store := store.NewMVCCStore()
	group1Raft, stopGroup1 := newSingleRaftForDistributionE2E(t, "group-1", kv.NewKvFSM(group1Store))
	defer stopGroup1()

	group2Store := store.NewMVCCStore()
	group2Raft, stopGroup2 := newSingleRaftForDistributionE2E(t, "group-2", kv.NewKvFSM(group2Store))
	defer stopGroup2()

	groups := map[uint64]*kv.ShardGroup{
		1: {Raft: group1Raft, Store: group1Store, Txn: kv.NewLeaderProxy(group1Raft)},
		2: {Raft: group2Raft, Store: group2Store, Txn: kv.NewLeaderProxy(group2Raft)},
	}
	shardStore := kv.NewShardStore(engine, groups)
	t.Cleanup(func() {
		require.NoError(t, shardStore.Close())
	})
	coordinator := kv.NewShardedCoordinator(engine, groups, 1, kv.NewHLC(), shardStore)

	catalog := distribution.NewCatalogStore(group1Store)
	initial, err := distribution.EnsureCatalogSnapshot(ctx, catalog, engine)
	require.NoError(t, err)
	require.Equal(t, uint64(1), initial.Version)
	require.Equal(t, initial.Version, engine.Version())

	followerEngine := distribution.NewEngine()
	require.NoError(t, followerEngine.ApplySnapshot(initial))
	watcher := distribution.NewCatalogWatcher(
		catalog,
		followerEngine,
		distribution.WithCatalogWatcherInterval(5*time.Millisecond),
	)
	watchCtx, cancelWatch := context.WithCancel(context.Background())
	watchErrCh := make(chan error, 1)
	go func() {
		watchErrCh <- watcher.Run(watchCtx)
	}()
	t.Cleanup(func() {
		cancelWatch()
		require.NoError(t, <-watchErrCh)
	})

	_, err = coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		IsTxn: false,
		Elems: []*kv.Elem[kv.OP]{
			{Op: kv.Put, Key: []byte("b"), Value: []byte("before-left")},
			{Op: kv.Put, Key: []byte("h"), Value: []byte("before-right")},
			{Op: kv.Put, Key: []byte("x"), Value: []byte("before-other-group")},
		},
	})
	require.NoError(t, err)

	readTS := shardStore.LastCommitTS()
	requireValueAt(t, shardStore, []byte("b"), []byte("before-left"), readTS)
	requireValueAt(t, shardStore, []byte("h"), []byte("before-right"), readTS)
	requireValueAt(t, shardStore, []byte("x"), []byte("before-other-group"), readTS)

	distServer := NewDistributionServer(engine, catalog, WithDistributionCoordinator(coordinator))
	splitResp, err := distServer.SplitRange(ctx, &pb.SplitRangeRequest{
		ExpectedCatalogVersion: initial.Version,
		RouteId:                1,
		SplitKey:               []byte("g"),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), splitResp.CatalogVersion)

	require.Eventually(t, func() bool {
		if engine.Version() != splitResp.CatalogVersion {
			return false
		}
		if followerEngine.Version() != splitResp.CatalogVersion {
			return false
		}
		left, ok := followerEngine.GetRoute([]byte("b"))
		if !ok || left.RouteID != splitResp.GetLeft().GetRouteId() {
			return false
		}
		right, ok := followerEngine.GetRoute([]byte("h"))
		return ok && right.RouteID == splitResp.GetRight().GetRouteId()
	}, time.Second, 10*time.Millisecond)

	left, ok := engine.GetRoute([]byte("b"))
	require.True(t, ok)
	require.Equal(t, splitResp.GetLeft().GetRouteId(), left.RouteID)
	require.Equal(t, uint64(1), left.GroupID)
	right, ok := engine.GetRoute([]byte("h"))
	require.True(t, ok)
	require.Equal(t, splitResp.GetRight().GetRouteId(), right.RouteID)
	require.Equal(t, uint64(1), right.GroupID)

	_, err = coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		IsTxn: false,
		Elems: []*kv.Elem[kv.OP]{
			{Op: kv.Put, Key: []byte("b"), Value: []byte("after-left")},
			{Op: kv.Put, Key: []byte("h"), Value: []byte("after-right")},
			{Op: kv.Put, Key: []byte("x"), Value: []byte("after-other-group")},
		},
	})
	require.NoError(t, err)

	readTS = shardStore.LastCommitTS()
	requireValueAt(t, shardStore, []byte("b"), []byte("after-left"), readTS)
	requireValueAt(t, shardStore, []byte("h"), []byte("after-right"), readTS)
	requireValueAt(t, shardStore, []byte("x"), []byte("after-other-group"), readTS)

	requireValueAt(t, group1Store, []byte("b"), []byte("after-left"), readTS)
	requireValueAt(t, group1Store, []byte("h"), []byte("after-right"), readTS)
	requireValueAt(t, group2Store, []byte("x"), []byte("after-other-group"), readTS)

	_, err = group1Store.GetAt(ctx, []byte("x"), readTS)
	require.Error(t, err)
	require.True(t, cerrs.Is(err, store.ErrKeyNotFound))
	_, err = group2Store.GetAt(ctx, []byte("h"), readTS)
	require.Error(t, err)
	require.True(t, cerrs.Is(err, store.ErrKeyNotFound))

	listResp, err := distServer.ListRoutes(ctx, &pb.ListRoutesRequest{})
	require.NoError(t, err)
	require.Equal(t, splitResp.CatalogVersion, listResp.CatalogVersion)
	require.Len(t, listResp.Routes, 3)
}

func TestMilestone1SplitRange_RestartReloadsCatalog(t *testing.T) {
	ctx := context.Background()
	catalogDir := filepath.Join(t.TempDir(), "catalog")

	baseStore, err := store.NewPebbleStore(catalogDir)
	require.NoError(t, err)

	catalog := distribution.NewCatalogStore(baseStore)
	initial, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{
		{
			RouteID:       1,
			Start:         []byte(""),
			End:           []byte("m"),
			GroupID:       1,
			State:         distribution.RouteStateActive,
			ParentRouteID: 0,
		},
		{
			RouteID:       2,
			Start:         []byte("m"),
			End:           nil,
			GroupID:       2,
			State:         distribution.RouteStateActive,
			ParentRouteID: 0,
		},
	})
	require.NoError(t, err)

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(initial))

	distServer := NewDistributionServer(
		engine,
		catalog,
		WithDistributionCoordinator(newDistributionCoordinatorStub(baseStore, true)),
	)
	splitResp, err := distServer.SplitRange(ctx, &pb.SplitRangeRequest{
		ExpectedCatalogVersion: initial.Version,
		RouteId:                1,
		SplitKey:               []byte("g"),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), splitResp.CatalogVersion)
	require.NoError(t, baseStore.Close())

	reopenedStore, err := store.NewPebbleStore(catalogDir)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, reopenedStore.Close())
	}()

	reopenedCatalog := distribution.NewCatalogStore(reopenedStore)
	restartedEngine := distribution.NewEngine()
	restartedSnapshot, err := distribution.EnsureCatalogSnapshot(ctx, reopenedCatalog, restartedEngine)
	require.NoError(t, err)
	require.Equal(t, splitResp.CatalogVersion, restartedSnapshot.Version)
	require.Len(t, restartedSnapshot.Routes, 3)

	left, ok := restartedEngine.GetRoute([]byte("b"))
	require.True(t, ok)
	require.Equal(t, splitResp.GetLeft().GetRouteId(), left.RouteID)
	right, ok := restartedEngine.GetRoute([]byte("h"))
	require.True(t, ok)
	require.Equal(t, splitResp.GetRight().GetRouteId(), right.RouteID)

	version, err := reopenedCatalog.Version(ctx)
	require.NoError(t, err)
	require.Equal(t, splitResp.CatalogVersion, version)

	nextRouteID, err := reopenedCatalog.NextRouteID(ctx)
	require.NoError(t, err)
	require.Equal(t, splitResp.GetRight().GetRouteId()+1, nextRouteID)
}

func requireValueAt(t *testing.T, st store.MVCCStore, key []byte, expected []byte, ts uint64) {
	t.Helper()

	actual, err := st.GetAt(context.Background(), key, ts)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func newSingleRaftForDistributionE2E(t *testing.T, id string, fsm raft.FSM) (*raft.Raft, func()) {
	t.Helper()

	addr, trans := raft.NewInmemTransport(raft.ServerAddress(id))
	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(id)
	cfg.HeartbeatTimeout = 50 * time.Millisecond
	cfg.ElectionTimeout = 100 * time.Millisecond
	cfg.LeaderLeaseTimeout = 50 * time.Millisecond

	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()
	snapshotStore := raft.NewInmemSnapshotStore()
	r, err := raft.NewRaft(cfg, fsm, logStore, stableStore, snapshotStore, trans)
	require.NoError(t, err)

	bootstrapErr := r.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{
			{
				Suffrage: raft.Voter,
				ID:       raft.ServerID(id),
				Address:  addr,
			},
		},
	}).Error()
	require.NoError(t, bootstrapErr)

	require.Eventually(t, func() bool {
		return r.State() == raft.Leader
	}, time.Second, 10*time.Millisecond)

	stop := func() {
		require.NoError(t, r.Shutdown().Error())
	}
	return r, stop
}
