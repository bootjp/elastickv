package server

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	"github.com/bootjp/elastickv/internal/encryptionwiring"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestBuildShardGroupsWithDedicatedTSOPreservesSingleDataGroupDir(t *testing.T) {
	t.Parallel()
	baseDir := t.TempDir()
	raftID := "n1"
	legacyDir := filepath.Join(baseDir, raftID)
	require.NoError(t, os.MkdirAll(legacyDir, noderuntime.RaftDirPerm))
	legacyMarker := filepath.Join(legacyDir, "legacy.marker")
	require.NoError(t, os.WriteFile(legacyMarker, []byte("keep"), 0o600))

	groups := []noderuntime.GroupSpec{
		{ID: noderuntime.DedicatedTSORaftGroupID, Address: "127.0.0.1:17000"},
		{ID: 1, Address: "127.0.0.1:17001"},
	}
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)
	factory, err := newRaftFactory(noderuntime.RaftEngineEtcd, nil)
	require.NoError(t, err)

	runtimes, shardGroups, err := buildShardGroups(
		raftID,
		baseDir,
		groups,
		true,
		true,
		noderuntime.BootstrapConfig{},
		factory,
		nil,
		kv.NewHLC(),
		nil,
		nil,
		nil,
		"",
		encryptionwiring.WriteWiring{},
		engine,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		for _, rt := range runtimes {
			rt.Close()
		}
	})
	require.Contains(t, shardGroups, uint64(noderuntime.DedicatedTSORaftGroupID))
	require.Contains(t, shardGroups, uint64(1))
	require.Nil(t, shardGroups[noderuntime.DedicatedTSORaftGroupID].Store)
	require.IsType(t, &kv.TSOStateMachine{}, runtimes[0].StateMachine)
	require.Nil(t, runtimes[0].Store)
	require.DirExists(t, filepath.Join(legacyDir, "fsm.db"))
	require.DirExists(t, filepath.Join(legacyDir, "group-0"))
	require.NoDirExists(t, filepath.Join(legacyDir, "group-0", "fsm.db"))
	require.NoDirExists(t, filepath.Join(legacyDir, "group-1"), "group 1 should stay in legacy dir")
	got, err := os.ReadFile(legacyMarker)
	require.NoError(t, err)
	require.Equal(t, []byte("keep"), got)
}

func TestBuildShardGroupsWithDedicatedTSOPreservesLegacyGroupZeroStore(t *testing.T) {
	t.Parallel()
	baseDir := t.TempDir()
	legacyStoreDir := filepath.Join(baseDir, "n1", "group-0", "fsm.db")
	require.NoError(t, os.MkdirAll(legacyStoreDir, noderuntime.RaftDirPerm))
	legacyMarker := filepath.Join(legacyStoreDir, "legacy.marker")
	require.NoError(t, os.WriteFile(legacyMarker, []byte("preserve"), 0o600))

	factory, err := newRaftFactory(noderuntime.RaftEngineEtcd, nil)
	require.NoError(t, err)
	runtimes, shardGroups, err := buildShardGroups(
		"n1",
		baseDir,
		[]noderuntime.GroupSpec{{ID: noderuntime.DedicatedTSORaftGroupID, Address: "127.0.0.1:17002"}},
		true,
		true,
		noderuntime.BootstrapConfig{},
		factory,
		nil,
		kv.NewHLC(),
		kv.NewActiveTimestampTracker(),
		nil,
		nil,
		"",
		encryptionwiring.WriteWiring{},
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() { closeRaftGroupRuntimes(runtimes) })
	require.Nil(t, shardGroups[noderuntime.DedicatedTSORaftGroupID].Store)
	require.Nil(t, runtimes[0].Store)

	got, err := os.ReadFile(legacyMarker)
	require.NoError(t, err)
	require.Equal(t, []byte("preserve"), got)
}

func TestBuildShardGroupsClosesEarlierStoresWhenLaterGroupFails(t *testing.T) {
	t.Parallel()
	baseDir := t.TempDir()
	badGroupDir := noderuntime.GroupDataDir(baseDir, "n1", 2, true)
	require.NoError(t, os.MkdirAll(badGroupDir, noderuntime.RaftDirPerm))
	require.NoError(t, os.WriteFile(
		filepath.Join(badGroupDir, noderuntime.RaftEngineMarkerFile),
		[]byte("unsupported\n"),
		noderuntime.RaftEngineMarkerPerm,
	))

	factory, err := newRaftFactory(noderuntime.RaftEngineEtcd, nil)
	require.NoError(t, err)
	runtimes, shardGroups, err := buildShardGroups(
		"n1",
		baseDir,
		[]noderuntime.GroupSpec{
			{ID: 1, Address: "127.0.0.1:17011"},
			{ID: 2, Address: "127.0.0.1:17012"},
		},
		true,
		true,
		noderuntime.BootstrapConfig{},
		factory,
		nil,
		kv.NewHLC(),
		kv.NewActiveTimestampTracker(),
		nil,
		nil,
		"",
		encryptionwiring.WriteWiring{},
		nil,
	)
	require.ErrorIs(t, err, noderuntime.ErrUnsupportedRaftEngine)
	require.Nil(t, runtimes)
	require.Nil(t, shardGroups)

	// Reopening the first group's Pebble directory proves the error path ran
	// raftGroupRuntime.Close, which owns both engine and store cleanup.
	reopened, err := store.NewPebbleStore(filepath.Join(
		noderuntime.GroupDataDir(baseDir, "n1", 1, true),
		"fsm.db",
	))
	require.NoError(t, err)
	require.NoError(t, reopened.Close())
}

func TestDedicatedTSOGroupContinuesAfterLegacyEncryptionEntry(t *testing.T) {
	t.Parallel()
	baseDir := t.TempDir()
	factory, err := newRaftFactory(noderuntime.RaftEngineEtcd, nil)
	require.NoError(t, err)
	clock := kv.NewHLC()
	runtimes, _, err := buildShardGroups(
		"n1",
		baseDir,
		[]noderuntime.GroupSpec{{ID: noderuntime.DedicatedTSORaftGroupID, Address: "127.0.0.1:17020"}},
		true,
		true,
		noderuntime.BootstrapConfig{},
		factory,
		nil,
		clock,
		kv.NewActiveTimestampTracker(),
		nil,
		nil,
		"",
		encryptionwiring.WriteWiring{},
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() { closeRaftGroupRuntimes(runtimes) })

	legacyEntry := append([]byte{fsmwire.OpRegistration}, fsmwire.EncodeRegistration(
		fsmwire.RegistrationPayload{DEKID: 1, FullNodeID: 2, LocalEpoch: 3},
	)...)
	result, err := runtimes[0].Engine.ProposeAdmin(context.Background(), legacyEntry)
	require.NoError(t, err)
	legacyErr, ok := result.Response.(error)
	require.Truef(t, ok, "legacy response type = %T, want error", result.Response)
	require.ErrorIs(t, legacyErr, kv.ErrTSOLegacyEncryptionEntryRejected)

	const ceilingMs = int64(1_700_000_123_456)
	coordinator := kv.NewCoordinatorWithEngine(nil, runtimes[0].Engine)
	t.Cleanup(func() { require.NoError(t, coordinator.Close()) })
	require.NoError(t, coordinator.ProposeHLCLease(context.Background(), ceilingMs))
	require.Equal(t, ceilingMs, clock.PhysicalCeiling())
}

func TestBuildShardGroupsWithEtcdEngineRoutesAcrossGroups(t *testing.T) {
	t.Parallel()
	baseDir := t.TempDir()
	groups := []noderuntime.GroupSpec{
		{ID: 1, Address: "127.0.0.1:15001"},
		{ID: 2, Address: "127.0.0.1:15002"},
	}

	factory, err := newRaftFactory(noderuntime.RaftEngineEtcd, nil)
	require.NoError(t, err)
	clock := kv.NewHLC()
	runtimes, shardGroups, err := buildShardGroups("n1", baseDir, groups, true, true, noderuntime.BootstrapConfig{},
		factory, nil, clock, kv.NewActiveTimestampTracker(), nil, nil, "", encryptionwiring.WriteWiring{}, nil)
	require.NoError(t, err)

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	shardStore := kv.NewShardStore(engine, shardGroups)
	t.Cleanup(func() {
		require.NoError(t, shardStore.Close())
		for _, rt := range runtimes {
			rt.Close()
		}
	})

	coord := kv.NewShardedCoordinator(engine, shardGroups, 1, clock, shardStore)
	_, err = coord.Dispatch(context.Background(), &kv.OperationGroup[kv.OP]{
		Elems: []*kv.Elem[kv.OP]{
			{Op: kv.Put, Key: []byte("b"), Value: []byte("left")},
			{Op: kv.Put, Key: []byte("x"), Value: []byte("right")},
		},
	})
	require.NoError(t, err)

	readTS := shardStore.LastCommitTS()
	value, err := shardStore.GetAt(context.Background(), []byte("b"), readTS)
	require.NoError(t, err)
	require.Equal(t, []byte("left"), value)

	value, err = shardStore.GetAt(context.Background(), []byte("x"), readTS)
	require.NoError(t, err)
	require.Equal(t, []byte("right"), value)

	_, err = shardGroups[1].Store.GetAt(context.Background(), []byte("x"), readTS)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	_, err = shardGroups[2].Store.GetAt(context.Background(), []byte("b"), readTS)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestBuildShardGroupsWithEtcdEngineRestartsAcrossGroups(t *testing.T) {
	t.Parallel()
	baseDir := t.TempDir()
	groups := []noderuntime.GroupSpec{
		{ID: 1, Address: "127.0.0.1:16001"},
		{ID: 2, Address: "127.0.0.1:16002"},
	}

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	sharedClock := kv.NewHLC()
	openShardStore := func(bootstrap bool) ([]*clusterbootstrap.GroupRuntime, map[uint64]*kv.ShardGroup, *kv.ShardStore) {
		factory, err := newRaftFactory(noderuntime.RaftEngineEtcd, nil)
		require.NoError(t, err)
		runtimes, shardGroups, err := buildShardGroups("n1", baseDir, groups, true, bootstrap,
			noderuntime.BootstrapConfig{}, factory, nil, sharedClock, kv.NewActiveTimestampTracker(), nil, nil, "",
			encryptionwiring.WriteWiring{}, nil)
		require.NoError(t, err)
		shardStore := kv.NewShardStore(engine, shardGroups)
		return runtimes, shardGroups, shardStore
	}

	runtimes, shardGroups, shardStore := openShardStore(true)
	coord := kv.NewShardedCoordinator(engine, shardGroups, 1, sharedClock, shardStore)

	_, err := coord.Dispatch(context.Background(), &kv.OperationGroup[kv.OP]{
		Elems: []*kv.Elem[kv.OP]{
			{Op: kv.Put, Key: []byte("b"), Value: []byte("left")},
			{Op: kv.Put, Key: []byte("x"), Value: []byte("right")},
		},
	})
	require.NoError(t, err)
	require.NoError(t, shardStore.Close())
	for _, rt := range runtimes {
		rt.Close()
	}

	runtimes, shardGroups, shardStore = openShardStore(false)
	t.Cleanup(func() {
		require.NoError(t, shardStore.Close())
		for _, rt := range runtimes {
			rt.Close()
		}
	})
	coord = kv.NewShardedCoordinator(engine, shardGroups, 1, kv.NewHLC(), shardStore)

	readTS := shardStore.LastCommitTS()

	value, err := shardStore.GetAt(context.Background(), []byte("b"), readTS)
	require.NoError(t, err)
	require.Equal(t, []byte("left"), value)

	value, err = shardStore.GetAt(context.Background(), []byte("x"), readTS)
	require.NoError(t, err)
	require.Equal(t, []byte("right"), value)

	_, err = coord.Dispatch(context.Background(), &kv.OperationGroup[kv.OP]{
		Elems: []*kv.Elem[kv.OP]{
			{Op: kv.Put, Key: []byte("c"), Value: []byte("again-left")},
			{Op: kv.Put, Key: []byte("z"), Value: []byte("again-right")},
		},
	})
	require.NoError(t, err)
}
