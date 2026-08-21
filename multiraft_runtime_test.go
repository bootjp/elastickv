package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestGroupDataDir(t *testing.T) {
	base := "/tmp/data"
	raftID := "n1"

	cases := []struct {
		name    string
		groupID uint64
		multi   bool
		want    string
	}{
		{name: "single", groupID: 1, want: filepath.Join(base, raftID)},
		{name: "reserved TSO group stays isolated in single data-group mode", groupID: 0, want: filepath.Join(base, raftID, "group-0")},
		{name: "multi", groupID: 2, multi: true, want: filepath.Join(base, raftID, "group-2")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, groupDataDir(base, raftID, tc.groupID, tc.multi))
		})
	}
}

func TestBuildShardGroupsWithDedicatedTSOPreservesSingleDataGroupDir(t *testing.T) {
	baseDir := t.TempDir()
	raftID := "n1"
	legacyDir := filepath.Join(baseDir, raftID)
	require.NoError(t, os.MkdirAll(legacyDir, raftDirPerm))
	legacyMarker := filepath.Join(legacyDir, "legacy.marker")
	require.NoError(t, os.WriteFile(legacyMarker, []byte("keep"), 0o600))

	groups := []groupSpec{
		{id: dedicatedTSORaftGroupID, address: "127.0.0.1:17000"},
		{id: 1, address: "127.0.0.1:17001"},
	}
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)
	factory, err := newRaftFactory(raftEngineEtcd, nil)
	require.NoError(t, err)

	runtimes, shardGroups, err := buildShardGroups(
		raftID,
		baseDir,
		groups,
		true,
		true,
		raftBootstrapConfig{},
		factory,
		nil,
		kv.NewHLC(),
		nil,
		nil,
		"",
		encryptionWriteWiring{},
		engine,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		for _, rt := range runtimes {
			rt.Close()
		}
	})
	require.Contains(t, shardGroups, uint64(dedicatedTSORaftGroupID))
	require.Contains(t, shardGroups, uint64(1))
	require.Nil(t, shardGroups[dedicatedTSORaftGroupID].Store)
	require.IsType(t, &kv.TSOStateMachine{}, runtimes[0].stateMachine)
	require.Nil(t, runtimes[0].store)
	require.DirExists(t, filepath.Join(legacyDir, "fsm.db"))
	require.DirExists(t, filepath.Join(legacyDir, "group-0"))
	require.NoDirExists(t, filepath.Join(legacyDir, "group-0", "fsm.db"))
	require.NoDirExists(t, filepath.Join(legacyDir, "group-1"), "group 1 should stay in legacy dir")
	got, err := os.ReadFile(legacyMarker)
	require.NoError(t, err)
	require.Equal(t, []byte("keep"), got)
}

func TestBuildShardGroupsWithDedicatedTSOPreservesLegacyGroupZeroStore(t *testing.T) {
	baseDir := t.TempDir()
	legacyStoreDir := filepath.Join(baseDir, "n1", "group-0", "fsm.db")
	require.NoError(t, os.MkdirAll(legacyStoreDir, raftDirPerm))
	legacyMarker := filepath.Join(legacyStoreDir, "legacy.marker")
	require.NoError(t, os.WriteFile(legacyMarker, []byte("preserve"), 0o600))

	factory, err := newRaftFactory(raftEngineEtcd, nil)
	require.NoError(t, err)
	runtimes, shardGroups, err := buildShardGroups(
		"n1",
		baseDir,
		[]groupSpec{{id: dedicatedTSORaftGroupID, address: "127.0.0.1:17002"}},
		true,
		true,
		raftBootstrapConfig{},
		factory,
		nil,
		kv.NewHLC(),
		nil,
		nil,
		"",
		encryptionWriteWiring{},
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() { closeRaftGroupRuntimes(runtimes) })
	require.Nil(t, shardGroups[dedicatedTSORaftGroupID].Store)
	require.Nil(t, runtimes[0].store)

	got, err := os.ReadFile(legacyMarker)
	require.NoError(t, err)
	require.Equal(t, []byte("preserve"), got)
}

func TestBuildShardGroupsClosesEarlierStoresWhenLaterGroupFails(t *testing.T) {
	baseDir := t.TempDir()
	badGroupDir := groupDataDir(baseDir, "n1", 2, true)
	require.NoError(t, os.MkdirAll(badGroupDir, raftDirPerm))
	require.NoError(t, os.WriteFile(
		filepath.Join(badGroupDir, raftEngineMarkerFile),
		[]byte("unsupported\n"),
		raftEngineMarkerPerm,
	))

	factory, err := newRaftFactory(raftEngineEtcd, nil)
	require.NoError(t, err)
	runtimes, shardGroups, err := buildShardGroups(
		"n1",
		baseDir,
		[]groupSpec{
			{id: 1, address: "127.0.0.1:17011"},
			{id: 2, address: "127.0.0.1:17012"},
		},
		true,
		true,
		raftBootstrapConfig{},
		factory,
		nil,
		kv.NewHLC(),
		nil,
		nil,
		"",
		encryptionWriteWiring{},
		nil,
	)
	require.ErrorIs(t, err, ErrUnsupportedRaftEngine)
	require.Nil(t, runtimes)
	require.Nil(t, shardGroups)

	// Reopening the first group's Pebble directory proves the error path ran
	// raftGroupRuntime.Close, which owns both engine and store cleanup.
	reopened, err := store.NewPebbleStore(filepath.Join(
		groupDataDir(baseDir, "n1", 1, true),
		"fsm.db",
	))
	require.NoError(t, err)
	require.NoError(t, reopened.Close())
}

func TestDedicatedTSOGroupContinuesAfterLegacyEncryptionEntry(t *testing.T) {
	baseDir := t.TempDir()
	factory, err := newRaftFactory(raftEngineEtcd, nil)
	require.NoError(t, err)
	clock := kv.NewHLC()
	runtimes, _, err := buildShardGroups(
		"n1",
		baseDir,
		[]groupSpec{{id: dedicatedTSORaftGroupID, address: "127.0.0.1:17020"}},
		true,
		true,
		raftBootstrapConfig{},
		factory,
		nil,
		clock,
		nil,
		nil,
		"",
		encryptionWriteWiring{},
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() { closeRaftGroupRuntimes(runtimes) })

	legacyEntry := append([]byte{fsmwire.OpRegistration}, fsmwire.EncodeRegistration(
		fsmwire.RegistrationPayload{DEKID: 1, FullNodeID: 2, LocalEpoch: 3},
	)...)
	result, err := runtimes[0].engine.ProposeAdmin(context.Background(), legacyEntry)
	require.NoError(t, err)
	legacyErr, ok := result.Response.(error)
	require.Truef(t, ok, "legacy response type = %T, want error", result.Response)
	require.ErrorIs(t, legacyErr, kv.ErrTSOLegacyEncryptionEntryRejected)

	const ceilingMs = int64(1_700_000_123_456)
	coordinator := kv.NewCoordinatorWithEngine(nil, runtimes[0].engine)
	t.Cleanup(func() { require.NoError(t, coordinator.Close()) })
	require.NoError(t, coordinator.ProposeHLCLease(context.Background(), ceilingMs))
	require.Equal(t, ceilingMs, clock.PhysicalCeiling())
}

func TestParseRaftEngineType(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		engineType, err := parseRaftEngineType("")
		require.NoError(t, err)
		require.Equal(t, raftEngineEtcd, engineType)
	})

	t.Run("etcd", func(t *testing.T) {
		engineType, err := parseRaftEngineType("etcd")
		require.NoError(t, err)
		require.Equal(t, raftEngineEtcd, engineType)
	})

	t.Run("invalid", func(t *testing.T) {
		_, err := parseRaftEngineType("nope")
		require.ErrorIs(t, err, ErrUnsupportedRaftEngine)
	})
}

func TestBuildShardGroupsWithEtcdEngineRoutesAcrossGroups(t *testing.T) {
	baseDir := t.TempDir()
	groups := []groupSpec{
		{id: 1, address: "127.0.0.1:15001"},
		{id: 2, address: "127.0.0.1:15002"},
	}

	factory, err := newRaftFactory(raftEngineEtcd, nil)
	require.NoError(t, err)
	clock := kv.NewHLC()
	runtimes, shardGroups, err := buildShardGroups("n1", baseDir, groups, true, true, raftBootstrapConfig{}, factory, nil, clock, nil, nil, "", encryptionWriteWiring{}, nil)
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
	baseDir := t.TempDir()
	groups := []groupSpec{
		{id: 1, address: "127.0.0.1:16001"},
		{id: 2, address: "127.0.0.1:16002"},
	}

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	sharedClock := kv.NewHLC()
	openShardStore := func(bootstrap bool) ([]*raftGroupRuntime, map[uint64]*kv.ShardGroup, *kv.ShardStore) {
		factory, err := newRaftFactory(raftEngineEtcd, nil)
		require.NoError(t, err)
		runtimes, shardGroups, err := buildShardGroups("n1", baseDir, groups, true, bootstrap, raftBootstrapConfig{}, factory, nil, sharedClock, nil, nil, "", encryptionWriteWiring{}, nil)
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

func TestEnsureRaftEngineDataDir(t *testing.T) {
	t.Run("writes marker for empty dir", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, ensureRaftEngineDataDir(dir, raftEngineEtcd))
		data, err := os.ReadFile(filepath.Join(dir, raftEngineMarkerFile))
		require.NoError(t, err)
		require.Equal(t, "etcd\n", string(data))
	})

	t.Run("detects etcd peers metadata artifact", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "etcd-raft-peers.bin"), []byte("placeholder"), 0o600))

		engineType, ok, err := detectRaftEngineFromDataDir(dir)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, raftEngineEtcd, engineType)
	})

	t.Run("detects bare wal dir as etcd artifact", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "wal"), 0o755))
		engineType, ok, err := detectRaftEngineFromDataDir(dir)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, raftEngineEtcd, engineType)
	})

	t.Run("detects bare snap dir as etcd artifact", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "snap"), 0o755))
		engineType, ok, err := detectRaftEngineFromDataDir(dir)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, raftEngineEtcd, engineType)
	})

	t.Run("rejects legacy hashicorp raft.db", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "raft.db"), []byte("legacy"), 0o600))
		err := ensureRaftEngineDataDir(dir, raftEngineEtcd)
		require.ErrorIs(t, err, ErrLegacyHashicorpDataDir)
	})

	t.Run("rejects legacy hashicorp logs.dat", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "logs.dat"), []byte("legacy"), 0o600))
		err := ensureRaftEngineDataDir(dir, raftEngineEtcd)
		require.ErrorIs(t, err, ErrLegacyHashicorpDataDir)
	})

	t.Run("rejects legacy hashicorp stable.dat", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "stable.dat"), []byte("legacy"), 0o600))
		err := ensureRaftEngineDataDir(dir, raftEngineEtcd)
		require.ErrorIs(t, err, ErrLegacyHashicorpDataDir)
	})

}
