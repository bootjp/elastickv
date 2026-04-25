package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestGroupDataDir(t *testing.T) {
	base := "/tmp/data"
	raftID := "n1"

	t.Run("single", func(t *testing.T) {
		require.Equal(t, filepath.Join(base, raftID), groupDataDir(base, raftID, 1, false))
	})

	t.Run("multi", func(t *testing.T) {
		require.Equal(t, filepath.Join(base, raftID, "group-2"), groupDataDir(base, raftID, 2, true))
	})
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

	factory, err := newRaftFactory(raftEngineEtcd)
	require.NoError(t, err)
	clock := kv.NewHLC()
	runtimes, shardGroups, err := buildShardGroups("n1", baseDir, groups, true, true, nil, factory, nil, clock)
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
		factory, err := newRaftFactory(raftEngineEtcd)
		require.NoError(t, err)
		runtimes, shardGroups, err := buildShardGroups("n1", baseDir, groups, true, bootstrap, nil, factory, nil, sharedClock)
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
