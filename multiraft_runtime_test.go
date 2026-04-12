package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
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

func TestBuildRuntimeForGroupBootstrap(t *testing.T) {
	baseDir := t.TempDir()

	factory, err := newRaftFactory(raftEngineHashicorp)
	require.NoError(t, err)

	st := store.NewMVCCStore()
	sm := etcdraftengine.AdaptHashicorpFSM(kv.NewKvFSM(st))

	runtime, err := buildRuntimeForGroup(
		"n1",
		groupSpec{id: 1, address: "127.0.0.1:0"},
		baseDir,
		true, // multi
		true, // bootstrap
		nil,
		st,
		sm,
		factory,
	)
	require.NoError(t, err)
	require.NotNil(t, runtime)
	require.NotNil(t, runtime.engine)
	t.Cleanup(func() { runtime.Close() })

	dir := groupDataDir(baseDir, "n1", 1, true)
	_, err = os.Stat(dir)
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(dir, "raft.db"))
	require.NoError(t, err)
}

func TestParseRaftEngineType(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		engineType, err := parseRaftEngineType("")
		require.NoError(t, err)
		require.Equal(t, raftEngineHashicorp, engineType)
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
	runtimes, shardGroups, err := buildShardGroups("n1", baseDir, groups, true, false, nil, factory, nil)
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

	coord := kv.NewShardedCoordinator(engine, shardGroups, 1, kv.NewHLC(), shardStore)
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

	openShardStore := func() ([]*raftGroupRuntime, map[uint64]*kv.ShardGroup, *kv.ShardStore) {
		factory, err := newRaftFactory(raftEngineEtcd)
		require.NoError(t, err)
		runtimes, shardGroups, err := buildShardGroups("n1", baseDir, groups, true, false, nil, factory, nil)
		require.NoError(t, err)
		shardStore := kv.NewShardStore(engine, shardGroups)
		return runtimes, shardGroups, shardStore
	}

	runtimes, shardGroups, shardStore := openShardStore()
	coord := kv.NewShardedCoordinator(engine, shardGroups, 1, kv.NewHLC(), shardStore)

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

	runtimes, shardGroups, shardStore = openShardStore()
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

	t.Run("rejects mismatched existing artifacts", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "raft.db"), []byte("not-a-real-db"), 0o600))
		err := ensureRaftEngineDataDir(dir, raftEngineEtcd)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrRaftEngineDataDir)
	})

	t.Run("detects mixed engine artifacts", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "raft.db"), []byte("not-a-real-db"), 0o600))
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "member", "wal"), 0o755))
		_, _, err := detectRaftEngineFromDataDir(dir)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrMixedRaftEngineArtifacts)
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

	t.Run("detects mixed engine artifacts with bare wal", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "raft.db"), []byte("dummy"), 0o600))
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "wal"), 0o755))
		_, _, err := detectRaftEngineFromDataDir(dir)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrMixedRaftEngineArtifacts)
	})
}
