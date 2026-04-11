package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/hashicorp/raft"
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

func TestNewRaftGroupBootstrap(t *testing.T) {
	baseDir := t.TempDir()

	st := store.NewMVCCStore()
	fsm := kv.NewKvFSM(st)

	r, tm, closeStores, err := newRaftGroup(
		"n1",
		groupSpec{id: 1, address: "127.0.0.1:0"},
		baseDir,
		true, // multi
		true, // bootstrap
		nil,
		fsm,
	)
	require.NoError(t, err)
	require.NotNil(t, r)
	require.NotNil(t, tm)
	t.Cleanup(func() {
		_ = r.Shutdown().Error()
		_ = tm.Close()
		if closeStores != nil {
			closeStores()
		}
	})

	dir := groupDataDir(baseDir, "n1", 1, true)
	_, err = os.Stat(dir)
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(dir, "raft.db"))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return r.State() == raft.Leader
	}, 5*time.Second, 10*time.Millisecond)
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

	runtimes, shardGroups, err := buildShardGroups("n1", baseDir, groups, true, false, nil, raftEngineEtcd, nil)
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
