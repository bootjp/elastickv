package etcd

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestMigrateFSMStoreSeedsEtcdDataDir(t *testing.T) {
	sourcePath := filepath.Join(t.TempDir(), "fsm.db")
	source, err := store.NewPebbleStore(sourcePath)
	require.NoError(t, err)
	require.NoError(t, source.PutAt(context.Background(), []byte("alpha"), []byte("one"), 10, 0))
	require.NoError(t, source.Close())

	destDataDir := filepath.Join(t.TempDir(), "raft")
	peers := []Peer{{NodeID: 1, ID: "n1", Address: "127.0.0.1:7001"}}
	stats, err := MigrateFSMStore(sourcePath, destDataDir, peers)
	require.NoError(t, err)
	require.Positive(t, stats.SnapshotBytes)

	destStore, err := store.NewPebbleStore(filepath.Join(t.TempDir(), "dest-fsm.db"))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, destStore.Close())
	})

	engine, err := Open(context.Background(), OpenConfig{
		NodeID:       1,
		LocalID:      "n1",
		LocalAddress: "127.0.0.1:7001",
		DataDir:      destDataDir,
		StateMachine: kv.NewKvFSMWithHLC(destStore, kv.NewHLC()),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, engine.Close())
	})

	value, err := destStore.GetAt(context.Background(), []byte("alpha"), ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, []byte("one"), value)
}

func TestMigrateFSMStorePersistsSingleNodePeer(t *testing.T) {
	sourcePath := filepath.Join(t.TempDir(), "fsm.db")
	source, err := store.NewPebbleStore(sourcePath)
	require.NoError(t, err)
	require.NoError(t, source.Close())

	destDataDir := filepath.Join(t.TempDir(), "raft")
	peers := []Peer{{NodeID: 1, ID: "n1", Address: "127.0.0.1:7001"}}
	_, err = MigrateFSMStore(sourcePath, destDataDir, peers)
	require.NoError(t, err)

	loaded, ok, err := LoadPersistedPeers(destDataDir)
	require.NoError(t, err)
	require.True(t, ok, "persisted peers file must exist after single-node migration")
	require.Len(t, loaded, 1)
	require.Equal(t, peers[0].NodeID, loaded[0].NodeID)
}

func TestMigrateFSMStorePersistsMultiNodePeers(t *testing.T) {
	sourcePath := filepath.Join(t.TempDir(), "fsm.db")
	source, err := store.NewPebbleStore(sourcePath)
	require.NoError(t, err)
	require.NoError(t, source.Close())

	destDataDir := filepath.Join(t.TempDir(), "raft")
	peers := []Peer{
		{NodeID: 1, ID: "n1", Address: "127.0.0.1:7001"},
		{NodeID: 2, ID: "n2", Address: "127.0.0.1:7002"},
		{NodeID: 3, ID: "n3", Address: "127.0.0.1:7003"},
	}
	_, err = MigrateFSMStore(sourcePath, destDataDir, peers)
	require.NoError(t, err)

	// Persisted peers must exist so the engine discovers all cluster members
	// even when FactoryConfig.Peers is empty (the common post-migration case).
	loaded, ok, err := LoadPersistedPeers(destDataDir)
	require.NoError(t, err)
	require.True(t, ok, "persisted peers file must exist after migration")
	require.Len(t, loaded, 3)
	for i, peer := range loaded {
		require.Equal(t, peers[i].NodeID, peer.NodeID)
		require.Equal(t, peers[i].ID, peer.ID)
		require.Equal(t, peers[i].Address, peer.Address)
	}
}
