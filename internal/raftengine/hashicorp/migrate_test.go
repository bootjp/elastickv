package hashicorp_test

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	hashicorpraftengine "github.com/bootjp/elastickv/internal/raftengine/hashicorp"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestParsePeers(t *testing.T) {
	t.Parallel()

	t.Run("empty", func(t *testing.T) {
		peers, err := hashicorpraftengine.ParsePeers("")
		require.NoError(t, err)
		require.Nil(t, peers)
	})

	t.Run("single", func(t *testing.T) {
		peers, err := hashicorpraftengine.ParsePeers("n1=127.0.0.1:7001")
		require.NoError(t, err)
		require.Len(t, peers, 1)
		require.Equal(t, "n1", peers[0].ID)
		require.Equal(t, "127.0.0.1:7001", peers[0].Address)
	})

	t.Run("multiple", func(t *testing.T) {
		peers, err := hashicorpraftengine.ParsePeers("n1=127.0.0.1:7001,n2=127.0.0.1:7002,n3=127.0.0.1:7003")
		require.NoError(t, err)
		require.Len(t, peers, 3)
	})

	t.Run("invalid_format", func(t *testing.T) {
		_, err := hashicorpraftengine.ParsePeers("bad-entry")
		require.Error(t, err)
	})

	t.Run("empty_id", func(t *testing.T) {
		_, err := hashicorpraftengine.ParsePeers("=127.0.0.1:7001")
		require.Error(t, err)
	})

	t.Run("empty_address", func(t *testing.T) {
		_, err := hashicorpraftengine.ParsePeers("n1=")
		require.Error(t, err)
	})

	t.Run("trailing_commas_skipped", func(t *testing.T) {
		peers, err := hashicorpraftengine.ParsePeers("n1=127.0.0.1:7001,,")
		require.NoError(t, err)
		require.Len(t, peers, 1)
	})
}

func TestMigrateFSMStoreValidation(t *testing.T) {
	t.Parallel()

	t.Run("missing_source", func(t *testing.T) {
		_, err := hashicorpraftengine.MigrateFSMStore("", t.TempDir(), []hashicorpraftengine.MigrationPeer{{ID: "n1", Address: "127.0.0.1:7001"}})
		require.Error(t, err)
		require.Contains(t, err.Error(), "source FSM store path is required")
	})

	t.Run("missing_dest", func(t *testing.T) {
		_, err := hashicorpraftengine.MigrateFSMStore("/some/path", "", []hashicorpraftengine.MigrationPeer{{ID: "n1", Address: "127.0.0.1:7001"}})
		require.Error(t, err)
		require.Contains(t, err.Error(), "destination data dir is required")
	})

	t.Run("missing_peers", func(t *testing.T) {
		_, err := hashicorpraftengine.MigrateFSMStore("/some/path", t.TempDir()+"/dest", nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least one peer is required")
	})

	t.Run("dest_already_exists", func(t *testing.T) {
		dest := t.TempDir() // already exists
		peers := []hashicorpraftengine.MigrationPeer{{ID: "n1", Address: "127.0.0.1:7001"}}
		_, err := hashicorpraftengine.MigrateFSMStore("/some/path", dest, peers)
		require.Error(t, err)
		require.Contains(t, err.Error(), "already exists")
	})
}

func TestMigrateFSMStoreSeedsHashicorpDataDir(t *testing.T) {
	// Write test data to a PebbleStore (the shared FSM store).
	sourcePath := filepath.Join(t.TempDir(), "fsm.db")
	source, err := store.NewPebbleStore(sourcePath)
	require.NoError(t, err)
	require.NoError(t, source.PutAt(context.Background(), []byte("alpha"), []byte("one"), 10, 0))
	require.NoError(t, source.PutAt(context.Background(), []byte("beta"), []byte("two"), 11, 0))
	require.NoError(t, source.Close())

	// Migrate to hashicorp raft format.
	destDataDir := filepath.Join(t.TempDir(), "raft")
	peers := []hashicorpraftengine.MigrationPeer{
		{ID: "n1", Address: "127.0.0.1:7001"},
	}
	stats, err := hashicorpraftengine.MigrateFSMStore(sourcePath, destDataDir, peers)
	require.NoError(t, err)
	require.Positive(t, stats.SnapshotBytes)
	require.Equal(t, 1, stats.Peers)

	// Boot a hashicorp raft engine from the migrated directory and verify
	// the FSM snapshot is restored.
	destStore, err := store.NewPebbleStore(filepath.Join(t.TempDir(), "dest-fsm.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, destStore.Close()) })

	sm := &storeStateMachine{store: destStore}
	factory := hashicorpraftengine.NewFactory(hashicorpraftengine.FactoryConfig{
		CommitTimeout:       50 * time.Millisecond,
		HeartbeatTimeout:    200 * time.Millisecond,
		ElectionTimeout:     2000 * time.Millisecond,
		LeaderLeaseTimeout:  100 * time.Millisecond,
		SnapshotRetainCount: 3,
	})

	result, err := factory.Create(raftengine.FactoryConfig{
		LocalID:      "n1",
		LocalAddress: "127.0.0.1:0",
		DataDir:      destDataDir,
		StateMachine: sm,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = result.Engine.Close()
		if result.Close != nil {
			_ = result.Close()
		}
	})

	// Wait for leader election.
	require.Eventually(t, func() bool {
		return result.Engine.State() == raftengine.StateLeader
	}, 5*time.Second, 10*time.Millisecond, "engine did not become leader")

	// Verify FSM data survived the migration.
	v, err := destStore.GetAt(context.Background(), []byte("alpha"), ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, []byte("one"), v)

	v, err = destStore.GetAt(context.Background(), []byte("beta"), ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, []byte("two"), v)
}

// storeStateMachine adapts store.MVCCStore to raftengine.StateMachine for
// migration tests. Only Snapshot/Restore are needed; Apply is unused.
type storeStateMachine struct {
	store store.MVCCStore
}

func (s *storeStateMachine) Apply(_ []byte) any { return nil }

func (s *storeStateMachine) Snapshot() (raftengine.Snapshot, error) {
	return s.store.Snapshot()
}

func (s *storeStateMachine) Restore(r io.Reader) error {
	return s.store.Restore(r)
}

func TestFSMSnapshotRoundTrip(t *testing.T) {
	t.Parallel()

	// Create source store with data.
	sourcePath := filepath.Join(t.TempDir(), "src.db")
	src, err := store.NewPebbleStore(sourcePath)
	require.NoError(t, err)
	require.NoError(t, src.PutAt(context.Background(), []byte("k"), []byte("v"), 1, 0))

	snap, err := src.Snapshot()
	require.NoError(t, err)
	var buf bytes.Buffer
	_, err = snap.WriteTo(&buf)
	require.NoError(t, err)
	require.NoError(t, snap.Close())
	require.NoError(t, src.Close())

	// Restore into destination store.
	dstPath := filepath.Join(t.TempDir(), "dst.db")
	dst, err := store.NewPebbleStore(dstPath)
	require.NoError(t, err)
	require.NoError(t, dst.Restore(&buf))

	v, err := dst.GetAt(context.Background(), []byte("k"), ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, []byte("v"), v)
	require.NoError(t, dst.Close())
}
