package hashicorp_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	hashicorpraftengine "github.com/bootjp/elastickv/internal/raftengine/hashicorp"
	"github.com/bootjp/elastickv/internal/raftengine/raftenginetest"
	"github.com/stretchr/testify/require"
)

func TestConformance(t *testing.T) {
	factory := hashicorpraftengine.NewFactory(hashicorpraftengine.FactoryConfig{
		CommitTimeout:       50 * time.Millisecond,
		HeartbeatTimeout:    200 * time.Millisecond,
		ElectionTimeout:     2000 * time.Millisecond,
		LeaderLeaseTimeout:  100 * time.Millisecond,
		SnapshotRetainCount: 3,
	})
	raftenginetest.RunConformanceSuite(t, factory)
}

func TestEngineCloseNilSafe(t *testing.T) {
	t.Parallel()

	t.Run("nil_engine", func(t *testing.T) {
		var e *hashicorpraftengine.Engine
		require.NoError(t, e.Close())
	})

	t.Run("nil_raft_via_New", func(t *testing.T) {
		e := hashicorpraftengine.New(nil)
		require.Nil(t, e)
	})
}

func TestRejectLegacyBoltDB(t *testing.T) {
	t.Parallel()

	factory := hashicorpraftengine.NewFactory(hashicorpraftengine.FactoryConfig{
		CommitTimeout:       50 * time.Millisecond,
		HeartbeatTimeout:    200 * time.Millisecond,
		ElectionTimeout:     2000 * time.Millisecond,
		LeaderLeaseTimeout:  100 * time.Millisecond,
		SnapshotRetainCount: 3,
	})

	for _, legacy := range []string{"logs.dat", "stable.dat"} {
		t.Run(legacy, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(dir, legacy), []byte("legacy"), 0o600))

			_, err := factory.Create(raftengine.FactoryConfig{
				LocalID:      "n1",
				LocalAddress: "127.0.0.1:0",
				DataDir:      dir,
				Bootstrap:    true,
				StateMachine: &raftenginetest.TestStateMachine{},
			})
			require.Error(t, err)
			require.Contains(t, err.Error(), "legacy boltdb")
		})
	}
}
