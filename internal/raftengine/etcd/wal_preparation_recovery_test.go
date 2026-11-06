package etcd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestPreparedIdentityRejectsMissingWALWithoutMutation(t *testing.T) {
	t.Parallel()
	for _, multi := range []bool{false, true} {
		name := "single"
		if multi {
			name = "multi"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			original := []byte("retained FSM must not be touched")
			require.NoError(t, os.WriteFile(filepath.Join(dir, "retained"), original, 0o600))
			id := identityFixture()
			id.PreparationDigest = raftbootstrap.Digest{9}
			cfg := OpenConfig{DataDir: dir, NodeID: id.NodeID, NativeIdentity: id,
				Bootstrap: true, StateMachine: &testStateMachine{}}
			peers := []Peer{{ID: "n7", NodeID: id.NodeID}}
			if multi {
				peers = append(peers, Peer{ID: "n8", NodeID: 8})
			}
			engine, err := Open(t.Context(), cfg)
			require.ErrorIs(t, err, ErrWALIdentity)
			require.Nil(t, engine)
			disk, err := openDiskState(cfg, peers)
			require.ErrorIs(t, err, ErrWALIdentity)
			require.Nil(t, disk)
			disk, err = bootstrapNewCluster(zap.NewNop(), filepath.Join(dir, walDirName),
				filepath.Join(dir, snapDirName), filepath.Join(dir, fsmSnapDirName), cfg, peers)
			require.ErrorIs(t, err, ErrWALIdentity)
			require.Nil(t, disk)
			entries, err := os.ReadDir(dir)
			require.NoError(t, err)
			require.Len(t, entries, 1)
			body, err := os.ReadFile(filepath.Join(dir, "retained"))
			require.NoError(t, err)
			require.Equal(t, original, body)
		})
	}
}
