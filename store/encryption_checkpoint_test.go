package store

import (
	"bytes"
	"io"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/stretchr/testify/require"
)

func TestEncryptionCheckpointDurableMetadataSurvivesReopenAndSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source")
	st, err := NewPebbleStore(path)
	require.NoError(t, err)
	checkpointStore, ok := st.(interface {
		SaveEncryptionSnapshotCheckpoint(encryption.SnapshotCheckpoint) error
	})
	require.True(t, ok)
	cp := encryption.SnapshotCheckpoint{Index: 890, State: [32]byte{8, 9}}
	require.NoError(t, checkpointStore.SaveEncryptionSnapshotCheckpoint(cp))
	require.Equal(t, uint64(0), st.LastCommitTS(), "operational metadata cannot issue a user timestamp")
	require.NoError(t, st.Close())
	st, err = NewPebbleStore(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	loader, ok := st.(interface {
		LoadEncryptionSnapshotCheckpoint() (encryption.SnapshotCheckpoint, bool, error)
	})
	require.True(t, ok)
	got, present, err := loader.LoadEncryptionSnapshotCheckpoint()
	require.NoError(t, err)
	require.True(t, present)
	require.Equal(t, cp, got)
	require.True(t, IsPebbleOperationalKey(encryptionCheckpointKey))
	snap, err := st.Snapshot()
	require.NoError(t, err)
	defer snap.Close()
	var payload bytes.Buffer
	_, err = snap.WriteTo(&payload)
	require.NoError(t, err)
	dst, err := NewPebbleStore(filepath.Join(t.TempDir(), "restored"))
	require.NoError(t, err)
	defer dst.Close()
	require.NoError(t, dst.Restore(io.NopCloser(&payload)))
	reader, ok := dst.(interface {
		LoadEncryptionSnapshotCheckpoint() (encryption.SnapshotCheckpoint, bool, error)
	})
	require.True(t, ok)
	got, present, err = reader.LoadEncryptionSnapshotCheckpoint()
	require.NoError(t, err)
	require.True(t, present)
	require.Equal(t, cp, got)
}
