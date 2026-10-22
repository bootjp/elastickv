package kv

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type checkpointApplier struct {
	fakeApplier
	checkpoint encryption.SnapshotCheckpoint
}

func (a *checkpointApplier) SnapshotCheckpoint() (encryption.SnapshotCheckpoint, error) {
	return a.checkpoint, nil
}

func TestEncryptionSnapshotCheckpointRoundTrip(t *testing.T) {
	cp := encryption.SnapshotCheckpoint{Index: 6, State: [32]byte{1, 2, 3}}
	for _, headerOnly := range []bool{false, true} {
		t.Run(map[bool]string{false: "full-physical-restore", true: "skip-gate-header-restore"}[headerOnly], func(t *testing.T) {
			source, ok := NewKvFSMWithHLC(store.NewMVCCStore(), NewHLC()).(*kvFSM)
			require.True(t, ok)
			source.encryption = &checkpointApplier{checkpoint: cp}
			require.NoError(t, source.captureEncryptionCheckpoint(6))
			snapshot, err := source.Snapshot()
			require.NoError(t, err)
			defer snapshot.Close()
			var payload bytes.Buffer
			_, err = snapshot.WriteTo(&payload)
			require.NoError(t, err)
			_, _, parsed, err := ReadSnapshotHeaderWithCheckpoint(bufio.NewReader(bytes.NewReader(payload.Bytes())))
			require.NoError(t, err)
			require.Equal(t, cp, parsed)
			target, ok := NewKvFSMWithHLC(store.NewMVCCStore(), NewHLC()).(*kvFSM)
			require.True(t, ok)
			if headerOnly {
				err = target.RestoreSnapshotHeader(bytes.NewReader(payload.Bytes()))
			} else {
				err = target.Restore(bytes.NewReader(payload.Bytes()))
			}
			require.NoError(t, err)
			require.Equal(t, cp, target.encryptionCheckpoint)
			// A rollback on disk must not overwrite the restored witness merely
			// because this process starts making newer snapshots.
			target.encryption = &checkpointApplier{checkpoint: encryption.SnapshotCheckpoint{Index: 5}}
			require.NoError(t, target.captureEncryptionCheckpoint(4))
			require.Equal(t, cp, target.encryptionCheckpoint)
			next, err := target.Snapshot()
			require.NoError(t, err)
			defer next.Close()
			var nextPayload bytes.Buffer
			_, err = next.WriteTo(&nextPayload)
			require.NoError(t, err)
			_, _, carried, err := ReadSnapshotHeaderWithCheckpoint(bufio.NewReader(&nextPayload))
			require.NoError(t, err)
			require.Equal(t, cp, carried)
		})
	}
}

func TestEncryptionSnapshotCheckpointRejectsTruncationWithoutMutation(t *testing.T) {
	fsm, ok := NewKvFSMWithHLC(store.NewMVCCStore(), NewHLC()).(*kvFSM)
	require.True(t, ok)
	fsm.encryptionCheckpoint = encryption.SnapshotCheckpoint{Index: 6, State: [32]byte{1}}
	snapshot, err := fsm.Snapshot()
	require.NoError(t, err)
	defer snapshot.Close()
	var payload bytes.Buffer
	_, err = snapshot.WriteTo(&payload)
	require.NoError(t, err)
	before := fsm.encryptionCheckpoint
	require.Error(t, fsm.RestoreSnapshotHeader(bytes.NewReader(payload.Bytes()[:snapshotCheckpointHeaderLen-1])))
	require.Equal(t, before, fsm.encryptionCheckpoint)
}
