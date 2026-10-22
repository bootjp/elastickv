package kv

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestEncryptionCheckpointSnapshotPreservesPrepareProtocol(t *testing.T) {
	for _, released := range []bool{false, true} {
		t.Run(map[bool]string{false: "sealed", true: "released"}[released], func(t *testing.T) {
			st, err := store.NewPebbleStore(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			source := newPrepareSafetyFSM(t, st)
			require.Nil(t, source.Apply(EncodePrepareEvidenceFence(0)))
			require.NoError(t, applyFSMRequest(t, source, preparedEvidenceRequest(pb.Phase_PREPARE, 20)))
			require.Nil(t, source.Apply(EncodePrepareEvidenceSeal()))
			require.Nil(t, source.Apply(EncodePrepareEvidenceFence(100)))
			if released {
				require.Nil(t, source.Apply(EncodePrepareEvidenceRelease(100)))
			}
			checkpoint := encryption.SnapshotCheckpoint{Index: 6, State: [32]byte{1, 2, 3}}
			source.encryption = &checkpointApplier{checkpoint: checkpoint}
			require.NoError(t, source.captureEncryptionCheckpoint(checkpoint.Index))
			image := jointRecoverySnapshot(t, source)
			_, _, parsed, err := ReadSnapshotHeaderWithCheckpoint(bufio.NewReader(bytes.NewReader(image)))
			require.NoError(t, err)
			require.Equal(t, checkpoint, parsed)

			dir := t.TempDir()
			destination, err := store.NewPebbleStore(dir)
			require.NoError(t, err)
			closed := false
			t.Cleanup(func() {
				if !closed {
					require.NoError(t, destination.Close())
				}
			})
			restored := newPrepareSafetyFSM(t, destination)
			require.NoError(t, restored.Restore(bytes.NewReader(image)))
			require.Equal(t, checkpoint, restored.encryptionCheckpoint)
			assertJointRecoveryDataGate(t, restored, 100, released)
			require.NoError(t, destination.Close())
			closed = true
			reopened, err := store.NewPebbleStore(dir)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, reopened.Close()) })
			recovered := newPrepareSafetyFSM(t, reopened)
			require.NoError(t, recovered.RestoreSnapshotHeader(bytes.NewReader(image)))
			require.Equal(t, checkpoint, recovered.encryptionCheckpoint)
			assertJointRecoveryDataGate(t, recovered, 100, released)
			if !released {
				require.ErrorIs(t, applyFSMRequest(t, recovered,
					prepareSafetyAdmittedCreation("raw", 1000, 100)), ErrPrepareCreationSealed)
			}
			require.ErrorIs(t, applyFSMRequest(t, recovered,
				preparedEvidenceRequest(pb.Phase_COMMIT, 21)), ErrTxnInvalidMeta)
			require.NoError(t, applyFSMRequest(t, recovered, preparedEvidenceRequest(pb.Phase_COMMIT, 20)))
			require.Nil(t, recovered.Apply(EncodePrepareEvidenceRelease(100)))
			require.Nil(t, recovered.Apply(EncodePrepareEvidenceRelease(100)))
			require.ErrorIs(t, applyFSMRequest(t, recovered,
				prepareSafetyAdmittedCreation("raw", 1000, 0)), ErrPrepareAdmissionFenced)
			require.ErrorIs(t, applyFSMRequest(t, recovered,
				prepareSafetyAdmittedCreation("raw", 100, 100)), ErrPrepareAdmissionFenced)
			require.NoError(t, applyFSMRequest(t, recovered, prepareSafetyAdmittedCreation("raw", 1000, 100)))
			value, err := reopened.GetAt(t.Context(), []byte("primary"), 20)
			require.NoError(t, err)
			require.Equal(t, []byte("value"), value)
			next := jointRecoverySnapshot(t, recovered)
			_, _, carried, err := ReadSnapshotHeaderWithCheckpoint(bufio.NewReader(bytes.NewReader(next)))
			require.NoError(t, err)
			require.Equal(t, checkpoint, carried)
		})
	}
}
