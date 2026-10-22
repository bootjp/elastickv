package kv

import (
	"bytes"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestPreparedAbortRemainsFinalAcrossSnapshotAndReopen(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version byte
		proof   uint64
	}{
		{name: "v1", version: txnMetaVersionV1},
		{name: "v3", version: txnMetaVersionV3, proof: 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertPreparedAbortRecovery(t, tc.proof, tc.version)
		})
	}
}

func assertPreparedAbortRecovery(t *testing.T, proof uint64, version byte) {
	t.Helper()
	source, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	defer func() { require.NoError(t, source.Close()) }()
	fsm := newPrepareSafetyFSM(t, source)
	if proof != 0 {
		require.Nil(t, fsm.Apply(EncodePrepareEvidenceFence(0)))
	}
	prepare := preparedEvidenceRequest(pb.Phase_PREPARE, proof)
	require.Equal(t, version, prepare.GetMutations()[0].GetValue()[0])
	require.NoError(t, applyFSMRequest(t, fsm, prepare))
	require.Nil(t, fsm.Apply(EncodePrepareEvidenceFence(0)))
	snapshot, err := fsm.Snapshot()
	require.NoError(t, err)
	var raw bytes.Buffer
	_, err = snapshot.WriteTo(&raw)
	require.NoError(t, err)
	require.NoError(t, snapshot.Close())
	dir := t.TempDir()
	restoredStore, err := store.NewPebbleStore(dir)
	require.NoError(t, err)
	closed := false
	t.Cleanup(func() {
		if !closed {
			require.NoError(t, restoredStore.Close())
		}
	})
	restored := newPrepareSafetyFSM(t, restoredStore)
	require.NoError(t, restored.Restore(bytes.NewReader(raw.Bytes())))
	for range 2 {
		require.NoError(t, applyFSMRequest(t, restored, preparedEvidenceRequest(pb.Phase_ABORT, 20)))
	}
	require.NoError(t, restoredStore.Close())
	closed = true
	reopenedStore, err := store.NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopenedStore.Close()) })
	reopened := newPrepareSafetyFSM(t, reopenedStore)
	require.NoError(t, applyFSMRequest(t, reopened, preparedEvidenceRequest(pb.Phase_ABORT, 20)))
	require.ErrorIs(t, applyFSMRequest(t, reopened, preparedEvidenceRequest(pb.Phase_COMMIT, 20)), ErrTxnAlreadyAborted)
	for _, key := range [][]byte{[]byte("primary"), txnLockKey([]byte("primary")), txnIntentKey([]byte("primary"))} {
		_, err = reopenedStore.GetAt(t.Context(), key, ^uint64(0))
		require.ErrorIs(t, err, store.ErrKeyNotFound)
	}
	_, err = reopenedStore.GetAt(t.Context(), txnRollbackKey([]byte("primary"), 10), ^uint64(0))
	require.NoError(t, err)
}
