package kv

import (
	"bytes"
	"context"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func preparedEvidenceRequest(phase pb.Phase, commitTS uint64) *pb.Request {
	meta := TxnMeta{PrimaryKey: []byte("primary"), LockTTLms: defaultTxnLockTTLms, CommitTS: commitTS}
	if phase == pb.Phase_PREPARE {
		meta.CommitTS = 0
		meta.PrepareCommitTS = commitTS
	}
	return &pb.Request{
		IsTxn: true, Phase: phase, Ts: 10,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(meta)},
			{Op: pb.Op_PUT, Key: []byte("primary"), Value: []byte("value")},
		},
	}
}

func TestPreparedCommitEvidenceCannotBeRetimestamped(t *testing.T) {
	t.Parallel()
	for _, phase := range []pb.Phase{pb.Phase_PREPARE, pb.Phase_COMMIT} {
		t.Run(phase.String(), func(t *testing.T) {
			st := store.NewMVCCStore()
			fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
			require.True(t, ok)
			require.Nil(t, fsm.Apply(EncodePrepareEvidenceFence(0)))
			require.NoError(t, applyFSMRequest(t, fsm, preparedEvidenceRequest(pb.Phase_PREPARE, 20)))
			require.ErrorIs(t, applyFSMRequest(t, fsm, preparedEvidenceRequest(phase, 21)), ErrTxnInvalidMeta)
			_, err := st.GetAt(context.Background(), []byte("primary"), ^uint64(0))
			require.ErrorIs(t, err, store.ErrKeyNotFound)
			require.NoError(t, applyFSMRequest(t, fsm, preparedEvidenceRequest(pb.Phase_COMMIT, 20)))
			value, err := st.GetAt(context.Background(), []byte("primary"), 20)
			require.NoError(t, err)
			require.Equal(t, []byte("value"), value)
		})
	}
}

func TestPrepareCommitEvidenceCannotBeAddedToLegacyIntent(t *testing.T) {
	t.Parallel()
	st := store.NewMVCCStore()
	fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)
	require.NoError(t, applyFSMRequest(t, fsm, preparedEvidenceRequest(pb.Phase_PREPARE, 0)))
	require.Nil(t, fsm.Apply(EncodePrepareEvidenceFence(0)))
	require.ErrorIs(t, applyFSMRequest(t, fsm, preparedEvidenceRequest(pb.Phase_PREPARE, 20)), ErrTxnInvalidMeta)
	// A blocked upgrade must leave the old transaction free to finish normally.
	require.NoError(t, applyFSMRequest(t, fsm, preparedEvidenceRequest(pb.Phase_COMMIT, 20)))
}

func TestPrepareCommitEvidenceSurvivesReopen(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.NewPebbleStore(dir)
	require.NoError(t, err)
	fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)
	require.Nil(t, fsm.Apply(EncodePrepareEvidenceFence(0)))
	require.NoError(t, applyFSMRequest(t, fsm, preparedEvidenceRequest(pb.Phase_PREPARE, 20)))
	require.NoError(t, st.Close())
	reopened, err := store.NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	raw, err := reopened.GetAt(ctx, txnLockKey([]byte("primary")), ^uint64(0))
	require.NoError(t, err)
	lock, err := decodeTxnLock(raw)
	require.NoError(t, err)
	require.Equal(t, uint64(20), lock.CommitTS)
	restarted, ok := NewKvFSMWithHLC(reopened, NewHLC()).(*kvFSM)
	require.True(t, ok)
	require.ErrorIs(t, applyFSMRequest(t, restarted, preparedEvidenceRequest(pb.Phase_COMMIT, 21)), ErrTxnInvalidMeta)
	require.NoError(t, applyFSMRequest(t, restarted, preparedEvidenceRequest(pb.Phase_COMMIT, 20)))
}

func TestPrepareCommitEvidenceSurvivesSnapshotRestore(t *testing.T) {
	t.Parallel()
	st := store.NewMVCCStore()
	source, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)
	require.Nil(t, source.Apply(EncodePrepareEvidenceFence(0)))
	require.NoError(t, applyFSMRequest(t, source, preparedEvidenceRequest(pb.Phase_PREPARE, 20)))
	snapshot, err := st.Snapshot()
	require.NoError(t, err)
	defer snapshot.Close()
	var raw bytes.Buffer
	_, err = snapshot.WriteTo(&raw)
	require.NoError(t, err)
	destination, ok := NewKvFSMWithHLC(store.NewMVCCStore(), NewHLC()).(*kvFSM)
	require.True(t, ok)
	require.NoError(t, destination.Restore(bytes.NewReader(raw.Bytes())))
	require.ErrorIs(t, applyFSMRequest(t, destination, preparedEvidenceRequest(pb.Phase_COMMIT, 21)), ErrTxnInvalidMeta)
	require.NoError(t, applyFSMRequest(t, destination, preparedEvidenceRequest(pb.Phase_COMMIT, 20)))
}

func TestPrepareEvidenceRequiresReplicatedEnableMarker(t *testing.T) {
	t.Parallel()
	fsm, ok := NewKvFSMWithHLC(store.NewMVCCStore(), NewHLC()).(*kvFSM)
	require.True(t, ok)
	require.ErrorIs(t, applyFSMRequest(t, fsm, preparedEvidenceRequest(pb.Phase_PREPARE, 20)), ErrTSOProtocolUnsupported)
	require.Nil(t, fsm.Apply(EncodePrepareEvidenceFence(0)))
	require.NoError(t, applyFSMRequest(t, fsm, preparedEvidenceRequest(pb.Phase_PREPARE, 20)))
	require.Nil(t, fsm.Apply(EncodePrepareEvidenceFence(30)))
	require.Nil(t, fsm.Apply(EncodePrepareEvidenceSeal()))
	require.ErrorIs(t, applyFSMRequest(t, fsm, preparedEvidenceRequest(pb.Phase_PREPARE, 20)), ErrPrepareCreationSealed)
	require.Nil(t, fsm.Apply(EncodePrepareEvidenceRelease(30)))
	require.ErrorIs(t, applyFSMRequest(t, fsm, preparedEvidenceRequest(pb.Phase_PREPARE, 20)), ErrPrepareAdmissionFenced)
	require.NoError(t, applyFSMRequest(t, fsm, preparedEvidenceRequest(pb.Phase_COMMIT, 20)))
}

func TestPrepareAdmissionPreservesAppliedReadStartsAndBackupFence(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name        string
		epoch       uint64
		proof       uint64
		backupFloor uint64
		wantErr     error
	}{
		{name: "old applied snapshot with a new proven commit", epoch: 30, proof: 40},
		{name: "old epoch cannot create after release", epoch: 0, proof: 40, wantErr: ErrPrepareAdmissionFenced},
		{name: "old commit cannot be relabeled", epoch: 30, proof: 20, wantErr: ErrPrepareAdmissionFenced},
		{name: "real backup pin still fences old prepare starts", epoch: 30, proof: 40, backupFloor: 25, wantErr: ErrBackupTimestampFenced},
	} {
		t.Run(test.name, func(t *testing.T) {
			fsm, ok := NewKvFSMWithHLC(store.NewMVCCStore(), NewHLC()).(*kvFSM)
			require.True(t, ok)
			require.Nil(t, fsm.Apply(EncodePrepareEvidenceFence(30)))
			require.Nil(t, fsm.Apply(EncodePrepareEvidenceSeal()))
			require.Nil(t, fsm.Apply(EncodePrepareEvidenceRelease(30)))
			if test.backupFloor != 0 {
				require.NoError(t, fsm.persistBackupTimestampFloor(t.Context(), test.backupFloor))
			}
			request := preparedEvidenceRequest(pb.Phase_PREPARE, test.proof)
			request.TimestampAdmissionFloor = test.epoch
			err := applyFSMRequest(t, fsm, request)
			if test.wantErr != nil {
				require.ErrorIs(t, err, test.wantErr)
				return
			}
			require.NoError(t, err)
			require.NoError(t, applyFSMRequest(t, fsm, preparedEvidenceRequest(pb.Phase_COMMIT, test.proof)))
		})
	}
}
