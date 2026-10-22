package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestProvenCommitDrainsMigrationAndCreationFences(t *testing.T) {
	t.Parallel()
	for _, primary := range []string{"n", "b"} {
		t.Run(primary, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			engine := distribution.NewEngine()
			route := distribution.RouteDescriptor{RouteID: 1, Start: []byte("a"), End: []byte("z"), GroupID: 1}
			applyComposed1Snapshot(t, engine, 1, []distribution.RouteDescriptor{route})
			fsm := newComposed1FSM(t, engine, 1)
			require.Nil(t, fsm.Apply(EncodePrepareEvidenceFence(0)))
			meta := TxnMeta{PrimaryKey: []byte(primary), CommitTS: 999, PrepareCommitTS: 60, LockTTLms: defaultTxnLockTTLms}
			request := &pb.Request{IsTxn: true, Phase: pb.Phase_PREPARE, Ts: 40, Mutations: []*pb.Mutation{
				{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(meta)},
				{Op: pb.Op_PUT, Key: []byte("n"), Value: []byte("prepared")},
			}}
			require.NoError(t, applyFSMRequest(t, fsm, request))
			lock, found, err := fsm.txnLockAtKey(ctx, txnLockKey([]byte("n")))
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, uint64(60), lock.CommitTS, "only the V3 proof timestamp may bind the lock")
			route.State = distribution.RouteStateWriteFenced
			applyComposed1Snapshot(t, engine, 2, []distribution.RouteDescriptor{route})
			applyTargetReadinessToFSM(t, fsm, store.TargetStagedReadinessState{
				JobID: 11, MigrationJobID: 11, RouteStart: []byte("m"), RouteEnd: []byte("z"),
				Armed: true, SourceWriteFence: true, TrackWrites: true, MinWriteTSExclusive: 1, RetentionPinTS: 1,
			})
			require.Nil(t, fsm.Apply(EncodePrepareEvidenceSeal()))
			request.Phase = pb.Phase_COMMIT
			meta.PrepareCommitTS, meta.CommitTS = 0, 61
			request.Mutations[0].Value = EncodeTxnMeta(meta)
			require.ErrorIs(t, applyFSMRequest(t, fsm, request), ErrTxnInvalidMeta)
			meta.CommitTS = 60
			request.Mutations[0].Value = EncodeTxnMeta(meta)
			require.NoError(t, applyFSMRequest(t, fsm, request))
			value, err := fsm.store.GetAt(ctx, []byte("n"), 60)
			require.NoError(t, err)
			require.Equal(t, []byte("prepared"), value)
			require.Equal(t, uint64(60), migrationTrackerMinimum(t, fsm))
			_, found, err = fsm.txnLockAtKey(ctx, txnLockKey([]byte("n")))
			require.NoError(t, err)
			require.False(t, found)
		})
	}
}

func TestPrepareProofTimestampControlsMigrationFloor(t *testing.T) {
	t.Parallel()
	for _, proof := range []uint64{60, 101} {
		fsm := newWriteFloorFSM(t)
		require.Nil(t, fsm.Apply(EncodePrepareEvidenceFence(0)))
		hint := uint64(999)
		if proof == 101 {
			hint = 1
		}
		request := &pb.Request{IsTxn: true, Phase: pb.Phase_PREPARE, Ts: 50, Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("b"), CommitTS: hint, PrepareCommitTS: proof})},
			{Op: pb.Op_PUT, Key: []byte("b"), Value: []byte("prepared")},
		}}
		err := applyFSMRequest(t, fsm, request)
		if proof == 60 {
			require.ErrorIs(t, err, ErrRouteWriteTimestampTooLow, "an inflated legacy hint must not bypass the route floor")
		} else {
			require.NoError(t, err, "the negotiated proof, not its legacy hint, determines admission")
		}
	}
}

func TestMigrationKeepsNewPrepareControlLocalAndOldNamesUserOwned(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	oldKeys := legacyPrepareEvidenceUserKeys()
	for _, key := range oldKeys {
		require.NoError(t, st.PutAt(ctx, key, []byte{1}, 10, 0))
	}
	fsm := newPrepareSafetyFSM(t, st)
	require.NoError(t, fsm.persistBackupTimestampFloor(ctx, 100))
	for _, command := range [][]byte{EncodePrepareEvidenceFence(0), EncodePrepareEvidenceSeal(), EncodePrepareEvidenceFence(100), EncodePrepareEvidenceRelease(100)} {
		require.Nil(t, fsm.Apply(command))
	}
	controls := [][]byte{backupTimestampFloorKey, prepareEvidenceEnabledKey, prepareEvidenceGateKey, prepareEvidenceTimestampFloorKey}
	brackets, err := distribution.PlanExportBrackets([]byte("!txn|"), []byte("!txn}"))
	require.NoError(t, err)
	for _, key := range controls {
		for _, bracket := range brackets {
			require.False(t, bracket.ContainsRawKey(key), "peer-local control must never enter an export family")
		}
		command, marshalErr := MarshalMigrationImportCommand(&pb.ImportRangeVersionsRequest{JobId: 99, BracketId: 1, BatchSeq: 1,
			Versions: []*pb.MVCCVersion{{Key: key, Value: []byte{1}, CommitTs: 200}}})
		require.NoError(t, marshalErr)
		applyErr, ok := fsm.Apply(command).(error)
		require.True(t, ok)
		require.ErrorIs(t, applyErr, ErrInvalidRequest)
	}
	cleanup := &pb.CleanupMigrationRequest{JobId: 7, RangeStart: []byte("!txn|"), RangeEnd: []byte("!txn}"), RouteStart: []byte("!"),
		KeyFamily: distribution.MigrationFamilyUser, ExcludeKnownInternal: true, RequiresRouteKeyCheck: true, MaxCommitTs: 1000}
	opts := migrationCleanupOptionsFromProto(cleanup, 0)
	exported, err := st.ExportVersions(ctx, store.ExportVersionsOptions{StartKey: cleanup.RangeStart, EndKey: cleanup.RangeEnd,
		MaxVersions: 100, AcceptVersion: opts.AcceptVersion})
	require.NoError(t, err)
	require.Len(t, exported.Versions, len(oldKeys))
	histories := make(map[string][]store.MVCCVersion)
	for _, version := range exported.Versions {
		histories[string(version.Key)] = append(histories[string(version.Key)], version)
	}
	assertPrepareNamespaceMigration(t, false, histories)
	before := make(map[string][]byte)
	for _, key := range controls {
		before[string(key)], err = st.GetAt(ctx, key, ^uint64(0))
		require.NoError(t, err)
	}
	command, err := MarshalMigrationCleanupCommand(cleanup)
	require.NoError(t, err)
	require.IsType(t, store.CleanupVersionsResult{}, fsm.Apply(command))
	for _, key := range oldKeys {
		_, err = st.GetAt(ctx, key, ^uint64(0))
		require.ErrorIs(t, err, store.ErrKeyNotFound, "old names must retire as ordinary migrated user versions")
	}
	for _, key := range controls {
		value, loadErr := st.GetAt(ctx, key, ^uint64(0))
		require.NoError(t, loadErr)
		require.Equal(t, before[string(key)], value)
	}
}
