package kv

import (
	"bytes"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func activateTransferSizeGuardForTest(t *testing.T, f *kvFSM) {
	t.Helper()
	req := &pb.TargetStagedReadinessRequest{JobId: 11, MigrationJobId: 11,
		Armed: true, TrackWrites: true, RouteStart: []byte("m"), RouteEnd: []byte("z"),
		ExpectedCutoverVersion: 1, MinWriteTsExclusive: 1, RetentionPinTs: 1,
		TransferSizeGuardVersion: 1, RequireConfigurationIndex: true, ExpectedConfigurationIndex: 9}
	command, err := MarshalTargetStagedReadinessCommand(req)
	require.NoError(t, err)
	require.Nil(t, f.Apply(command))
}

func TestMigrationSizeGuardRawRequestAtomicVerdict(t *testing.T) {
	f := newMigrationWriteTrackerFSM(t)
	installSourceOwnershipForTest(t, f, 0)
	activateTransferSizeGuardForTest(t, f)
	request := &pb.Request{Ts: 50, ObservedRouteVersion: EncodeObservedRouteVersion(1), Mutations: []*pb.Mutation{
		{Op: pb.Op_PUT, Key: []byte("n"), Value: []byte("must not land")},
		{Op: pb.Op_PUT, Key: []byte("o"), Value: make([]byte, store.MigrationPagePayloadBudget+(1<<19))},
	}}
	err := f.applyRequestErr(t.Context(), request)
	require.ErrorIs(t, err, ErrMigrationTransferTooLarge)
	require.True(t, isRawApplyVerdict(err), "size rejection must not halt the raw batch")
	_, err = f.store.GetAt(t.Context(), []byte("n"), 100)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	require.Zero(t, migrationTrackerMinimum(t, f), "failed admission must not change the source tracker")
}

func TestMigrationSizeGuardLegacyAndOutsideRangeUnchanged(t *testing.T) {
	value := make([]byte, store.MigrationPagePayloadBudget+(1<<19))
	for _, test := range []struct {
		name  string
		guard bool
		key   string
	}{{"legacy tracker", false, "n"}, {"outside moving range", true, "b"}} {
		t.Run(test.name, func(t *testing.T) {
			f := newMigrationWriteTrackerFSM(t)
			if test.guard {
				require.NoError(t, f.RecordAppliedConfiguration(9))
			} else {
				installSourceOwnershipForTest(t, f, 0)
			}
			if test.guard {
				activateTransferSizeGuardForTest(t, f)
			}
			req := &pb.Request{Ts: 50, ObservedRouteVersion: EncodeObservedRouteVersion(1), Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte(test.key), Value: value}}}
			require.NoError(t, f.applyRequestErr(t.Context(), req))
			actual, err := f.store.GetAt(t.Context(), []byte(test.key), 100)
			require.NoError(t, err)
			require.Len(t, actual, len(value))
		})
	}
}

func sizeGuardTxnRequest(phase pb.Phase, value []byte) *pb.Request {
	key := []byte("n")
	return &pb.Request{IsTxn: true, Phase: phase, Ts: 50, ObservedRouteVersion: EncodeObservedRouteVersion(1), Mutations: []*pb.Mutation{
		{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: key, CommitTS: 60, LockTTLms: defaultTxnLockTTLms})},
		{Op: pb.Op_PUT, Key: key, Value: value},
	}}
}

func TestMigrationSizeGuardChecksEncodedPrepareIntent(t *testing.T) {
	value := make([]byte, store.MigrationPagePayloadBudget-130)
	require.True(t, store.MigrationVersionFitsTransfer([]byte("n"), value), "the raw version itself fits")
	f := newMigrationWriteTrackerFSM(t)
	installSourceOwnershipForTest(t, f, 0)
	activateTransferSizeGuardForTest(t, f)
	err := f.applyRequestErr(t.Context(), sizeGuardTxnRequest(pb.Phase_PREPARE, value))
	require.ErrorIs(t, err, ErrMigrationTransferTooLarge, "the encoded intent and its physical key exceed the transfer budget")
	_, err = f.store.GetAt(t.Context(), txnLockKey([]byte("n")), 100)
	require.ErrorIs(t, err, store.ErrKeyNotFound, "a rejected PREPARE must not leave even its small lock row")
	require.Zero(t, migrationTrackerMinimum(t, f))
	raw := &pb.Request{Ts: 70, ObservedRouteVersion: EncodeObservedRouteVersion(1), Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("n"), Value: value}}}
	require.NoError(t, f.applyRequestErr(t.Context(), raw), "encoded-intent overhead must not reduce the raw limit")
}

func TestMigrationSizeGuardOnePhaseRejectsAtomically(t *testing.T) {
	f := newMigrationWriteTrackerFSM(t)
	installSourceOwnershipForTest(t, f, 0)
	activateTransferSizeGuardForTest(t, f)
	req := sizeGuardTxnRequest(pb.Phase_NONE, make([]byte, store.MigrationPagePayloadBudget+(1<<19)))
	req.Mutations = append(req.Mutations, &pb.Mutation{Op: pb.Op_PUT, Key: []byte("o"), Value: []byte("no partial transaction")})
	require.ErrorIs(t, f.applyRequestErr(t.Context(), req), ErrMigrationTransferTooLarge)
	_, err := f.store.GetAt(t.Context(), []byte("o"), 100)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	require.Zero(t, migrationTrackerMinimum(t, f))
}

func TestMigrationSizeGuardExistingPrepareResolutionsDrain(t *testing.T) {
	value := make([]byte, store.MigrationPagePayloadBudget+(1<<19))
	for _, phase := range []pb.Phase{pb.Phase_COMMIT, pb.Phase_ABORT} {
		t.Run(phase.String(), func(t *testing.T) {
			f := newMigrationWriteTrackerFSM(t)
			installSourceOwnershipForTest(t, f, 0)
			require.NoError(t, f.applyRequestErr(t.Context(), sizeGuardTxnRequest(pb.Phase_PREPARE, value)))
			activateTransferSizeGuardForTest(t, f)
			require.NoError(t, f.applyRequestErr(t.Context(), sizeGuardTxnRequest(phase, nil)), "existing resolution must not be reclassified as new creation")
			_, err := f.store.GetAt(t.Context(), txnLockKey([]byte("n")), 100)
			require.ErrorIs(t, err, store.ErrKeyNotFound)
			actual, err := f.store.GetAt(t.Context(), []byte("n"), 100)
			if phase == pb.Phase_COMMIT {
				require.NoError(t, err)
				require.Len(t, actual, len(value))
			} else {
				require.ErrorIs(t, err, store.ErrKeyNotFound)
			}
		})
	}
}

func TestMigrationSizeGuardRawBatchKeepsIndependentVerdicts(t *testing.T) {
	f := newMigrationWriteTrackerFSM(t)
	installSourceOwnershipForTest(t, f, 0)
	activateTransferSizeGuardForTest(t, f)
	bad := &pb.Request{Ts: 50, ObservedRouteVersion: EncodeObservedRouteVersion(1), Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("n"), Value: make([]byte, store.MigrationPagePayloadBudget+(1<<19))}}}
	good := &pb.Request{Ts: 60, ObservedRouteVersion: EncodeObservedRouteVersion(1), Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("o"), Value: []byte("accepted independently")}}}
	command, err := marshalRaftCommand([]*pb.Request{bad, good})
	require.NoError(t, err)
	f.SetApplyIndex(12)
	result, ok := f.Apply(command).(*fsmApplyResponse)
	require.True(t, ok, "size rejection must not produce a halt")
	require.ErrorIs(t, result.results[0], ErrMigrationTransferTooLarge)
	require.NoError(t, result.results[1])
	actual, err := f.store.GetAt(t.Context(), []byte("o"), 100)
	require.NoError(t, err)
	require.Equal(t, []byte("accepted independently"), actual)
}

func TestMigrationSizeGuardActivationOrdersAndLegacyRefresh(t *testing.T) {
	value := make([]byte, store.MigrationPagePayloadBudget+(1<<19))
	for _, writeFirst := range []bool{false, true} {
		f := newMigrationWriteTrackerFSM(t)
		installSourceOwnershipForTest(t, f, 0)
		req := &pb.Request{Ts: 50, ObservedRouteVersion: EncodeObservedRouteVersion(1), Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("n"), Value: value}}}
		if writeFirst {
			require.NoError(t, f.applyRequestErr(t.Context(), req), "a write ordered before explicit activation retains the legacy verdict")
		}
		activateTransferSizeGuardForTest(t, f)
		// An older runner's tracker refresh must not silently clear the guard.
		refresh, err := MarshalTargetStagedReadinessCommand(&pb.TargetStagedReadinessRequest{JobId: 11, MigrationJobId: 11,
			Armed: true, TrackWrites: true, RouteStart: []byte("m"), RouteEnd: []byte("z"),
			ExpectedCutoverVersion: 1, MinWriteTsExclusive: 1, RetentionPinTs: 1})
		require.NoError(t, err)
		require.Nil(t, f.Apply(refresh))
		req.Ts = 70
		require.ErrorIs(t, f.applyRequestErr(t.Context(), req), ErrMigrationTransferTooLarge, "all writes ordered after activation see the guard")
		actual, err := f.store.GetAt(t.Context(), []byte("n"), 100)
		if writeFirst {
			require.NoError(t, err)
			require.Len(t, actual, len(value), "activation must not delete or rewrite existing oversized history")
		} else {
			require.ErrorIs(t, err, store.ErrKeyNotFound)
		}
	}
}

func TestMigrationSizeGuardStaleActivationConfiguration(t *testing.T) {
	f := newMigrationWriteTrackerFSM(t)
	installSourceOwnershipForTest(t, f, 0)
	command, err := MarshalTargetStagedReadinessCommand(&pb.TargetStagedReadinessRequest{JobId: 11, MigrationJobId: 11,
		Armed: true, TrackWrites: true, RouteStart: []byte("m"), RouteEnd: []byte("z"), MinWriteTsExclusive: 1, RetentionPinTs: 1,
		TransferSizeGuardVersion: 1, RequireConfigurationIndex: true, ExpectedConfigurationIndex: 9})
	require.NoError(t, err)
	require.NoError(t, f.RecordAppliedConfiguration(12))
	response, ok := f.Apply(command).(error)
	require.True(t, ok)
	require.ErrorIs(t, response, ErrMigrationConfigurationChanged)
	reader, ok := f.store.(store.MigrationTargetReadinessReader)
	require.True(t, ok)
	states, err := reader.MigrationTargetReadinessStates(t.Context())
	require.NoError(t, err)
	require.Zero(t, states[0].TransferSizeGuardVersion)
}

func TestMigrationSizeGuardFullAtomicRequestBoundUnchanged(t *testing.T) {
	value := make([]byte, 32<<20)
	req := &pb.Request{Ts: 50, Mutations: []*pb.Mutation{
		{Op: pb.Op_PUT, Key: []byte("n"), Value: value}, {Op: pb.Op_PUT, Key: []byte("o"), Value: value},
	}}
	for _, mut := range req.Mutations {
		require.True(t, store.MigrationVersionFitsTransfer(mut.Key, mut.Value))
	}
	require.GreaterOrEqual(t, proto.Size(req), maxMarshaledCommandSize)
	_, err := marshalRaftCommand([]*pb.Request{req})
	require.ErrorContains(t, err, "marshaled request too large", "fitting individual transfer rows cannot bypass the full atomic command limit")
	_, err = marshalRaftCommand([]*pb.Request{{Ts: 50, Mutations: req.Mutations[:1]}, {Ts: 60, Mutations: req.Mutations[1:]}})
	require.ErrorContains(t, err, "marshaled request batch too large")
}

func TestMigrationSizeGuardNormalizedDerivedWriters(t *testing.T) {
	value := make([]byte, store.MigrationPagePayloadBudget+(1<<19))
	for _, key := range [][]byte{
		store.HashFieldKey([]byte("n"), []byte("field")),
		store.ZSetMemberKey([]byte("n"), []byte("member")),
		store.SetMemberKey([]byte("n"), []byte("member")),
		store.StreamEntryKey([]byte("n"), 123, 4),
	} {
		f := newMigrationWriteTrackerFSM(t)
		installSourceOwnershipForTest(t, f, 0)
		activateTransferSizeGuardForTest(t, f)
		req := &pb.Request{Ts: 50, ObservedRouteVersion: EncodeObservedRouteVersion(1), Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: key, Value: value}}}
		require.ErrorIs(t, f.applyRequestErr(t.Context(), req), ErrMigrationTransferTooLarge, "derived physical key %q must use its logical moving range", key)
	}
}

func TestMigrationSizeGuardCommitOnlyUsesPreparedBytes(t *testing.T) {
	f := newMigrationWriteTrackerFSM(t)
	installSourceOwnershipForTest(t, f, 0)
	activateTransferSizeGuardForTest(t, f)
	require.NoError(t, f.applyRequestErr(t.Context(), sizeGuardTxnRequest(pb.Phase_PREPARE, []byte("persisted intent"))))
	request := sizeGuardTxnRequest(pb.Phase_COMMIT, make([]byte, store.MigrationPagePayloadBudget+(1<<19)))
	require.NoError(t, f.applyRequestErr(t.Context(), request), "resolution may carry unused caller bytes, but cannot create an oversized unprepared value")
	actual, err := f.store.GetAt(t.Context(), []byte("n"), 100)
	require.NoError(t, err)
	require.Equal(t, []byte("persisted intent"), actual)
}

func TestMigrationSizeGuardRestoredFSMAppliesGuard(t *testing.T) {
	f := newMigrationWriteTrackerFSM(t)
	installSourceOwnershipForTest(t, f, 0)
	activateTransferSizeGuardForTest(t, f)
	snapshot, err := f.store.Snapshot()
	require.NoError(t, err)
	var raw bytes.Buffer
	_, err = snapshot.WriteTo(&raw)
	require.NoError(t, err)
	require.NoError(t, snapshot.Close())
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	require.NoError(t, st.Restore(bytes.NewReader(raw.Bytes())))
	restored := NewKvFSMWithHLC(st, nil, WithRouteHistory(nil, 1))
	command, err := marshalRaftCommand([]*pb.Request{{Ts: 50, ObservedRouteVersion: EncodeObservedRouteVersion(1),
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("n"), Value: make([]byte, store.MigrationPagePayloadBudget+(1<<19))}}}})
	require.NoError(t, err)
	response, ok := restored.Apply(command).(error)
	require.True(t, ok)
	require.ErrorIs(t, response, ErrMigrationTransferTooLarge, "restored FSM must reject by the persisted size guard, not missing ownership or epoch")
}
