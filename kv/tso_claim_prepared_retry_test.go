package kv

import (
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestTSOProductionClaimsPreparedSubsetCheckpointRetainsOriginalStart(t *testing.T) {
	withClaimedCheckpointStores(t, func(t *testing.T, initial store.MVCCStore) {
		f, st, allocator, operation := preparedClaimPrimaryCheckpoint(t, initial)
		commitTS := operation.CommitTS
		resolution, err := claimedResolutionRequest(operation, 1, []byte("b"), pb.Phase_COMMIT)
		require.NoError(t, err)
		resolution.Mutations = []*pb.Mutation{resolution.Mutations[0], {Op: pb.Op_PUT, Key: []byte("c")}}
		require.NoError(t, ValidateClaimedTxnResolution(t.Context(), allocator, st, resolution))
		meta, muts, err := extractTxnMeta(resolution.Mutations)
		require.NoError(t, err)
		storeMuts, err := f.buildCommitStoreMutations(t.Context(), muts, meta, 1, commitTS, true, nil)
		require.NoError(t, err)
		expected, err := f.expectedPreparedClaimVersions(t.Context(), storeMuts, 1, commitTS)
		require.NoError(t, err)
		require.Len(t, expected, 2, "both current prepared versions are valid at the original startTS")
		require.NoError(t, applyClaimedCheckpointRequest(t, f, resolution, 4))
		assertPreparedClaimResolution(t, st, operation)
		require.NoError(t, applyClaimedCheckpointRequest(t, f, resolution, 4), "subset replay remains idempotent")
	})
}

func TestTSOProductionClaimsPreparedFullRetryAfterPrimaryCheckpoint(t *testing.T) {
	withClaimedCheckpointStores(t, func(t *testing.T, initial store.MVCCStore) {
		f, st, allocator, operation := preparedClaimPrimaryCheckpoint(t, initial)
		resolution, err := claimedResolutionRequest(operation, 1, []byte("b"), pb.Phase_COMMIT)
		require.NoError(t, err)
		require.Len(t, resolution.Mutations, 3, "retry includes both resolved b and still-prepared c")
		require.NoError(t, ValidateClaimedTxnResolution(t.Context(), allocator, st, resolution))
		err = applyClaimedCheckpointRequest(t, f, resolution, 4)
		if key, ok := store.WriteConflictKey(err); ok {
			t.Logf("actual full retry conflict key: %q", key)
		}
		require.NoError(t, err, "actual composed store capability must resolve c while retaining original-start OCC")
		assertPreparedClaimResolution(t, st, operation)
		require.NoError(t, applyClaimedCheckpointRequest(t, f, resolution, 4), "replay of the same full retry must remain idempotent")
	})
}

func assertPreparedClaimResolution(t *testing.T, st store.MVCCStore, operation *OperationGroup[OP]) {
	t.Helper()
	for key, value := range map[string]string{"b": "primary", "c": "same-group-secondary"} {
		actual, err := st.GetAt(t.Context(), []byte(key), ^uint64(0))
		require.NoError(t, err)
		require.Equal(t, []byte(value), actual)
		for _, internal := range [][]byte{txnLockKey([]byte(key)), txnIntentKey([]byte(key))} {
			_, err := st.GetAt(t.Context(), internal, ^uint64(0))
			require.ErrorIs(t, err, store.ErrKeyNotFound)
		}
	}
	record, err := st.GetAt(t.Context(), txnCommitKey([]byte("b"), operation.StartTS), ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, encodeTxnCommitRecord(operation.CommitTS), record)
}

func TestTSOProductionClaimsPreparedRetryRejectsMismatchedDecision(t *testing.T) {
	withClaimedCheckpointStores(t, func(t *testing.T, initial store.MVCCStore) {
		f, st, allocator, operation := preparedClaimPrimaryCheckpoint(t, initial)
		key := txnCommitKey([]byte("b"), operation.StartTS)
		wrong := encodeTxnCommitRecord(operation.CommitTS + 1)
		require.NoError(t, st.PutAt(t.Context(), key, wrong, operation.CommitTS+1, 0))
		resolution, err := claimedResolutionRequest(operation, 1, []byte("b"), pb.Phase_COMMIT)
		require.NoError(t, err)
		require.NoError(t, ValidateClaimedTxnResolution(t.Context(), allocator, st, resolution))
		require.ErrorIs(t, applyClaimedCheckpointRequest(t, f, resolution, 4), ErrTxnInvalidMeta)
		unchanged, err := st.GetAt(t.Context(), key, ^uint64(0))
		require.NoError(t, err)
		require.Equal(t, wrong, unchanged)
		assertClaimedSecondaryStillPrepared(t, st)
	})
}

func TestTSOProductionClaimsPreparedRetryRejectsChangedDecisionVersion(t *testing.T) {
	withClaimedCheckpointStores(t, func(t *testing.T, initial store.MVCCStore) {
		f, st, _, operation := preparedClaimPrimaryCheckpoint(t, initial)
		key := txnCommitKey([]byte("b"), operation.StartTS)
		require.NoError(t, st.PutAt(t.Context(), key, encodeTxnCommitRecord(operation.CommitTS), operation.CommitTS+1, 0))
		resolution, err := claimedResolutionRequest(operation, 1, []byte("b"), pb.Phase_COMMIT)
		require.NoError(t, err)
		require.ErrorIs(t, applyClaimedCheckpointRequest(t, f, resolution, 4), store.ErrWriteConflict)
		latest, exists, err := st.LatestCommitTS(t.Context(), key)
		require.NoError(t, err)
		require.True(t, exists)
		require.Equal(t, operation.CommitTS+1, latest)
		assertClaimedSecondaryStillPrepared(t, st)
	})
}

func TestTSOProductionClaimsPreparedRetryPreservesUserOCC(t *testing.T) {
	for _, includePrimary := range []bool{false, true} {
		t.Run(map[bool]string{false: "subset", true: "full"}[includePrimary], func(t *testing.T) {
			withClaimedCheckpointStores(t, func(t *testing.T, initial store.MVCCStore) {
				f, st, _, operation := preparedClaimPrimaryCheckpoint(t, initial)
				require.NoError(t, st.PutAt(t.Context(), []byte("c"), []byte("newer-value"), operation.CommitTS+1, 0))
				resolution, err := claimedResolutionRequest(operation, 1, []byte("b"), pb.Phase_COMMIT)
				require.NoError(t, err)
				if !includePrimary {
					resolution.Mutations = []*pb.Mutation{resolution.Mutations[0], {Op: pb.Op_PUT, Key: []byte("c")}}
				}
				err = applyClaimedCheckpointRequest(t, f, resolution, 4)
				require.ErrorIs(t, err, store.ErrWriteConflict)
				key, ok := store.WriteConflictKey(err)
				require.True(t, ok)
				require.Equal(t, []byte("c"), key, "the unresolved user key retains original-start OCC")
				value, err := st.GetAt(t.Context(), []byte("c"), ^uint64(0))
				require.NoError(t, err)
				require.Equal(t, []byte("newer-value"), value)
				for _, internal := range [][]byte{txnLockKey([]byte("c")), txnIntentKey([]byte("c"))} {
					_, err := st.GetAt(t.Context(), internal, ^uint64(0))
					require.NoError(t, err, "conflict must not partially clean prepared state")
				}
			})
		})
	}
}

func TestTSOProductionClaimsPreparedRetryPreservesLaterPrimaryWrite(t *testing.T) {
	withClaimedCheckpointStores(t, func(t *testing.T, initial store.MVCCStore) {
		f, st, allocator, operation := preparedClaimPrimaryCheckpoint(t, initial)
		later := &pb.Request{IsTxn: true, Ts: operation.CommitTS, TimestampClaimGroupId: 1,
			Mutations: []*pb.Mutation{
				{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("b"), LockTTLms: defaultTxnLockTTLms})},
				{Op: pb.Op_PUT, Key: []byte("b"), Value: []byte("later-transaction")},
			}}
		require.NoError(t, IssueRequestTimestampClaim(t.Context(), allocator, later))
		require.NoError(t, ValidateRequestTimestampClaim(t.Context(), allocator, later))
		require.NoError(t, applyClaimedCheckpointRequest(t, f, later, 4))
		resolution, err := claimedResolutionRequest(operation, 1, []byte("b"), pb.Phase_COMMIT)
		require.NoError(t, err)
		require.NoError(t, ValidateClaimedTxnResolution(t.Context(), allocator, st, resolution))
		for range 2 {
			require.NoError(t, applyClaimedCheckpointRequest(t, f, resolution, 5))
			b, err := st.GetAt(t.Context(), []byte("b"), ^uint64(0))
			require.NoError(t, err)
			require.Equal(t, []byte("later-transaction"), b)
			c, err := st.GetAt(t.Context(), []byte("c"), ^uint64(0))
			require.NoError(t, err)
			require.Equal(t, []byte("same-group-secondary"), c)
			for _, key := range [][]byte{txnLockKey([]byte("c")), txnIntentKey([]byte("c"))} {
				_, err := st.GetAt(t.Context(), key, ^uint64(0))
				require.ErrorIs(t, err, store.ErrKeyNotFound)
			}
		}
	})
}

func TestTSOProductionClaimsPreparedRetryRequiresRetainedBinding(t *testing.T) {
	withClaimedCheckpointStores(t, func(t *testing.T, initial store.MVCCStore) {
		f, st, _, operation := preparedClaimPrimaryCheckpoint(t, initial)
		resolution, err := claimedResolutionRequest(operation, 1, []byte("b"), pb.Phase_COMMIT)
		require.NoError(t, err)
		resolution.TimestampIntent = proto.CloneOf(resolution.TimestampIntent)
		resolution.TimestampIntent.OperationId[0] ^= 1
		require.ErrorIs(t, applyClaimedCheckpointRequest(t, f, resolution, 4), ErrTSOClaimInvalid)
		assertClaimedSecondaryStillPrepared(t, st)
	})
}

func TestTSOProductionClaimsPreparedFullRetryAfterPebbleReopen(t *testing.T) {
	dir := t.TempDir()
	st, err := store.NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() {
		if st != nil {
			require.NoError(t, st.Close())
		}
	})
	f, _, allocator, operation := preparedClaimPrimaryCheckpoint(t, st)
	index, present, err := f.LastAppliedIndex()
	require.NoError(t, err)
	require.True(t, present)
	require.Equal(t, uint64(3), index)
	require.NoError(t, st.Close())
	st = nil
	st, err = store.NewPebbleStore(dir)
	require.NoError(t, err)
	f, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)
	assertClaimedSecondaryStillPrepared(t, st)
	resolution, err := claimedResolutionRequest(operation, 1, []byte("b"), pb.Phase_COMMIT)
	require.NoError(t, err)
	require.NoError(t, ValidateClaimedTxnResolution(t.Context(), allocator, st, resolution))
	require.NoError(t, applyClaimedCheckpointRequest(t, f, resolution, 4))
	assertPreparedClaimResolution(t, st, operation)
	index, present, err = f.LastAppliedIndex()
	require.NoError(t, err)
	require.True(t, present)
	require.Equal(t, uint64(4), index)
	require.NoError(t, st.Close())
	st = nil
	st, err = store.NewPebbleStore(dir)
	require.NoError(t, err)
	f, ok = NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)
	assertPreparedClaimResolution(t, st, operation)
	require.NoError(t, applyClaimedCheckpointRequest(t, f, resolution, 4))
	assertPreparedClaimResolution(t, st, operation)
}

func withClaimedCheckpointStores(t *testing.T, run func(*testing.T, store.MVCCStore)) {
	t.Helper()
	for _, kind := range []string{"memory", "pebble"} {
		t.Run(kind, func(t *testing.T) {
			var st store.MVCCStore
			if kind == "memory" {
				st = store.NewMVCCStore()
			} else {
				var err error
				st, err = store.NewPebbleStore(t.TempDir())
				require.NoError(t, err)
			}
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			run(t, st)
		})
	}
}

func applyClaimedCheckpointRequest(t *testing.T, f *kvFSM, request *pb.Request, index uint64) error {
	t.Helper()
	f.SetApplyIndex(index)
	return applyFSMRequest(t, f, request)
}

func assertClaimedSecondaryStillPrepared(t *testing.T, st store.MVCCStore) {
	t.Helper()
	_, err := st.GetAt(t.Context(), []byte("c"), ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	for _, key := range [][]byte{txnLockKey([]byte("c")), txnIntentKey([]byte("c"))} {
		_, err := st.GetAt(t.Context(), key, ^uint64(0))
		require.NoError(t, err)
	}
}

func preparedClaimPrimaryCheckpoint(t *testing.T, st store.MVCCStore) (*kvFSM, store.MVCCStore, TimestampAllocator, *OperationGroup[OP]) {
	t.Helper()
	_, allocator, _ := newProductionClaimFixture(t)
	f, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)
	require.NoError(t, f.applyPrepareEvidenceFence(1))
	request := &pb.Request{IsTxn: true, Phase: pb.Phase_PREPARE, Ts: 1, TimestampClaimGroupId: 1,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("b"), LockTTLms: defaultTxnLockTTLms})},
			{Op: pb.Op_PUT, Key: []byte("b"), Value: []byte("primary")},
			{Op: pb.Op_PUT, Key: []byte("c"), Value: []byte("same-group-secondary")},
		}}
	require.NoError(t, IssueRequestTimestampClaim(t.Context(), allocator, request))
	require.NoError(t, ValidateRequestTimestampClaim(t.Context(), allocator, request))
	require.NoError(t, applyClaimedCheckpointRequest(t, f, request, 2))
	claim, err := DecodeRequestTimestampClaim(request.TimestampClaim)
	require.NoError(t, err)
	commitTS := claim.Proof.Record.Timestamp
	operation := &OperationGroup[OP]{IsTxn: true, StartTS: 1, CommitTS: commitTS,
		TimestampIntent: request.TimestampIntent, TimestampClaim: request.TimestampClaim}
	// Form the checkpoint through the real claim-aware COMMIT handler and
	// atomic store capability, resolving only the primary, not its participant c.
	primary, err := claimedResolutionRequest(operation, 1, []byte("b"), pb.Phase_COMMIT)
	require.NoError(t, err)
	primary.Mutations = primary.Mutations[:2]
	require.NoError(t, ValidateClaimedTxnResolution(t.Context(), allocator, st, primary))
	require.NoError(t, applyClaimedCheckpointRequest(t, f, primary, 3))
	assertClaimedSecondaryStillPrepared(t, st)
	return f, st, allocator, operation
}
