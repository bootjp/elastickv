package kv

import (
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestTSOProductionClaimsPreparedSubsetCheckpointRetainsOriginalStart(t *testing.T) {
	f, st, allocator, operation := preparedClaimPrimaryCheckpoint(t)
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
	err = f.handleCommitRequest(t.Context(), resolution)
	require.ErrorIs(t, err, ErrTSOProtocolUnsupported, "valid subset resolution must reach the missing store capability, not lose its original startTS")
}

func TestTSOProductionClaimsPreparedFullRetryAfterPrimaryCheckpoint(t *testing.T) {
	f, st, allocator, operation := preparedClaimPrimaryCheckpoint(t)
	resolution, err := claimedResolutionRequest(operation, 1, []byte("b"), pb.Phase_COMMIT)
	require.NoError(t, err)
	require.Len(t, resolution.Mutations, 3, "retry includes both resolved b and still-prepared c")
	require.NoError(t, ValidateClaimedTxnResolution(t.Context(), allocator, st, resolution))
	require.NoError(t, f.handleCommitRequest(t.Context(), resolution), "actual composed store capability must resolve c while retaining original-start OCC")
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
	require.NoError(t, f.handleCommitRequest(t.Context(), resolution), "replay of the same full retry must remain idempotent")
}

func preparedClaimPrimaryCheckpoint(t *testing.T) (*kvFSM, store.MVCCStore, TimestampAllocator, *OperationGroup[OP]) {
	t.Helper()
	_, allocator, _ := newProductionClaimFixture(t)
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
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
	require.NoError(t, f.handlePrepareRequest(t.Context(), request))
	claim, err := DecodeRequestTimestampClaim(request.TimestampClaim)
	require.NoError(t, err)
	commitTS := claim.Proof.Record.Timestamp
	// Checkpoint after resolving only the primary. The other key remains
	// prepared; these real writes do not implement the missing atomic applier.
	require.NoError(t, st.ApplyMutationsRaftAt(t.Context(), []*store.KVPairMutation{
		{Op: store.OpTypePut, Key: []byte("b"), Value: []byte("primary")},
		{Op: store.OpTypeDelete, Key: txnLockKey([]byte("b"))},
		{Op: store.OpTypeDelete, Key: txnIntentKey([]byte("b"))},
		{Op: store.OpTypePut, Key: txnCommitKey([]byte("b"), 1), Value: encodeTxnCommitRecord(commitTS)},
	}, nil, commitTS, commitTS, 0))
	operation := &OperationGroup[OP]{IsTxn: true, StartTS: 1, CommitTS: commitTS,
		TimestampIntent: request.TimestampIntent, TimestampClaim: request.TimestampClaim}
	return f, st, allocator, operation
}
