package kv

import (
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestTSOProductionClaimsResolutionRetainedMetadata(t *testing.T) {
	coordinator, _, groups, _, _ := newClaimedShardedFixture(t)
	request := &pb.Request{IsTxn: true, Phase: pb.Phase_PREPARE, Ts: 1, TimestampClaimGroupId: 1,
		ReadKeys: [][]byte{[]byte("read-a"), []byte("read-b")}, WriteFenceBypassKeys: [][]byte{[]byte("b")},
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("b"), LockTTLms: defaultTxnLockTTLms})},
			{Op: pb.Op_PUT, Key: []byte("b"), Value: []byte("value")},
		}}
	require.NoError(t, IssueRequestTimestampClaim(t.Context(), coordinator.tsAllocator, request))
	_, err := groups[1].Txn.Commit(t.Context(), []*pb.Request{request})
	require.NoError(t, err)
	claim, err := DecodeRequestTimestampClaim(request.TimestampClaim)
	require.NoError(t, err)
	operation := &OperationGroup[OP]{IsTxn: true, StartTS: 1, CommitTS: claim.Proof.Record.Timestamp,
		TimestampIntent: request.TimestampIntent, TimestampClaim: request.TimestampClaim}
	for _, phase := range []pb.Phase{pb.Phase_COMMIT, pb.Phase_ABORT} {
		resolution, err := claimedResolutionRequest(operation, 1, []byte("b"), phase)
		require.NoError(t, err)
		require.NoError(t, ValidateClaimedTxnResolution(t.Context(), coordinator.tsAllocator, groups[1].Store, resolution))
		resolution.ReadKeys = [][]byte{[]byte("read-b")}
		require.NoError(t, ValidateClaimedTxnResolution(t.Context(), coordinator.tsAllocator, groups[1].Store, resolution))
		resolution.WriteFenceBypassKeys = nil
		require.NoError(t, ValidateClaimedTxnResolution(t.Context(), coordinator.tsAllocator, groups[1].Store, resolution), "resolver omission must remain supported")
		for name, change := range map[string]func(*pb.Request){
			"read alias":   func(r *pb.Request) { r.ReadKeys = append(r.ReadKeys, []byte("foreign-read")) },
			"fence bypass": func(r *pb.Request) { r.WriteFenceBypassKeys = [][]byte{[]byte("b"), []byte("foreign-key")} },
		} {
			t.Run(phase.String()+"/"+name, func(t *testing.T) {
				forged := proto.CloneOf(resolution)
				change(forged)
				require.ErrorIs(t, ValidateClaimedTxnResolution(t.Context(), coordinator.tsAllocator, groups[1].Store, forged), ErrTSOClaimInvalid)
			})
		}
	}
}
