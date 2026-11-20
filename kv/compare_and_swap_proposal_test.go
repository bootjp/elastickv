package kv

import (
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
)

func TestCASProposalShape(t *testing.T) {
	t.Parallel()
	key := []byte("cas-key")
	elem := &Elem[OP]{Op: CompareAndSwap, Key: key, ExpectedCommitTS: 5}
	makeProposal := func(start, commit uint64) *pb.Request {
		return onePhaseTxnRequestWithPrevCommit(start, commit, 0, key, []*Elem[OP]{elem}, nil, 0, nil)
	}
	require.NoError(t, ValidateCASProposalShape([]*pb.Request{makeProposal(6, 7)}))
	unstamped := []*pb.Request{makeProposal(0, 0)}
	mut, err := ForwardCASMutation(unstamped)
	require.NoError(t, err)
	require.EqualValues(t, 5, mut.GetExpectedCommitTs())
	require.ErrorIs(t, ValidateCASProposalShape(unstamped), ErrInvalidRequest)

	for _, tc := range []struct {
		name string
		edit func(*pb.Request)
	}{
		{"raw", func(req *pb.Request) { req.IsTxn = false }},
		{"prepare", func(req *pb.Request) { req.Phase = pb.Phase_PREPARE }},
		{"missing predicate", func(req *pb.Request) { req.Mutations[1].ExpectedCommitTs = 0 }},
		{"predicate on ordinary put", func(req *pb.Request) { req.Mutations[1].Op = pb.Op_PUT }},
		{"extra mutation", func(req *pb.Request) { req.Mutations = append(req.Mutations, req.Mutations[1]) }},
		{"read dependency", func(req *pb.Request) { req.ReadKeys = [][]byte{[]byte("other")} }},
		{"prior attempt", func(req *pb.Request) {
			req.Mutations[0].Value = EncodeTxnMeta(TxnMeta{PrimaryKey: key, CommitTS: 7, PrevCommitTS: 4})
		}},
		{"different primary", func(req *pb.Request) {
			req.Mutations[0].Value = EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("other"), CommitTS: 7})
		}},
		{"unrelated bypass", func(req *pb.Request) { req.WriteFenceBypassKeys = [][]byte{[]byte("other")} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := makeProposal(6, 7)
			tc.edit(req)
			require.ErrorIs(t, ValidateCASProposalShape([]*pb.Request{req}), ErrInvalidRequest)
			_, err := marshalRaftCommand([]*pb.Request{req})
			require.ErrorIs(t, err, ErrInvalidRequest)
		})
	}
	require.ErrorIs(t, ValidateCASProposalShape([]*pb.Request{makeProposal(6, 7), makeProposal(8, 9)}), ErrInvalidRequest)
}

func TestCASForwardMismatchRequiresMatchingRequestShape(t *testing.T) {
	t.Parallel()
	key := []byte("cas-key")
	err := ForwardDecisionStatus(pb.ForwardFailurePhase_FORWARD_FAILURE_PHASE_ONE_PHASE_APPLY,
		pb.ForwardFailureReason_FORWARD_FAILURE_REASON_CAS_MISMATCH,
		pb.ForwardDecisionClass_FORWARD_DECISION_CLASS_DEFINITIVELY_ABORTED)
	elem := &Elem[OP]{Op: CompareAndSwap, Key: key, ExpectedCommitTS: 5}
	req := onePhaseTxnRequestWithPrevCommit(0, 0, 0, key, []*Elem[OP]{elem}, nil, 0, nil)
	proven := classifyForwardRPCError(err, []*pb.Request{req})
	require.True(t, IsCertifiedCASMismatch(proven))
	req.Mutations[1].Op = pb.Op_PUT
	req.Mutations[1].ExpectedCommitTs = 0
	unknown := classifyForwardRPCError(err, []*pb.Request{req})
	require.False(t, IsCertifiedCASMismatch(unknown))
	require.ErrorIs(t, unknown, ErrTransactionOutcomeUnknown)
}
