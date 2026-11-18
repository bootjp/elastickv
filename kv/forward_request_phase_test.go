package kv

import (
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
)

func TestForwardFailurePhaseForRequests(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		phase pb.Phase
		want  pb.ForwardFailurePhase
	}{
		{phase: pb.Phase_NONE, want: pb.ForwardFailurePhase_FORWARD_FAILURE_PHASE_ONE_PHASE_APPLY},
		{phase: pb.Phase_PREPARE, want: pb.ForwardFailurePhase_FORWARD_FAILURE_PHASE_PREPARE},
		{phase: pb.Phase_COMMIT, want: pb.ForwardFailurePhase_FORWARD_FAILURE_PHASE_COMMIT},
		{phase: pb.Phase_ABORT, want: pb.ForwardFailurePhase_FORWARD_FAILURE_PHASE_ABORT},
	} {
		got, ok := ForwardFailurePhaseForRequests([]*pb.Request{{IsTxn: true, Phase: tc.phase}})
		require.True(t, ok)
		require.Equal(t, tc.want, got)
	}
	for _, reqs := range [][]*pb.Request{
		nil,
		{nil},
		{{IsTxn: false, Phase: pb.Phase_PREPARE}},
		{{IsTxn: true, Phase: pb.Phase(99)}},
		{{IsTxn: true, Phase: pb.Phase_NONE}, {IsTxn: true, Phase: pb.Phase_NONE}},
	} {
		_, ok := ForwardFailurePhaseForRequests(reqs)
		require.False(t, ok)
	}
}

func TestForwardOnePhaseDecisionEligible(t *testing.T) {
	t.Parallel()
	valid := onePhaseForwardTxnRequests(0)[0]
	for _, tc := range []struct {
		name string
		reqs []*pb.Request
		want bool
	}{
		{name: "first transaction attempt", reqs: onePhaseForwardTxnRequests(0), want: true},
		{name: "prior transaction attempt", reqs: onePhaseForwardTxnRequests(17)},
		{name: "single raw request", reqs: []*pb.Request{{Phase: pb.Phase_NONE}}, want: true},
		{name: "missing metadata", reqs: []*pb.Request{{IsTxn: true, Phase: pb.Phase_NONE}}},
		{name: "malformed metadata", reqs: []*pb.Request{{IsTxn: true, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte(TxnMetaPrefix), Value: []byte("bad")}}}}},
		{name: "wrong metadata operation", reqs: []*pb.Request{{IsTxn: true, Mutations: []*pb.Mutation{{Op: pb.Op_DEL, Key: []byte(TxnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("k")})}}}}},
		{name: "duplicate metadata", reqs: []*pb.Request{{IsTxn: true, Mutations: append(append([]*pb.Mutation{}, valid.Mutations...), valid.Mutations[0])}}},
		{name: "mixed batch", reqs: []*pb.Request{valid, valid}},
		{name: "prepare", reqs: []*pb.Request{{IsTxn: true, Phase: pb.Phase_PREPARE, Mutations: valid.Mutations}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, ForwardOnePhaseDecisionEligible(tc.reqs))
		})
	}
}
