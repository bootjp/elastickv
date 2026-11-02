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
