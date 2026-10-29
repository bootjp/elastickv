package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newReadWatermarkInternal(t *testing.T, proposer raftengine.Proposer, opts ...InternalOption) (*Internal, *kv.HLC) {
	t.Helper()
	clock := kv.NewHLC()
	return NewInternalWithEngine(
		nil,
		internalAdminLeaderView(raftengine.StateLeader, 0),
		clock,
		nil,
		append([]InternalOption{WithInternalReadWatermarkProposer(proposer)}, opts...)...,
	), clock
}

// A forwarded advance goes through the read-watermark proposer even when no
// admin proposer is wired (the admin server is optional).
func TestInternalForwardReadWatermarkAdvanceProposes(t *testing.T) {
	t.Parallel()
	proposer := &internalAdminProposer{}
	internal, clock := newReadWatermarkInternal(t, proposer)
	payload := kv.EncodeWatermarkAdvance(clock.Next())

	resp, err := internal.ForwardAdminProposal(context.Background(), &pb.ForwardAdminProposalRequest{Payload: payload})
	require.NoError(t, err)
	require.Equal(t, uint64(17), resp.GetCommitIndex())
	require.Equal(t, payload, proposer.payload)
}

func TestInternalForwardReadWatermarkAdvanceRefusals(t *testing.T) {
	t.Parallel()
	const futureMs = uint64(1) << 47
	tests := []struct {
		name    string
		payload []byte
		code    codes.Code
	}{
		{name: "zero", payload: kv.EncodeWatermarkAdvance(0), code: codes.PermissionDenied},
		{name: "read-latest sentinel", payload: kv.EncodeWatermarkAdvance(^uint64(0)), code: codes.PermissionDenied},
		{name: "far future", payload: kv.EncodeWatermarkAdvance(futureMs << 16), code: codes.FailedPrecondition},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			proposer := &internalAdminProposer{}
			internal, _ := newReadWatermarkInternal(t, proposer)
			_, err := internal.ForwardAdminProposal(context.Background(), &pb.ForwardAdminProposalRequest{Payload: tc.payload})
			require.Equal(t, tc.code, status.Code(err))
			require.Nil(t, proposer.payload, "a refused advance must not be proposed")
		})
	}
}

// An advance the group already covers is answered without a proposal.
func TestInternalForwardReadWatermarkAdvanceCoveredIsNoop(t *testing.T) {
	t.Parallel()
	st := store.NewMVCCStore()
	require.NoError(t, st.ApplyMutationsRaftAt(context.Background(), nil, nil, 50, 50, 1))
	proposer := &internalAdminProposer{}
	internal, _ := newReadWatermarkInternal(t, proposer, WithInternalStore(st))

	_, err := internal.ForwardAdminProposal(context.Background(), &pb.ForwardAdminProposalRequest{Payload: kv.EncodeWatermarkAdvance(50)})
	require.NoError(t, err)
	require.Nil(t, proposer.payload)
}

// Without the read-watermark proposer (the advance is opt-in until every
// replica understands it) a forwarded advance is refused, and never falls
// back to the admin proposer.
func TestInternalForwardReadWatermarkAdvanceDisabled(t *testing.T) {
	t.Parallel()
	admin := &internalAdminProposer{}
	clock := kv.NewHLC()
	internal := NewInternalWithEngine(nil, internalAdminLeaderView(raftengine.StateLeader, 0), clock, nil,
		WithInternalAdminProposer(admin))
	_, err := internal.ForwardAdminProposal(context.Background(), &pb.ForwardAdminProposalRequest{Payload: kv.EncodeWatermarkAdvance(clock.Next())})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Nil(t, admin.payload)
}
