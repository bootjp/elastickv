package adapter

import (
	"context"
	stderrors "errors"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type internalAdminLeaderView struct {
	state     raftengine.State
	readIndex uint64
}

func (v internalAdminLeaderView) State() raftengine.State { return v.state }
func (internalAdminLeaderView) Leader() raftengine.LeaderInfo {
	return raftengine.LeaderInfo{ID: "leader", Address: "leader:50051"}
}
func (internalAdminLeaderView) VerifyLeader(context.Context) error { return nil }
func (v internalAdminLeaderView) LinearizableRead(context.Context) (uint64, error) {
	return v.readIndex, nil
}

func TestInternalForwardLeaseReadUsesLeaderBarrier(t *testing.T) {
	t.Parallel()
	internal := NewInternalWithEngine(
		nil,
		internalAdminLeaderView{state: raftengine.StateLeader, readIndex: 23},
		nil,
		nil,
		WithInternalLastCommitTimestamp(func() uint64 { return 37 }),
	)
	resp, err := internal.ForwardLeaseRead(context.Background(), &pb.ForwardLeaseReadRequest{})
	require.NoError(t, err)
	require.Equal(t, uint64(23), resp.GetAppliedIndex())
	require.Equal(t, uint64(37), resp.GetLastCommitTs())

	internal.leader = internalAdminLeaderView{state: raftengine.StateFollower}
	_, err = internal.ForwardLeaseRead(context.Background(), &pb.ForwardLeaseReadRequest{})
	require.ErrorIs(t, err, ErrNotLeader)
}

type internalAdminProposer struct {
	payload  []byte
	response any
}

func (p *internalAdminProposer) Propose(context.Context, []byte) (*raftengine.ProposalResult, error) {
	return nil, stderrors.New("unexpected user proposal")
}

func (p *internalAdminProposer) ProposeAdmin(
	_ context.Context,
	payload []byte,
) (*raftengine.ProposalResult, error) {
	p.payload = append([]byte(nil), payload...)
	return &raftengine.ProposalResult{CommitIndex: 17, Response: p.response}, nil
}

func TestInternalForwardAdminProposalUsesLeaderProposer(t *testing.T) {
	t.Parallel()
	proposer := &internalAdminProposer{}
	internal := NewInternalWithEngine(
		nil,
		internalAdminLeaderView{state: raftengine.StateLeader},
		nil,
		nil,
		WithInternalAdminProposer(proposer),
	)

	resp, err := internal.ForwardAdminProposal(
		context.Background(),
		&pb.ForwardAdminProposalRequest{Payload: []byte("pin")},
	)
	require.NoError(t, err)
	require.Equal(t, uint64(17), resp.GetCommitIndex())
	require.Equal(t, []byte("pin"), proposer.payload)
}

func TestInternalForwardAdminProposalFailsClosed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		state     raftengine.State
		proposer  raftengine.Proposer
		errTarget error
		errCode   codes.Code
		errText   string
	}{
		{name: "follower", state: raftengine.StateFollower, errTarget: ErrNotLeader},
		{
			name: "apply response", state: raftengine.StateLeader,
			proposer: &internalAdminProposer{response: stderrors.New("apply failed")},
			errText:  "apply failed",
		},
		{
			name: "capacity response", state: raftengine.StateLeader,
			proposer: &internalAdminProposer{response: kv.ErrTooManyActiveBackups},
			errCode:  codes.ResourceExhausted,
			errText:  kv.ErrTooManyActiveBackups.Error(),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			internal := NewInternalWithEngine(
				nil,
				internalAdminLeaderView{state: tc.state},
				nil,
				nil,
				WithInternalAdminProposer(tc.proposer),
			)
			_, err := internal.ForwardAdminProposal(context.Background(), &pb.ForwardAdminProposalRequest{})
			if tc.errTarget != nil {
				require.ErrorIs(t, err, tc.errTarget)
			}
			if tc.errCode != codes.OK {
				require.Equal(t, tc.errCode, status.Code(err))
			}
			if tc.errText != "" {
				require.ErrorContains(t, err, tc.errText)
			}
		})
	}
}
