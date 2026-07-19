package kv

import (
	"context"
	"net"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type adminProposalLeaderView struct {
	state raftengine.State
	addr  string
}

func (v *adminProposalLeaderView) State() raftengine.State { return v.state }
func (v *adminProposalLeaderView) Leader() raftengine.LeaderInfo {
	return raftengine.LeaderInfo{ID: "leader", Address: v.addr}
}
func (v *adminProposalLeaderView) VerifyLeader(context.Context) error { return nil }
func (v *adminProposalLeaderView) LinearizableRead(context.Context) (uint64, error) {
	return 0, nil
}

type recordingAdminProposer struct {
	mu         sync.Mutex
	adminCalls int
	payload    []byte
}

func (p *recordingAdminProposer) Propose(context.Context, []byte) (*raftengine.ProposalResult, error) {
	return &raftengine.ProposalResult{CommitIndex: 1}, nil
}

func (p *recordingAdminProposer) ProposeAdmin(_ context.Context, data []byte) (*raftengine.ProposalResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.adminCalls++
	p.payload = append([]byte(nil), data...)
	return &raftengine.ProposalResult{CommitIndex: 7}, nil
}

type forwardingAdminServer struct {
	pb.UnimplementedInternalServer

	mu      sync.Mutex
	payload []byte
}

func (s *forwardingAdminServer) ForwardAdminProposal(
	_ context.Context,
	req *pb.ForwardAdminProposalRequest,
) (*pb.ForwardAdminProposalResponse, error) {
	s.mu.Lock()
	s.payload = append([]byte(nil), req.GetPayload()...)
	s.mu.Unlock()
	return &pb.ForwardAdminProposalResponse{CommitIndex: 123}, nil
}

func TestLeaderAdminProposerUsesLocalVerifiedLeader(t *testing.T) {
	t.Parallel()
	local := &recordingAdminProposer{}
	proposer := NewLeaderAdminProposer(
		&adminProposalLeaderView{state: raftengine.StateLeader},
		local,
		&GRPCConnCache{},
	)

	result, err := proposer.ProposeAdmin(context.Background(), []byte("pin"))
	require.NoError(t, err)
	require.Equal(t, uint64(7), result.CommitIndex)
	require.Equal(t, 1, local.adminCalls)
	require.Equal(t, []byte("pin"), local.payload)
}

func TestLeaderAdminProposerForwardsFollowerProposal(t *testing.T) {
	t.Parallel()
	var lc net.ListenConfig
	lis, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	service := &forwardingAdminServer{}
	server := grpc.NewServer()
	pb.RegisterInternalServer(server, service)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(func() {
		server.Stop()
		_ = lis.Close()
	})

	cache := &GRPCConnCache{}
	t.Cleanup(func() { require.NoError(t, cache.Close()) })
	local := &recordingAdminProposer{}
	proposer := NewLeaderAdminProposer(
		&adminProposalLeaderView{state: raftengine.StateFollower, addr: lis.Addr().String()},
		local,
		cache,
	)
	result, err := proposer.ProposeAdmin(context.Background(), []byte("pin"))
	require.NoError(t, err)
	require.Equal(t, uint64(123), result.CommitIndex)
	require.Zero(t, local.adminCalls)
	service.mu.Lock()
	require.Equal(t, []byte("pin"), service.payload)
	service.mu.Unlock()
}
