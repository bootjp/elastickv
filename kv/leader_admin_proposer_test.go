package kv

import (
	"context"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/raftengine/raftenginetest"
	"github.com/bootjp/elastickv/internal/testutil/grpctest"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func adminProposalLeaderView(state raftengine.State, addr string) *raftenginetest.StaticLeaderView {
	return &raftenginetest.StaticLeaderView{
		StateValue:  state,
		LeaderValue: raftengine.LeaderInfo{ID: "leader", Address: addr},
	}
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

	mu            sync.Mutex
	payload       []byte
	authorization []string
}

func (s *forwardingAdminServer) ForwardAdminProposal(
	ctx context.Context,
	req *pb.ForwardAdminProposalRequest,
) (*pb.ForwardAdminProposalResponse, error) {
	s.mu.Lock()
	s.payload = append([]byte(nil), req.GetPayload()...)
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.authorization = append([]string(nil), md.Get("authorization")...)
	}
	s.mu.Unlock()
	return &pb.ForwardAdminProposalResponse{CommitIndex: 123}, nil
}

func TestLeaderAdminProposerUsesLocalVerifiedLeader(t *testing.T) {
	t.Parallel()
	local := &recordingAdminProposer{}
	proposer := NewLeaderAdminProposer(
		adminProposalLeaderView(raftengine.StateLeader, ""),
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
	service := &forwardingAdminServer{}
	addr := grpctest.Serve(t, func(server *grpc.Server) { pb.RegisterInternalServer(server, service) })

	cache := &GRPCConnCache{}
	t.Cleanup(func() { require.NoError(t, cache.Close()) })
	local := &recordingAdminProposer{}
	proposer := NewLeaderAdminProposer(
		adminProposalLeaderView(raftengine.StateFollower, addr),
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

func TestLeaderAdminProposerForwardsBearerToken(t *testing.T) {
	t.Parallel()
	service := &forwardingAdminServer{}
	addr := grpctest.Serve(t, func(server *grpc.Server) { pb.RegisterInternalServer(server, service) })

	cache := &GRPCConnCache{}
	t.Cleanup(func() { require.NoError(t, cache.Close()) })
	proposer := NewLeaderAdminProposer(
		adminProposalLeaderView(raftengine.StateFollower, addr),
		&recordingAdminProposer{},
		cache,
		WithLeaderAdminToken("s3cret"),
	)
	_, err := proposer.ProposeAdmin(context.Background(), []byte("pin"))
	require.NoError(t, err)
	service.mu.Lock()
	require.Equal(t, []string{"Bearer s3cret"}, service.authorization)
	service.mu.Unlock()
}
