package raftadmin

import (
	"context"
	stderrors "errors"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	engine raftengine.Engine
	admin  raftengine.Admin
	// interceptor is invoked from AddVoter/AddLearner before the
	// underlying Raft engine proposes the conf-change. nil = no
	// pre-step (the pre-7c posture). Stage 7c wires the encryption-
	// aware adapter via NewServerWithInterceptor; encryption-unaware
	// builds keep this nil.
	interceptor MembershipChangeInterceptor

	pb.UnimplementedRaftAdminServer
}

func NewServer(engine raftengine.Engine) *Server {
	return NewServerWithInterceptor(engine, nil)
}

// NewServerWithInterceptor constructs a Server with an optional
// pre-step that runs before AddVoter/AddLearner propose the
// conf-change. See [MembershipChangeInterceptor]. Passing nil is
// equivalent to [NewServer] — the pre-step is skipped and the
// conf-change runs exactly as the pre-7c posture.
func NewServerWithInterceptor(engine raftengine.Engine, interceptor MembershipChangeInterceptor) *Server {
	admin, _ := any(engine).(raftengine.Admin)
	return &Server{
		engine:      engine,
		admin:       admin,
		interceptor: interceptor,
	}
}

func (s *Server) Status(context.Context, *pb.RaftAdminStatusRequest) (*pb.RaftAdminStatusResponse, error) {
	if s == nil || s.engine == nil {
		return nil, grpcStatus(codes.FailedPrecondition, "raft engine is not configured")
	}
	current := s.engine.Status()
	return &pb.RaftAdminStatusResponse{
		State:             stateToProto(current.State),
		LeaderId:          current.Leader.ID,
		LeaderAddress:     current.Leader.Address,
		Term:              current.Term,
		CommitIndex:       current.CommitIndex,
		AppliedIndex:      current.AppliedIndex,
		LastLogIndex:      current.LastLogIndex,
		LastSnapshotIndex: current.LastSnapshotIndex,
		FsmPending:        current.FSMPending,
		NumPeers:          current.NumPeers,
		LastContactNanos:  current.LastContact.Nanoseconds(),
	}, nil
}

func (s *Server) Configuration(ctx context.Context, _ *pb.RaftAdminConfigurationRequest) (*pb.RaftAdminConfigurationResponse, error) {
	if s == nil || s.engine == nil {
		return nil, grpcStatus(codes.FailedPrecondition, "raft engine is not configured")
	}
	cfg, err := s.engine.Configuration(ctx)
	if err != nil {
		return nil, adminError(err)
	}
	resp := &pb.RaftAdminConfigurationResponse{
		Servers: make([]*pb.RaftAdminMember, 0, len(cfg.Servers)),
	}
	for _, server := range cfg.Servers {
		resp.Servers = append(resp.Servers, &pb.RaftAdminMember{
			Id:       server.ID,
			Address:  server.Address,
			Suffrage: server.Suffrage,
		})
	}
	return resp, nil
}

func (s *Server) AddVoter(ctx context.Context, req *pb.RaftAdminAddVoterRequest) (*pb.RaftAdminConfigurationChangeResponse, error) {
	if s == nil || s.admin == nil {
		return nil, grpcStatus(codes.Unimplemented, "add voter is not supported by this raft engine")
	}
	if req == nil || req.Id == "" || req.Address == "" {
		return nil, grpcStatus(codes.InvalidArgument, "id and address are required")
	}
	// Stage 7c §3.1 pre-step: run the encryption-aware adapter (if any)
	// before the conf-change proposal so the new node's writer-registry
	// row exists at apply time and any §6.1 uint16 collision halts here
	// rather than after the conf-change is durable.
	if s.interceptor != nil {
		if err := s.interceptor.PreAddMember(ctx, req.Id); err != nil {
			return nil, adminError(err)
		}
	}
	index, err := s.admin.AddVoter(ctx, req.Id, req.Address, req.PreviousIndex)
	if err != nil {
		return nil, adminError(err)
	}
	return &pb.RaftAdminConfigurationChangeResponse{Index: index}, nil
}

func (s *Server) AddLearner(ctx context.Context, req *pb.RaftAdminAddLearnerRequest) (*pb.RaftAdminConfigurationChangeResponse, error) {
	if s == nil || s.admin == nil {
		return nil, grpcStatus(codes.Unimplemented, "add learner is not supported by this raft engine")
	}
	if req == nil || req.Id == "" || req.Address == "" {
		return nil, grpcStatus(codes.InvalidArgument, "id and address are required")
	}
	if s.interceptor != nil {
		if err := s.interceptor.PreAddMember(ctx, req.Id); err != nil {
			return nil, adminError(err)
		}
	}
	index, err := s.admin.AddLearner(ctx, req.Id, req.Address, req.PreviousIndex)
	if err != nil {
		return nil, adminError(err)
	}
	return &pb.RaftAdminConfigurationChangeResponse{Index: index}, nil
}

func (s *Server) PromoteLearner(ctx context.Context, req *pb.RaftAdminPromoteLearnerRequest) (*pb.RaftAdminConfigurationChangeResponse, error) {
	if s == nil || s.admin == nil {
		return nil, grpcStatus(codes.Unimplemented, "promote learner is not supported by this raft engine")
	}
	if req == nil || req.Id == "" {
		return nil, grpcStatus(codes.InvalidArgument, "id is required")
	}
	index, err := s.admin.PromoteLearner(ctx, req.Id, req.PreviousIndex, req.MinAppliedIndex, req.SkipMinAppliedCheck)
	if err != nil {
		return nil, adminError(err)
	}
	return &pb.RaftAdminConfigurationChangeResponse{Index: index}, nil
}

func (s *Server) RemoveServer(ctx context.Context, req *pb.RaftAdminRemoveServerRequest) (*pb.RaftAdminConfigurationChangeResponse, error) {
	if s == nil || s.admin == nil {
		return nil, grpcStatus(codes.Unimplemented, "remove server is not supported by this raft engine")
	}
	if req == nil || req.Id == "" {
		return nil, grpcStatus(codes.InvalidArgument, "id is required")
	}
	index, err := s.admin.RemoveServer(ctx, req.Id, req.PreviousIndex)
	if err != nil {
		return nil, adminError(err)
	}
	return &pb.RaftAdminConfigurationChangeResponse{Index: index}, nil
}

func (s *Server) TransferLeadership(ctx context.Context, req *pb.RaftAdminTransferLeadershipRequest) (*pb.RaftAdminTransferLeadershipResponse, error) {
	if s == nil || s.admin == nil {
		return nil, grpcStatus(codes.Unimplemented, "leadership transfer is not supported by this raft engine")
	}
	if req != nil && usesUnsupportedGatedTransfer(req) {
		return nil, grpcStatus(codes.FailedPrecondition, "gated leadership transfer is not supported by this raft engine")
	}
	targetID, targetAddress, hasTarget, err := transferLeadershipTarget(req)
	if err != nil {
		return nil, err
	}
	if hasTarget {
		if err := s.admin.TransferLeadershipToServer(ctx, targetID, targetAddress); err != nil {
			return nil, adminError(err)
		}
		return &pb.RaftAdminTransferLeadershipResponse{}, nil
	}
	if err := s.admin.TransferLeadership(ctx); err != nil {
		return nil, adminError(err)
	}
	return &pb.RaftAdminTransferLeadershipResponse{}, nil
}

func transferLeadershipTarget(req *pb.RaftAdminTransferLeadershipRequest) (string, string, bool, error) {
	if req == nil || (req.GetTargetId() == "" && req.GetTargetAddress() == "") {
		return "", "", false, nil
	}
	if req.GetTargetId() == "" || req.GetTargetAddress() == "" {
		return "", "", false, grpcStatus(codes.InvalidArgument, "target_id and target_address must be provided together")
	}
	return req.GetTargetId(), req.GetTargetAddress(), true, nil
}

func usesUnsupportedGatedTransfer(req *pb.RaftAdminTransferLeadershipRequest) bool {
	return req.GetGated() || req.GetMaxLag() != 0 || len(req.GetTargetCandidates()) != 0
}

func stateToProto(state raftengine.State) pb.RaftAdminState {
	switch state {
	case raftengine.StateFollower:
		return pb.RaftAdminState_RAFT_ADMIN_STATE_FOLLOWER
	case raftengine.StateCandidate:
		return pb.RaftAdminState_RAFT_ADMIN_STATE_CANDIDATE
	case raftengine.StateLeader:
		return pb.RaftAdminState_RAFT_ADMIN_STATE_LEADER
	case raftengine.StateShutdown:
		return pb.RaftAdminState_RAFT_ADMIN_STATE_SHUTDOWN
	case raftengine.StateUnknown:
		return pb.RaftAdminState_RAFT_ADMIN_STATE_UNKNOWN
	default:
		return pb.RaftAdminState_RAFT_ADMIN_STATE_UNKNOWN
	}
}

func adminError(err error) error {
	switch {
	case err == nil:
		return nil
	case stderrors.Is(err, context.Canceled):
		return grpcStatus(codes.Canceled, err.Error())
	case stderrors.Is(err, context.DeadlineExceeded):
		return grpcStatus(codes.DeadlineExceeded, err.Error())
	default:
		return grpcStatus(codes.FailedPrecondition, err.Error())
	}
}

//nolint:wrapcheck // gRPC status errors must stay raw so codes survive transport.
func grpcStatus(code codes.Code, msg string) error {
	return status.Error(code, msg)
}
