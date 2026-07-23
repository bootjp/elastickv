package raftadmin

import (
	"context"
	stderrors "errors"
	"sync"

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
	// membershipMu serializes the pre-add interceptor and the configuration
	// proposal through completion. This closes the window where a concurrent
	// membership RPC could mutate encryption metadata before the engine exposes
	// the first request's pending configuration change in Status.
	membershipMu sync.Mutex

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
		State:              stateToProto(current.State),
		LeaderId:           current.Leader.ID,
		LeaderAddress:      current.Leader.Address,
		Term:               current.Term,
		CommitIndex:        current.CommitIndex,
		AppliedIndex:       current.AppliedIndex,
		LastLogIndex:       current.LastLogIndex,
		LastSnapshotIndex:  current.LastSnapshotIndex,
		FsmPending:         current.FSMPending,
		NumPeers:           current.NumPeers,
		LastContactNanos:   current.LastContact.Nanoseconds(),
		ConfigurationIndex: current.ConfigurationIndex,
		PendingConfChange:  current.PendingConfChange,
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
	return s.addMember(ctx, req.Id, req.Address, req.PreviousIndex, s.admin.AddVoter)
}

func (s *Server) AddLearner(ctx context.Context, req *pb.RaftAdminAddLearnerRequest) (*pb.RaftAdminConfigurationChangeResponse, error) {
	if s == nil || s.admin == nil {
		return nil, grpcStatus(codes.Unimplemented, "add learner is not supported by this raft engine")
	}
	if req == nil || req.Id == "" || req.Address == "" {
		return nil, grpcStatus(codes.InvalidArgument, "id and address are required")
	}
	return s.addMember(ctx, req.Id, req.Address, req.PreviousIndex, s.admin.AddLearner)
}

func (s *Server) addMember(
	ctx context.Context,
	id string,
	address string,
	previousIndex uint64,
	propose func(context.Context, string, string, uint64) (uint64, error),
) (*pb.RaftAdminConfigurationChangeResponse, error) {
	s.membershipMu.Lock()
	defer s.membershipMu.Unlock()
	if err := s.requireMembershipChangeReady(); err != nil {
		return nil, err
	}
	// Stage 7c pre-registers encryption metadata before proposing membership.
	// Keep that pre-step and the proposal inside the same serialized region.
	if s.interceptor != nil {
		if err := s.interceptor.PreAddMember(ctx, id); err != nil {
			return nil, adminError(err)
		}
	}
	index, err := propose(ctx, id, address, previousIndex)
	if err != nil {
		return nil, adminError(err)
	}
	return &pb.RaftAdminConfigurationChangeResponse{Index: index}, nil
}

func (s *Server) requireMembershipChangeReady() error {
	if s.engine.Status().PendingConfChange {
		return adminError(raftengine.ErrMembershipChangePending)
	}
	return nil
}

func (s *Server) PromoteLearner(ctx context.Context, req *pb.RaftAdminPromoteLearnerRequest) (*pb.RaftAdminConfigurationChangeResponse, error) {
	if s == nil || s.admin == nil {
		return nil, grpcStatus(codes.Unimplemented, "promote learner is not supported by this raft engine")
	}
	if req == nil || req.Id == "" {
		return nil, grpcStatus(codes.InvalidArgument, "id is required")
	}
	s.membershipMu.Lock()
	defer s.membershipMu.Unlock()
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
	s.membershipMu.Lock()
	defer s.membershipMu.Unlock()
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
	switch {
	case req != nil && req.Gated:
		return s.transferLeadershipGated(ctx, req)
	case hasUngatedTransferTarget(req):
		return s.transferLeadershipToServer(ctx, req)
	default:
		return s.transferLeadershipDefault(ctx)
	}
}

func hasUngatedTransferTarget(req *pb.RaftAdminTransferLeadershipRequest) bool {
	return req != nil && (req.TargetId != "" || req.TargetAddress != "")
}

func (s *Server) transferLeadershipToServer(ctx context.Context, req *pb.RaftAdminTransferLeadershipRequest) (*pb.RaftAdminTransferLeadershipResponse, error) {
	if req.TargetId == "" || req.TargetAddress == "" {
		return nil, grpcStatus(codes.InvalidArgument, "target_id and target_address must be provided together")
	}
	if err := s.admin.TransferLeadershipToServer(ctx, req.TargetId, req.TargetAddress); err != nil {
		return nil, adminError(err)
	}
	return &pb.RaftAdminTransferLeadershipResponse{}, nil
}

func (s *Server) transferLeadershipDefault(ctx context.Context) (*pb.RaftAdminTransferLeadershipResponse, error) {
	if err := s.admin.TransferLeadership(ctx); err != nil {
		return nil, adminError(err)
	}
	return &pb.RaftAdminTransferLeadershipResponse{}, nil
}

func (s *Server) transferLeadershipGated(ctx context.Context, req *pb.RaftAdminTransferLeadershipRequest) (*pb.RaftAdminTransferLeadershipResponse, error) {
	candidates, err := gatedTransferTargets(req)
	if err != nil {
		return nil, err
	}
	if err := s.admin.TransferLeadershipToServerIfEligible(ctx, candidates, req.MaxLag); err != nil {
		return nil, adminError(err)
	}
	return &pb.RaftAdminTransferLeadershipResponse{}, nil
}

func gatedTransferTargets(req *pb.RaftAdminTransferLeadershipRequest) ([]raftengine.TransferTarget, error) {
	candidates, err := repeatedTransferTargets(req.TargetCandidates)
	if err != nil {
		return nil, err
	}
	if len(candidates) > 0 {
		return candidates, nil
	}
	if req.TargetId == "" || req.TargetAddress == "" {
		return nil, grpcStatus(codes.InvalidArgument, "gated leadership transfer requires target_candidates or target_id and target_address")
	}
	return []raftengine.TransferTarget{{
		ID:      req.TargetId,
		Address: req.TargetAddress,
	}}, nil
}

func repeatedTransferTargets(targets []*pb.TransferTarget) ([]raftengine.TransferTarget, error) {
	candidates := make([]raftengine.TransferTarget, 0, len(targets))
	for _, candidate := range targets {
		if candidate == nil || candidate.TargetId == "" || candidate.TargetAddress == "" {
			return nil, grpcStatus(codes.InvalidArgument, "target_candidates require target_id and target_address")
		}
		candidates = append(candidates, raftengine.TransferTarget{
			ID:      candidate.TargetId,
			Address: candidate.TargetAddress,
		})
	}
	return candidates, nil
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
