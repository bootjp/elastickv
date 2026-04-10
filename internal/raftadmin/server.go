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

	pb.UnimplementedRaftAdminServer
}

func NewServer(engine raftengine.Engine) *Server {
	admin, _ := any(engine).(raftengine.Admin)
	return &Server{
		engine: engine,
		admin:  admin,
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
	index, err := s.admin.AddVoter(ctx, req.Id, req.Address, req.PreviousIndex)
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
	if req != nil && (req.TargetId != "" || req.TargetAddress != "") {
		if req.TargetId == "" || req.TargetAddress == "" {
			return nil, grpcStatus(codes.InvalidArgument, "target_id and target_address must be provided together")
		}
		if err := s.admin.TransferLeadershipToServer(ctx, req.TargetId, req.TargetAddress); err != nil {
			return nil, adminError(err)
		}
		return &pb.RaftAdminTransferLeadershipResponse{}, nil
	}
	if err := s.admin.TransferLeadership(ctx); err != nil {
		return nil, adminError(err)
	}
	return &pb.RaftAdminTransferLeadershipResponse{}, nil
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
