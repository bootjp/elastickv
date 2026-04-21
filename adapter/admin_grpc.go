package adapter

import (
	"context"
	"crypto/subtle"
	"strings"
	"sync"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// AdminGroup exposes per-Raft-group state to the Admin service. It is a narrow
// subset of raftengine.Engine so tests can supply an in-memory fake without
// standing up a real Raft cluster.
type AdminGroup interface {
	Status() raftengine.Status
}

// NodeIdentity is the value form of the protobuf NodeIdentity message used for
// AdminServer configuration. It avoids copying pb.NodeIdentity, which embeds a
// protoimpl.MessageState (and a mutex).
type NodeIdentity struct {
	NodeID      string
	GRPCAddress string
}

func (n NodeIdentity) toProto() *pb.NodeIdentity {
	return &pb.NodeIdentity{NodeId: n.NodeID, GrpcAddress: n.GRPCAddress}
}

// AdminServer implements the node-side Admin gRPC service described in
// docs/admin_ui_key_visualizer_design.md §4 (Layer A). Phase 0 only implements
// GetClusterOverview and GetRaftGroups; remaining RPCs return Unimplemented so
// the generated client can still compile against older nodes during rollout.
type AdminServer struct {
	self    NodeIdentity
	members []NodeIdentity

	groupsMu sync.RWMutex
	groups   map[uint64]AdminGroup

	pb.UnimplementedAdminServer
}

// NewAdminServer constructs an AdminServer. `self` identifies the local node
// for responses that return node identity. `members` is the static membership
// snapshot shipped to the admin binary; callers that already have a membership
// source may pass nil and let the admin binary's fan-out layer discover peers
// by other means.
func NewAdminServer(self NodeIdentity, members []NodeIdentity) *AdminServer {
	cloned := append([]NodeIdentity(nil), members...)
	return &AdminServer{
		self:    self,
		members: cloned,
		groups:  make(map[uint64]AdminGroup),
	}
}

// RegisterGroup binds a Raft group ID to its engine so the Admin service can
// report leader and log state for that group.
func (s *AdminServer) RegisterGroup(groupID uint64, g AdminGroup) {
	if g == nil {
		return
	}
	s.groupsMu.Lock()
	s.groups[groupID] = g
	s.groupsMu.Unlock()
}

// GetClusterOverview returns the local node identity, the configured member
// list, and per-group leader identity collected from the engines registered
// via RegisterGroup.
func (s *AdminServer) GetClusterOverview(
	_ context.Context,
	_ *pb.GetClusterOverviewRequest,
) (*pb.GetClusterOverviewResponse, error) {
	leaders := s.snapshotLeaders()
	members := make([]*pb.NodeIdentity, 0, len(s.members))
	for _, m := range s.members {
		members = append(members, m.toProto())
	}
	return &pb.GetClusterOverviewResponse{
		Self:         s.self.toProto(),
		Members:      members,
		GroupLeaders: leaders,
	}, nil
}

// GetRaftGroups returns per-group state snapshots. Phase 0 wires commit/applied
// indices only; per-follower contact and term history land in later phases.
func (s *AdminServer) GetRaftGroups(
	_ context.Context,
	_ *pb.GetRaftGroupsRequest,
) (*pb.GetRaftGroupsResponse, error) {
	s.groupsMu.RLock()
	out := make([]*pb.RaftGroupState, 0, len(s.groups))
	for id, g := range s.groups {
		st := g.Status()
		out = append(out, &pb.RaftGroupState{
			RaftGroupId:  id,
			LeaderNodeId: st.Leader.ID,
			LeaderTerm:   st.Term,
			CommitIndex:  st.CommitIndex,
			AppliedIndex: st.AppliedIndex,
		})
	}
	s.groupsMu.RUnlock()
	return &pb.GetRaftGroupsResponse{Groups: out}, nil
}

func (s *AdminServer) snapshotLeaders() []*pb.GroupLeader {
	s.groupsMu.RLock()
	defer s.groupsMu.RUnlock()
	out := make([]*pb.GroupLeader, 0, len(s.groups))
	for id, g := range s.groups {
		st := g.Status()
		out = append(out, &pb.GroupLeader{
			RaftGroupId:  id,
			LeaderNodeId: st.Leader.ID,
			LeaderTerm:   st.Term,
		})
	}
	return out
}

// AdminTokenAuth builds a gRPC unary+stream interceptor pair enforcing
// "authorization: Bearer <token>" metadata against the supplied token. An
// empty token disables enforcement; callers should pair that mode with a
// --adminInsecureNoAuth flag so operators knowingly opt in.
func AdminTokenAuth(token string) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	if token == "" {
		return nil, nil
	}
	expected := []byte(token)
	check := func(ctx context.Context) error {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return status.Error(codes.Unauthenticated, "missing authorization metadata")
		}
		values := md.Get("authorization")
		if len(values) == 0 {
			return status.Error(codes.Unauthenticated, "missing authorization header")
		}
		got, ok := strings.CutPrefix(values[0], "Bearer ")
		if !ok {
			return status.Error(codes.Unauthenticated, "authorization is not a bearer token")
		}
		if subtle.ConstantTimeCompare([]byte(got), expected) != 1 {
			return status.Error(codes.Unauthenticated, "invalid admin token")
		}
		return nil
	}
	unary := func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		if !strings.HasPrefix(info.FullMethod, "/Admin/") {
			return handler(ctx, req)
		}
		if err := check(ctx); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
	stream := func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		if !strings.HasPrefix(info.FullMethod, "/Admin/") {
			return handler(srv, ss)
		}
		if err := check(ss.Context()); err != nil {
			return err
		}
		return handler(srv, ss)
	}
	return unary, stream
}

// ErrAdminTokenRequired is returned by NewAdminServer helpers when the operator
// failed to supply a token and also did not opt into insecure mode.
var ErrAdminTokenRequired = errors.New("admin token file required; pass --adminInsecureNoAuth to run without")
