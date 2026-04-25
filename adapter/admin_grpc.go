package adapter

import (
	"context"
	"crypto/subtle"
	"sort"
	"strings"
	"sync"
	"time"

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
// standing up a real Raft cluster. Configuration is polled on each
// GetClusterOverview to pick up scale-out / scale-in events without the
// operator having to restart the admin binary.
type AdminGroup interface {
	Status() raftengine.Status
	Configuration(ctx context.Context) (raftengine.Configuration, error)
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

	// now is the clock used for LastContactUnixMs and any other
	// timestamping this service needs. It's a per-server field (not a
	// package global) so `-race` tests that swap the clock on one server
	// instance cannot contend with concurrent RPCs on another instance.
	now func() time.Time

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
		now:     time.Now,
	}
}

// SetClock overrides the clock used by GetRaftGroups, letting tests inject a
// fixed time without mutating any package-global state. Concurrent RPCs on
// other AdminServer instances are unaffected.
func (s *AdminServer) SetClock(now func() time.Time) {
	if now == nil {
		now = time.Now
	}
	s.groupsMu.Lock()
	s.now = now
	s.groupsMu.Unlock()
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

// GetClusterOverview returns the local node identity, the current member
// list, and per-group leader identity collected from the engines registered
// via RegisterGroup. The member list is the union of (a) the bootstrap seed
// supplied to NewAdminServer and (b) the live Configuration of every
// registered Raft group — the latter picks up scale-out nodes added after
// startup so the admin binary's fan-out discovery does not miss them.
func (s *AdminServer) GetClusterOverview(
	ctx context.Context,
	_ *pb.GetClusterOverviewRequest,
) (*pb.GetClusterOverviewResponse, error) {
	leaders := s.snapshotLeaders()
	members := s.snapshotMembers(ctx)
	return &pb.GetClusterOverviewResponse{
		Self:         s.self.toProto(),
		Members:      members,
		GroupLeaders: leaders,
	}, nil
}

// snapshotMembers unions the seed members with the live Configuration of each
// registered group, preferring the live address when the same NodeID appears
// in both sources. A stale bootstrap entry cannot outvote a readdressed node:
// if n2 was moved from 10.0.0.12 to 10.0.0.22, the overview reports the
// current 10.0.0.22 so fan-out dials the right target. Configuration errors
// on a single group do not fail the RPC — other groups plus the seed list
// still produce useful output.
func (s *AdminServer) snapshotMembers(ctx context.Context) []*pb.NodeIdentity {
	groups := s.cloneGroupsSorted()
	addrByID, order := collectLiveMembers(ctx, groups, s.self.NodeID)
	mergeSeedMembers(s.members, s.self.NodeID, addrByID, &order)

	out := make([]*pb.NodeIdentity, 0, len(order))
	for _, id := range order {
		out = append(out, &pb.NodeIdentity{NodeId: id, GrpcAddress: addrByID[id]})
	}
	return out
}

// groupEntry pairs a Raft group ID with its AdminGroup so callers can iterate
// in a deterministic (ID-ascending) order. Sorting matters for
// collectLiveMembers: when two groups report the same NodeID with different
// addresses (e.g., mid-readdress), the iteration order picks which address
// wins, and a Go map's range order is unspecified.
type groupEntry struct {
	id    uint64
	group AdminGroup
}

// cloneGroupsSorted snapshots the registered groups under the read lock and
// returns them sorted by group ID so iteration and tie-break decisions are
// stable across calls.
func (s *AdminServer) cloneGroupsSorted() []groupEntry {
	s.groupsMu.RLock()
	defer s.groupsMu.RUnlock()
	out := make([]groupEntry, 0, len(s.groups))
	for id, g := range s.groups {
		out = append(out, groupEntry{id: id, group: g})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })
	return out
}

// collectLiveMembers polls Configuration for each group (in ascending group
// ID order supplied by the caller) and returns the union of server IDs
// (excluding self) with their live addresses. When two groups report the
// same server ID with different addresses — e.g. mid-readdress before every
// group has converged — the lowest-ID group wins, which is stable across
// calls and matches "trust the primary group" intuition.
//
// Entries with an empty `srv.Address` (the etcd engine can emit those when
// peer metadata is still missing) are skipped: storing a blank address would
// shadow a usable seed entry for the same NodeID and cause GetClusterOverview
// to drop the peer from fan-out altogether. Letting the seed list backfill
// keeps the peer reachable until the live Configuration converges.
//
// Per-group Configuration calls run concurrently because a sequential loop
// would stall the entire RPC behind any one slow group; results are written
// into a pre-allocated slice indexed by the sorted-order position so the
// merge step still walks groups in ascending-ID order and preserves the
// deterministic tie-break.
// configResult bundles a Configuration RPC outcome with its position in the
// caller-supplied groups slice so the merge step can re-sort by group-ID
// even when results land out of completion order.
type configResult struct {
	i   int
	cfg raftengine.Configuration
	err error
}

// fanoutConfigurationCalls launches a Configuration(ctx) goroutine per
// group and collects results. Returns whatever has landed by the time ctx
// fires; remaining goroutines drain into the (buffered) channel and exit
// asynchronously when their per-RPC ctx unwinds. The early-return is the
// reason this lives in its own function: reading a shared []configResult
// slice across the cancel boundary would race the still-running goroutines.
func fanoutConfigurationCalls(ctx context.Context, groups []groupEntry) []configResult {
	resultsCh := make(chan configResult, len(groups))
	for i, entry := range groups {
		go func(i int, entry groupEntry) {
			cfg, err := entry.group.Configuration(ctx)
			resultsCh <- configResult{i: i, cfg: cfg, err: err}
		}(i, entry)
	}
	got := make([]configResult, 0, len(groups))
	for range groups {
		select {
		case res := <-resultsCh:
			got = append(got, res)
		case <-ctx.Done():
			return got
		}
	}
	return got
}

func collectLiveMembers(
	ctx context.Context,
	groups []groupEntry,
	selfID string,
) (addrByID map[string]string, order []string) {
	got := fanoutConfigurationCalls(ctx, groups)

	// Merge in the original group-ID order so the lowest-ID-wins tie-break
	// stays deterministic. (Completion order would otherwise depend on
	// which Configuration() returned first.)
	sort.Slice(got, func(a, b int) bool { return got[a].i < got[b].i })

	addrByID = make(map[string]string)
	order = make([]string, 0)
	for _, res := range got {
		if res.err != nil {
			continue
		}
		for _, srv := range res.cfg.Servers {
			if srv.ID == "" || srv.ID == selfID || srv.Address == "" {
				continue
			}
			if _, dup := addrByID[srv.ID]; dup {
				continue
			}
			addrByID[srv.ID] = srv.Address
			order = append(order, srv.ID)
		}
	}
	return addrByID, order
}

// mergeSeedMembers fills in seed entries for IDs no live Configuration
// reported. Seeds never overwrite a live address.
func mergeSeedMembers(
	seeds []NodeIdentity,
	selfID string,
	addrByID map[string]string,
	order *[]string,
) {
	for _, m := range seeds {
		if m.NodeID == "" || m.NodeID == selfID {
			continue
		}
		if _, known := addrByID[m.NodeID]; known {
			continue
		}
		addrByID[m.NodeID] = m.GRPCAddress
		*order = append(*order, m.NodeID)
	}
}

// GetRaftGroups returns per-group state snapshots. Phase 0 wires commit/applied
// indices only; per-follower contact and term history land in later phases.
func (s *AdminServer) GetRaftGroups(
	_ context.Context,
	_ *pb.GetRaftGroupsRequest,
) (*pb.GetRaftGroupsResponse, error) {
	s.groupsMu.RLock()
	defer s.groupsMu.RUnlock()
	ids := sortedGroupIDs(s.groups)
	out := make([]*pb.RaftGroupState, 0, len(ids))
	now := s.now()
	for _, id := range ids {
		st := s.groups[id].Status()
		// Translate LastContact (duration since the last contact with the
		// leader, per raftengine.Status) into an absolute unix-ms so UI
		// clients can diff against their own clock instead of having to
		// reason about the server's uptime. The etcd engine returns a
		// sentinel negative duration when contact is unknown (e.g., a
		// follower that has never heard from a leader). Report that case
		// as `LastContactUnixMs=0` (epoch) so the UI can render "unknown"
		// / "never contacted" rather than treating it as "freshly
		// contacted just now".
		var lastContactUnixMs int64
		if st.LastContact >= 0 {
			lastContactUnixMs = now.Add(-st.LastContact).UnixMilli()
		}
		out = append(out, &pb.RaftGroupState{
			RaftGroupId:       id,
			LeaderNodeId:      st.Leader.ID,
			LeaderTerm:        st.Term,
			CommitIndex:       st.CommitIndex,
			AppliedIndex:      st.AppliedIndex,
			LastContactUnixMs: lastContactUnixMs,
		})
	}
	return &pb.GetRaftGroupsResponse{Groups: out}, nil
}

func (s *AdminServer) snapshotLeaders() []*pb.GroupLeader {
	s.groupsMu.RLock()
	defer s.groupsMu.RUnlock()
	ids := sortedGroupIDs(s.groups)
	out := make([]*pb.GroupLeader, 0, len(ids))
	for _, id := range ids {
		st := s.groups[id].Status()
		out = append(out, &pb.GroupLeader{
			RaftGroupId:  id,
			LeaderNodeId: st.Leader.ID,
			LeaderTerm:   st.Term,
		})
	}
	return out
}

// sortedGroupIDs returns the map's keys in ascending order so Admin responses
// are deterministic across calls — admin tooling and tests both rely on stable
// ordering.
func sortedGroupIDs(m map[uint64]AdminGroup) []uint64 {
	ids := make([]uint64, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// adminMethodPrefix is "/Admin/" today but is derived from the generated
// service descriptor so a future proto package declaration (which would
// package-qualify the service name) does not silently bypass the auth gate.
var adminMethodPrefix = "/" + pb.Admin_ServiceDesc.ServiceName + "/"

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
		if !strings.HasPrefix(info.FullMethod, adminMethodPrefix) {
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
		if !strings.HasPrefix(info.FullMethod, adminMethodPrefix) {
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
