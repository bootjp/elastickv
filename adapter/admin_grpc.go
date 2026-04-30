package adapter

import (
	"bytes"
	"context"
	"crypto/subtle"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/keyviz"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// KeyVizSampler is the read-side abstraction the Admin service needs
// from the keyviz package: a time-bounded matrix snapshot. Defined
// here (not in keyviz) so tests can pass an in-memory fake without
// constructing a full *keyviz.MemSampler. *keyviz.MemSampler
// satisfies this interface.
type KeyVizSampler interface {
	// Snapshot returns the matrix columns in [from, to). Either
	// bound may be the zero time meaning unbounded on that side.
	// Implementations must return rows the caller can mutate freely
	// (a deep copy) — see keyviz.MemSampler.Snapshot.
	Snapshot(from, to time.Time) []keyviz.MatrixColumn
}

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

	// sampler exposes the keyviz heatmap matrix to GetKeyVizMatrix.
	// Nil means keyviz is disabled — the RPC returns Unavailable.
	// Guarded by groupsMu (same lock as groups/now) so RegisterSampler
	// pairs atomically with concurrent RPC reads.
	sampler KeyVizSampler

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

// RegisterSampler wires the keyviz sampler used by GetKeyVizMatrix.
// Without this call (or with a nil sampler) the RPC returns
// codes.Unavailable so callers can distinguish "keyviz disabled"
// from "no data yet".
func (s *AdminServer) RegisterSampler(sampler KeyVizSampler) {
	s.groupsMu.Lock()
	s.sampler = sampler
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
	live := collectLiveMembers(ctx, groups, s.self.NodeID)
	mergeSeedMembers(s.members, s.self.NodeID, &live)

	out := make([]*pb.NodeIdentity, 0, len(live.order))
	for _, id := range live.order {
		out = append(out, &pb.NodeIdentity{NodeId: id, GrpcAddress: live.addrByID[id]})
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
//
// Each spawned goroutine checks ctx before issuing the RPC so a goroutine
// scheduled after the parent ctx already fired exits immediately instead
// of doing wasted gRPC work. After the RPC the goroutine drains its result
// without blocking thanks to the len(groups)-buffered channel.
// configFanoutMaxConcurrency caps how many Configuration polls run at the
// same time so a node hosting hundreds of Raft groups does not spawn a
// matching goroutine + gRPC burst on every GetClusterOverview. Sized to
// cover typical multi-raft deployments while keeping the goroutine /
// connection footprint bounded under load. Smaller than maxDiscoveredNodes
// (per-fanout target cap) on purpose: this is per-RPC concurrency, not
// total target count.
const configFanoutMaxConcurrency = 64

func fanoutConfigurationCalls(ctx context.Context, groups []groupEntry) []configResult {
	resultsCh := make(chan configResult, len(groups))
	// sem bounds the number of goroutines actively running Configuration at
	// once. We still spawn len(groups) goroutines total, but only
	// configFanoutMaxConcurrency of them can be inside Configuration at the
	// same time — the rest park on the semaphore acquire. Using buffered
	// channel sends/receives as the semaphore avoids an extra dep.
	sem := make(chan struct{}, configFanoutMaxConcurrency)
	for i, entry := range groups {
		go func(i int, entry groupEntry) {
			// Bail out early if the parent already cancelled — avoids
			// taking the semaphore + RPC path just to fail the call.
			if err := ctx.Err(); err != nil {
				resultsCh <- configResult{i: i, err: err}
				return
			}
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				resultsCh <- configResult{i: i, err: ctx.Err()}
				return
			}
			defer func() { <-sem }()
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

// liveMembers bundles the result of polling every Raft group's Configuration:
// addrByID lists the usable (non-blank) addresses, seenID is every NodeID any
// group reported (even with blank address) so seed backfill can distinguish
// "node still exists with bad metadata" from "node was removed", and
// authoritative is true only when EVERY group's Configuration succeeded
// (and at least one ran). A single group erroring or missing means the
// merged view is incomplete: a node only present in the failed group
// would otherwise be incorrectly treated as removed and dropped, so seed
// pruning must wait until live membership is proven across all groups.
type liveMembers struct {
	addrByID      map[string]string
	seenID        map[string]struct{}
	order         []string
	authoritative bool
}

func collectLiveMembers(
	ctx context.Context,
	groups []groupEntry,
	selfID string,
) liveMembers {
	got := fanoutConfigurationCalls(ctx, groups)

	// Merge in the original group-ID order so the lowest-ID-wins tie-break
	// stays deterministic. (Completion order would otherwise depend on
	// which Configuration() returned first.)
	sort.Slice(got, func(a, b int) bool { return got[a].i < got[b].i })

	live := liveMembers{
		addrByID: map[string]string{},
		seenID:   map[string]struct{}{},
		order:    []string{},
	}
	successes := 0
	for _, res := range got {
		if res.err != nil {
			continue
		}
		successes++
		for _, srv := range res.cfg.Servers {
			if srv.ID == "" || srv.ID == selfID {
				continue
			}
			live.seenID[srv.ID] = struct{}{}
			if srv.Address == "" {
				// Known node with missing metadata — seed will backfill.
				continue
			}
			if _, dup := live.addrByID[srv.ID]; dup {
				continue
			}
			live.addrByID[srv.ID] = srv.Address
			live.order = append(live.order, srv.ID)
		}
	}
	// Authoritative only when every queried group reported successfully
	// AND at least one group ran. If even a single Configuration call
	// errored or the fanout returned early on ctx cancellation, we cannot
	// distinguish "removed from cluster" from "the group that knew about
	// this node was unreachable", so seeds must be allowed to fall through.
	live.authoritative = successes > 0 && successes == len(groups) && len(got) == len(groups)
	return live
}

// mergeSeedMembers fills in seed entries against the live membership:
//
//   - If the NodeID was seen in some live config but with a blank address,
//     the seed supplies the address (handles the etcd convergence transient).
//   - If the NodeID was not seen at all and the live result is authoritative
//     (every queried group's Configuration succeeded), the seed is a removed
//     node — drop it instead of re-advertising it forever.
//   - Otherwise (cold start, partial failure, ctx cancellation, or every
//     Configuration errored), fall back to the seed: a node only known to
//     the failed group must not be silently dropped.
//
// Codex flagged two regressions in iterations of this function:
// (a) the original version re-added every removed seed, never converging on
// scale-in; (b) the round-24 fix flipped to drop on any single success,
// which dropped peers visible only in a partially-failing group. The
// current contract requires *every* group to report cleanly before the
// pruning path kicks in.
func mergeSeedMembers(seeds []NodeIdentity, selfID string, live *liveMembers) {
	for _, m := range seeds {
		if m.NodeID == "" || m.NodeID == selfID {
			continue
		}
		if _, hasAddr := live.addrByID[m.NodeID]; hasAddr {
			continue
		}
		_, seen := live.seenID[m.NodeID]
		if !seen && live.authoritative {
			// Live config is authoritative and doesn't know this node:
			// it was removed via raft RemoveServer. Skip.
			continue
		}
		live.addrByID[m.NodeID] = m.GRPCAddress
		live.order = append(live.order, m.NodeID)
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

// GetKeyVizMatrix renders the keyviz heatmap matrix for the [from, to)
// range supplied by the request, returning one KeyVizRow per tracked
// route or virtual bucket and a parallel column-timestamp slice.
//
// Series selection (Reads / Writes / ReadBytes / WriteBytes) maps from
// the request's KeyVizSeries enum to the matching keyviz.MatrixRow
// counter; KEYVIZ_SERIES_UNSPECIFIED defaults to Reads.
//
// Returns codes.Unavailable when no sampler is registered (keyviz
// disabled) so callers can distinguish that from "no data yet"
// (which yields a successful empty response).
func (s *AdminServer) GetKeyVizMatrix(
	_ context.Context,
	req *pb.GetKeyVizMatrixRequest,
) (*pb.GetKeyVizMatrixResponse, error) {
	s.groupsMu.RLock()
	sampler := s.sampler
	s.groupsMu.RUnlock()
	if sampler == nil {
		return nil, errors.WithStack(status.Error(codes.Unavailable, "keyviz sampler not configured on this node"))
	}
	from := unixMsToTime(req.GetFromUnixMs())
	to := unixMsToTime(req.GetToUnixMs())
	cols := sampler.Snapshot(from, to)
	pickValue := matrixSeriesPicker(req.GetSeries())
	return matrixToProto(cols, pickValue, clampRowBudget(int(req.GetRows()))), nil
}

// keyVizRowBudgetCap is the upper bound on the per-request rows
// budget — design doc §4.1 caps rows at 1024 to bound server work
// (sort + payload) for adversarial / over-large requests.
const keyVizRowBudgetCap = 1024

// clampRowBudget enforces design §4.1's upper bound. A request of 0
// (or negative) means "no cap" and is preserved; anything past the
// cap is silently clamped — clients asking for more rows than the
// server is willing to render get the most rows the server will
// render, not an error.
func clampRowBudget(requested int) int {
	if requested > keyVizRowBudgetCap {
		return keyVizRowBudgetCap
	}
	return requested
}

// unixMsToTime converts a Unix-millisecond timestamp into a time.Time,
// returning the zero Time when the input is zero so the sampler reads
// an unbounded range on that side.
func unixMsToTime(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms)
}

// matrixSeriesPicker returns a callback that extracts the requested
// counter from a MatrixRow. KEYVIZ_SERIES_UNSPECIFIED falls through
// to Writes per design doc §4.1 — write traffic is the primary
// signal the heatmap is built around, and the read path is wired in
// a follow-up phase.
func matrixSeriesPicker(series pb.KeyVizSeries) func(keyviz.MatrixRow) uint64 {
	switch series {
	case pb.KeyVizSeries_KEYVIZ_SERIES_READS:
		return func(r keyviz.MatrixRow) uint64 { return r.Reads }
	case pb.KeyVizSeries_KEYVIZ_SERIES_READ_BYTES:
		return func(r keyviz.MatrixRow) uint64 { return r.ReadBytes }
	case pb.KeyVizSeries_KEYVIZ_SERIES_WRITE_BYTES:
		return func(r keyviz.MatrixRow) uint64 { return r.WriteBytes }
	case pb.KeyVizSeries_KEYVIZ_SERIES_UNSPECIFIED, pb.KeyVizSeries_KEYVIZ_SERIES_WRITES:
		return func(r keyviz.MatrixRow) uint64 { return r.Writes }
	default:
		return func(r keyviz.MatrixRow) uint64 { return r.Writes }
	}
}

// matrixToProto pivots the column-major MatrixColumn slice into the
// row-major proto layout: one KeyVizRow per distinct RouteID with a
// values slice aligned to the column_unix_ms parallel slice. Idle
// routes (zero in every column) are not emitted by the sampler, so
// the row set already reflects observed activity in [from, to).
//
// rowBudget caps how many rows the response carries — passing
// 0 means "no cap." When the budget would be exceeded, rows are
// sorted by total activity across the requested series and the
// top-N retained, so callers asking for a compact matrix do not
// receive a payload that scales with the route count.
func matrixToProto(cols []keyviz.MatrixColumn, pick func(keyviz.MatrixRow) uint64, rowBudget int) *pb.GetKeyVizMatrixResponse {
	resp := &pb.GetKeyVizMatrixResponse{
		ColumnUnixMs: make([]int64, len(cols)),
	}
	rowsByID := make(map[uint64]*pb.KeyVizRow)
	order := make([]uint64, 0)
	for j, col := range cols {
		resp.ColumnUnixMs[j] = col.At.UnixMilli()
		for _, mr := range col.Rows {
			pr, ok := rowsByID[mr.RouteID]
			if !ok {
				pr = newKeyVizRowFrom(mr, len(cols))
				rowsByID[mr.RouteID] = pr
				order = append(order, mr.RouteID)
			}
			pr.Values[j] = pick(mr)
		}
	}
	resp.Rows = make([]*pb.KeyVizRow, len(order))
	for i, id := range order {
		resp.Rows[i] = rowsByID[id]
	}
	resp.Rows = applyKeyVizRowBudget(resp.Rows, rowBudget)
	sortKeyVizRowsByStart(resp.Rows)
	return resp
}

// applyKeyVizRowBudget caps rows to budget by total activity per row
// (sum of per-column values), preserving the top-N rows. budget <= 0
// means "no cap."
//
// NOTE: design doc §5.5 specifies a "lexicographic walk + greedy
// merge of low-activity adjacent ranges" algorithm — we simplify to
// activity-descending truncation for Phase 1 because it covers the
// common UI need (highlight hotspots) without needing the synthetic
// virtual-bucket plumbing the merge requires. Phase 2 should swap
// this for the spec'd merge so low-activity ranges become coarse
// aggregates instead of being silently dropped.
func applyKeyVizRowBudget(rows []*pb.KeyVizRow, budget int) []*pb.KeyVizRow {
	if budget <= 0 || len(rows) <= budget {
		return rows
	}
	sort.Slice(rows, func(i, j int) bool {
		return rowActivityTotal(rows[i]) > rowActivityTotal(rows[j])
	})
	return rows[:budget]
}

func rowActivityTotal(r *pb.KeyVizRow) uint64 {
	var sum uint64
	for _, v := range r.Values {
		sum += v
	}
	return sum
}

// newKeyVizRowFrom seeds a proto row from the first MatrixRow seen
// for a given RouteID. Values is allocated with len == numCols so
// every column gets a deterministic slot (zero-valued by default).
//
// route_count surfaces MemberRoutesTotal (the true number of routes
// folded into the bucket) — not just len(MemberRoutes), which the
// sampler caps at MaxMemberRoutesPerSlot. When the visible list is
// shorter than the total, route_ids_truncated lets consumers know
// to trust route_count for drill-down decisions.
func newKeyVizRowFrom(mr keyviz.MatrixRow, numCols int) *pb.KeyVizRow {
	total := mr.MemberRoutesTotal
	if !mr.Aggregate && total == 0 {
		// Individual slots fall through to RouteCount=1 when the
		// sampler predates MemberRoutesTotal or never set it.
		total = 1
	}
	row := &pb.KeyVizRow{
		BucketId:          bucketIDFor(mr),
		Start:             append([]byte(nil), mr.Start...),
		End:               append([]byte(nil), mr.End...),
		Aggregate:         mr.Aggregate,
		RouteCount:        total,
		RouteIdsTruncated: mr.Aggregate && total > uint64(len(mr.MemberRoutes)),
		RaftGroupId:       mr.RaftGroupID,
		LeaderTerm:        mr.LeaderTerm,
		Values:            make([]uint64, numCols),
	}
	if mr.Aggregate {
		row.RouteIds = append([]uint64(nil), mr.MemberRoutes...)
	}
	return row
}

func bucketIDFor(mr keyviz.MatrixRow) string {
	if mr.Aggregate {
		return "virtual:" + strconv.FormatUint(mr.RouteID, 10)
	}
	return "route:" + strconv.FormatUint(mr.RouteID, 10)
}

func sortKeyVizRowsByStart(rows []*pb.KeyVizRow) {
	sort.Slice(rows, func(i, j int) bool {
		if c := bytes.Compare(rows[i].Start, rows[j].Start); c != 0 {
			return c < 0
		}
		return rows[i].BucketId < rows[j].BucketId
	})
}
