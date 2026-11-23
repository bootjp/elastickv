package kv

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/testutil/grpctest"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type leaseReadForwardServer struct {
	pb.UnimplementedInternalServer
	clockMillis   atomic.Int64
	calls         atomic.Int32
	mu            sync.Mutex
	authorization []string
}

func (s *leaseReadForwardServer) ForwardLeaseRead(
	ctx context.Context,
	_ *pb.ForwardLeaseReadRequest,
) (*pb.ForwardLeaseReadResponse, error) {
	s.calls.Add(1)
	md, _ := metadata.FromIncomingContext(ctx)
	s.mu.Lock()
	s.authorization = append([]string(nil), md.Get("authorization")...)
	s.mu.Unlock()
	return &pb.ForwardLeaseReadResponse{AppliedIndex: 44, LastCommitTs: 91, LeaderUnixMillis: s.clockMillis.Load()}, nil
}

func TestShardedCoordinatorVerifyLeader_LeaderReturnsNil(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), nil, 1)

	st := store.NewMVCCStore()
	r, stop := newSingleRaft(t, "shard-leader", NewKvFSMWithHLC(st, NewHLC()))
	t.Cleanup(stop)

	re := r
	groups := map[uint64]*ShardGroup{
		1: {Engine: re, Store: st, Txn: NewLeaderProxyWithEngine(re)},
	}
	coord := NewShardedCoordinator(engine, groups, 1, NewHLC(), NewShardStore(engine, groups))

	require.NoError(t, coord.VerifyLeader(context.Background()))
	require.NoError(t, coord.VerifyLeaderForKey(context.Background(), []byte("b")))
}

func TestShardedCoordinatorVerifyLeader_MissingGroup(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{}, 1, NewHLC(), nil)

	require.ErrorIs(t, coord.VerifyLeader(context.Background()), ErrLeaderNotFound)
	require.ErrorIs(t, coord.VerifyLeaderForKey(context.Background(), []byte("k")), ErrLeaderNotFound)
}

// TestShardedCoordinatorLeaseReadAllGroups_FencesEveryLeader asserts that
// LeaseReadAllGroups establishes the lease bound on EVERY owned group, not
// just the default group (codex P1-A): a multi-shard scan visits all routes,
// so a default-group-only fence would leave non-default groups unfenced.
func TestShardedCoordinatorLeaseReadAllGroups_FencesEveryLeader(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	s1 := store.NewMVCCStore()
	require.NoError(t, s1.PutAt(context.Background(), []byte("a"), []byte("one"), 70, 0))
	r1, stop1 := newSingleRaft(t, "lrag-g1", NewKvFSMWithHLC(s1, NewHLC()))
	t.Cleanup(stop1)
	s2 := store.NewMVCCStore()
	require.NoError(t, s2.PutAt(context.Background(), []byte("z"), []byte("two"), 90, 0))
	r2, stop2 := newSingleRaft(t, "lrag-g2", NewKvFSMWithHLC(s2, NewHLC()))
	t.Cleanup(stop2)

	groups := map[uint64]*ShardGroup{
		1: {Engine: r1, Store: s1, Txn: NewLeaderProxyWithEngine(r1)},
		2: {Engine: r2, Store: s2, Txn: NewLeaderProxyWithEngine(r2)},
	}
	coord := NewShardedCoordinator(engine, groups, 1, NewHLC(), NewShardStore(engine, groups))

	maxCommitTS, err := coord.LeaseReadAllGroupsTimestamp(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(90), maxCommitTS)
}

func TestShardedCoordinatorLeaseReadAllGroups_ForwardsFollowerToGroupLeader(t *testing.T) {
	t.Parallel()
	service := &leaseReadForwardServer{}
	addr := grpctest.Serve(t, func(server *grpc.Server) { pb.RegisterInternalServer(server, service) })

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)
	follower := newStubFollowerEngine(addr)
	group := &ShardGroup{Engine: follower, Store: store.NewMVCCStore()}
	proxy := NewLeaderProxyForShardGroup(group)
	group.SetLeaderReadToken("admin-secret")
	t.Cleanup(func() { require.NoError(t, proxy.Close()) })
	group.Txn = proxy
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{1: group}, 1, NewHLC(), nil)

	maxCommitTS, err := coord.LeaseReadAllGroupsTimestamp(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(91), maxCommitTS)
	require.Equal(t, int32(1), service.calls.Load())
	service.mu.Lock()
	require.Equal(t, []string{"Bearer admin-secret"}, service.authorization)
	service.mu.Unlock()
	_, err = LeaseReadTimeThrough(CoordinatorForwarder{Inner: coord}, t.Context())
	require.ErrorContains(t, err, "missing leader wall time")
	service.clockMillis.Store(123456789)
	instant, err := LeaseReadTimeThrough(CoordinatorForwarder{Inner: coord}, t.Context())
	require.NoError(t, err)
	require.Equal(t, service.clockMillis.Load(), instant.UnixMilli())

}

func TestShardedCoordinatorLeaseReadAllGroups_UsesConfiguredAllShardGroups(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)

	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		0: {Engine: newStubFollowerEngine("")},
		1: {Engine: newStubLeaderEngine()},
	}, 1, NewHLC(), nil).WithAllShardGroups(1)

	require.NoError(t, coord.LeaseReadAllGroups(context.Background()))
}

// TestShardedCoordinatorLeaseReadAllGroups_FailsClosedOnUnreadableGroup
// asserts LeaseReadAllGroups fails closed when ANY group cannot confirm its
// lease: a partially-fenced multi-shard read is exactly the stale read the
// fence guards against. Group 2 has no engine, so its lease read errors.
func TestShardedCoordinatorLeaseReadAllGroups_FailsClosedOnUnreadableGroup(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	s1 := store.NewMVCCStore()
	r1, stop1 := newSingleRaft(t, "lrag-fail-g1", NewKvFSMWithHLC(s1, NewHLC()))
	t.Cleanup(stop1)

	groups := map[uint64]*ShardGroup{
		1: {Engine: r1, Store: s1, Txn: NewLeaderProxyWithEngine(r1)},
		2: {Store: store.NewMVCCStore()}, // no Engine: lease read fails closed
	}
	coord := NewShardedCoordinator(engine, groups, 1, NewHLC(), NewShardStore(engine, groups))

	require.ErrorIs(t, coord.LeaseReadAllGroups(context.Background()), ErrLeaderNotFound)
}

// TestShardedCoordinatorLeaseReadAllGroups_EmptyGroups asserts the no-groups
// case fails closed rather than silently confirming freshness on nothing.
func TestShardedCoordinatorLeaseReadAllGroups_EmptyGroups(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{}, 1, NewHLC(), nil)

	require.ErrorIs(t, coord.LeaseReadAllGroups(context.Background()), ErrLeaderNotFound)
}

type clockDuringRead struct {
	*ShardedCoordinator
	sample func() time.Time
}

func (c clockDuringRead) LeaseReadTime(context.Context) (time.Time, error) { return c.sample(), nil }

func TestTimedSnapshotChecksEachDataGroup(t *testing.T) {
	t.Parallel()
	routes := distribution.NewEngine()
	routes.UpdateRoute(nil, []byte("m"), 1)
	routes.UpdateRoute([]byte("m"), nil, 2)
	groups := map[uint64]*ShardGroup{}
	for _, id := range []uint64{1, 2} {
		st := store.NewMVCCStore()
		require.NoError(t, st.PutAt(t.Context(), []byte("initial"), nil, 100/id, 0))
		raft, stop := newSingleRaft(t, fmt.Sprintf("timed-%d", id), NewKvFSMWithHLC(st, NewHLC()))
		t.Cleanup(stop)
		groups[id] = &ShardGroup{Engine: raft, Store: st, Txn: NewLeaderProxyWithEngine(raft)}
	}
	st := NewShardStore(routes, groups)
	leader := NewShardedCoordinator(routes, groups, 1, NewHLC(), st)
	instant := time.UnixMilli(500)
	coord := clockDuringRead{leader, func() time.Time {
		require.NoError(t, groups[2].Store.PutAt(t.Context(), []byte("z"), []byte("new"), 51, 0))
		return instant
	}}
	r := NewReadRequest(t.Context(), coord, st, nil)
	defer r.Close()
	_, err := BeginTimedReadTimestamp(r.Context(), coord, st, "script")
	require.ErrorIs(t, err, store.ErrWriteConflict)
	// No group changes in the next sample, whose maximum is still 100.
	quiet := clockDuringRead{leader, func() time.Time { return instant }}
	second := NewReadRequest(t.Context(), quiet, st, nil)
	defer second.Close()
	snap, err := BeginTimedReadTimestamp(second.Context(), quiet, st, "script")
	require.NoError(t, err)
	require.EqualValues(t, 100, snap.Timestamp())
	require.Equal(t, instant, snap.Instant)
	require.NoError(t, snap.Validate(second.Context()))
	require.NoError(t, groups[2].Store.PutAt(t.Context(), []byte("z"), []byte("late"), 52, 0))
	require.ErrorIs(t, snap.Validate(second.Context()), store.ErrWriteConflict)
}

func TestTimedMutationParticipantWatermarks(t *testing.T) {
	t.Parallel()
	f := newTwoGroupReadFixture(t)
	shards := NewShardStore(f.shards.engine, f.groups)
	coord := clockDuringRead{NewShardedCoordinator(shards.engine, f.groups, 1, NewHLC(), shards), time.Now}
	require.NoError(t, f.onePhase(t, 1, 1, 100, "a", "hot"))
	require.NoError(t, f.onePhase(t, 2, 1, 50, "b", "cold"))

	read := NewReadRequest(t.Context(), coord, shards, nil)
	defer read.Close()
	snap, err := BeginTimedReadTimestamp(read.Context(), coord, shards, "lua")
	require.NoError(t, err)
	for _, tc := range []struct {
		name          string
		writes, reads []string
		floor         uint64
		conflict      bool
	}{
		{"only hot participant", []string{"a"}, []string{"a"}, 100, false},
		{"only cold participant", []string{"b"}, []string{"b", "c"}, 50, false},
		{"cold read covered by intent", []string{"a", "b"}, []string{"a", "b"}, 50, false},
		{"cold read only participant", []string{"a"}, []string{"b"}, 0, true},
		{"unlocked key on cold write participant", []string{"a", "b"}, []string{"c"}, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &OperationGroup[OP]{IsTxn: true, StartTS: snap.Timestamp()}
			for _, k := range tc.writes {
				req.Elems = append(req.Elems, &Elem[OP]{Op: Put, Key: []byte(k)})
			}
			for _, k := range tc.reads {
				req.ReadKeys = append(req.ReadKeys, []byte(k))
			}
			floor, version, err := snap.mutationFloor(req)
			if tc.conflict {
				require.ErrorIs(t, err, store.ErrWriteConflict)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.floor, floor)
			require.EqualValues(t, 1, version)
		})
	}

	// An unrelated slow group must not prevent an update of a hot key.
	require.NoError(t, snap.Validate(read.Context()))
	op := &OperationGroup[OP]{IsTxn: true, StartTS: 100, CommitTS: 101,
		ReadKeys: [][]byte{[]byte("a")}, Elems: []*Elem[OP]{{Op: Put, Key: []byte("a"), Value: []byte("updated")}}}
	_, err = snap.Dispatch(read.Context(), coord, op)
	require.NoError(t, err)
	require.EqualValues(t, 100, op.StartTS)
	require.Zero(t, op.ObservedRouteVersion)
}
