package adapter

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const casRPCGroup = 2

type casRPCFollower struct {
	raftengine.Engine
	leader raftengine.LeaderInfo
}

func (e *casRPCFollower) State() raftengine.State       { return raftengine.StateFollower }
func (e *casRPCFollower) Leader() raftengine.LeaderInfo { return e.leader }
func (e *casRPCFollower) Status() raftengine.Status {
	return raftengine.Status{State: e.State(), Leader: e.Leader()}
}

type casRPCProposalFault struct {
	*etcdraftengine.Engine
	attempts     atomic.Int32
	loseResult   atomic.Bool
	fenceVerdict atomic.Bool
	park         atomic.Bool
	parked       chan struct{}
	release      chan struct{}
}

func (e *casRPCProposalFault) Propose(ctx context.Context, payload []byte) (*raftengine.ProposalResult, error) {
	isCAS := raftengine.IsVersionCASRaftPayload(payload)
	if isCAS {
		e.attempts.Add(1)
		if e.fenceVerdict.Load() {
			// Inject an indexed FSM refusal at the proposer result boundary.
			return &raftengine.ProposalResult{CommitIndex: 17, Response: kv.ErrRouteWriteFenced}, nil
		}
		if e.park.Load() {
			close(e.parked)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-e.release:
			}
		}
	}
	result, err := e.Engine.Propose(ctx, payload)
	if isCAS && err == nil && e.loseResult.Load() {
		return nil, raftengine.ErrLeadershipLost
	}
	return result, err
}

type casRPCFixture struct {
	ctx                      context.Context
	leader, single, follower pb.TransactionalKVClient
	internal                 pb.InternalClient
	local                    store.MVCCStore
	proposals                *casRPCProposalFault
	forwards                 atomic.Int32
	dropReply                atomic.Bool
	allowCAS                 atomic.Bool
}

func replicateCASFixtureCatalog(t testing.TB, ctx context.Context, proposer raftengine.Proposer, local store.MVCCStore, group uint64) {
	t.Helper()
	catalog := distribution.NewCatalogStore(local)
	plan, err := catalog.BuildBootstrapPlanAt(ctx, 1, []distribution.Route{
		{GroupID: group, State: distribution.RouteStateActive},
	})
	require.NoError(t, err)
	replicateCASCatalogMutations(t, ctx, proposer, 1, plan.Mutations)
}

func replicateCASCatalogMutations(t testing.TB, ctx context.Context, proposer raftengine.Proposer, startTS uint64, muts []*store.KVPairMutation) {
	t.Helper()
	key := distribution.CatalogVersionKey()
	request := &pb.Request{IsTxn: true, Ts: startTS, Mutations: []*pb.Mutation{
		{Op: pb.Op_PUT, Key: []byte(kv.TxnMetaPrefix), Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: key, CommitTS: startTS + 1})},
	}}
	for _, mut := range muts {
		request.Mutations = append(request.Mutations, &pb.Mutation{Op: pb.Op_PUT, Key: mut.Key, Value: mut.Value})
		request.ReadKeys = append(request.ReadKeys, mut.Key)
	}
	txn := kv.NewTransactionWithProposer(proposer)
	defer txn.Close()
	_, err := txn.Commit(ctx, []*pb.Request{request})
	require.NoError(t, err)
}

func newCASRPCFixture(t *testing.T) *casRPCFixture {
	t.Helper()
	return newCASRPCFixtureWithAllocator(t, nil)
}

func newCASRPCFixtureWithAllocator(t *testing.T, allocator kv.TimestampAllocator) *casRPCFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	t.Cleanup(cancel)
	local, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, local.Close()) })
	f := &casRPCFixture{ctx: ctx, local: local}
	f.allowCAS.Store(true)
	routes := distribution.NewEngine()
	require.NoError(t, routes.ApplySnapshot(distribution.CatalogSnapshot{Version: 1, Routes: []distribution.RouteDescriptor{
		{RouteID: 1, GroupID: casRPCGroup, State: distribution.RouteStateActive},
	}}))
	hlc := kv.NewHLC()
	factory := etcdraftengine.NewFactory(etcdraftengine.FactoryConfig{
		TickInterval: 10 * time.Millisecond, HeartbeatTick: 1, ElectionTick: 10,
		MaxSizePerMsg: 1 << 20, MaxInflightMsg: 256,
	})
	node, err := factory.Create(raftengine.FactoryConfig{
		LocalID: "cas-rpc-leader", LocalAddress: "cas-rpc-leader", DataDir: t.TempDir(), Bootstrap: true,
		StateMachine: kv.NewKvFSMWithHLC(local, hlc, kv.WithRouteHistory(kv.WrapDistributionEngine(routes), casRPCGroup),
			kv.WithRaftBootstrapIdentity(raftbootstrap.UUID{1}, casRPCGroup)),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, node.Engine.Close())
		if node.Close != nil {
			require.NoError(t, node.Close())
		}
	})
	etcdEngine, ok := node.Engine.(*etcdraftengine.Engine)
	require.True(t, ok, "expected an etcd raft engine, got %T", node.Engine)
	engine := &casRPCProposalFault{
		Engine: etcdEngine, parked: make(chan struct{}), release: make(chan struct{}),
	}
	f.proposals = engine
	require.Eventually(t, func() bool { return engine.State() == raftengine.StateLeader }, 5*time.Second, 10*time.Millisecond)
	localProof := func(ctx context.Context) (context.Context, error) {
		if !f.allowCAS.Load() {
			return nil, kv.ErrVersionCASUnavailable
		}
		s := engine.Status()
		return raftengine.WithVersionCASProof(ctx, raftengine.VersionCASProof{Term: s.Term, ConfigurationIndex: s.ConfigurationIndex}), nil
	}
	proof := kv.WithVersionCASAdmission(localProof)
	leaderProxy := kv.NewLeaderProxyWithEngine(engine, proof)
	t.Cleanup(func() { require.NoError(t, leaderProxy.Close()) })
	group := &kv.ShardGroup{Engine: engine, Store: local, Txn: leaderProxy}
	group.ConfigureVersionReadAdmission(func(context.Context) error { return nil })
	groups := map[uint64]*kv.ShardGroup{casRPCGroup: group}
	shards := kv.NewShardStore(routes, groups)
	coord := kv.NewShardedCoordinator(routes, groups, casRPCGroup, hlc, shards)
	coord.WithTSOAllocator(allocator)
	t.Cleanup(func() { require.NoError(t, coord.Close()) })
	activated, err := engine.Propose(ctx, kv.EncodeCommitTSFenceActivation())
	require.NoError(t, err)
	require.NotNil(t, activated)
	require.Nil(t, activated.Response)
	replicateCASFixtureCatalog(t, ctx, engine, local, casRPCGroup)
	proofCtx, err := localProof(ctx)
	require.NoError(t, err)
	identity, err := engine.Propose(proofCtx, kv.EncodeMigrationVersionIdentityActivation())
	require.NoError(t, err)
	require.NotNil(t, identity)
	require.Nil(t, identity.Response)
	require.NoError(t, coord.RecoverHLCLease(ctx))

	txn := NewInternalMigrationTransaction(engine, nil, local, casRPCGroup, proof)
	t.Cleanup(txn.Close)
	leaderRPC := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info.FullMethod != pb.Internal_ForwardCompareAndSwapV2_FullMethodName {
			return handler(ctx, req)
		}
		f.forwards.Add(1)
		resp, err := handler(ctx, req)
		if err == nil && f.dropReply.Load() {
			return nil, status.Error(codes.Unavailable, "injected lost reply after commit")
		}
		return resp, err
	}))
	pb.RegisterTransactionalKVServer(leaderRPC, NewGRPCServer(shards, coord))
	pb.RegisterInternalServer(leaderRPC, NewInternalWithEngine(txn, engine, hlc, nil,
		WithInternalTimestampAllocator(allocator), WithInternalStore(local), WithInternalWriteGate(coord), WithInternalLastCommitTimestamp(local.LastCommitTS),
		WithInternalCASForwardRouting(casRPCGroup, shards.CheckVersionCASForwardRoute)))
	leaderAddr, leaderConn := serveCASRPC(t, leaderRPC)
	f.leader = pb.NewTransactionalKVClient(leaderConn)
	f.internal = pb.NewInternalClient(leaderConn)

	singleCoord := kv.NewCoordinatorWithEngine(txn, engine, kv.WithHLC(hlc))
	t.Cleanup(func() { require.NoError(t, singleCoord.Close()) })
	singleRPC := grpc.NewServer()
	pb.RegisterTransactionalKVServer(singleRPC, NewGRPCServer(local, singleCoord))
	_, singleConn := serveCASRPC(t, singleRPC)
	f.single = pb.NewTransactionalKVClient(singleConn)

	follower := &casRPCFollower{leader: raftengine.LeaderInfo{ID: "cas-rpc-leader", Address: leaderAddr}}
	followerProxy := kv.NewLeaderProxyWithEngine(follower, proof)
	t.Cleanup(func() { require.NoError(t, followerProxy.Close()) })
	followerStore := store.NewMVCCStore()
	followerGroups := map[uint64]*kv.ShardGroup{casRPCGroup: {Engine: follower, Store: followerStore, Txn: followerProxy}}
	followerCoord := kv.NewShardedCoordinator(routes, followerGroups, 1, kv.NewHLC(), followerStore)
	t.Cleanup(func() { require.NoError(t, followerCoord.Close()) })
	followerRPC := grpc.NewServer()
	pb.RegisterTransactionalKVServer(followerRPC, NewGRPCServer(followerStore, followerCoord))
	_, followerConn := serveCASRPC(t, followerRPC)
	f.follower = pb.NewTransactionalKVClient(followerConn)
	return f
}

func serveCASRPC(t *testing.T, srv *grpc.Server) (string, *grpc.ClientConn) {
	t.Helper()
	var lc net.ListenConfig
	listener, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = srv.Serve(listener) }()
	t.Cleanup(srv.Stop)
	addr := listener.Addr().String()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return addr, conn
}

func (f *casRPCFixture) put(t *testing.T, key, value []byte) uint64 {
	t.Helper()
	resp, err := f.leader.Put(f.ctx, &pb.PutRequest{Key: key, Value: value})
	require.NoError(t, err)
	require.True(t, resp.GetSuccess())
	ts, found, err := f.local.LatestCommitTS(f.ctx, key)
	require.NoError(t, err)
	require.True(t, found)
	require.NotZero(t, ts)
	return ts
}

func TestCompareAndSwapPublicRPC(t *testing.T) {
	f := newCASRPCFixture(t)
	for _, tc := range []struct {
		name   string
		client pb.TransactionalKVClient
	}{
		{"sharded leader", f.leader},
		{"sharded follower ingress", f.follower},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key := []byte(tc.name)
			original := f.put(t, key, []byte("original"))
			get, err := f.leader.Get(f.ctx, &pb.GetRequest{Key: key, NeedCommitTs: true})
			require.NoError(t, err)
			require.Equal(t, original, get.GetCommitTs())
			req := &pb.CompareAndSwapRequest{Key: key, ExpectedCommitTs: get.GetCommitTs()}
			applied, err := tc.client.CompareAndSwap(f.ctx, req)
			require.NoError(t, err)
			require.IsType(t, &pb.CompareAndSwapResponse_NewCommitTs{}, applied.GetResult())
			require.Greater(t, applied.GetNewCommitTs(), original)
			get, err = f.leader.Get(f.ctx, &pb.GetRequest{Key: key, NeedCommitTs: true})
			require.NoError(t, err)
			require.Empty(t, get.GetValue())
			require.Equal(t, applied.GetNewCommitTs(), get.GetCommitTs())

			mismatch, err := tc.client.CompareAndSwap(f.ctx, req)
			require.NoError(t, err)
			require.IsType(t, &pb.CompareAndSwapResponse_Mismatch{}, mismatch.GetResult())
			require.True(t, mismatch.GetMismatch())
			req.ExpectedCommitTs = applied.GetNewCommitTs()
			sameValue, err := tc.client.CompareAndSwap(f.ctx, req)
			require.NoError(t, err)
			require.Greater(t, sameValue.GetNewCommitTs(), applied.GetNewCommitTs())
			req.Key = []byte("missing")
			req.ExpectedCommitTs = ^uint64(0)
			missing, err := tc.client.CompareAndSwap(f.ctx, req)
			require.NoError(t, err)
			require.True(t, missing.GetMismatch())
		})
	}

	for _, tc := range []struct {
		req  *pb.CompareAndSwapRequest
		code codes.Code
	}{
		{&pb.CompareAndSwapRequest{ExpectedCommitTs: 1}, codes.InvalidArgument},
		{&pb.CompareAndSwapRequest{Key: []byte("no expected version")}, codes.InvalidArgument},
		{&pb.CompareAndSwapRequest{Key: []byte(kv.TxnMetaPrefix), ExpectedCommitTs: 1}, codes.PermissionDenied},
	} {
		before := f.forwards.Load()
		resp, err := f.follower.CompareAndSwap(f.ctx, tc.req)
		require.Equal(t, tc.code, status.Code(err))
		require.Nil(t, resp)
		require.Equal(t, before, f.forwards.Load())
	}
}

// The legacy coordinator has no authoritative route history. Even a local
// leader must use the protected sharded coordinator to admit a version CAS.
func TestCompareAndSwapLegacyCoordinatorRequiresRouteHistory(t *testing.T) {
	f := newCASRPCFixture(t)
	key := []byte("legacy-coordinator")
	version := f.put(t, key, []byte("before"))
	resp, err := f.single.CompareAndSwap(f.ctx, &pb.CompareAndSwapRequest{
		Key: key, ExpectedCommitTs: version, Value: []byte("after"),
	})
	require.Nil(t, resp)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Zero(t, f.proposals.attempts.Load())
	get, err := f.leader.Get(f.ctx, &pb.GetRequest{Key: key, NeedCommitTs: true})
	require.NoError(t, err)
	require.Equal(t, version, get.GetCommitTs())
	require.Equal(t, []byte("before"), get.GetValue())
}

func TestCompareAndSwapLostForwardReply(t *testing.T) {
	f := newCASRPCFixture(t)
	key := []byte("lost-forward")
	original := f.put(t, key, []byte("before"))
	f.dropReply.Store(true)
	req := &pb.CompareAndSwapRequest{Key: key, ExpectedCommitTs: original, Value: []byte("committed")}
	resp, err := f.follower.CompareAndSwap(f.ctx, req)
	require.Nil(t, resp)
	require.True(t, isIndeterminateGRPCMutation(err), "RPC error: %v", err)
	require.EqualValues(t, 1, f.forwards.Load())
	f.dropReply.Store(false)
	get, err := f.leader.Get(f.ctx, &pb.GetRequest{Key: key, NeedCommitTs: true})
	require.NoError(t, err)
	require.Equal(t, req.GetValue(), get.GetValue())
	require.Greater(t, get.GetCommitTs(), original)
	mismatch, err := f.follower.CompareAndSwap(f.ctx, req)
	require.NoError(t, err)
	require.True(t, mismatch.GetMismatch())
	require.EqualValues(t, 2, f.forwards.Load())
}

func TestCompareAndSwapLostLocalProposalResult(t *testing.T) {
	f := newCASRPCFixture(t)
	key := []byte("lost-local-result")
	original := f.put(t, key, []byte("before"))
	f.proposals.loseResult.Store(true)
	req := &pb.CompareAndSwapRequest{Key: key, ExpectedCommitTs: original, Value: []byte("committed")}
	resp, err := f.leader.CompareAndSwap(f.ctx, req)
	require.Nil(t, resp)
	require.True(t, isIndeterminateGRPCMutation(err), "RPC error: %v", err)
	require.EqualValues(t, 1, f.proposals.attempts.Load())
	f.proposals.loseResult.Store(false)
	get, err := f.leader.Get(f.ctx, &pb.GetRequest{Key: key, NeedCommitTs: true})
	require.NoError(t, err)
	require.Equal(t, req.GetValue(), get.GetValue())
	require.Greater(t, get.GetCommitTs(), original)
}

func TestCompareAndSwapTimestampRejectionIsNotMismatch(t *testing.T) {
	f := newCASRPCFixture(t)
	key := []byte("target")
	original := f.put(t, key, []byte("before"))
	f.proposals.park.Store(true)
	type rpcResult struct {
		resp *pb.CompareAndSwapResponse
		err  error
	}
	done := make(chan rpcResult, 1)
	go func() {
		resp, err := f.follower.CompareAndSwap(f.ctx, &pb.CompareAndSwapRequest{Key: key, ExpectedCommitTs: original})
		done <- rpcResult{resp: resp, err: err}
	}()
	select {
	case <-f.proposals.parked:
	case got := <-done:
		t.Fatalf("CAS returned before proposal reached the gate: %v", got.err)
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	f.put(t, []byte("unrelated"), []byte("advances write fence"))
	f.proposals.park.Store(false)
	close(f.proposals.release)
	got := <-done
	require.Nil(t, got.resp)
	require.Equal(t, codes.Aborted, status.Code(got.err))
	require.EqualValues(t, 1, f.proposals.attempts.Load())
	require.EqualValues(t, 1, f.forwards.Load())
	get, err := f.leader.Get(f.ctx, &pb.GetRequest{Key: key, NeedCommitTs: true})
	require.NoError(t, err)
	require.Equal(t, original, get.GetCommitTs())
}

func TestCompareAndSwapForwardPreflight(t *testing.T) {
	f := newCASRPCFixture(t)
	key := []byte("protected-from-forged-stamp")
	original := f.put(t, key, []byte("before"))
	req := &pb.Request{
		IsTxn:             true,
		VersionCasGroupId: casRPCGroup,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(kv.TxnMetaPrefix), Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: key})},
			{Op: pb.Op_COMPARE_AND_SWAP, Key: key, ExpectedCommitTs: original, Value: []byte("after")},
		},
	}
	envelope := &pb.ForwardRequest{IsTxn: true, Requests: []*pb.Request{req}, DecisionDetailVersion: kv.ForwardDecisionSchemaVersion}
	legacy, err := f.internal.Forward(f.ctx, envelope)
	require.Nil(t, legacy)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	req.Ts = original
	resp, err := f.internal.ForwardCompareAndSwapV2(f.ctx, envelope)
	require.Nil(t, resp)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	req.Ts = 0
	req.VersionCasGroupId = casRPCGroup + 1
	resp, err = f.internal.ForwardCompareAndSwapV2(f.ctx, envelope)
	require.Nil(t, resp)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	req.VersionCasGroupId = casRPCGroup
	req.ObservedRouteVersion = 1
	resp, err = f.internal.ForwardCompareAndSwapV2(f.ctx, envelope)
	require.Nil(t, resp)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	req.ObservedRouteVersion = 0
	req.WriteFenceBypassKeys = [][]byte{key}
	resp, err = f.internal.ForwardCompareAndSwapV2(f.ctx, envelope)
	require.Nil(t, resp)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	req.WriteFenceBypassKeys = nil

	f.allowCAS.Store(false)
	cas := &pb.CompareAndSwapRequest{Key: key, ExpectedCommitTs: original, Value: []byte("after")}
	for _, client := range []pb.TransactionalKVClient{f.leader, f.follower} {
		respCAS, err := client.CompareAndSwap(f.ctx, cas)
		require.Nil(t, respCAS)
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
	}
	get, err := f.leader.Get(f.ctx, &pb.GetRequest{Key: key, NeedCommitTs: true})
	require.NoError(t, err)
	require.Equal(t, original, get.GetCommitTs())
	require.Equal(t, []byte("before"), get.GetValue())
	require.Zero(t, f.proposals.attempts.Load())
}

func TestCompareAndSwapForwardRequiresConfiguredPeerAuth(t *testing.T) {
	unary, _ := SplitMigrationTokenAuth("test-peer-token", false, false)
	server := grpc.NewServer(grpc.UnaryInterceptor(unary))
	pb.RegisterInternalServer(server, &pb.UnimplementedInternalServer{})
	_, conn := serveCASRPC(t, server)
	client := pb.NewInternalClient(conn)
	_, err := client.ForwardCompareAndSwapV2(t.Context(), &pb.ForwardRequest{Requests: []*pb.Request{{
		Mutations: []*pb.Mutation{{Op: pb.Op_COMPARE_AND_SWAP}},
	}}})
	require.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestCompareAndSwapIndexedLockRejection(t *testing.T) {
	f := newCASRPCFixture(t)
	key := []byte("cas-locked")
	version := f.put(t, key, []byte("original"))
	req := &pb.Request{
		IsTxn: true, Phase: pb.Phase_PREPARE, Ts: version + 1,
		Mutations: []*pb.Mutation{
			{Key: []byte(kv.TxnMetaPrefix), Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: key, CommitTS: version + 2, LockTTLms: 10000})},
			{Key: key, Value: []byte("prepared")},
		},
	}
	res, err := f.internal.Forward(f.ctx, &pb.ForwardRequest{IsTxn: true, Requests: []*pb.Request{req}})
	require.NoError(t, err)
	require.True(t, res.GetSuccess())
	for _, client := range []pb.TransactionalKVClient{f.leader, f.follower} {
		reply, err := client.CompareAndSwap(f.ctx, &pb.CompareAndSwapRequest{Key: key, ExpectedCommitTs: version, Value: []byte("replacement")})
		require.Nil(t, reply)
		st := status.Convert(err)
		require.Equal(t, codes.Aborted, st.Code())
		require.Len(t, st.Details(), 1)
		info, ok := st.Details()[0].(*errdetails.ErrorInfo)
		require.True(t, ok)
		require.Equal(t, "CAS_LOCKED", info.GetReason())
	}
	value, err := f.local.GetAt(f.ctx, key, ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, []byte("original"), value)
}

func TestCompareAndSwapCertifiedRouteFence(t *testing.T) {
	f := newCASRPCFixture(t)
	key := []byte("route-fence")
	version := f.put(t, key, []byte("before"))
	f.proposals.fenceVerdict.Store(true)
	for _, tc := range []struct {
		name   string
		client pb.TransactionalKVClient
	}{{"leader", f.leader}, {"follower", f.follower}} {
		t.Run(tc.name, func(t *testing.T) {
			reply, err := tc.client.CompareAndSwap(f.ctx, &pb.CompareAndSwapRequest{
				Key: key, ExpectedCommitTs: version, Value: []byte("after"),
			})
			require.Nil(t, reply)
			st := status.Convert(err)
			require.Equal(t, codes.Aborted, st.Code())
			require.Len(t, st.Details(), 1)
			info, ok := st.Details()[0].(*errdetails.ErrorInfo)
			require.True(t, ok)
			require.Equal(t, "CAS_ROUTE_FENCED", info.GetReason())
		})
	}
	require.EqualValues(t, 1, f.forwards.Load())
	require.EqualValues(t, 2, f.proposals.attempts.Load())
}

// Reserve the CAS start timestamp and pause before issuing its commit
// timestamp. Other callers can still obtain timestamps and commit writes.
type casCommitTimestampGate struct {
	clock   *kv.HLC
	calls   atomic.Int32
	parked  chan struct{}
	release chan struct{}
}

func (g *casCommitTimestampGate) Next(ctx context.Context) (uint64, error) {
	if g.calls.Add(1) == 2 {
		close(g.parked)
		select {
		case <-g.release:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	return g.clock.Next(), nil
}

func TestCompareAndSwapInterveningKeyChangeReturnsMismatch(t *testing.T) {
	for _, ingress := range []string{"leader", "follower"} {
		t.Run(ingress, func(t *testing.T) {
			gate := &casCommitTimestampGate{clock: kv.NewHLC(), parked: make(chan struct{}), release: make(chan struct{})}
			gate.calls.Store(2) // Fixture setup may allocate timestamps.
			f := newCASRPCFixtureWithAllocator(t, gate)
			key := []byte("intervening")
			version := f.put(t, key, []byte("before"))
			client := f.leader
			if ingress == "follower" {
				client = f.follower
			}
			gate.calls.Store(0)
			type result struct {
				reply *pb.CompareAndSwapResponse
				err   error
			}
			done := make(chan result, 1)
			go func() {
				resp, err := client.CompareAndSwap(f.ctx, &pb.CompareAndSwapRequest{
					Key: key, ExpectedCommitTs: version, Value: []byte("stale replacement"),
				})
				done <- result{resp, err}
			}()
			select {
			case <-gate.parked:
			case early := <-done:
				t.Fatalf("CAS returned before timestamp barrier: %v, %v", early.reply, early.err)
			case <-f.ctx.Done():
				t.Fatal(f.ctx.Err())
			}
			intervening := f.put(t, key, []byte("new value"))
			close(gate.release)
			got := <-done
			require.NoError(t, got.err)
			require.True(t, got.reply.GetMismatch())
			stored, err := f.leader.Get(f.ctx, &pb.GetRequest{Key: key, NeedCommitTs: true})
			require.NoError(t, err)
			require.Equal(t, intervening, stored.GetCommitTs())
			require.Equal(t, []byte("new value"), stored.GetValue())
		})
	}
}
