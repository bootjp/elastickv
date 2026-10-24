package adapter

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	etcdraft "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type phaseDAcceptanceNode struct {
	id        string
	clock     *kv.HLC
	state     *kv.TSOStateMachine
	routes    *distribution.Engine
	groups    map[uint64]*kv.ShardGroup
	shards    *kv.ShardStore
	local     *kv.RaftTSOAllocator
	routed    *kv.LeaderRoutedTSOAllocator
	batch     *kv.BatchAllocator
	results   [3]*raftengine.FactoryResult
	listeners [3]net.Listener
	servers   [3]*grpc.Server
	clients   [3]*grpc.ClientConn
	txns      [3]*kv.TransactionManager
}

type phaseDAcceptanceCluster struct {
	token          string
	nodes          [3]*phaseDAcceptanceNode
	releaseBlocked atomic.Bool
	adminCalls     atomic.Int32
	stateCalls     atomic.Int32
	forwardCalls   atomic.Int32
	releaseFaults  atomic.Int32
}

// phaseDReleaseFaultEngine keeps the injected release fault active when a
// data leader becomes local to the TSO leader and no admin RPC is needed.
type phaseDReleaseFaultEngine struct {
	raftengine.Engine
	cluster *phaseDAcceptanceCluster
}

// ProposeAdmin rejects only release entries while the shared fault is active.
func (e *phaseDReleaseFaultEngine) ProposeAdmin(ctx context.Context, payload []byte) (*raftengine.ProposalResult, error) {
	if e.cluster.releaseBlocked.Load() && isPhaseDAcceptanceRelease(payload) {
		e.cluster.releaseFaults.Add(1)
		return nil, status.Error(codes.Unavailable, "injected local release proposal failure")
	}
	return e.Engine.ProposeAdmin(ctx, payload)
}

// isPhaseDAcceptanceRelease recognizes the existing fixed-size release envelope.
func isPhaseDAcceptanceRelease(payload []byte) bool {
	release := kv.EncodePrepareEvidenceRelease(1)
	return len(payload) == len(release) && bytes.Equal(payload[:2], release[:2])
}

func newPhaseDAcceptanceCluster(t *testing.T) *phaseDAcceptanceCluster {
	t.Helper()
	credential := make([]byte, 32)
	_, err := rand.Read(credential)
	require.NoError(t, err)
	c := &phaseDAcceptanceCluster{token: hex.EncodeToString(credential)}
	t.Cleanup(func() { c.close(t) })
	for n := range c.nodes {
		node := &phaseDAcceptanceNode{id: fmt.Sprintf("n%d", n), clock: kv.NewHLC(),
			routes: distribution.NewEngine(), groups: make(map[uint64]*kv.ShardGroup)}
		c.nodes[n] = node
		node.clock.SetPhysicalCeiling(time.Now().Add(2 * time.Minute).UnixMilli())
		node.state = kv.NewTSOStateMachine(node.clock)
		node.routes.UpdateRoute(nil, []byte("m"), 1)
		node.routes.UpdateRoute([]byte("m"), nil, 2)
		for group := range node.listeners {
			listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
			require.NoError(t, err)
			node.listeners[group] = listener
		}
	}
	factory := etcdraft.NewFactory(etcdraft.FactoryConfig{TickInterval: 20 * time.Millisecond,
		HeartbeatTick: 1, ElectionTick: 15, MaxSizePerMsg: 1 << 20, MaxInflightMsg: 64})
	for group := range uint64(3) {
		peers := make([]raftengine.Server, 0, len(c.nodes))
		for _, node := range c.nodes {
			peers = append(peers, raftengine.Server{ID: node.id, Address: node.listeners[group].Addr().String(), Suffrage: "voter"})
		}
		for _, node := range c.nodes {
			shard := &kv.ShardGroup{}
			var fsm raftengine.StateMachine = node.state
			if group != 0 {
				shard.Store = store.NewMVCCStore()
				fsm = kv.NewKvFSMWithHLC(shard.Store, node.clock)
			} else {
				shard.TSOState = node.state
			}
			node.groups[group] = shard
			result, err := factory.Create(raftengine.FactoryConfig{LocalID: node.id,
				LocalAddress: node.listeners[group].Addr().String(),
				DataDir:      filepath.Join(t.TempDir(), node.id, fmt.Sprintf("g%d", group)),
				Peers:        peers, Bootstrap: true, StateMachine: fsm})
			require.NoError(t, err)
			node.results[group], shard.Engine = result, result.Engine
			if group == 2 {
				shard.Engine = &phaseDReleaseFaultEngine{Engine: result.Engine, cluster: c}
			}
			shard.Txn = kv.NewLeaderProxyWithEngine(result.Engine,
				kv.WithTimestampAdmission(shard.Store, func() kv.TimestampAllocator { return node.batch }))
			shard.SetLeaderReadToken(c.token)
		}
	}
	for _, node := range c.nodes {
		c.serveNode(t, node)
	}
	for group := range 3 {
		c.placeLeader(t, group, group)
	}
	return c
}

func (c *phaseDAcceptanceCluster) serveNode(t *testing.T, node *phaseDAcceptanceNode) {
	t.Helper()
	node.shards = kv.NewShardStore(node.routes, node.groups)
	local, err := kv.NewRaftTSOAllocator(node.groups[0], node.clock,
		kv.WithTSOCutoverFloorProvider(node.shards), kv.WithTSOPhaseDActivationGate(node.shards))
	require.NoError(t, err)
	node.local = local
	node.routed, err = kv.NewLeaderRoutedTSOAllocator(local, node.groups[0].Engine,
		kv.WithTSOCutoverActivation(), kv.WithTSORoutedClock(node.clock))
	require.NoError(t, err)
	node.batch, err = kv.NewBatchAllocator(node.routed, 8)
	require.NoError(t, err)
	for group := range uint64(3) {
		result := node.results[group]
		shard := node.groups[group]
		node.txns[group] = kv.NewTransactionWithProposer(result.Engine,
			kv.WithTimestampAdmission(shard.Store, func() kv.TimestampAllocator { return node.batch }))
		options := []InternalOption{WithInternalTimestampAllocator(node.batch),
			WithInternalPrepareEvidenceToken(c.token),
			WithInternalTSOCutoverState(node.state), WithInternalPrimaryCommitEvidence(node.shards),
			WithInternalAdminProposer(result.Engine)}
		if group == 0 {
			options = append(options, WithKVForwardRejected())
		} else {
			options = append(options, WithInternalStore(shard.Store))
		}
		auth, _ := AdminTokenAuth(c.token)
		server := grpc.NewServer(grpc.ChainUnaryInterceptor(auth, c.observeRPC(group)))
		node.servers[group] = server
		require.NotNil(t, result.RegisterTransport)
		result.RegisterTransport(server)
		pb.RegisterRawKVServer(server, NewGRPCServer(node.shards, nil))
		pb.RegisterInternalServer(server, NewInternalWithEngine(node.txns[group], result.Engine, node.clock, nil, options...))
		pb.RegisterDistributionServer(server, NewDistributionServer(node.routes, nil, WithDistributionTimestampAllocator(local)))
		go func() { _ = server.Serve(node.listeners[group]) }()
		node.clients[group], err = grpc.NewClient(node.listeners[group].Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
	}
}

func (c *phaseDAcceptanceCluster) observeRPC(group uint64) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		switch info.FullMethod {
		case pb.Internal_GetPrepareEvidenceState_FullMethodName:
			c.stateCalls.Add(1)
		case pb.Internal_Forward_FullMethodName:
			c.forwardCalls.Add(1)
		case pb.Internal_ForwardAdminProposal_FullMethodName:
			c.adminCalls.Add(1)
			admin, ok := request.(*pb.ForwardAdminProposalRequest)
			if ok && group == 2 && c.releaseBlocked.Load() && isPhaseDAcceptanceRelease(admin.GetPayload()) {
				c.releaseFaults.Add(1)
				return nil, status.Error(codes.Unavailable, "injected release RPC failure")
			}
		}
		return next(ctx, request)
	}
}

func (c *phaseDAcceptanceCluster) leader(t *testing.T, group int) int {
	t.Helper()
	leader := -1
	require.Eventually(t, func() bool {
		leader = -1
		for n, node := range c.nodes {
			if node.results[group].Engine.State() == raftengine.StateLeader {
				if leader != -1 {
					return false
				}
				leader = n
			}
		}
		return leader != -1
	}, 10*time.Second, 20*time.Millisecond, "group %d needs exactly one leader", group)
	return leader
}

func (c *phaseDAcceptanceCluster) placeLeader(t *testing.T, group, target int) {
	t.Helper()
	current := c.leader(t, group)
	if current != target {
		admin, ok := c.nodes[current].results[group].Engine.(raftengine.Admin)
		require.True(t, ok)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, admin.TransferLeadershipToServer(ctx, c.nodes[target].id, c.nodes[target].listeners[group].Addr().String()))
	}
	require.Eventually(t, func() bool {
		for _, node := range c.nodes {
			if node.results[group].Engine.Leader().ID != c.nodes[target].id {
				return false
			}
		}
		return c.nodes[target].results[group].Engine.State() == raftengine.StateLeader
	}, 10*time.Second, 20*time.Millisecond, "group %d must be led by %s", group, c.nodes[target].id)
	configuration, err := c.nodes[target].results[group].Engine.Configuration(context.Background())
	require.NoError(t, err)
	require.Len(t, configuration.Servers, 3)
	for _, member := range configuration.Servers {
		require.Equal(t, "voter", member.Suffrage)
	}
}

func (c *phaseDAcceptanceCluster) close(t *testing.T) {
	t.Helper()
	for _, node := range c.nodes {
		if node == nil {
			continue
		}
		node.close(t)
	}
}

func (node *phaseDAcceptanceNode) close(t *testing.T) {
	t.Helper()
	for _, txn := range node.txns {
		if txn != nil {
			txn.Close()
		}
	}
	node.closeEngines(t)
	node.closeRPC(t)
	if node.routed != nil {
		require.NoError(t, node.routed.Close())
	}
	if node.shards != nil {
		require.NoError(t, node.shards.Close())
	}
	for _, shard := range node.groups {
		if shard.Store != nil {
			require.NoError(t, shard.Store.Close())
		}
	}
}

func (node *phaseDAcceptanceNode) closeEngines(t *testing.T) {
	t.Helper()
	for _, result := range node.results {
		if result != nil {
			require.NoError(t, result.Engine.Close())
			if result.Close != nil {
				require.NoError(t, result.Close())
			}
		}
	}
}

func (node *phaseDAcceptanceNode) closeRPC(t *testing.T) {
	t.Helper()
	for group, listener := range node.listeners {
		if node.clients[group] != nil {
			require.NoError(t, node.clients[group].Close())
		}
		if node.servers[group] != nil {
			node.servers[group].Stop()
		} else if listener != nil {
			_ = listener.Close()
		}
	}
}

func phaseDAcceptanceRequest(phase pb.Phase, key string, start, commit uint64) *pb.Request {
	meta := kv.TxnMeta{PrimaryKey: []byte("b-primary"), LockTTLms: 60000, CommitTS: commit}
	if phase == pb.Phase_PREPARE {
		meta.CommitTS, meta.PrepareCommitTS = 0, commit
	}
	return &pb.Request{IsTxn: true, Phase: phase, Ts: start, Mutations: []*pb.Mutation{
		{Op: pb.Op_PUT, Key: []byte(kv.TxnMetaPrefix), Value: kv.EncodeTxnMeta(meta)},
		{Op: pb.Op_PUT, Key: []byte(key), Value: []byte("value-" + key)},
	}}
}

func TestInternalPhaseDAcceptanceThreeVotersThreeGroups(t *testing.T) {
	c := newPhaseDAcceptanceCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	auth := c.adoptCutover(t, ctx)
	start, proof := c.prepare(t, auth)
	window := c.finishAfterLeaderChange(t, auth, proof)
	committed := c.resolve(t, auth, start, proof, window)
	c.verifyReplicas(t, auth, start, proof, window, committed)
	require.Positive(t, c.adminCalls.Load())
	require.Positive(t, c.stateCalls.Load())
	require.GreaterOrEqual(t, c.forwardCalls.Load(), int32(6))
	t.Logf("3 voters x 3 groups; B=%d start=%d proof=%d read=%d; Forward=%d Admin=%d Capability=%d ReleaseFaults=%d",
		window.GetPhaseDFloor(), start, proof, window.GetTimestamp(), c.forwardCalls.Load(), c.adminCalls.Load(), c.stateCalls.Load(), c.releaseFaults.Load())
}

func TestInternalPhaseDAcceptanceReleaseLeaderColocation(t *testing.T) {
	c := newPhaseDAcceptanceCluster(t)
	c.placeLeader(t, 2, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	auth := c.adoptCutover(t, ctx)
	_, proof := c.prepare(t, auth)
	c.finishAfterLeaderChange(t, auth, proof)
}

func (c *phaseDAcceptanceCluster) adoptCutover(t *testing.T, ctx context.Context) context.Context {
	t.Helper()
	auth := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+c.token)
	primaryClient := pb.NewInternalClient(c.nodes[1].clients[1])
	_, err := primaryClient.GetPrepareEvidenceState(ctx, &pb.PrepareEvidenceStateRequest{CapabilityOnly: true})
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	_, err = primaryClient.ForwardAdminProposal(ctx, &pb.ForwardAdminProposalRequest{Payload: kv.EncodePrepareEvidenceFence(0)})
	require.Equal(t, codes.Unauthenticated, status.Code(err))

	tso := pb.NewDistributionClient(c.nodes[0].clients[0])
	_, err = tso.GetTimestamp(auth, &pb.GetTimestampRequest{Count: 8, ActivateCutover: true})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		for _, node := range c.nodes {
			if !node.state.CutoverActive() {
				return false
			}
		}
		return true
	}, 5*time.Second, 10*time.Millisecond)
	for _, node := range c.nodes {
		for _, conn := range node.clients {
			capability, err := pb.NewInternalClient(conn).GetPrepareEvidenceState(auth, &pb.PrepareEvidenceStateRequest{CapabilityOnly: true})
			require.NoError(t, err)
			require.True(t, capability.GetSupported())
			require.True(t, capability.GetDedicatedTsoConfigured())
			require.True(t, capability.GetCutoverApplied())
		}
	}
	return auth
}

func (c *phaseDAcceptanceCluster) prepare(t *testing.T, auth context.Context) (uint64, uint64) {
	t.Helper()
	require.NoError(t, c.nodes[0].shards.CheckPhaseDProtocol(auth))
	start, err := c.nodes[2].batch.Next(auth)
	require.NoError(t, err)
	proof, err := c.nodes[2].batch.NextAfter(auth, start)
	require.NoError(t, err)
	for group, key := range map[uint64]string{1: "b-primary", 2: "x-secondary"} {
		response, err := c.nodes[0].groups[group].Txn.Commit(auth, []*pb.Request{phaseDAcceptanceRequest(pb.Phase_PREPARE, key, start, proof)})
		require.NoError(t, err)
		require.NotZero(t, response.CommitIndex)
	}
	unknown, err := c.nodes[2].shards.PrimaryCommitEvidence(auth, []byte("b-primary"), start)
	require.NoError(t, err)
	require.False(t, unknown.Readable, "the secondary leader must not treat its follower-local primary copy as authoritative")
	return start, proof
}

func (c *phaseDAcceptanceCluster) finishAfterLeaderChange(t *testing.T, auth context.Context, proof uint64) *pb.GetTimestampResponse {
	t.Helper()
	c.releaseBlocked.Store(true)
	previousAllocation := c.nodes[0].state.AllocationFloor()
	tso := pb.NewDistributionClient(c.nodes[0].clients[0])
	_, err := tso.GetTimestamp(auth, &pb.GetTimestampRequest{Count: 2, ActivateCutover: true, ActivatePhaseD: true})
	require.Error(t, err, "release failure must withhold the first Phase-D window")
	require.True(t, c.nodes[0].state.PhaseDActive())
	require.Equal(t, previousAllocation, c.nodes[0].state.AllocationFloor(), "an unfinished release must not reserve a new window")
	barrier := c.nodes[0].state.PhaseDFloor()
	require.GreaterOrEqual(t, barrier, proof)
	require.Positive(t, c.releaseFaults.Load(), "the fault must hit a remote RPC or local release proposal")

	c.placeLeader(t, 0, 1)
	require.Eventually(t, func() bool { return c.nodes[1].state.PhaseDActive() }, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, barrier, c.nodes[1].state.PhaseDFloor())
	_, err = pb.NewDistributionClient(c.nodes[1].clients[0]).GetTimestamp(auth, &pb.GetTimestampRequest{Count: 2})
	require.Error(t, err, "the replacement TSO leader must also finish release before issuing")
	require.Equal(t, previousAllocation, c.nodes[1].state.AllocationFloor())
	c.releaseBlocked.Store(false)
	window, err := pb.NewDistributionClient(c.nodes[1].clients[0]).GetTimestamp(auth, &pb.GetTimestampRequest{Count: 2})
	require.NoError(t, err)
	require.Equal(t, barrier, window.GetPhaseDFloor())
	require.Greater(t, window.GetTimestamp(), barrier)
	require.True(t, window.GetPhaseDActive())
	// The remaining resolution checks use node 2 as the secondary leader.
	c.placeLeader(t, 2, 2)
	return window
}

func (c *phaseDAcceptanceCluster) resolve(t *testing.T, auth context.Context, start, proof uint64, window *pb.GetTimestampResponse) map[uint64]uint64 {
	t.Helper()
	wrong := phaseDAcceptanceRequest(pb.Phase_COMMIT, "b-primary", start, proof+1)
	_, err := c.nodes[0].groups[1].Txn.Commit(auth, []*pb.Request{wrong})
	require.Error(t, err, "first primary commit must match the durable prepare timestamp")
	committed := make(map[uint64]uint64)
	for group, key := range map[uint64]string{1: "b-primary", 2: "x-secondary"} {
		response, err := c.nodes[0].groups[group].Txn.Commit(auth, []*pb.Request{phaseDAcceptanceRequest(pb.Phase_COMMIT, key, start, proof)})
		require.NoError(t, err)
		require.NotZero(t, response.CommitIndex)
		committed[group] = response.CommitIndex
	}
	_, err = c.nodes[0].groups[1].Txn.Commit(auth, []*pb.Request{phaseDAcceptanceRequest(pb.Phase_PREPARE, "b-forged", start, proof)})
	require.Error(t, err, "a new pre-Phase-D prepare must not use an old valid timestamp as fresh evidence")
	forged := &pb.Request{Ts: ^uint64(0) - 1, TimestampAdmissionFloor: window.GetPhaseDFloor(),
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("b-forged-epoch"), Value: []byte("forged")}}}
	_, err = c.nodes[0].groups[1].Txn.Commit(auth, []*pb.Request{forged})
	require.Error(t, err, "an incoming epoch cannot authorize an unreserved future timestamp")
	fresh := &pb.Request{Ts: window.GetTimestamp(), Mutations: []*pb.Mutation{
		{Op: pb.Op_PUT, Key: []byte("b-new-local"), Value: []byte("new-local")},
	}}
	_, err = c.nodes[1].groups[1].Txn.Commit(auth, []*pb.Request{fresh})
	require.NoError(t, err, "the leader-local transaction manager must stamp the new epoch too")
	require.Zero(t, fresh.GetTimestampAdmissionFloor(), "local admission must clone the caller's request before enqueueing")
	voucherProof := window.GetTimestamp() + 1
	voucherPrepare := phaseDAcceptanceRequest(pb.Phase_PREPARE, "b-voucher", start, voucherProof)
	voucherPrepare.Mutations[0].Value = kv.EncodeTxnMeta(kv.TxnMeta{
		PrimaryKey: []byte("b-voucher"), PrepareCommitTS: voucherProof, LockTTLms: 60000,
	})
	_, err = c.nodes[0].groups[1].Txn.Commit(auth, []*pb.Request{voucherPrepare})
	require.NoError(t, err, "a fresh post-D proof may retain a pre-D applied-read voucher")
	voucherCommit := phaseDAcceptanceRequest(pb.Phase_COMMIT, "b-voucher", start, voucherProof)
	voucherCommit.Mutations[0].Value = kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("b-voucher"), CommitTS: voucherProof})
	voucherResponse, err := c.nodes[0].groups[1].Txn.Commit(auth, []*pb.Request{voucherCommit})
	require.NoError(t, err)
	committed[1] = voucherResponse.CommitIndex
	return committed
}

func (c *phaseDAcceptanceCluster) verifyReplicas(t *testing.T, auth context.Context, start, proof uint64, window *pb.GetTimestampResponse, committed map[uint64]uint64) {
	t.Helper()
	voucherProof := window.GetTimestamp() + 1
	for _, node := range c.nodes {
		require.Eventually(t, func() bool {
			for group, key := range map[uint64]string{1: "b-primary", 2: "x-secondary"} {
				if node.groups[group].Engine.Status().AppliedIndex < committed[group] {
					return false
				}
				value, readErr := node.groups[group].Store.GetAt(auth, []byte(key), proof)
				if readErr != nil || !bytes.Equal(value, []byte("value-"+key)) {
					return false
				}
			}
			return true
		}, 5*time.Second, 10*time.Millisecond, "committed values must reach %s in both data groups", node.id)
		for group, key := range map[uint64]string{1: "b-primary", 2: "x-secondary"} {
			_, err := node.groups[group].Store.GetAt(auth, []byte(key), start)
			require.ErrorIs(t, err, store.ErrKeyNotFound)
		}
		_, err := node.groups[1].Store.GetAt(auth, []byte("b-forged"), ^uint64(0))
		require.ErrorIs(t, err, store.ErrKeyNotFound)
		_, err = node.groups[1].Store.GetAt(auth, []byte("b-forged-epoch"), ^uint64(0))
		require.ErrorIs(t, err, store.ErrKeyNotFound)
		require.Eventually(t, func() bool {
			value, readErr := node.groups[1].Store.GetAt(auth, []byte("b-new-local"), window.GetTimestamp())
			if readErr != nil || !bytes.Equal(value, []byte("new-local")) {
				return false
			}
			value, readErr = node.groups[1].Store.GetAt(auth, []byte("b-voucher"), voucherProof)
			return readErr == nil && bytes.Equal(value, []byte("value-b-voucher"))
		}, 5*time.Second, 10*time.Millisecond)
	}
}
