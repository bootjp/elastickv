package adapter

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/raftengine"
	etcdraft "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/internal/raftengine/raftenginetest"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	quorumAcceptanceGroup          = uint64(7)
	quorumAcceptanceCatalogVersion = uint64(4)
	quorumAcceptanceTick           = 10 * time.Millisecond
	quorumAcceptanceElectionTicks  = 500
	quorumAcceptanceHeartbeatTicks = 10
	quorumAcceptanceElectionWait   = 3 * quorumAcceptanceTick * quorumAcceptanceElectionTicks
	quorumAcceptanceSnapshotEvery  = 8
)

// Real data-group elections, WAL, transport, ConfChanges and native snapshots.
// Internal handlers are called directly; this is NOT peer-auth/TLS acceptance.
// The root below is an authoritative immutable manifest stub, NOT a root Raft
// quorum or proof of atomic root-catalog publication. No production gate opens.
type quorumAcceptanceNode struct {
	peer                       etcdraft.Peer
	dir                        string
	transport                  *etcdraft.GRPCTransport
	server                     *grpc.Server
	listener                   net.Listener
	engine                     *etcdraft.Engine
	store                      store.MVCCStore
	fsm                        raftengine.StateMachine
	tx                         *kv.TransactionManager
	internal                   *Internal
	client                     *quorumAcceptanceClient
	namespace, partition, size atomic.Bool
	mode7, mode8, guards       atomic.Uint64
	writeFences, timestamps    atomic.Uint64
	exports                    []*pb.ExportRangeVersionsRequest
}

type quorumAcceptanceClient struct {
	SplitMigrationClient
	node *quorumAcceptanceNode
}

func (c *quorumAcceptanceClient) ProbeMigrationState(ctx context.Context, req *pb.ProbeMigrationStateRequest, _ ...grpc.CallOption) (*pb.ProbeMigrationStateResponse, error) {
	return c.node.internal.ProbeMigrationState(ctx, req)
}

func (c *quorumAcceptanceClient) CleanupMigration(ctx context.Context, req *pb.CleanupMigrationRequest, _ ...grpc.CallOption) (*pb.CleanupMigrationResponse, error) {
	if req.GetMode() == pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_OWNERSHIP_PARTITION {
		c.node.mode7.Add(1)
	}
	if req.GetMode() == pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_NAMESPACE_ADMISSION {
		c.node.mode8.Add(1)
	}
	return c.node.internal.CleanupMigration(ctx, req)
}

func (c *quorumAcceptanceClient) ApplyTargetStagedReadiness(ctx context.Context, req *pb.TargetStagedReadinessRequest, _ ...grpc.CallOption) (*pb.TargetStagedReadinessResponse, error) {
	if req.GetTransferSizeGuardVersion() != 0 {
		c.node.guards.Add(1)
	}
	if req.GetSourceWriteFence() {
		c.node.writeFences.Add(1)
	}
	return c.node.internal.ApplyTargetStagedReadiness(ctx, req)
}

func (c *quorumAcceptanceClient) ProbeMigrationLocks(ctx context.Context, req *pb.ProbeMigrationLocksRequest, _ ...grpc.CallOption) (*pb.ProbeMigrationLocksResponse, error) {
	return c.node.internal.ProbeMigrationLocks(ctx, req)
}

func (c *quorumAcceptanceClient) IssueMigrationTimestamp(ctx context.Context, req *pb.IssueMigrationTimestampRequest, _ ...grpc.CallOption) (*pb.IssueMigrationTimestampResponse, error) {
	c.node.timestamps.Add(1)
	return c.node.internal.IssueMigrationTimestamp(ctx, req)
}

func (c *quorumAcceptanceClient) ExportRangeVersions(ctx context.Context, req *pb.ExportRangeVersionsRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[pb.ExportRangeVersionsResponse], error) {
	c.node.exports = append(c.node.exports, proto.CloneOf(req))
	capture := &captureExportRangeVersionsStream{ctx: ctx}
	err := c.node.internal.ExportRangeVersions(req, capture)
	return &quorumAcceptanceExportStream{splitMigrationStreamStub: &splitMigrationStreamStub{responses: capture.responses}, terminal: err}, nil
}

type quorumAcceptanceExportStream struct {
	*splitMigrationStreamStub
	terminal error
}

func (s *quorumAcceptanceExportStream) Recv() (*pb.ExportRangeVersionsResponse, error) {
	response, err := s.splitMigrationStreamStub.Recv()
	if errors.Is(err, io.EOF) && s.terminal != nil {
		return nil, s.terminal
	}
	return response, err
}

type quorumAcceptanceCluster struct{ nodes []*quorumAcceptanceNode }

func newQuorumAcceptanceCluster(t *testing.T, ctx context.Context) *quorumAcceptanceCluster {
	t.Helper()
	c := &quorumAcceptanceCluster{}
	for index := range 4 {
		listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
		require.NoError(t, err)
		id := fmt.Sprintf("quorum-%d", index+1)
		n := &quorumAcceptanceNode{peer: etcdraft.Peer{NodeID: etcdraft.DeriveNodeID(id), ID: id, Address: listener.Addr().String()}, dir: t.TempDir(), listener: listener}
		n.namespace.Store(true)
		n.partition.Store(true)
		n.size.Store(true)
		c.nodes = append(c.nodes, n)
	}
	peers := make([]etcdraft.Peer, len(c.nodes))
	for index, n := range c.nodes {
		peers[index] = n.peer
	}
	for _, n := range c.nodes {
		n.transport = etcdraft.NewGRPCTransport(peers)
		n.server = grpc.NewServer(internalutil.GRPCServerOptions()...)
		n.transport.Register(n.server)
		go func() { _ = n.server.Serve(n.listener) }()
	}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			if n.tx != nil {
				n.tx.Close()
			}
			if n.engine != nil {
				require.NoError(t, n.engine.Close())
			}
			n.server.Stop()
			require.NoError(t, n.transport.Close())
			if n.store != nil {
				require.NoError(t, n.store.Close())
			}
		}
	})
	var wg sync.WaitGroup
	errs := make(chan error, 3)
	for _, n := range c.nodes[:3] {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- n.open(ctx, peers[:3], true) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	c.leader(t)
	c.waitConfiguration(t, 3)
	return c
}

func (n *quorumAcceptanceNode) open(ctx context.Context, peers []etcdraft.Peer, bootstrap bool) error {
	n.store = store.NewMVCCStore()
	n.fsm = kv.NewKvFSMWithHLC(n.store, nil, kv.WithRouteHistory(nil, quorumAcceptanceGroup))
	engine, err := etcdraft.Open(ctx, etcdraft.OpenConfig{NodeID: n.peer.NodeID, LocalID: n.peer.ID, LocalAddress: n.peer.Address,
		DataDir: n.dir, Peers: peers, Bootstrap: bootstrap, JoinAsLearner: !bootstrap && len(peers) != 0, Transport: n.transport, StateMachine: n.fsm,
		// Large-value acceptance is not a one-second failover latency test.
		// Race-instrumented 64 MiB applies on a small CI runner must fit the
		// modeled election window. Explicit transfers still exercise churn.
		TickInterval: quorumAcceptanceTick, ElectionTick: quorumAcceptanceElectionTicks, HeartbeatTick: quorumAcceptanceHeartbeatTicks})
	if err != nil {
		return errors.WithStack(err)
	}
	n.engine = engine
	n.tx = kv.NewTransactionWithProposer(engine, kv.WithMigrationRequestAdmission(n.store, quorumAcceptanceGroup))
	n.internal = NewInternalWithEngine(n.tx, engine, nil, nil, WithInternalStore(n.store), WithInternalMigrationProposer(engine),
		WithInternalMigrationCleanupGate(func(context.Context) error { return nil }),
		WithInternalMigrationOwnershipReady(func() bool { return kv.MigrationRequestAdmissionReady(n.tx) }),
		WithMigrationNamespaceAdmissionReady(func() bool {
			return n.namespace.Load() && kv.MigrationNamespaceInstalledVersion(n.store, n.fsm) == kv.MigrationNamespaceVersion
		}),
		WithInternalMigrationPartitionProtocolReady(func() bool { return n.partition.Load() && kv.MigrationPartitionRequestAdmissionReady(n.tx) }),
		WithInternalMigrationTransferSizeGuardReady(func() bool { return n.size.Load() && kv.MigrationRequestAdmissionReady(n.tx) }))
	n.client = &quorumAcceptanceClient{node: n}
	return nil
}

func (c *quorumAcceptanceCluster) leader(t *testing.T) *quorumAcceptanceNode {
	t.Helper()
	var views []raftengine.LeaderView
	var active []*quorumAcceptanceNode
	for _, n := range c.nodes {
		if n.engine != nil {
			views = append(views, n.engine)
			active = append(active, n)
		}
	}
	return active[raftenginetest.WaitForLeader(t, quorumAcceptanceElectionWait, views...)]
}

func (c *quorumAcceptanceCluster) waitConfiguration(t *testing.T, count int) uint64 {
	t.Helper()
	var epoch uint64
	require.Eventually(t, func() bool {
		epoch = 0
		for _, n := range c.nodes[:count] {
			cfg, err := n.engine.Configuration(t.Context())
			index, supported := n.engine.AppliedConfigurationIndex()
			if err != nil || !supported || index == 0 || len(cfg.Servers) != count || n.engine.Status().PendingConfChange {
				return false
			}
			if epoch != 0 && epoch != index {
				return false
			}
			epoch = index
		}
		return true
	}, 5*time.Second, 20*time.Millisecond)
	return epoch
}

func (c *quorumAcceptanceCluster) members(ctx context.Context, groupID uint64) ([]SplitMigrationVoter, error) {
	if groupID != quorumAcceptanceGroup {
		return nil, errors.New("quorum fixture has no runtime for the requested group")
	}
	var engine *etcdraft.Engine
	for _, n := range c.nodes {
		if n.engine != nil && n.engine.State() == raftengine.StateLeader {
			engine = n.engine
			break
		}
	}
	if engine == nil {
		return nil, errors.WithStack(raftengine.ErrNotLeader)
	}
	cfg, err := engine.Configuration(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	var members []SplitMigrationVoter
	for _, configured := range cfg.Servers {
		member, err := c.configuredMember(configured.ID)
		if err != nil {
			return nil, err
		}
		members = append(members, member)
	}
	return members, nil
}

func (c *quorumAcceptanceCluster) configuredMember(id string) (SplitMigrationVoter, error) {
	for _, n := range c.nodes {
		if n.peer.ID == id {
			return SplitMigrationVoter{ID: n.peer.ID, Address: n.peer.Address, Client: n.client}, nil
		}
	}
	return SplitMigrationVoter{}, errors.New("configured apply member missing from real cluster")
}

// The stub models a committed immutable root manifest and current descriptors.
// All data-group controls/ACKs still go through real Raft and real stores.
type quorumAcceptanceRoot struct {
	manifest distribution.OwnershipPartitionManifest
	later    *distribution.OwnershipPartitionManifest
}

func (r quorumAcceptanceRoot) version() uint64 {
	if r.later != nil {
		return r.later.Version
	}
	return r.manifest.Version
}
func (r quorumAcceptanceRoot) routes() []distribution.RouteDescriptor {
	if r.later == nil {
		return r.manifest.Children
	}
	return append([]distribution.RouteDescriptor{r.manifest.Children[0]}, r.later.Children...)
}

func (r quorumAcceptanceRoot) AttestMigrationOwnership(_ context.Context, req *pb.AttestMigrationOwnershipRequest, _ ...grpc.CallOption) (*pb.AttestMigrationOwnershipResponse, error) {
	partitions, legacy, err := decodeMigrationPartitionPriors(req)
	if err != nil {
		return nil, err
	}
	prior, epoch := migrationPartitionAttestationPrior(partitions, legacy)
	var raw []byte
	var unchanged *distribution.RouteDescriptor
	switch {
	case prior.RouteID == r.manifest.Parent.RouteID:
		raw, err = distribution.EncodeOwnershipPartitionManifest(r.manifest)
	case r.later != nil && prior.RouteID == r.later.Parent.RouteID:
		raw, err = distribution.EncodeOwnershipPartitionManifest(*r.later)
	default:
		for _, route := range r.routes() {
			if route.RouteID == prior.RouteID {
				copy := route
				unchanged = &copy
			}
		}
	}
	if err != nil {
		return nil, err
	}
	control, err := kv.BuildMigrationPartitionControl(partitions, legacy, raw, unchanged, r.version(), epoch)
	if err != nil {
		return nil, err
	}
	encoded, err := kv.EncodeMigrationPartitionControl(control)
	return &pb.AttestMigrationOwnershipResponse{OwnershipProtocol: migrationOwnershipProtocol, PartitionTransition: encoded}, err
}

func (c *quorumAcceptanceCluster) consumer(t *testing.T, leader *quorumAcceptanceNode, root quorumAcceptanceRoot) *DistributionServer {
	t.Helper()
	mirror := distribution.NewEngine()
	require.NoError(t, mirror.ApplySnapshot(distribution.CatalogSnapshot{Version: root.version(), Routes: root.routes()}))
	groups := map[uint64]*kv.ShardGroup{quorumAcceptanceGroup: {Store: leader.store, Txn: leader.tx, Engine: leader.engine}}
	return NewDistributionServer(mirror, nil, WithSplitMigrationGroupClientFactory(func(_ context.Context, groupID uint64) (SplitMigrationClient, error) {
		if groupID != quorumAcceptanceGroup {
			return nil, errors.New("quorum fixture has no client for the requested group")
		}
		return leader.client, nil
	}),
		WithSplitMigrationVoterFactory(c.members), WithMigrationOwnershipRefresh(groups, func(context.Context) (MigrationOwnershipAttestor, error) { return root, nil }),
		WithMigrationPartitionRuntime(kv.NewShardStore(mirror, groups), c.members),
		WithSplitMigrationApplyMemberSnapshotFactory(func(ctx context.Context, groupID uint64) (SplitMigrationApplyMemberSnapshot, error) {
			members, err := c.members(ctx, groupID)
			index, supported := leader.engine.AppliedConfigurationIndex()
			if !supported || leader.engine.Status().PendingConfChange {
				return SplitMigrationApplyMemberSnapshot{}, errors.WithStack(kv.ErrMigrationConfigurationChanged)
			}
			return SplitMigrationApplyMemberSnapshot{ConfigurationIndex: index, Members: members}, err
		}))
}

func quorumAcceptanceEventually(t *testing.T, operation func() error) {
	t.Helper()
	var last error
	require.Eventually(t, func() bool { last = operation(); return last == nil }, 5*time.Second, 20*time.Millisecond, "last protocol error: %v", &last)
	require.NoError(t, last)
}

func quorumAcceptanceAcquire(t *testing.T, ctx context.Context, n *quorumAcceptanceNode, id, epoch, version uint64, route distribution.RouteDescriptor, start string) *pb.TargetStagedReadinessRequest {
	t.Helper()
	control := splitMigrationControlRequest(distribution.SplitJob{JobID: id, SplitKey: []byte(start)}, route.End, version, 1, false, false, true, 1)
	control.RequireConfigurationIndex, control.ExpectedConfigurationIndex = true, epoch
	_, err := n.client.ApplyTargetStagedReadiness(ctx, control)
	require.NoError(t, err)
	hash, err := distribution.OwnershipPartitionDescriptorHash(route)
	require.NoError(t, err)
	_, err = n.client.CleanupMigration(ctx, &pb.CleanupMigrationRequest{JobId: id, Mode: pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_SOURCE_OWNERSHIP_ACQUIRE,
		RequireConfigurationIndex: true, ExpectedConfigurationIndex: epoch,
		Ownership: &pb.MigrationOwnershipProof{Generation: "0123456789abcdef0123456789abcdef", LocalGroupId: quorumAcceptanceGroup, OwnerGroupId: quorumAcceptanceGroup,
			CatalogVersion: version, RouteId: route.RouteID, ParentRouteId: route.ParentRouteID, RouteStart: []byte(start), RouteEnd: route.End,
			MinWriteTsExclusive: route.MinWriteTSExclusive, WriteFloorKnown: true, SourceTrackerVersion: version, DescriptorHash: hash, Role: pb.MigrationOwnershipRole_MIGRATION_OWNERSHIP_ROLE_OWNER}})
	require.NoError(t, err)
	return control
}

func (c *quorumAcceptanceCluster) completeInitialOwner(t *testing.T, ctx context.Context, leader *quorumAcceptanceNode, epoch uint64, route distribution.RouteDescriptor) {
	t.Helper()
	quorumAcceptanceAcquire(t, ctx, leader, 11, epoch, 1, route, "a")
	hash, err := distribution.OwnershipPartitionDescriptorHash(route)
	require.NoError(t, err)
	// The authoritative root stub has committed the unchanged-owner rollback
	// at V2. Retire the SOURCE tracker atomically with its terminal OWNER proof;
	// leaving it armed would model an invalid pair of overlapping live jobs.
	proof := &pb.MigrationOwnershipProof{Generation: "0123456789abcdef0123456789abcdef", LocalGroupId: quorumAcceptanceGroup, OwnerGroupId: quorumAcceptanceGroup,
		CatalogVersion: 2, RouteId: route.RouteID, RouteStart: route.Start, RouteEnd: route.End, MinWriteTsExclusive: route.MinWriteTSExclusive,
		WriteFloorKnown: true, DescriptorHash: hash, Role: pb.MigrationOwnershipRole_MIGRATION_OWNERSHIP_ROLE_OWNER}
	_, err = leader.client.CleanupMigration(ctx, &pb.CleanupMigrationRequest{JobId: 11, Mode: pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_OWNERSHIP_CLEAR,
		RequireConfigurationIndex: true, ExpectedConfigurationIndex: epoch, Ownership: proof})
	require.NoError(t, err)
	quorumAcceptanceEventually(t, func() error {
		for _, n := range c.nodes[:3] {
			ack, err := n.client.ProbeMigrationState(ctx, &pb.ProbeMigrationStateRequest{JobId: 11, Kind: pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_OWNERSHIP_APPLIED,
				RequireConfigurationIndex: true, ExpectedConfigurationIndex: epoch, Ownership: proof})
			if err != nil {
				return err
			}
			if !ack.GetReady() || !ack.GetOwnershipCleared() || ack.GetAppliedConfigurationIndex() != epoch {
				return errors.New("completed initial owner is not ACKed by every actual voter")
			}
		}
		return nil
	})
	for _, n := range c.nodes[:3] {
		reader, ok := n.store.(store.MigrationOwnershipStore)
		require.True(t, ok)
		view, err := reader.MigrationServingState(ctx)
		require.NoError(t, err)
		require.Empty(t, view.Readiness, "native ownership clear must remove the old guard, not raw fixture cleanup")
		require.Len(t, view.Ownership, 1)
		require.True(t, store.MigrationOwnershipEvidenceValid(view.Ownership[0]))
		require.Zero(t, view.Ownership[0].SourceTrackerVersion)
	}
}

func TestMigrationQuorumPartitionAcceptance(t *testing.T) {
	t.Setenv("ELASTICKV_RAFT_SNAPSHOT_COUNT", fmt.Sprint(quorumAcceptanceSnapshotEvery))
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	c := newQuorumAcceptanceCluster(t, ctx)
	leader := c.leader(t)
	epoch := c.waitConfiguration(t, 3)
	members, err := c.members(ctx, quorumAcceptanceGroup)
	require.NoError(t, err)
	c.nodes[2].namespace.Store(false)
	require.Error(t, EnsureMigrationNamespaceAdmission(ctx, leader.client, members))
	require.Zero(t, leader.mode8.Load(), "unsupported member must prevent namespace proposal")
	c.nodes[2].namespace.Store(true)
	quorumAcceptanceEventually(t, func() error { return EnsureMigrationNamespaceAdmission(ctx, leader.client, members) })
	parent := distribution.RouteDescriptor{RouteID: 1, Start: []byte("a"), End: []byte("z"), GroupID: quorumAcceptanceGroup, MinWriteTSExclusive: 1}
	c.completeInitialOwner(t, ctx, leader, epoch, parent)
	left, right := parent, parent
	left.RouteID, left.ParentRouteID, left.End = 2, 1, []byte("m")
	right.RouteID, right.ParentRouteID, right.Start = 3, 1, []byte("m")
	left.SplitAtHLC, right.SplitAtHLC = 1, 1
	root := quorumAcceptanceRoot{manifest: distribution.OwnershipPartitionManifest{PreviousVersion: 2, Version: 3, Parent: parent, Children: []distribution.RouteDescriptor{left, right}}}
	consumer := c.consumer(t, leader, root)
	c.nodes[2].partition.Store(false)
	require.Error(t, consumer.reconcileMigrationPartitions(ctx, quorumAcceptanceGroup))
	require.Zero(t, leader.mode7.Load(), "field10=0 must prevent partition proposal despite real voter quorum")
	c.nodes[2].partition.Store(true)
	quorumAcceptanceEventually(t, func() error { return consumer.reconcileMigrationPartitions(ctx, quorumAcceptanceGroup) })
	_, err = leader.tx.Commit(ctx, []*pb.Request{quorumAcceptancePrepare(pb.Phase_PREPARE, 3)})
	require.NoError(t, err, "retained-left PREPARE precedes the later split, learner join and restart")
	kept, moving := right, right
	kept.RouteID, kept.ParentRouteID, kept.End, kept.SplitAtHLC = 4, 3, []byte("t"), 2
	moving.RouteID, moving.ParentRouteID, moving.Start, moving.SplitAtHLC = 5, 3, []byte("t"), 2
	root.later = &distribution.OwnershipPartitionManifest{PreviousVersion: 3, Version: quorumAcceptanceCatalogVersion, Parent: right, Children: []distribution.RouteDescriptor{kept, moving}}
	consumer = c.consumer(t, leader, root)
	quorumAcceptanceEventually(t, func() error { return consumer.reconcileMigrationPartitions(ctx, quorumAcceptanceGroup) })
	control := quorumAcceptanceAcquire(t, ctx, leader, 12, epoch, quorumAcceptanceCatalogVersion, moving, "t")
	job := distribution.SplitJob{JobID: 12, SourceRouteID: 5, SplitKey: []byte("t")}
	c.nodes[2].size.Store(false)
	_, err = consumer.ensureSplitJobTransferControl(ctx, job, quorumAcceptanceGroup, leader.client, control, nil)
	require.Error(t, err)
	require.Zero(t, leader.guards.Load(), "field9=0 must prevent guard proposal")
	c.nodes[2].size.Store(true)
	var witness *splitTransferAdmissionWitness
	quorumAcceptanceEventually(t, func() error {
		witness, err = consumer.ensureSplitJobTransferControl(ctx, job, quorumAcceptanceGroup, leader.client, control, nil)
		return err
	})
	require.NoError(t, consumer.revalidateSplitTransferAdmission(ctx, witness))
	c.exerciseLearner(t, ctx, leader, consumer, job, control, witness, root)
}

func (c *quorumAcceptanceCluster) exerciseLearner(t *testing.T, ctx context.Context, leader *quorumAcceptanceNode, consumer *DistributionServer, job distribution.SplitJob, control *pb.TargetStagedReadinessRequest, oldWitness *splitTransferAdmissionWitness, root quorumAcceptanceRoot) {
	t.Helper()
	joining := c.nodes[3]
	peers := make([]etcdraft.Peer, len(c.nodes))
	for index, n := range c.nodes {
		peers[index] = n.peer
	}
	require.NoError(t, joining.open(ctx, peers, false))
	joiningEpoch, joiningSupported := joining.engine.AppliedConfigurationIndex()
	require.False(t, joiningSupported)
	require.Zero(t, joiningEpoch, "prejoin negotiation must not invent a candidate epoch")
	installed, err := joining.client.ProbeMigrationState(ctx, &pb.ProbeMigrationStateRequest{JobId: 1, Kind: pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_NAMESPACE_ADMISSION})
	require.NoError(t, err)
	require.Equal(t, kv.MigrationNamespaceVersion, installed.GetNamespaceAdmissionVersion())
	require.False(t, installed.GetReady())
	oldEpoch, supported := leader.engine.AppliedConfigurationIndex()
	require.True(t, supported)
	quorumAcceptanceEventually(t, func() error {
		return EnsureMigrationNamespaceAdmissionWithSnapshot(ctx, c.namespaceSnapshot, func(ctx context.Context, command kv.NamespaceAdmissionCommand) error {
			req, err := kv.NewNamespaceAdmissionRequest(1, command)
			if err != nil {
				return err
			}
			_, err = leader.client.CleanupMigration(ctx, req)
			return err
		}, []NamespaceAdmissionPeer{{MemberID: joining.peer.NodeID, Address: joining.peer.Address, Client: joining.client}})
	})
	require.NoError(t, kv.CheckMigrationNamespacePrejoin(ctx, leader.store, oldEpoch, joining.peer.NodeID, kv.MigrationNamespaceVersion))
	newEpoch, err := leader.engine.AddLearner(ctx, joining.peer.ID, joining.peer.Address, oldEpoch)
	require.NoError(t, err)
	require.Greater(t, newEpoch, oldEpoch, "test must observe a real ConfChange, not reuse old ACKs")
	t.Logf("real ConfChange: epoch %d -> %d; three voters plus one learner", oldEpoch, newEpoch)
	require.Equal(t, newEpoch, c.waitConfiguration(t, 4))
	cfg, err := leader.engine.Configuration(ctx)
	require.NoError(t, err)
	var voters, learners int
	for _, member := range cfg.Servers {
		if member.Suffrage == etcdraft.SuffrageVoter {
			voters++
		}
		if member.Suffrage == etcdraft.SuffrageLearner {
			learners++
			require.Equal(t, joining.peer.ID, member.ID)
		}
	}
	require.Equal(t, 3, voters)
	require.Equal(t, 1, learners)
	require.Error(t, consumer.revalidateSplitTransferAdmission(ctx, oldWitness), "persisted guard does not authorize an old-epoch witness")
	members, err := c.members(ctx, quorumAcceptanceGroup)
	require.NoError(t, err)
	require.Len(t, members, 4)
	joining.namespace.Store(false)
	before := leader.mode8.Load()
	require.Error(t, EnsureMigrationNamespaceAdmission(ctx, leader.client, members))
	require.Equal(t, before, leader.mode8.Load())
	joining.namespace.Store(true)
	quorumAcceptanceEventually(t, func() error { return EnsureMigrationNamespaceAdmission(ctx, leader.client, members) })
	joining.partition.Store(false)
	before = leader.mode7.Load()
	require.Error(t, consumer.reconcileMigrationPartitions(ctx, quorumAcceptanceGroup))
	require.Equal(t, before, leader.mode7.Load())
	joining.partition.Store(true)
	quorumAcceptanceEventually(t, func() error { return consumer.reconcileMigrationPartitions(ctx, quorumAcceptanceGroup) })
	joining.size.Store(false)
	before = leader.guards.Load()
	fresh := splitMigrationControlRequest(job, control.GetRouteEnd(), control.GetExpectedCutoverVersion(), control.GetMinWriteTsExclusive(), false, false, true, 1)
	_, err = consumer.ensureSplitJobTransferControl(ctx, job, quorumAcceptanceGroup, leader.client, fresh, nil)
	require.Error(t, err)
	require.Equal(t, before, leader.guards.Load())
	joining.size.Store(true)
	var witness *splitTransferAdmissionWitness
	quorumAcceptanceEventually(t, func() error {
		witness, err = consumer.ensureSplitJobTransferControl(ctx, job, quorumAcceptanceGroup, leader.client, fresh, nil)
		return err
	})
	require.Equal(t, newEpoch, witness.snapshot.ConfigurationIndex)
	require.Len(t, witness.snapshot.Members, 4)
	c.checkReplicas(t, ctx, newEpoch)
	c.retainedAndRestart(t, ctx, root, newEpoch)
}

func (c *quorumAcceptanceCluster) namespaceSnapshot(ctx context.Context) (NamespaceAdmissionSnapshot, error) {
	members, err := c.members(ctx, quorumAcceptanceGroup)
	if err != nil {
		return NamespaceAdmissionSnapshot{}, err
	}
	result := NamespaceAdmissionSnapshot{}
	for _, member := range members {
		response, err := member.Client.ProbeMigrationState(ctx, &pb.ProbeMigrationStateRequest{JobId: 1, Kind: pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_NAMESPACE_ADMISSION})
		if err != nil {
			return result, err
		}
		if result.ConfigurationIndex != 0 && result.ConfigurationIndex != response.GetAppliedConfigurationIndex() {
			return result, errors.WithStack(kv.ErrMigrationConfigurationChanged)
		}
		result.ConfigurationIndex = response.GetAppliedConfigurationIndex()
		result.Members = append(result.Members, NamespaceAdmissionPeer{MemberID: etcdraft.DeriveNodeID(member.ID), Address: member.Address, Client: member.Client})
	}
	return result, nil
}

func (c *quorumAcceptanceCluster) checkReplicas(t *testing.T, ctx context.Context, epoch uint64) {
	t.Helper()
	var first store.MigrationOwnershipPartitionState
	for index, n := range c.nodes {
		require.NoError(t, kv.RequireMigrationNamespaceAdmission(ctx, n.store))
		actual, err := kv.MigrationConfigurationIndex(ctx, n.store)
		require.NoError(t, err)
		require.Equal(t, epoch, actual)
		reader, ok := n.store.(store.MigrationOwnershipPartitionStore)
		require.True(t, ok)
		state, err := reader.MigrationOwnershipPartitions(ctx)
		require.NoError(t, err)
		require.NotEmpty(t, state.Partitions)
		require.NotEmpty(t, state.Receipts)
		for _, partition := range state.Partitions {
			require.Equal(t, epoch, partition.ConfigurationIndex)
		}
		if index == 0 {
			first = state
		} else {
			require.Equal(t, first, state)
		}
		response, err := n.client.ProbeMigrationState(ctx, &pb.ProbeMigrationStateRequest{JobId: 12, Kind: pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_OWNERSHIP_CAPABILITY, RequireConfigurationIndex: true, ExpectedConfigurationIndex: epoch})
		require.NoError(t, err)
		require.True(t, response.GetReady())
		require.Equal(t, uint32(1), response.GetTransferSizeGuardVersion())
		require.Equal(t, uint32(1), response.GetOwnershipPartitionProtocol())
		require.Equal(t, uint32(1), response.GetNamespaceAdmissionVersion())
		guard := &pb.ProbeMigrationStateRequest{JobId: 12, Kind: pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_CONTROL_APPLIED,
			RouteStart: []byte("t"), RouteEnd: []byte("z"), ExpectedCatalogVersion: quorumAcceptanceCatalogVersion, MigrationJobId: 12,
			MinWriteTsExclusive: 1, RetentionPinTs: 1, TrackWrites: true, TransferSizeGuardVersion: 1,
			RequireConfigurationIndex: true, ExpectedConfigurationIndex: epoch}
		response, err = n.client.ProbeMigrationState(ctx, guard)
		require.NoError(t, err)
		require.True(t, response.GetReady())
		require.Equal(t, epoch, response.GetAppliedConfigurationIndex())
		// Advisory admission checks ownership, not the encoded-row size verdict.
		// The public leader commits below exercise the real FSM's typed guard.
		require.NoError(t, kv.CheckMigrationRequest(ctx, n.store, quorumAcceptanceGroup, quorumAcceptanceWriteRequest("u", []byte("moving-small"), 101)),
			"every configured member must restore coherent moving-range ownership admission")
		require.NoError(t, kv.CheckMigrationRequest(ctx, n.store, quorumAcceptanceGroup, quorumAcceptanceWriteRequest("b", []byte("left"), 110)))
		guards, ok := n.store.(store.MigrationTargetReadinessReader)
		require.True(t, ok)
		states, err := guards.MigrationTargetReadinessStates(ctx)
		require.NoError(t, err)
		require.Len(t, states, 1, "completed job11 must not remain a live overlapping SOURCE guard")
		require.Equal(t, uint64(12), states[0].JobID)
		require.Equal(t, uint32(1), states[0].TransferSizeGuardVersion)
	}
}

func quorumAcceptanceWriteRequest(key string, value []byte, timestamp uint64) *pb.Request {
	return &pb.Request{Ts: timestamp, ObservedRouteVersion: kv.EncodeObservedRouteVersion(quorumAcceptanceCatalogVersion), Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte(key), Value: value}}}
}

func (c *quorumAcceptanceCluster) commitToCurrentLeader(ctx context.Context, requests []*pb.Request) (*quorumAcceptanceNode, *kv.TransactionResponse, error) {
	ticker := time.NewTicker(splitPartitionApplyPoll)
	defer ticker.Stop()
	for {
		if leader := c.currentLeader(); leader != nil {
			// Selection is not a read barrier. A newly elected voter must apply
			// its committed ownership controls before local request admission.
			_, err := leader.engine.LinearizableRead(ctx)
			if err == nil {
				response, commitErr := leader.tx.Commit(ctx, requests)
				if !errors.Is(commitErr, raftengine.ErrNotLeader) {
					return leader, response, commitErr
				}
			} else if !errors.Is(err, raftengine.ErrNotLeader) {
				return leader, nil, err
			}
		}
		select {
		case <-ctx.Done():
			return nil, nil, errors.WithStack(ctx.Err())
		case <-ticker.C:
		}
	}
}

func (c *quorumAcceptanceCluster) currentLeader() *quorumAcceptanceNode {
	for _, node := range c.nodes {
		if node.engine != nil && node.engine.State() == raftengine.StateLeader {
			return node
		}
	}
	return nil
}

func (c *quorumAcceptanceCluster) write(ctx context.Context, key string, value []byte, timestamp uint64) (*quorumAcceptanceNode, uint64, error) {
	leader, response, err := c.commitToCurrentLeader(ctx, []*pb.Request{quorumAcceptanceWriteRequest(key, value, timestamp)})
	if err != nil {
		return leader, 0, err
	}
	return leader, response.CommitIndex, nil
}

func quorumAcceptancePrepare(phase pb.Phase, version uint64) *pb.Request {
	return &pb.Request{Ts: 200, IsTxn: true, Phase: phase, ObservedRouteVersion: kv.EncodeObservedRouteVersion(version), Mutations: []*pb.Mutation{
		{Op: pb.Op_PUT, Key: []byte(kv.TxnMetaPrefix), Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("c"), CommitTS: 210, LockTTLms: 60000})},
		{Op: pb.Op_PUT, Key: []byte("c"), Value: []byte("prepared-left")},
	}}
}

func (c *quorumAcceptanceCluster) retainedAndRestart(t *testing.T, ctx context.Context, root quorumAcceptanceRoot, epoch uint64) {
	t.Helper()
	large := make([]byte, store.MigrationPagePayloadBudget+(1<<19))
	_, _, err := c.write(ctx, "u", large, 100)
	require.ErrorIs(t, err, kv.ErrMigrationTransferTooLarge)
	_, _, err = c.write(ctx, "u", []byte("moving-small"), 101)
	require.NoError(t, err, "moving-range creation must remain available below the transfer budget")
	leader, target, err := c.write(ctx, "b", large, 110)
	require.NoError(t, err, "retained-left writes keep the global size contract")
	require.Positive(t, target)
	t.Logf("retained write ACK index %d; cached status applied %d; current applied %d", target, leader.engine.Status().AppliedIndex, leader.engine.AppliedIndex())
	// Cross a snapshot interval after the value's ACK without snapshotting
	// 64 MiB on every control, rejection and post-restart probe in this test.
	for i := range quorumAcceptanceSnapshotEvery {
		_, _, err = c.write(ctx, "a-snapshot", []byte("snapshot barrier"), uint64(111+i))
		require.NoError(t, err)
	}
	// All members, including the learner, must have an actual persisted native
	// snapshot covering the ACKed write itself. Status() is a periodically
	// refreshed diagnostic and can lag the proposal's returned CommitIndex.
	require.Eventually(t, func() bool {
		for _, n := range c.nodes {
			if n.engine.Status().LastSnapshotIndex < target {
				return false
			}
		}
		return true
	}, 5*time.Second, 20*time.Millisecond)
	t.Logf("all four members persisted native snapshots covering ACKed write index %d", target)
	for _, n := range c.nodes {
		n.tx.Close()
		n.tx = nil
		require.NoError(t, n.engine.Close())
		n.engine = nil
		require.NoError(t, n.store.Close())
		n.store = nil
	}
	for _, n := range c.nodes {
		require.NoError(t, n.open(ctx, nil, false))
	}
	leader = c.leader(t)
	require.Equal(t, epoch, c.waitConfiguration(t, 4))
	// Election reports leadership, not completion of a linearizable data read.
	readIndex, err := leader.engine.LinearizableRead(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, readIndex, target)
	require.GreaterOrEqual(t, leader.engine.AppliedIndex(), readIndex)
	c.checkReplicas(t, ctx, epoch)
	for _, n := range c.nodes {
		value, err := n.store.GetAt(ctx, []byte("b"), 1000)
		require.NoError(t, err, "every voter and learner must restore the ACKed retained-left value")
		require.True(t, bytes.Equal(large, value))
	}
	mirror := distribution.NewEngine()
	require.NoError(t, mirror.ApplySnapshot(distribution.CatalogSnapshot{Version: root.version(), Routes: root.routes()}))
	reader := kv.NewShardStore(mirror, map[uint64]*kv.ShardGroup{quorumAcceptanceGroup: {Store: leader.store, Engine: leader.engine, Txn: leader.tx}})
	value, err := reader.GetGroupAt(ctx, quorumAcceptanceGroup, []byte("b"), 1000)
	require.NoError(t, err)
	require.True(t, bytes.Equal(large, value), "native restart must restore the ACKed retained-left value")
	_, _, err = c.write(ctx, "u", large, 100)
	require.ErrorIs(t, err, kv.ErrMigrationTransferTooLarge)
	t.Log("typed moving-range size rejection and unrestricted retained-left oversized data survive native restart on all four members")
	leader, _, err = c.commitToCurrentLeader(ctx, []*pb.Request{quorumAcceptancePrepare(pb.Phase_COMMIT, quorumAcceptanceCatalogVersion)})
	require.NoError(t, err, "native snapshot must retain PREPARE resolution evidence")
	reader = kv.NewShardStore(mirror, map[uint64]*kv.ShardGroup{quorumAcceptanceGroup: {Store: leader.store, Engine: leader.engine, Txn: leader.tx}})
	value, err = reader.GetGroupAt(ctx, quorumAcceptanceGroup, []byte("c"), 1000)
	require.NoError(t, err)
	require.Equal(t, []byte("prepared-left"), value)
	leader, _, err = c.write(ctx, "d", []byte("left-after-restart"), 1000)
	require.NoError(t, err)
	consumer := c.consumer(t, leader, root)
	quorumAcceptanceEventually(t, func() error { return consumer.reconcileMigrationPartitions(ctx, quorumAcceptanceGroup) })
	c.checkNoQuorum(t, ctx, leader)
}

func (c *quorumAcceptanceCluster) checkNoQuorum(t *testing.T, ctx context.Context, leader *quorumAcceptanceNode) {
	t.Helper()
	// Keep the leader AND the real learner running, but remove the other two
	// voters. A learner must never turn this into a two-of-four write quorum.
	for _, n := range c.nodes[:3] {
		if n != leader {
			n.tx.Close()
			n.tx = nil
			require.NoError(t, n.engine.Close())
			n.engine = nil
		}
	}
	attempt, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	// Exercise proposal quorum directly: a preliminary read fence would reject
	// before proposing and would not establish that the learner cannot vote.
	_, err := leader.tx.Commit(attempt, []*pb.Request{quorumAcceptanceWriteRequest("e", []byte("must-not-commit"), 1100)})
	require.Error(t, err)
	_, err = leader.store.GetAt(ctx, []byte("e"), 2000)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestMigrationQuorumPreparedDrainAndHistoricalPreflight(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	c := newQuorumAcceptanceCluster(t, ctx)
	leader := c.leader(t)
	epoch := c.waitConfiguration(t, 3)
	members, err := c.members(ctx, quorumAcceptanceGroup)
	require.NoError(t, err)
	quorumAcceptanceEventually(t, func() error { return EnsureMigrationNamespaceAdmission(ctx, leader.client, members) })
	consumer, root := c.partitionedSourceForDrain(t, ctx, leader, epoch)
	moving := root.later.Children[1]
	control := quorumAcceptanceAcquire(t, ctx, leader, 12, epoch, quorumAcceptanceCatalogVersion, moving, "t")
	large := make([]byte, store.MigrationPagePayloadBudget+(1<<19))
	for _, key := range []string{"u", "w"} {
		prepare := quorumAcceptanceDrainRequest(pb.Phase_PREPARE, key, large)
		require.Less(t, proto.Size(prepare), internalutil.GRPCMaxMessageBytes, "legacy PREPARE fits the unchanged 64 MiB transport limit")
		leader, _, err = c.commitToCurrentLeader(ctx, []*pb.Request{prepare})
		require.NoError(t, err, "oversized PREPARE predates transfer-size activation")
	}
	locks, err := leader.client.ProbeMigrationLocks(ctx, &pb.ProbeMigrationLocksRequest{RouteStart: moving.Start, RouteEnd: moving.End, Limit: 10})
	require.NoError(t, err)
	require.Equal(t, uint32(2), locks.GetPendingCount())
	job := distribution.SplitJob{JobID: 12, SourceRouteID: 5, SplitKey: []byte("t")}
	consumer = c.consumer(t, leader, root)
	var witness *splitTransferAdmissionWitness
	quorumAcceptanceEventually(t, func() error {
		witness, err = consumer.ensureSplitJobTransferControl(ctx, job, quorumAcceptanceGroup, leader.client, control, nil)
		return err
	})
	require.NoError(t, consumer.revalidateSplitTransferAdmission(ctx, witness))
	for _, resolution := range []struct {
		phase pb.Phase
		key   string
	}{{pb.Phase_COMMIT, "u"}, {pb.Phase_ABORT, "w"}} {
		// No replacement value is sent: COMMIT must use authenticated bytes
		// from the oversized old intent, not admit a new oversized creation.
		leader, _, err = c.commitToCurrentLeader(ctx, []*pb.Request{quorumAcceptanceDrainRequest(resolution.phase, resolution.key, nil)})
		require.NoError(t, err, "existing oversized PREPARE must drain after guard activation")
	}
	locks, err = leader.client.ProbeMigrationLocks(ctx, &pb.ProbeMigrationLocksRequest{RouteStart: moving.Start, RouteEnd: moving.End, Limit: 10})
	require.NoError(t, err)
	require.Zero(t, locks.GetPendingCount())
	committed, err := leader.store.GetAt(ctx, []byte("u"), 60)
	require.NoError(t, err)
	require.True(t, bytes.Equal(large, committed))
	_, err = leader.store.GetAt(ctx, []byte("w"), 1000)
	require.ErrorIs(t, err, store.ErrKeyNotFound, "ABORT must not publish its prepared oversized value")
	leader, index, err := c.write(ctx, "u", []byte("small latest"), 90)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		for _, n := range c.nodes[:3] {
			if n.engine.AppliedIndex() < index {
				return false
			}
		}
		return true
	}, 5*time.Second, 20*time.Millisecond)
	consumer = c.consumer(t, leader, root)
	c.historicalPreflightBeforeFence(t, ctx, leader, consumer, root, large)
}

func quorumAcceptanceDrainRequest(phase pb.Phase, key string, value []byte) *pb.Request {
	return &pb.Request{Ts: 50, IsTxn: true, Phase: phase, ObservedRouteVersion: kv.EncodeObservedRouteVersion(quorumAcceptanceCatalogVersion), Mutations: []*pb.Mutation{
		{Op: pb.Op_PUT, Key: []byte(kv.TxnMetaPrefix), Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte(key), CommitTS: 60, LockTTLms: 60000})},
		{Op: pb.Op_PUT, Key: []byte(key), Value: value},
	}}
}

func (c *quorumAcceptanceCluster) partitionedSourceForDrain(t *testing.T, ctx context.Context, leader *quorumAcceptanceNode, epoch uint64) (*DistributionServer, quorumAcceptanceRoot) {
	t.Helper()
	parent := distribution.RouteDescriptor{RouteID: 1, Start: []byte("a"), End: []byte("z"), GroupID: quorumAcceptanceGroup, MinWriteTSExclusive: 1}
	c.completeInitialOwner(t, ctx, leader, epoch, parent)
	left, right := parent, parent
	left.RouteID, left.ParentRouteID, left.End, left.SplitAtHLC = 2, 1, []byte("m"), 1
	right.RouteID, right.ParentRouteID, right.Start, right.SplitAtHLC = 3, 1, []byte("m"), 1
	root := quorumAcceptanceRoot{manifest: distribution.OwnershipPartitionManifest{PreviousVersion: 2, Version: 3, Parent: parent, Children: []distribution.RouteDescriptor{left, right}}}
	consumer := c.consumer(t, leader, root)
	quorumAcceptanceEventually(t, func() error { return consumer.reconcileMigrationPartitions(ctx, quorumAcceptanceGroup) })
	kept, moving := right, right
	kept.RouteID, kept.ParentRouteID, kept.End, kept.SplitAtHLC = 4, 3, []byte("t"), 2
	moving.RouteID, moving.ParentRouteID, moving.Start, moving.SplitAtHLC = 5, 3, []byte("t"), 2
	root.later = &distribution.OwnershipPartitionManifest{PreviousVersion: 3, Version: quorumAcceptanceCatalogVersion, Parent: right, Children: []distribution.RouteDescriptor{kept, moving}}
	consumer = c.consumer(t, leader, root)
	quorumAcceptanceEventually(t, func() error { return consumer.reconcileMigrationPartitions(ctx, quorumAcceptanceGroup) })
	return consumer, root
}

func (c *quorumAcceptanceCluster) historicalPreflightBeforeFence(t *testing.T, ctx context.Context, leader *quorumAcceptanceNode, consumer *DistributionServer, root quorumAcceptanceRoot, large []byte) {
	t.Helper()
	// Only root metadata uses the explicit catalog/coordinator stub. Every
	// data-group request, control, timestamp, export and verdict remains real.
	metadata := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, metadata.Close()) })
	catalog := distribution.NewCatalogStore(metadata, distribution.WithCatalogRouteDescriptorV2Writes(true))
	var version uint64
	for _, routes := range [][]distribution.RouteDescriptor{{root.manifest.Parent}, {root.manifest.Parent}, root.manifest.Children, root.routes()} {
		snapshot, err := catalog.Save(ctx, version, routes)
		require.NoError(t, err)
		version = snapshot.Version
	}
	require.Equal(t, quorumAcceptanceCatalogVersion, version)
	job, err := distribution.InitializeSplitJobPlan(distribution.SplitJob{JobID: 12, SourceRouteID: 3, SplitKey: []byte("t"), TargetGroupID: 9}, root.later.Parent, 1000)
	require.NoError(t, err)
	job.WorkingSourceRouteID, job.SnapshotTS, job.Phase = 5, 40, distribution.SplitJobPhaseFence
	job.WriteTrackerArmed, job.SourceRetentionPinTS = true, 1
	job.Cursor = []byte("saved-copy-progress")
	require.NoError(t, catalog.CreateSplitJob(ctx, job))
	consumer.catalog, consumer.coordinator = catalog, newDistributionCoordinatorStub(metadata, true)
	_, err = consumer.splitMigrationGroupFactory(ctx, job.TargetGroupID)
	require.Error(t, err, "a source-only fixture must never substitute its leader for a missing target group")
	for range len(job.BracketProgress) + 3 {
		// The whole runner additionally negotiates the target namespace. This
		// source-only harness exercises the real preflight/fence phase and its
		// compensation helper, not a fictitious cross-group runner acceptance.
		err = consumer.finalizeSplitJobFence(ctx, job)
		if errors.Is(err, errMigrationOversizedSourceVersion) {
			require.NoError(t, consumer.abandonOversizedSplitTransfer(ctx, job, err))
		}
		job = loadTransferJob(t, ctx, catalog, job.JobID)
		if job.Phase == distribution.SplitJobPhaseAbandoning {
			break
		}
		require.NoError(t, err)
	}
	require.True(t, errors.Is(err, errMigrationOversizedSourceVersion), "only the authenticated structured source verdict authorizes compensation")
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	details := status.Convert(err).Details()
	require.Len(t, details, 1)
	info, ok := details[0].(*errdetails.ErrorInfo)
	require.True(t, ok)
	require.Equal(t, migrationTransferErrorDomain, info.GetDomain())
	require.Equal(t, migrationOversizedVersionReason, info.GetReason())
	require.Equal(t, "60", info.GetMetadata()["commit_ts"], "the rejected historical committed version predates the latest small value")
	require.Equal(t, distribution.SplitJobPhaseAbandoning, job.Phase)
	require.Equal(t, distribution.SplitJobPhaseFence, job.AbandonFromPhase)
	require.False(t, job.TransferPreflightDone)
	require.Positive(t, job.TransferPreflightProgress[0].AcceptedRows, "preserve the valid streamed page before the later historical error")
	require.NotEmpty(t, job.TransferPreflightProgress[0].Cursor)
	require.Positive(t, job.TransferPreflightTS)
	require.Zero(t, job.FenceTS)
	require.Equal(t, []byte("saved-copy-progress"), job.Cursor)
	require.NotEmpty(t, job.LastError)
	snapshot, err := catalog.Snapshot(ctx)
	require.NoError(t, err)
	require.Equal(t, quorumAcceptanceCatalogVersion, snapshot.Version)
	require.Equal(t, root.routes(), snapshot.Routes, "historical oversize must be found before publishing a catalog write fence")
	require.NotEmpty(t, leader.exports)
	require.Positive(t, leader.timestamps.Load())
	for _, request := range leader.exports {
		require.Zero(t, request.GetMinCommitTs(), "preflight must include retained history, not only the latest small value")
		require.Equal(t, job.TransferPreflightTS, request.GetMaxCommitTs())
	}
	for _, n := range c.nodes[:3] {
		require.Zero(t, n.writeFences.Load(), "no source write-fence control may precede full-history preflight")
		reader, ok := n.store.(store.MigrationTargetReadinessReader)
		require.True(t, ok)
		states, err := reader.MigrationTargetReadinessStates(ctx)
		require.NoError(t, err)
		require.Len(t, states, 1)
		require.Equal(t, uint32(1), states[0].TransferSizeGuardVersion)
		require.False(t, states[0].SourceWriteFence)
		require.False(t, states[0].SourceReadFence)
		historical, err := n.store.GetAt(ctx, []byte("u"), 60)
		require.NoError(t, err)
		require.True(t, bytes.Equal(large, historical), "every data replica must preserve the oversized pre-guard COMMIT")
		_, err = n.store.GetAt(ctx, []byte("w"), 1000)
		require.ErrorIs(t, err, store.ErrKeyNotFound, "every data replica must preserve the old PREPARE's ABORT")
		latest, err := n.store.GetAt(ctx, []byte("u"), 1000)
		require.NoError(t, err)
		require.Equal(t, []byte("small latest"), latest)
	}
}
