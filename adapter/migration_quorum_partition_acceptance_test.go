package adapter

import (
	"bytes"
	"context"
	"fmt"
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
	"google.golang.org/grpc"
)

const (
	quorumAcceptanceGroup          = uint64(7)
	quorumAcceptanceCatalogVersion = uint64(4)
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
	return c.node.internal.ApplyTargetStagedReadiness(ctx, req)
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
		TickInterval: 10 * time.Millisecond, ElectionTick: 100, HeartbeatTick: 1})
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
	return active[raftenginetest.WaitForLeader(t, 5*time.Second, views...)]
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

func (c *quorumAcceptanceCluster) members(ctx context.Context, _ uint64) ([]SplitMigrationVoter, error) {
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
		for _, n := range c.nodes {
			if configured.ID == n.peer.ID {
				members = append(members, SplitMigrationVoter{ID: n.peer.ID, Address: n.peer.Address, Client: n.client})
			}
		}
	}
	if len(members) != len(cfg.Servers) {
		return nil, errors.New("configured apply member missing from real cluster")
	}
	return members, nil
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
	return NewDistributionServer(mirror, nil, WithSplitMigrationGroupClientFactory(func(context.Context, uint64) (SplitMigrationClient, error) { return leader.client, nil }),
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
	t.Setenv("ELASTICKV_RAFT_SNAPSHOT_COUNT", "1")
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
	c.retainedAndRestart(t, ctx, leader, root, newEpoch)
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

func quorumAcceptanceWrite(ctx context.Context, n *quorumAcceptanceNode, key string, value []byte, timestamp uint64) (uint64, error) {
	response, err := n.tx.Commit(ctx, []*pb.Request{quorumAcceptanceWriteRequest(key, value, timestamp)})
	if err != nil {
		return 0, err
	}
	return response.CommitIndex, nil
}

func quorumAcceptancePrepare(phase pb.Phase, version uint64) *pb.Request {
	return &pb.Request{Ts: 200, IsTxn: true, Phase: phase, ObservedRouteVersion: kv.EncodeObservedRouteVersion(version), Mutations: []*pb.Mutation{
		{Op: pb.Op_PUT, Key: []byte(kv.TxnMetaPrefix), Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("c"), CommitTS: 210, LockTTLms: 60000})},
		{Op: pb.Op_PUT, Key: []byte("c"), Value: []byte("prepared-left")},
	}}
}

func (c *quorumAcceptanceCluster) retainedAndRestart(t *testing.T, ctx context.Context, leader *quorumAcceptanceNode, root quorumAcceptanceRoot, epoch uint64) {
	t.Helper()
	large := make([]byte, store.MigrationPagePayloadBudget+(1<<19))
	_, err := quorumAcceptanceWrite(ctx, leader, "u", large, 100)
	require.ErrorIs(t, err, kv.ErrMigrationTransferTooLarge)
	_, err = quorumAcceptanceWrite(ctx, leader, "u", []byte("moving-small"), 101)
	require.NoError(t, err, "moving-range creation must remain available below the transfer budget")
	target, err := quorumAcceptanceWrite(ctx, leader, "b", large, 110)
	require.NoError(t, err, "retained-left writes keep the global size contract")
	require.Positive(t, target)
	t.Logf("retained write ACK index %d; cached status applied %d; current applied %d", target, leader.engine.Status().AppliedIndex, leader.engine.AppliedIndex())
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
	_, err = quorumAcceptanceWrite(ctx, leader, "u", large, 100)
	require.ErrorIs(t, err, kv.ErrMigrationTransferTooLarge)
	t.Log("typed moving-range size rejection and unrestricted retained-left oversized data survive native restart on all four members")
	_, err = leader.tx.Commit(ctx, []*pb.Request{quorumAcceptancePrepare(pb.Phase_COMMIT, quorumAcceptanceCatalogVersion)})
	require.NoError(t, err, "native snapshot must retain PREPARE resolution evidence")
	value, err = reader.GetGroupAt(ctx, quorumAcceptanceGroup, []byte("c"), 1000)
	require.NoError(t, err)
	require.Equal(t, []byte("prepared-left"), value)
	_, err = quorumAcceptanceWrite(ctx, leader, "d", []byte("left-after-restart"), 1000)
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
	_, err := quorumAcceptanceWrite(attempt, leader, "e", []byte("must-not-commit"), 1100)
	require.Error(t, err)
	_, err = leader.store.GetAt(ctx, []byte("e"), 2000)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}
