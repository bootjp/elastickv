package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type partitionRootClient struct {
	root  *DistributionServer
	calls int
}

func (c *partitionRootClient) AttestMigrationOwnership(ctx context.Context, req *pb.AttestMigrationOwnershipRequest, _ ...grpc.CallOption) (*pb.AttestMigrationOwnershipResponse, error) {
	c.calls++
	return c.root.AttestMigrationOwnership(ctx, req)
}

type partitionProbeClient struct {
	*ownershipRuntimeClient
	old, wrong, deny bool
}

func (c *partitionProbeClient) ProbeMigrationState(ctx context.Context, req *pb.ProbeMigrationStateRequest, opts ...grpc.CallOption) (*pb.ProbeMigrationStateResponse, error) {
	resp, err := c.ownershipRuntimeClient.ProbeMigrationState(ctx, req, opts...)
	if err != nil {
		return nil, err
	}
	if c.old {
		resp.OwnershipPartitionProtocol = 0
	}
	if req.GetKind() == pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_PARTITION_APPLIED {
		if c.wrong {
			resp.PartitionReceiptId = []byte("wrong receipt")
		}
		if c.deny {
			resp.Ready = false
		}
	}
	return resp, nil
}

type partitionRefreshHarness struct {
	consumer, root *DistributionServer
	rootStore      store.MVCCStore
	rootClient     *partitionRootClient
	clients        []*partitionProbeClient
	stores         []store.MVCCStore
	parent         distribution.RouteDescriptor
}

func newPartitionRefreshHarness(t *testing.T, local uint64) *partitionRefreshHarness {
	t.Helper()
	h := &partitionRefreshHarness{rootStore: store.NewMVCCStore()}
	h.parent = distribution.RouteDescriptor{RouteID: 1, Start: []byte("a"), End: []byte("z"), GroupID: 7, State: distribution.RouteStateActive, MinWriteTSExclusive: 100}
	catalog := distribution.NewCatalogStore(h.rootStore, distribution.WithCatalogRouteDescriptorV2Writes(true))
	_, err := catalog.Save(t.Context(), 0, []distribution.RouteDescriptor{h.parent})
	require.NoError(t, err)
	coord := &ownershipCatalogCoordinator{distributionCoordinatorStub: newDistributionCoordinatorStub(h.rootStore, true)}
	h.root = NewDistributionServer(distribution.NewEngine(), catalog, WithDistributionCoordinator(coord))
	h.rootClient = &partitionRootClient{root: h.root}
	hash, err := distribution.OwnershipPartitionDescriptorHash(h.parent)
	require.NoError(t, err)
	role := store.MigrationOwner
	if local != 7 {
		role = store.MigrationNonOwner
	}
	state := store.MigrationOwnershipState{JobID: 11, Generation: "0123456789abcdef0123456789abcdef", LocalGroupID: local, OwnerGroupID: 7,
		CatalogVersion: 1, ConfigurationIndex: 9, RouteID: 1, RouteStart: h.parent.Start, RouteEnd: h.parent.End, DescriptorHash: hash, Role: role, WriteFloorKnown: true, MinWriteTSExclusive: 100}
	for range 3 {
		st := store.NewMVCCStore()
		client, _ := newOwnershipRuntimeClient(t, st, local)
		writer, ok := st.(store.MigrationOwnershipStore)
		require.True(t, ok)
		require.NoError(t, writer.ClearMigrationStateWithOwnership(t.Context(), state, 10))
		tx := kv.NewTransactionWithProposer(client.internal.migrationProposer, kv.WithMigrationRequestAdmission(st, local))
		t.Cleanup(tx.Close)
		client.internal.transactionManager = tx
		client.internal.migrationPartitionProtocolReady = func() bool { return true }
		h.stores = append(h.stores, st)
		h.clients = append(h.clients, &partitionProbeClient{ownershipRuntimeClient: client})
	}
	proposer, ok := h.clients[0].internal.migrationProposer.(*ownershipRuntimeProposer)
	require.True(t, ok)
	for _, client := range h.clients[1:] {
		p, ok := client.internal.migrationProposer.(*ownershipRuntimeProposer)
		require.True(t, ok)
		proposer.followers = append(proposer.followers, p.fsm)
	}
	configuration, ok := proposer.fsm.(interface {
		AppliedConfigurationIndex() uint64
		RecordAppliedConfiguration(uint64) error
	})
	require.True(t, ok)
	groups := map[uint64]*kv.ShardGroup{local: {Store: h.stores[0], Txn: h.clients[0].internal.transactionManager, Engine: ownershipRefreshEngine{configuration: configuration}}}
	members := func(context.Context, uint64) ([]SplitMigrationVoter, error) {
		return []SplitMigrationVoter{{ID: "voter1", Address: "one", Client: h.clients[0]}, {ID: "voter2", Address: "two", Client: h.clients[1]}, {ID: "learner", Address: "three", Client: h.clients[2]}}, nil
	}
	mirror := distribution.NewEngine()
	h.consumer = NewDistributionServer(mirror, nil,
		WithSplitMigrationGroupClientFactory(func(context.Context, uint64) (SplitMigrationClient, error) { return h.clients[0], nil }),
		WithSplitMigrationVoterFactory(members),
		WithMigrationOwnershipRefresh(groups, func(context.Context) (MigrationOwnershipAttestor, error) { return h.rootClient, nil }),
		WithMigrationPartitionRuntime(kv.NewShardStore(mirror, groups), members))
	require.True(t, h.consumer.MigrationPartitionRuntimeConfigured(local))
	return h
}

func (h *partitionRefreshHarness) split(t *testing.T, parent distribution.RouteDescriptor, leftID, rightID uint64, key string) []distribution.RouteDescriptor {
	t.Helper()
	snapshot, err := h.root.catalog.Snapshot(t.Context())
	require.NoError(t, err)
	left, right := parent, parent
	left.RouteID, left.ParentRouteID, left.End, left.SplitAtHLC = leftID, parent.RouteID, []byte(key), parent.SplitAtHLC+1
	right.RouteID, right.ParentRouteID, right.Start, right.SplitAtHLC = rightID, parent.RouteID, []byte(key), parent.SplitAtHLC+1
	m := distribution.OwnershipPartitionManifest{PreviousVersion: snapshot.Version, Version: snapshot.Version + 1, Parent: parent, Children: []distribution.RouteDescriptor{left, right}}
	readTS := h.rootStore.LastCommitTS()
	manifest, err := h.root.catalog.BuildOwnershipPartitionMutationAt(t.Context(), readTS, m)
	require.NoError(t, err)
	mutations := []*store.KVPairMutation{manifest, {Op: store.OpTypeDelete, Key: distribution.CatalogRouteKey(parent.RouteID)},
		{Op: store.OpTypePut, Key: distribution.CatalogVersionKey(), Value: distribution.EncodeCatalogVersion(m.Version)}}
	for _, route := range m.Children {
		encoded, err := distribution.EncodeRouteDescriptorForCatalogWrite(route, true)
		require.NoError(t, err)
		mutations = append(mutations, &store.KVPairMutation{Op: store.OpTypePut, Key: distribution.CatalogRouteKey(route.RouteID), Value: encoded})
	}
	require.NoError(t, h.rootStore.ApplyMutations(t.Context(), mutations, [][]byte{distribution.CatalogVersionKey(), manifest.Key}, readTS, readTS+1))
	return m.Children
}

func (h *partitionRefreshHarness) frontier(t *testing.T, i int) store.MigrationOwnershipPartitionState {
	t.Helper()
	reader, ok := h.stores[i].(store.MigrationOwnershipPartitionStore)
	require.True(t, ok)
	state, err := reader.MigrationOwnershipPartitions(t.Context())
	require.NoError(t, err)
	return state
}

func TestPartitionRefreshThreeApplyMembersAndHistoricalNonowner(t *testing.T) {
	for _, local := range []uint64{7, 9} {
		t.Run(map[uint64]string{7: "owner", 9: "historical-nonowner"}[local], func(t *testing.T) {
			h := newPartitionRefreshHarness(t, local)
			h.split(t, h.parent, 2, 3, "m")
			require.NoError(t, h.consumer.reconcileMigrationPartitions(t.Context(), local))
			state := h.frontier(t, 0)
			require.Len(t, state.Partitions, 2)
			require.Len(t, state.Receipts, 1, "unchanged polling must not grow receipt history")
			for i := 1; i < len(h.stores); i++ {
				require.Equal(t, state, h.frontier(t, i))
			}
			for _, p := range state.Partitions {
				require.EqualValues(t, 2, p.CatalogVersion)
				require.EqualValues(t, 100, p.MinWriteTSExclusive)
				if local == 9 {
					require.Equal(t, store.MigrationNonOwner, p.Role)
				}
			}
			// No root leader/data leader equivalence or runner activation gate.
			require.Nil(t, h.consumer.coordinator)
			require.NoError(t, h.consumer.RunMigrationOwnershipRefreshOnce(t.Context()))
			require.Equal(t, state, h.frontier(t, 0))
		})
	}
}

func TestPartitionRefreshLearnerCapabilityAndReceiptAreMandatory(t *testing.T) {
	h := newPartitionRefreshHarness(t, 7)
	h.split(t, h.parent, 2, 3, "m")
	h.clients[2].old = true
	require.Error(t, h.consumer.reconcileMigrationPartitions(t.Context(), 7))
	require.Empty(t, h.clients[0].cleanups, "no all-member proof means no proposal")
	h.clients[2].old = false
	h.clients[2].wrong = true
	require.Error(t, h.consumer.reconcileMigrationPartitions(t.Context(), 7))
	require.Len(t, h.frontier(t, 0).Partitions, 2, "a failed ACK does not undo committed apply")
	h.clients[2].wrong = false
	require.NoError(t, h.consumer.reconcileMigrationPartitions(t.Context(), 7))
	require.Len(t, h.frontier(t, 0).Receipts, 1)
}

func TestPartitionRefreshRetainedManifestChainAndCurrentC(t *testing.T) {
	h := newPartitionRefreshHarness(t, 7)
	children := h.split(t, h.parent, 2, 3, "m")
	h.split(t, children[1], 4, 5, "t")
	// Both ledger steps predate the data group's first attempt; reconciliation
	// must finish all geometry, not ACK the first intermediate manifest.
	require.NoError(t, h.consumer.reconcileMigrationPartitions(t.Context(), 7))
	state := h.frontier(t, 0)
	require.Len(t, state.Partitions, 3)
	for _, p := range state.Partitions {
		require.EqualValues(t, 3, p.CatalogVersion)
	}
	require.Len(t, state.Receipts, 2)
	ids := make([][]byte, len(state.Partitions))
	for i, p := range state.Partitions {
		ids[i] = p.PartitionID
	}
	// An unrelated root C requires unchanged refresh, never descriptor guesses.
	snapshot, err := h.root.catalog.Snapshot(t.Context())
	require.NoError(t, err)
	_, err = h.root.catalog.Save(t.Context(), snapshot.Version, snapshot.Routes)
	require.NoError(t, err)
	require.NoError(t, h.consumer.RunMigrationOwnershipRefreshOnce(t.Context()))
	state = h.frontier(t, 0)
	for i, p := range state.Partitions {
		require.Equal(t, ids[i], p.PartitionID)
		require.EqualValues(t, 4, p.CatalogVersion)
	}
}

func TestPartitionRefreshRuntimeGroundedAndChangedDescriptorRefuses(t *testing.T) {
	h := newPartitionRefreshHarness(t, 7)
	h.consumer.ownershipRefresh.partitionRuntime.members = nil
	require.False(t, h.consumer.MigrationPartitionRuntimeConfigured(7))
	require.Error(t, h.consumer.reconcileMigrationPartitions(t.Context(), 7))
	h = newPartitionRefreshHarness(t, 7)
	changed := h.parent
	changed.End = []byte("y")
	_, err := h.root.catalog.Save(t.Context(), 1, []distribution.RouteDescriptor{changed})
	require.NoError(t, err)
	require.Error(t, h.consumer.reconcileMigrationPartitions(t.Context(), 7))
	require.Empty(t, h.frontier(t, 0).Partitions)
}

func TestPartitionRefreshForegroundCanDriveRemoteSourceLeader(t *testing.T) {
	h := newPartitionRefreshHarness(t, 7)
	h.split(t, h.parent, 2, 3, "m")
	g := h.consumer.ownershipRefresh.groups[7]
	engine, ok := g.Engine.(ownershipRefreshEngine)
	require.True(t, ok)
	engine.follower = true
	g.Engine = engine
	require.NoError(t, h.consumer.reconcileMigrationPartitions(t.Context(), 7))
	require.Len(t, h.frontier(t, 0).Partitions, 2)
	// A lagging local view cannot turn an absent local candidate into success.
	g.Store = store.NewMVCCStore()
	leader, ok := h.clients[0].internal.leader.(ownershipRuntimeLeader)
	require.True(t, ok)
	configuration := leader.configuration
	localFSM := kv.NewKvFSMWithHLC(g.Store, nil)
	aware, ok := localFSM.(interface{ RecordAppliedConfiguration(uint64) error })
	require.True(t, ok)
	require.NoError(t, aware.RecordAppliedConfiguration(configuration.AppliedConfigurationIndex()))
	require.Error(t, h.consumer.reconcileMigrationPartitions(t.Context(), 7))
}

func TestPartitionRefreshUnadoptedLegacyUsesRootAndPreservesReceipts(t *testing.T) {
	h := newPartitionRefreshHarness(t, 7)
	writer, ok := h.stores[0].(store.MigrationOwnershipStore)
	require.True(t, ok)
	before, err := writer.MigrationServingState(t.Context())
	require.NoError(t, err)
	require.NoError(t, h.consumer.reconcileMigrationPartitions(t.Context(), 7), "same C still requires root and all-member ACK")
	require.Empty(t, h.clients[0].cleanups)
	require.NotZero(t, h.rootClient.calls)
	_, err = h.root.catalog.Save(t.Context(), 1, []distribution.RouteDescriptor{h.parent})
	require.NoError(t, err)
	require.NoError(t, h.consumer.RunMigrationOwnershipRefreshOnce(t.Context()))
	view, err := writer.MigrationServingState(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 2, view.Ownership[0].CatalogVersion)
	require.Equal(t, before.Ownership[0].Receipts, view.Ownership[0].Receipts)
	require.Empty(t, h.frontier(t, 0).Partitions, "ordinary legacy refresh never manufactures child authority")
	require.Len(t, h.clients[0].cleanups, 1)
	require.Equal(t, pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_OWNERSHIP_REFRESH, h.clients[0].cleanups[0].GetMode())
}
