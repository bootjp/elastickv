package adapter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestOwnershipRefreshConsumerObsoleteProofDoesNotStarveCurrentOwner(t *testing.T) {
	root, active := ownershipRefreshCatalog(t)
	snapshot, err := root.catalog.Snapshot(t.Context())
	require.NoError(t, err)
	for snapshot.Version < 16 {
		snapshot.Routes[0].MinWriteTSExclusive++
		_, err = root.catalog.Save(t.Context(), snapshot.Version, snapshot.Routes)
		require.NoError(t, err)
		snapshot, err = root.catalog.Snapshot(t.Context())
		require.NoError(t, err)
	}
	active.CatalogVersion = 15
	obsolete := proto.CloneOf(active)
	obsolete.CatalogVersion, obsolete.RouteId, obsolete.OwnerGroupId = 12, 999, 9
	obsolete.Role = pb.MigrationOwnershipRole_MIGRATION_OWNERSHIP_ROLE_NONOWNER
	obsolete.DescriptorHash = bytes.Repeat([]byte{1}, sha256.Size)
	st := store.NewMVCCStore()
	_, _ = newOwnershipRuntimeClient(t, st, 7)
	writer, ok := st.(store.MigrationOwnershipStore)
	require.True(t, ok)
	seedOwnershipRefreshState(t, writer, 6, obsolete, 10)
	seedOwnershipRefreshState(t, writer, 7, active, 11)
	rootClient, closeClient := newBufconnDistributionClient(t, root)
	defer closeClient()
	consumer, client, _ := ownershipRefreshConsumer(t, st, rootClient, active)
	require.Error(t, consumer.RunMigrationOwnershipRefreshOnce(t.Context()), "old removed route must not be refreshed")
	view, err := writer.MigrationServingState(t.Context())
	require.NoError(t, err)
	require.Len(t, view.Ownership, 2)
	require.EqualValues(t, 12, view.Ownership[0].CatalogVersion)
	require.Equal(t, store.MigrationNonOwner, view.Ownership[0].Role)
	require.EqualValues(t, 16, view.Ownership[1].CatalogVersion)
	require.Equal(t, store.MigrationOwner, view.Ownership[1].Role)
	require.Len(t, client.cleanups, 1)
	coordinator, ok := root.coordinator.(*ownershipCatalogCoordinator)
	require.True(t, ok)
	reads := coordinator.reads
	require.NoError(t, consumer.RunMigrationOwnershipRefreshOnce(t.Context()))
	require.Equal(t, reads, coordinator.reads, "unchanged C must not poll root once per failed historical proof")
	require.Len(t, client.cleanups, 1)
	snapshot.Routes[0].MinWriteTSExclusive++
	_, err = root.catalog.Save(t.Context(), snapshot.Version, snapshot.Routes)
	require.NoError(t, err)
	require.NoError(t, consumer.engine.ApplySnapshot(distribution.CatalogSnapshot{Version: 17, Routes: []distribution.RouteDescriptor{{RouteID: 100, GroupID: 100}}}))
	require.Error(t, consumer.RunMigrationOwnershipRefreshOnce(t.Context()))
	view, err = writer.MigrationServingState(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 12, view.Ownership[0].CatalogVersion)
	require.EqualValues(t, 17, view.Ownership[1].CatalogVersion, "new C bypasses the historical proof's retry delay")
}

func seedOwnershipRefreshState(t *testing.T, writer store.MigrationOwnershipStore, jobID uint64, proof *pb.MigrationOwnershipProof, index uint64) {
	t.Helper()
	state, err := kv.MigrationOwnershipStateFromProto(jobID, proof)
	require.NoError(t, err)
	state.ConfigurationIndex = 9
	require.NoError(t, writer.ClearMigrationStateWithOwnership(t.Context(), state, index))
}

func TestOwnershipRefreshConsumerSequentialSplitRequiresRemainingRangeReacquisition(t *testing.T) {
	root, original := ownershipRefreshCatalog(t)
	snapshot := advanceOwnershipRefreshCatalog(t, root, 12)
	original.CatalogVersion = snapshot.Version
	// Job 1 owned [m,z]. Job 2 keeps the same left route ID but shrinks its
	// descriptor to [m,x], moving [x,z] to another group. This is not an
	// unchanged-descriptor refresh of Job 1, even though its left side remains.
	snapshot.Routes[1].End = []byte("x")
	right := distribution.RouteDescriptor{RouteID: 4, ParentRouteID: 3,
		Start: []byte("x"), End: []byte("z"), GroupID: 9,
		State: distribution.RouteStateActive, MinWriteTSExclusive: 200}
	snapshot.Routes = append(snapshot.Routes, right)
	_, err := root.catalog.Save(t.Context(), snapshot.Version, snapshot.Routes)
	require.NoError(t, err)
	snapshot = advanceOwnershipRefreshCatalog(t, root, 15)
	encoded, err := distribution.EncodeRouteDescriptorForCatalogWrite(right, true)
	require.NoError(t, err)
	digest := sha256.Sum256(encoded)
	nonowner := proto.CloneOf(original)
	nonowner.CatalogVersion, nonowner.RouteId, nonowner.ParentRouteId = snapshot.Version, right.RouteID, right.ParentRouteID
	nonowner.RouteStart, nonowner.RouteEnd = bytes.Clone(right.Start), bytes.Clone(right.End)
	nonowner.OwnerGroupId, nonowner.MinWriteTsExclusive = right.GroupID, right.MinWriteTSExclusive
	nonowner.Role, nonowner.DescriptorHash = pb.MigrationOwnershipRole_MIGRATION_OWNERSHIP_ROLE_NONOWNER, digest[:]
	st := store.NewMVCCStore()
	_, _ = newOwnershipRuntimeClient(t, st, 7)
	writer, ok := st.(store.MigrationOwnershipStore)
	require.True(t, ok)
	seedOwnershipRefreshState(t, writer, 7, original, 10)
	seedOwnershipRefreshState(t, writer, 8, nonowner, 11)
	before, err := writer.MigrationServingState(t.Context())
	require.NoError(t, err)
	advanceOwnershipRefreshCatalog(t, root, 16)
	rootClient, closeClient := newBufconnDistributionClient(t, root)
	defer closeClient()
	consumer, client, _ := ownershipRefreshConsumer(t, st, rootClient, nonowner)
	require.ErrorContains(t, consumer.RunMigrationOwnershipRefreshOnce(t.Context()), "descriptor changed")
	require.Len(t, client.cleanups, 1, "no proposal may clip or refresh the hash-changed old proof")
	require.EqualValues(t, 8, client.cleanups[0].GetJobId())
	after, err := writer.MigrationServingState(t.Context())
	require.NoError(t, err)
	require.Len(t, after.Ownership, 2)
	assertSequentialSplitOwnershipRetained(t, before.Ownership, after.Ownership, nonowner)
	retirement, ok := st.(store.MigrationRetirementReader)
	require.True(t, ok)
	retired, err := retirement.MigrationRetired(t.Context(), 8)
	require.NoError(t, err)
	require.True(t, retired, "refresh must not reopen the retired nonowner job")
	for _, key := range []string{"n", "y"} {
		err := kv.CheckMigrationRequest(t.Context(), st, 7, &pb.Request{Ts: 300, ObservedRouteVersion: 16,
			Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte(key), Value: []byte("not admitted")}}})
		require.ErrorIs(t, err, kv.ErrRouteCutoverPending, "remaining-left proof needs reacquisition; moved-right stays nonowner")
	}
}

func advanceOwnershipRefreshCatalog(t *testing.T, root *DistributionServer, version uint64) distribution.CatalogSnapshot {
	t.Helper()
	snapshot, err := root.catalog.Snapshot(t.Context())
	require.NoError(t, err)
	for snapshot.Version < version {
		snapshot.Routes[0].MinWriteTSExclusive++
		_, err = root.catalog.Save(t.Context(), snapshot.Version, snapshot.Routes)
		require.NoError(t, err)
		snapshot, err = root.catalog.Snapshot(t.Context())
		require.NoError(t, err)
	}
	return snapshot
}

func assertSequentialSplitOwnershipRetained(t *testing.T, before, after []store.MigrationOwnershipState, nonowner *pb.MigrationOwnershipProof) {
	t.Helper()
	prior := make(map[uint64]store.MigrationOwnershipState, len(before))
	for _, state := range before {
		prior[state.JobID] = state
	}
	for _, state := range after {
		if state.JobID == 7 {
			require.Equal(t, prior[7], state, "old full [m,z] OWNER proof must remain byte-for-byte intact at C12")
			continue
		}
		require.EqualValues(t, 8, state.JobID)
		expected := proto.CloneOf(nonowner)
		expected.CatalogVersion = 16
		require.True(t, proto.Equal(expected, kv.MigrationOwnershipProofFromState(state)))
		require.Equal(t, prior[8].Receipts, state.Receipts, "NONOWNER receipt C15 must survive its valid unchanged-descriptor refresh")
	}
}

type ownershipRefreshEngine struct {
	raftengine.Engine
	configuration raftengine.AppliedConfigurationAware
	follower      bool
	read          func(context.Context) error
}

func (e ownershipRefreshEngine) State() raftengine.State {
	if e.follower {
		return raftengine.StateFollower
	}
	return raftengine.StateLeader
}

func (e ownershipRefreshEngine) VerifyLeader(context.Context) error { return nil }
func (e ownershipRefreshEngine) LinearizableRead(ctx context.Context) (uint64, error) {
	if e.read != nil {
		return 0, e.read(ctx)
	}
	return 1, nil
}
func (e ownershipRefreshEngine) AppliedConfigurationIndex() (uint64, bool) {
	index := e.configuration.AppliedConfigurationIndex()
	return index, index != 0
}

func (e ownershipRefreshEngine) Status() raftengine.Status {
	return raftengine.Status{State: e.State(), Term: 1}
}

func ownershipRefreshConsumer(t *testing.T, st store.MVCCStore, root pb.DistributionClient, prior *pb.MigrationOwnershipProof) (*DistributionServer, *ownershipRuntimeClient, raftengine.AppliedConfigurationAware) {
	t.Helper()
	id := prior.GetLocalGroupId()
	client, configuration := newOwnershipRuntimeClient(t, st, id)
	writer, ok := st.(store.MigrationOwnershipStore)
	require.True(t, ok)
	view, err := writer.MigrationServingState(t.Context())
	require.NoError(t, err)
	if len(view.Ownership) == 0 {
		state, err := kv.MigrationOwnershipStateFromProto(7, prior)
		require.NoError(t, err)
		state.ConfigurationIndex = 9
		require.NoError(t, writer.ClearMigrationStateWithOwnership(t.Context(), state, 10))
	}
	mirror := distribution.NewEngine()
	require.NoError(t, mirror.ApplySnapshot(distribution.CatalogSnapshot{Version: prior.GetCatalogVersion() + 1, Routes: []distribution.RouteDescriptor{
		{RouteID: 99, Start: []byte("a"), End: []byte("z"), GroupID: 99},
	}}))
	server := NewDistributionServer(mirror, nil,
		WithSplitJobRunnerReadinessGate(func(context.Context) error { return nil }),
		WithSplitMigrationGroupClientFactory(func(context.Context, uint64) (SplitMigrationClient, error) { return client, nil }),
		WithSplitMigrationVoterFactory(func(context.Context, uint64) ([]SplitMigrationVoter, error) {
			return []SplitMigrationVoter{{ID: "data", Address: "data", Client: client}}, nil
		}),
		WithMigrationOwnershipRefresh(map[uint64]*kv.ShardGroup{id: {Store: st, Engine: ownershipRefreshEngine{configuration: configuration}}},
			func(context.Context) (MigrationOwnershipAttestor, error) { return root, nil }),
	)
	return server, client, configuration
}

func TestOwnershipRefreshConsumerRejectsConfigurationInterleaving(t *testing.T) {
	root, prior := ownershipRefreshCatalog(t)
	rootClient, closeClient := newBufconnDistributionClient(t, root)
	defer closeClient()
	st := store.NewMVCCStore()
	consumer, client, configuration := ownershipRefreshConsumer(t, st, rootClient, prior)
	client.beforeCleanup = func() { require.NoError(t, configuration.RecordAppliedConfiguration(11)) }
	require.Error(t, consumer.RunMigrationOwnershipRefreshOnce(t.Context()))
	writer, ok := st.(store.MigrationOwnershipStore)
	require.True(t, ok)
	view, err := writer.MigrationServingState(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 1, view.Ownership[0].CatalogVersion)
	client.beforeCleanup = nil
	require.NoError(t, consumer.RunMigrationOwnershipRefreshOnce(t.Context()))
	require.Len(t, client.cleanups, 2)
	require.EqualValues(t, 11, client.cleanups[1].GetExpectedConfigurationIndex())
}

type refreshProbeClient struct {
	SplitMigrationClient
	probe func(*pb.ProbeMigrationStateRequest, *pb.ProbeMigrationStateResponse)
}

func (c refreshProbeClient) ProbeMigrationState(ctx context.Context, req *pb.ProbeMigrationStateRequest, opts ...grpc.CallOption) (*pb.ProbeMigrationStateResponse, error) {
	resp, err := c.SplitMigrationClient.ProbeMigrationState(ctx, req, opts...)
	if err == nil {
		c.probe(req, resp)
	}
	return resp, err
}

func TestOwnershipRefreshConsumerRetriesExactAppliedAckWithoutNewProposal(t *testing.T) {
	root, prior := ownershipRefreshCatalog(t)
	rootClient, closeClient := newBufconnDistributionClient(t, root)
	defer closeClient()
	consumer, client, _ := ownershipRefreshConsumer(t, store.NewMVCCStore(), rootClient, prior)
	blocked := true
	probes := 0
	follower := refreshProbeClient{SplitMigrationClient: client, probe: func(req *pb.ProbeMigrationStateRequest, resp *pb.ProbeMigrationStateResponse) {
		probes++
		if req.GetKind() == pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_OWNERSHIP_APPLIED && blocked {
			resp.Ready = false
		}
	}}
	consumer.splitMigrationVoterFactory = func(context.Context, uint64) ([]SplitMigrationVoter, error) {
		return []SplitMigrationVoter{{ID: "leader", Address: "leader", Client: client}, {ID: "follower", Address: "follower", Client: follower}}, nil
	}
	require.ErrorIs(t, consumer.RunMigrationOwnershipRefreshOnce(t.Context()), errSplitMigrationVoterBarrierIncomplete)
	require.Len(t, client.cleanups, 1)
	blocked = false
	require.NoError(t, consumer.RunMigrationOwnershipRefreshOnce(t.Context()))
	require.Len(t, client.cleanups, 1)
	previous := probes
	require.NoError(t, consumer.RunMigrationOwnershipRefreshOnce(t.Context()))
	require.Equal(t, previous, probes, "same proof/config/term and C must not poll each voter again")
}

func TestOwnershipRefreshConsumerRejectsLegacyAndGenerationMismatch(t *testing.T) {
	for _, mismatch := range []string{"legacy", "legacy v1", "generation", "configuration", "exact proof", "closed readiness"} {
		t.Run(mismatch, func(t *testing.T) {
			root, prior := ownershipRefreshCatalog(t)
			rootClient, closeClient := newBufconnDistributionClient(t, root)
			defer closeClient()
			consumer, client, _ := ownershipRefreshConsumer(t, store.NewMVCCStore(), rootClient, prior)
			peerClient := refreshProbeClient{SplitMigrationClient: client, probe: func(req *pb.ProbeMigrationStateRequest, resp *pb.ProbeMigrationStateResponse) {
				switch mismatch {
				case "legacy":
					resp.OwnershipProtocol = 0
				case "legacy v1":
					resp.OwnershipProtocol = 1
				case "generation":
					resp.OwnershipGeneration = "different-native-generation"
				case "configuration":
					resp.AppliedConfigurationIndex++
				case "exact proof":
					if req.GetKind() == pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_OWNERSHIP_APPLIED {
						resp.Ownership.CatalogVersion++
					}
				case "closed readiness":
					resp.Ready = false
				}
			}}
			consumer.splitMigrationVoterFactory = func(context.Context, uint64) ([]SplitMigrationVoter, error) {
				return []SplitMigrationVoter{{ID: "voter", Address: "voter", Client: peerClient}}, nil
			}
			require.Error(t, consumer.RunMigrationOwnershipRefreshOnce(t.Context()))
			if mismatch != "exact proof" {
				require.Empty(t, client.cleanups)
			}
		})
	}
}

func TestOwnershipRefreshConsumerRetainsNonownerAndNativeStateOnRestart(t *testing.T) {
	root, prior := ownershipRefreshCatalog(t)
	prior = proto.CloneOf(prior)
	prior.LocalGroupId, prior.Role = 9, pb.MigrationOwnershipRole_MIGRATION_OWNERSHIP_ROLE_NONOWNER
	rootClient, closeClient := newBufconnDistributionClient(t, root)
	defer closeClient()
	dir := t.TempDir()
	st, err := store.NewPebbleStore(dir)
	require.NoError(t, err)
	consumer, _, _ := ownershipRefreshConsumer(t, st, rootClient, prior)
	require.NoError(t, consumer.RunMigrationOwnershipRefreshOnce(t.Context()))
	writer, ok := st.(store.MigrationOwnershipStore)
	require.True(t, ok)
	before, err := writer.MigrationServingState(t.Context())
	require.NoError(t, err)
	require.Equal(t, store.MigrationNonOwner, before.Ownership[0].Role)
	require.NoError(t, st.Close())
	reopened, err := store.NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	consumer, client, _ := ownershipRefreshConsumer(t, reopened, rootClient, prior)
	require.NoError(t, consumer.RunMigrationOwnershipRefreshOnce(t.Context()))
	require.Empty(t, client.cleanups)
	writer, ok = reopened.(store.MigrationOwnershipStore)
	require.True(t, ok)
	after, err := writer.MigrationServingState(t.Context())
	require.NoError(t, err)
	require.Equal(t, before.Ownership, after.Ownership)
	retirement, ok := reopened.(store.MigrationRetirementReader)
	require.True(t, ok)
	retired, err := retirement.MigrationRetired(t.Context(), 7)
	require.NoError(t, err)
	require.True(t, retired)
}

func TestOwnershipRefreshConsumerRejectsUnsupportedRoot(t *testing.T) {
	_, prior := ownershipRefreshCatalog(t)
	rootClient, closeClient := newBufconnDistributionClient(t, &pb.UnimplementedDistributionServer{})
	defer closeClient()
	consumer, client, _ := ownershipRefreshConsumer(t, store.NewMVCCStore(), rootClient, prior)
	err := consumer.RunMigrationOwnershipRefreshOnce(t.Context())
	require.Equal(t, codes.Unimplemented, status.Code(err))
	require.Empty(t, client.cleanups)
}

func TestOwnershipRefreshConsumerRotatesGroupsAfterExhaustedBudget(t *testing.T) {
	root, prior := ownershipRefreshCatalog(t)
	rootClient, closeClient := newBufconnDistributionClient(t, root)
	defer closeClient()
	consumer, client, _ := ownershipRefreshConsumer(t, store.NewMVCCStore(), rootClient, prior)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	consumer.ownershipRefresh.groups[1] = &kv.ShardGroup{Store: store.NewMVCCStore(), Engine: ownershipRefreshEngine{read: func(context.Context) error {
		cancel()
		return context.Canceled
	}}}
	require.Error(t, consumer.RunMigrationOwnershipRefreshOnce(ctx))
	require.Empty(t, client.cleanups)
	ctx, cancel = context.WithCancel(t.Context())
	defer cancel()
	require.Error(t, consumer.RunMigrationOwnershipRefreshOnce(ctx))
	require.Len(t, client.cleanups, 1, "an earlier slow group cannot consume every pass's budget")
}

func TestOwnershipRefreshConsumerReplicatesWithoutJobHistory(t *testing.T) {
	root, prior := ownershipRefreshCatalog(t)
	rootClient, closeClient := newBufconnDistributionClient(t, root)
	defer closeClient()
	st := store.NewMVCCStore()
	consumer, client, _ := ownershipRefreshConsumer(t, st, rootClient, prior)
	require.NoError(t, consumer.RunMigrationOwnershipRefreshOnce(t.Context()))
	require.Len(t, client.cleanups, 1)
	require.Equal(t, pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_OWNERSHIP_REFRESH, client.cleanups[0].GetMode())
	require.EqualValues(t, 9, client.cleanups[0].GetExpectedConfigurationIndex())
	writer, ok := st.(store.MigrationOwnershipStore)
	require.True(t, ok)
	view, err := writer.MigrationServingState(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 2, view.Ownership[0].CatalogVersion)
	require.Equal(t, prior.GetGeneration(), view.Ownership[0].Generation)
	require.Len(t, view.Ownership[0].Receipts, 1)
	require.EqualValues(t, 1, view.Ownership[0].Receipts[0].CatalogVersion)
	require.NoError(t, consumer.RunMigrationOwnershipRefreshOnce(t.Context()))
	require.Len(t, client.cleanups, 1, "mirror shape is only a trigger, never a replacement descriptor")
}

func ownershipRefreshCatalog(t *testing.T) (*DistributionServer, *pb.MigrationOwnershipProof) {
	t.Helper()
	st := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(st, distribution.WithCatalogRouteDescriptorV2Writes(true))
	routes := []distribution.RouteDescriptor{
		{RouteID: 2, ParentRouteID: 1, Start: []byte("a"), End: []byte("m"), GroupID: 7},
		{RouteID: 3, ParentRouteID: 1, Start: []byte("m"), End: []byte("z"), GroupID: 7, MinWriteTSExclusive: 100},
	}
	_, err := catalog.Save(t.Context(), 0, routes)
	require.NoError(t, err)
	snapshot, err := catalog.Snapshot(t.Context())
	require.NoError(t, err)
	job := sampleDistributionSplitJob(7)
	job.SourceRouteID, job.SourceGroupID, job.TargetGroupID, job.SplitKey = 1, 7, 9, []byte("m")
	job.Phase, job.SnapshotTS, job.FenceTS = distribution.SplitJobPhaseAbandoning, 80, 100
	prior, err := splitOwnershipProof(snapshot, job, 7, "0123456789abcdef0123456789abcdef")
	require.NoError(t, err)
	routes[0].GroupID = 11
	_, err = catalog.Save(t.Context(), snapshot.Version, routes)
	require.NoError(t, err)
	coordinator := &ownershipCatalogCoordinator{distributionCoordinatorStub: newDistributionCoordinatorStub(st, true)}
	return NewDistributionServer(distribution.NewEngine(), catalog, WithDistributionCoordinator(coordinator)), prior
}

func TestOwnershipAttestationRPCUsesCommittedCatalogWithoutJobHistory(t *testing.T) {
	server, prior := ownershipRefreshCatalog(t)
	client, closeClient := newBufconnDistributionClient(t, server)
	defer closeClient()
	resp, err := client.AttestMigrationOwnership(t.Context(), &pb.AttestMigrationOwnershipRequest{JobId: 7, Prior: prior})
	require.NoError(t, err)
	require.EqualValues(t, 2, resp.GetOwnershipProtocol())
	require.Equal(t, prior.GetCatalogVersion()+1, resp.GetOwnership().GetCatalogVersion())
	require.Equal(t, prior.GetDescriptorHash(), resp.GetOwnership().GetDescriptorHash())
	server.coordinator = newDistributionCoordinatorStub(store.NewMVCCStore(), false)
	_, err = client.AttestMigrationOwnership(t.Context(), &pb.AttestMigrationOwnershipRequest{JobId: 7, Prior: prior})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestOwnershipAttestationRPCPreservesProtocolTwoSourceBoundary(t *testing.T) {
	server, prior := ownershipRefreshCatalog(t)
	prior.WriteFloorKnown, prior.SourceTrackerVersion, prior.SourceAppliedIndex = true, 1, 10
	client, closeClient := newBufconnDistributionClient(t, server)
	defer closeClient()
	resp, err := client.AttestMigrationOwnership(t.Context(), &pb.AttestMigrationOwnershipRequest{JobId: 7, Prior: prior})
	require.NoError(t, err)
	require.EqualValues(t, 2, resp.GetOwnershipProtocol())
	require.True(t, resp.GetOwnership().GetWriteFloorKnown())
	require.EqualValues(t, 1, resp.GetOwnership().GetSourceTrackerVersion())
	require.EqualValues(t, 10, resp.GetOwnership().GetSourceAppliedIndex())
	expected := proto.CloneOf(prior)
	expected.CatalogVersion = 2
	require.True(t, proto.Equal(expected, resp.GetOwnership()), "only C may advance across the authenticated producer RPC")
}

type legacyOwnershipAttestationServer struct {
	pb.UnimplementedDistributionServer
	proof *pb.MigrationOwnershipProof
}

func (s legacyOwnershipAttestationServer) AttestMigrationOwnership(context.Context, *pb.AttestMigrationOwnershipRequest) (*pb.AttestMigrationOwnershipResponse, error) {
	return &pb.AttestMigrationOwnershipResponse{OwnershipProtocol: 1, Ownership: s.proof}, nil
}

func TestOwnershipRefreshConsumerRejectsProtocolOneRoot(t *testing.T) {
	_, prior := ownershipRefreshCatalog(t)
	refreshed := proto.CloneOf(prior)
	refreshed.CatalogVersion = 2
	client, closeClient := newBufconnDistributionClient(t, legacyOwnershipAttestationServer{proof: refreshed})
	defer closeClient()
	consumer, data, _ := ownershipRefreshConsumer(t, store.NewMVCCStore(), client, prior)
	require.ErrorIs(t, consumer.RunMigrationOwnershipRefreshOnce(t.Context()), store.ErrNotSupported)
	require.Empty(t, data.cleanups, "protocol 1 cannot attest the protocol 2 source boundary")
}

func TestOwnershipAttestationRPCRequiresMigrationCredential(t *testing.T) {
	for _, enforced := range []bool{false, true} {
		unary, _ := SplitMigrationTokenAuth(t.Name(), false, enforced)
		for _, token := range []string{"", "other", t.Name()} {
			ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs("authorization", "Bearer "+token))
			ctx = peer.NewContext(ctx, &peer.Peer{AuthInfo: migrationTLSInfoFixture(t)})
			called := false
			_, err := unary(ctx, &pb.AttestMigrationOwnershipRequest{}, &grpc.UnaryServerInfo{
				FullMethod: pb.Distribution_AttestMigrationOwnership_FullMethodName,
			}, func(context.Context, any) (any, error) { called = true; return nil, nil })
			if token == t.Name() {
				require.NoError(t, err)
				require.True(t, called)
			} else {
				require.Equal(t, codes.Unauthenticated, status.Code(err))
				require.False(t, called)
			}
		}
		plain := metadata.NewIncomingContext(t.Context(), metadata.Pairs("authorization", "Bearer "+t.Name()))
		_, err := unary(plain, &pb.AttestMigrationOwnershipRequest{}, &grpc.UnaryServerInfo{
			FullMethod: pb.Distribution_AttestMigrationOwnership_FullMethodName,
		}, func(context.Context, any) (any, error) {
			t.Fatal("plaintext must not reach the root producer")
			return nil, nil
		})
		require.Equal(t, codes.Unauthenticated, status.Code(err))
	}
}
