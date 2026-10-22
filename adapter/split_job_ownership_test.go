package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ownershipRuntimeLeader struct {
	mockInternalLeader
	configuration raftengine.AppliedConfigurationAware
}

func (l ownershipRuntimeLeader) AppliedConfigurationIndex() (uint64, bool) {
	index := l.configuration.AppliedConfigurationIndex()
	return index, index != 0
}

type ownershipRuntimeProposer struct {
	fsm   kv.FSM
	index uint64
}

func (p *ownershipRuntimeProposer) Propose(_ context.Context, data []byte) (*raftengine.ProposalResult, error) {
	p.index++
	aware, ok := p.fsm.(raftengine.ApplyIndexAware)
	if !ok {
		return nil, store.ErrNotSupported
	}
	aware.SetApplyIndex(p.index)
	return &raftengine.ProposalResult{CommitIndex: p.index, Response: p.fsm.Apply(data)}, nil
}

func (p *ownershipRuntimeProposer) ProposeAdmin(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	return p.Propose(ctx, data)
}

type ownershipRuntimeClient struct {
	splitMigrationClientStub
	internal      *Internal
	beforeCleanup func()
	blockGuard    bool
}

func (c *ownershipRuntimeClient) CleanupMigration(ctx context.Context, req *pb.CleanupMigrationRequest, _ ...grpc.CallOption) (*pb.CleanupMigrationResponse, error) {
	c.cleanups = append(c.cleanups, req)
	if c.beforeCleanup != nil {
		c.beforeCleanup()
	}
	return c.internal.CleanupMigration(ctx, req)
}

func (c *ownershipRuntimeClient) ProbeMigrationState(ctx context.Context, req *pb.ProbeMigrationStateRequest, _ ...grpc.CallOption) (*pb.ProbeMigrationStateResponse, error) {
	if c.blockGuard && (req.GetKind() == pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_SOURCE_GUARD_PRESENT ||
		req.GetKind() == pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_METADATA_CLEARED) {
		return &pb.ProbeMigrationStateResponse{}, nil
	}
	return c.internal.ProbeMigrationState(ctx, req)
}

func newOwnershipRuntimeClient(t *testing.T, st store.MVCCStore, groupID uint64) (*ownershipRuntimeClient, raftengine.AppliedConfigurationAware) {
	t.Helper()
	fsm := kv.NewKvFSMWithHLC(st, nil, kv.WithRouteHistory(nil, groupID))
	configuration, ok := fsm.(raftengine.AppliedConfigurationAware)
	require.True(t, ok)
	require.NoError(t, configuration.RecordAppliedConfiguration(9))
	proposer := &ownershipRuntimeProposer{fsm: fsm, index: 20}
	i := NewInternalWithEngine(nil, ownershipRuntimeLeader{configuration: configuration}, nil, nil,
		WithInternalStore(st), WithInternalMigrationProposer(proposer), WithInternalMigrationOwnershipReady(func() bool { return true }),
		WithInternalMigrationCleanupGate(func(context.Context) error { return nil }))
	return &ownershipRuntimeClient{internal: i}, configuration
}

type ownershipCatalogCoordinator struct {
	*distributionCoordinatorStub
	reads int
}

func (c *ownershipCatalogCoordinator) LinearizableReadForKey(ctx context.Context, _ []byte) (uint64, error) {
	c.reads++
	return c.LinearizableRead(ctx)
}

func TestSplitOwnershipRollbackActualCommittedCAndTargetDown(t *testing.T) {
	for _, changeConfiguration := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal", true: "configuration interleaving"}[changeConfiguration], func(t *testing.T) {
			catalogStore := store.NewMVCCStore()
			catalog := distribution.NewCatalogStore(catalogStore, distribution.WithCatalogRouteDescriptorV2Writes(true))
			_, err := catalog.Save(t.Context(), 0, []distribution.RouteDescriptor{
				{RouteID: 2, ParentRouteID: 1, Start: []byte("a"), End: []byte("m"), GroupID: 7},
				{RouteID: 3, ParentRouteID: 1, Start: []byte("m"), End: []byte("z"), GroupID: 7, State: distribution.RouteStateWriteFenced},
			})
			require.NoError(t, err)
			job := sampleDistributionSplitJob(7)
			job.SourceRouteID, job.SourceGroupID, job.TargetGroupID, job.SplitKey = 1, 7, 9, []byte("m")
			job.Phase, job.AbandonFromPhase = distribution.SplitJobPhaseAbandoning, distribution.SplitJobPhaseDeltaCopy
			job.SnapshotTS, job.FenceTS = 80, 100
			require.NoError(t, catalog.CreateSplitJob(t.Context(), job))
			sourceStore := store.NewMVCCStore()
			source, configuration := newOwnershipRuntimeClient(t, sourceStore, 7)
			guardWriter, ok := sourceStore.(store.MigrationTargetReadinessRaftWriter)
			require.True(t, ok)
			require.NoError(t, guardWriter.ApplyTargetStagedReadinessAt(t.Context(), store.TargetStagedReadinessState{
				JobID: job.JobID, MigrationJobID: job.JobID, RouteStart: job.SplitKey, RouteEnd: []byte("z"),
				Armed: true, SourceWriteFence: true, ExpectedCutoverVersion: 1, MinWriteTSExclusive: 100, RetentionPinTS: 1,
			}, 10))
			coordinator := &ownershipCatalogCoordinator{distributionCoordinatorStub: newDistributionCoordinatorStub(catalogStore, true)}
			target := &abandonUnavailableTarget{}
			targetLookups := 0
			server := NewDistributionServer(distribution.NewEngine(), catalog, WithDistributionCoordinator(coordinator),
				WithSplitMigrationGroupClientFactory(func(context.Context, uint64) (SplitMigrationClient, error) { return source, nil }),
				WithSplitMigrationVoterFactory(func(context.Context, uint64) ([]SplitMigrationVoter, error) {
					return []SplitMigrationVoter{{ID: "source", Address: "source:1", Client: source}}, nil
				}),
				WithSplitMigrationClientFactory(func(context.Context, distribution.SplitJob, uint64) (SplitMigrationClient, SplitMigrationClient, error) {
					targetLookups++
					return source, target, nil
				}))
			source.beforeCleanup = func() {
				current, err := catalog.Snapshot(t.Context())
				require.NoError(t, err)
				require.Nil(t, abandonedSplitJobFencedRoute(current.Routes, job), "rollback must already be committed")
				reader, ok := sourceStore.(store.MigrationTargetReadinessReader)
				require.True(t, ok)
				guards, err := reader.MigrationTargetReadinessStates(t.Context())
				require.NoError(t, err)
				require.Len(t, guards, 1, "durable guard remains until atomic ownership replacement")
				if changeConfiguration {
					require.NoError(t, configuration.RecordAppliedConfiguration(11))
				}
			}
			err = server.abandonSplitJob(t.Context(), job)
			if changeConfiguration {
				require.Equal(t, codes.Aborted, status.Code(err))
				source.beforeCleanup = nil
				require.NoError(t, server.abandonSplitJob(t.Context(), job))
			} else {
				require.NoError(t, err)
			}
			require.Zero(t, targetLookups, "source recovery must not contact the unavailable target")
			reader, ok := sourceStore.(store.MigrationOwnershipStore)
			require.True(t, ok)
			view, err := reader.MigrationServingState(t.Context())
			require.NoError(t, err)
			require.Empty(t, view.Readiness)
			require.Len(t, view.Ownership, 1)
			committed, err := catalog.Snapshot(t.Context())
			require.NoError(t, err)
			require.Equal(t, committed.Version, view.Ownership[0].CatalogVersion)
			require.Equal(t, store.MigrationOwner, view.Ownership[0].Role)
			require.NotEmpty(t, view.Ownership[0].Generation)
			require.Positive(t, coordinator.reads)
			job, _, err = catalog.SplitJob(t.Context(), job.JobID)
			require.NoError(t, err)
			require.ErrorContains(t, server.abandonSplitJob(t.Context(), job), "target offline")
		})
	}
}

func TestMigrationOwnershipCapabilityDefaultDenyAndUnknownEpoch(t *testing.T) {
	st := store.NewMVCCStore()
	client, _ := newOwnershipRuntimeClient(t, st, 7)
	req := &pb.ProbeMigrationStateRequest{JobId: 7, Kind: pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_OWNERSHIP_CAPABILITY, RequireConfigurationIndex: true}
	resp, err := client.ProbeMigrationState(t.Context(), req)
	require.NoError(t, err)
	require.True(t, resp.GetReady())
	client.internal.migrationOwnershipReady = nil
	resp, err = client.ProbeMigrationState(t.Context(), req)
	require.NoError(t, err)
	require.False(t, resp.GetReady())
	client.internal.migrationOwnershipReady = func() bool { return true }
	client.internal.leader = migrationConfigurationLeader{index: 0}
	resp, err = client.ProbeMigrationState(t.Context(), req)
	require.NoError(t, err)
	require.False(t, resp.GetReady(), "a copied store row cannot replace an actual runtime epoch")
	require.Zero(t, resp.GetOwnershipProtocol())
}

func TestSplitOwnershipCapabilityRejectsOldUnknownFieldEcho(t *testing.T) {
	old := &splitMigrationClientStub{}
	server := &DistributionServer{splitMigrationVoterFactory: func(context.Context, uint64) ([]SplitMigrationVoter, error) {
		return []SplitMigrationVoter{{ID: "old", Address: "old:1", Client: old}}, nil
	}}
	_, _, err := server.splitOwnershipConfiguration(t.Context(), 7, 7, old)
	require.ErrorIs(t, err, errSplitMigrationVoterBarrierIncomplete)
}

func TestAttestUnchangedMigrationOwnershipUsesCommittedDescriptor(t *testing.T) {
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
	coordinator := &ownershipCatalogCoordinator{distributionCoordinatorStub: newDistributionCoordinatorStub(st, true)}
	server := NewDistributionServer(distribution.NewEngine(), catalog, WithDistributionCoordinator(coordinator))
	// This change concerns a different route; no migration job lookup or
	// watcher history is needed to attest the retained descriptor.
	routes[0].GroupID = 11
	_, err = catalog.Save(t.Context(), snapshot.Version, routes)
	require.NoError(t, err)
	refreshed, err := server.AttestUnchangedMigrationOwnership(t.Context(), prior)
	require.NoError(t, err)
	require.Equal(t, prior.GetCatalogVersion()+1, refreshed.GetCatalogVersion())
	require.Equal(t, prior.GetDescriptorHash(), refreshed.GetDescriptorHash())
	require.Equal(t, prior.GetGeneration(), refreshed.GetGeneration())
	require.Equal(t, snapshot.Version, prior.GetCatalogVersion(), "producer must not mutate durable prior evidence")
	require.Equal(t, 1, coordinator.reads)
	routes[1].GroupID = 9
	_, err = catalog.Save(t.Context(), refreshed.GetCatalogVersion(), routes)
	require.NoError(t, err)
	_, err = server.AttestUnchangedMigrationOwnership(t.Context(), refreshed)
	require.ErrorContains(t, err, "descriptor changed")
}
