package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// The existing runtime client executes real Internal/FSM/store commands, not
// Raft elections. Only catalog RPCs use bufconn; this is not quorum acceptance.
type resplitAcceptanceRuntime struct {
	server  *DistributionServer
	client  pb.DistributionClient
	clients map[uint64]*resplitAcceptanceClient
	groups  map[uint64]*kv.ShardGroup
	reader  *kv.ShardStore
}

type resplitAcceptanceClient struct{ *splitRuntimeClient }

func (c *resplitAcceptanceClient) IssueMigrationTimestamp(ctx context.Context, req *pb.IssueMigrationTimestampRequest, _ ...grpc.CallOption) (*pb.IssueMigrationTimestampResponse, error) {
	return c.internal.IssueMigrationTimestamp(ctx, req)
}

func (c *resplitAcceptanceClient) ProbeMigrationLocks(ctx context.Context, req *pb.ProbeMigrationLocksRequest, _ ...grpc.CallOption) (*pb.ProbeMigrationLocksResponse, error) {
	return c.internal.ProbeMigrationLocks(ctx, req)
}

func newResplitAcceptanceRuntime(t *testing.T) *resplitAcceptanceRuntime {
	t.Helper()
	root, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, root.Close()) })
	catalog := distribution.NewCatalogStore(root, distribution.WithCatalogRouteDescriptorV2Writes(true))
	_, err = catalog.Save(t.Context(), 0, []distribution.RouteDescriptor{{
		RouteID: 1, Start: []byte("a"), End: []byte("z"), GroupID: 1}})
	require.NoError(t, err)
	engine := distribution.NewEngine()
	r := &resplitAcceptanceRuntime{clients: make(map[uint64]*resplitAcceptanceClient), groups: make(map[uint64]*kv.ShardGroup)}
	for _, id := range []uint64{1, 2, 3} {
		local, openErr := store.NewPebbleStore(t.TempDir())
		require.NoError(t, openErr)
		t.Cleanup(func() { require.NoError(t, local.Close()) })
		client := &resplitAcceptanceClient{newSplitRuntimeClient(t, engine, local, id)}
		r.clients[id] = client
		proposer, ok := client.internal.migrationProposer.(*ownershipRuntimeProposer)
		require.True(t, ok)
		configuration, ok := proposer.fsm.(raftengine.AppliedConfigurationAware)
		require.True(t, ok)
		tx := kv.NewTransactionWithProposer(proposer, kv.WithMigrationRequestAdmission(client.internal.store, id))
		t.Cleanup(tx.Close)
		r.groups[id] = &kv.ShardGroup{Store: client.internal.store, Txn: tx,
			Engine: ownershipRefreshEngine{configuration: configuration}}
	}
	for _, key := range []string{"n", "y"} {
		require.NoError(t, r.clients[1].internal.store.PutAt(t.Context(), []byte(key), []byte("original-"+key), 10, 0))
	}
	r.server = NewDistributionServer(engine, catalog,
		WithDistributionCoordinator(&ownershipCatalogCoordinator{distributionCoordinatorStub: newDistributionCoordinatorStub(root, true)}),
		WithDistributionKnownRaftGroups(1, 2, 3),
		WithSplitMigrationCapabilityGate(r.capability),
		WithSplitJobRunnerReady(), WithSplitJobRunnerReadinessGate(func(context.Context) error { return nil }),
		WithSplitMigrationClientFactory(func(_ context.Context, job distribution.SplitJob, _ uint64) (SplitMigrationClient, SplitMigrationClient, error) {
			return r.clients[job.SourceGroupID], r.clients[job.TargetGroupID], nil
		}),
		WithSplitMigrationGroupClientFactory(func(_ context.Context, id uint64) (SplitMigrationClient, error) { return r.clients[id], nil }),
		WithSplitMigrationVoterFactory(func(_ context.Context, id uint64) ([]SplitMigrationVoter, error) {
			return []SplitMigrationVoter{{ID: "runtime", Address: "runtime", Client: r.clients[id]}}, nil
		}),
		WithSplitPromotionClientFactory(func(_ context.Context, job distribution.SplitJob) (SplitPromotionClient, error) {
			return r.clients[job.TargetGroupID], nil
		}))
	client, closeClient := newBufconnDistributionClient(t, r.server)
	r.client = client
	t.Cleanup(closeClient)
	WithMigrationOwnershipRefresh(r.groups, func(context.Context) (MigrationOwnershipAttestor, error) { return r.client, nil })(r.server)
	r.reader = kv.NewShardStore(engine, r.groups)
	return r
}

func (r *resplitAcceptanceRuntime) capability(ctx context.Context) error {
	for _, client := range r.clients {
		response, err := client.ProbeMigrationState(ctx, &pb.ProbeMigrationStateRequest{JobId: 1,
			Kind: pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_OWNERSHIP_CAPABILITY, RequireConfigurationIndex: true})
		if err != nil {
			return errors.WithStack(err)
		}
		if !response.GetReady() || response.GetOwnershipProtocol() < 2 || response.GetAppliedConfigurationIndex() == 0 {
			return errors.WithStack(store.ErrNotSupported)
		}
	}
	return nil
}

func (r *resplitAcceptanceRuntime) migrate(t *testing.T, route, target uint64, split string, afterTick ...func(distribution.SplitJob)) distribution.SplitJob {
	t.Helper()
	snapshot, err := r.server.catalog.Snapshot(t.Context())
	require.NoError(t, err)
	started, err := r.client.StartSplitMigration(t.Context(), &pb.StartSplitMigrationRequest{
		ExpectedCatalogVersion: snapshot.Version, RouteId: route, TargetGroupId: target, SplitKey: []byte(split)})
	require.NoError(t, err)
	for range 200 {
		require.NoError(t, r.server.RunSplitJobRunnerOnce(t.Context()))
		job, found, loadErr := r.server.catalog.SplitJob(t.Context(), started.GetJobId())
		require.NoError(t, loadErr)
		require.True(t, found)
		for _, check := range afterTick {
			check(job)
		}
		if job.Phase == distribution.SplitJobPhaseDone {
			return job
		}
	}
	t.Fatal("bounded runner did not finish migration")
	return distribution.SplitJob{}
}

func (r *resplitAcceptanceRuntime) ownership(t *testing.T, id uint64) []store.MigrationOwnershipState {
	t.Helper()
	reader, ok := r.groups[id].Store.(store.MigrationOwnershipStore)
	require.True(t, ok)
	view, err := reader.MigrationServingState(t.Context())
	require.NoError(t, err)
	require.NotEmpty(t, view.Ownership)
	for _, proof := range view.Ownership {
		require.True(t, store.MigrationOwnershipEvidenceValid(proof))
		require.Positive(t, proof.AppliedIndex)
		require.NotEmpty(t, proof.Receipts, "proof must originate from committed typed FSM controls")
	}
	return view.Ownership
}

func (r *resplitAcceptanceRuntime) put(t *testing.T, group uint64, key, value string, ts uint64) error {
	t.Helper()
	snapshot, err := r.server.catalog.Snapshot(t.Context())
	require.NoError(t, err)
	_, err = r.groups[group].Txn.Commit(t.Context(), []*pb.Request{{Ts: ts,
		ObservedRouteVersion: kv.EncodeObservedRouteVersion(snapshot.Version),
		Mutations:            []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte(key), Value: []byte(value)}}}})
	return err
}

func (r *resplitAcceptanceRuntime) read(t *testing.T, key, expected string) {
	t.Helper()
	value, err := r.reader.GetAt(t.Context(), []byte(key), ^uint64(0))
	if assert.NoError(t, err, "public routed read of %q", key) {
		assert.Equal(t, expected, string(value))
	}
}

func TestMigrationResplitAcceptanceBaselineMigratedOwner(t *testing.T) {
	r := newResplitAcceptanceRuntime(t)
	job := r.migrate(t, 1, 2, "m")
	owners, nonowners := r.ownership(t, 2), r.ownership(t, 1)
	require.Equal(t, store.MigrationOwner, owners[0].Role)
	require.Equal(t, store.MigrationNonOwner, nonowners[0].Role)
	for _, key := range []string{"n", "y"} {
		r.read(t, key, "original-"+key)
		require.ErrorIs(t, r.put(t, 2, key, "too-old", job.FenceTS), kv.ErrRouteWriteTimestampTooLow)
		require.NoError(t, r.put(t, 2, key, "baseline", job.FenceTS+10))
		r.read(t, key, "baseline")
		require.Error(t, r.put(t, 1, key, "stale-source", job.FenceTS+20))
		_, err := r.groups[1].Store.GetAt(t.Context(), []byte(key), ^uint64(0))
		require.ErrorIs(t, err, store.ErrKeyNotFound)
	}
}

func TestMigrationResplitAcceptanceSameGroupChildren(t *testing.T) {
	r := newResplitAcceptanceRuntime(t)
	job := r.migrate(t, 1, 2, "m")
	owners, nonowners := r.ownership(t, 2), r.ownership(t, 1)
	require.Equal(t, store.MigrationOwner, owners[0].Role)
	require.Equal(t, store.MigrationNonOwner, nonowners[0].Role)
	r.read(t, "n", "original-n")
	r.read(t, "y", "original-y")
	require.NoError(t, r.put(t, 2, "n", "original-n", job.FenceTS+10), "baseline migrated owner must accept writes before the re-split")
	snapshot, err := r.server.catalog.Snapshot(t.Context())
	require.NoError(t, err)
	split, err := r.client.SplitRange(t.Context(), &pb.SplitRangeRequest{
		ExpectedCatalogVersion: snapshot.Version, RouteId: owners[0].RouteID, SplitKey: []byte("t")})
	require.NoError(t, err)
	require.Equal(t, job.FenceTS, split.GetLeft().GetMinWriteTsExclusive())
	require.Equal(t, job.FenceTS, split.GetRight().GetMinWriteTsExclusive())
	assert.NoError(t, r.server.RunMigrationOwnershipRefreshOnce(t.Context()), "re-split partition must be recoverable from committed catalog")
	for _, key := range []string{"n", "y"} {
		t.Run(key, func(t *testing.T) {
			r.read(t, key, "original-"+key)
			assert.ErrorIs(t, r.put(t, 2, key, "too-old", job.FenceTS), kv.ErrRouteWriteTimestampTooLow)
			if assert.NoError(t, r.put(t, 2, key, "after-split", job.FenceTS+100)) {
				r.read(t, key, "after-split")
			}
			assert.Error(t, r.put(t, 1, key, "stale-source", job.FenceTS+200))
			_, getErr := r.groups[1].Store.GetAt(t.Context(), []byte(key), ^uint64(0))
			assert.ErrorIs(t, getErr, store.ErrKeyNotFound)
		})
	}
}

func TestMigrationResplitAcceptanceLaterCrossGroupRetainsLeft(t *testing.T) {
	r := newResplitAcceptanceRuntime(t)
	first := r.migrate(t, 1, 2, "m")
	owner := r.ownership(t, 2)[0]
	require.NoError(t, r.put(t, 2, "n", "original-n", first.FenceTS+10), "baseline migrated owner must accept writes before the second move")
	checkedTrackerWrites := false
	second := r.migrate(t, owner.RouteID, 3, "t", func(job distribution.SplitJob) {
		if checkedTrackerWrites || !job.WriteTrackerArmed {
			return
		}
		for _, key := range []string{"n", "y"} {
			ts := r.groups[2].Store.LastCommitTS() + 1
			require.NoError(t, r.put(t, 2, key, "original-"+key, ts), "source tracker must preserve full-parent creation before fencing")
		}
		checkedTrackerWrites = true
	})
	require.True(t, checkedTrackerWrites)
	require.GreaterOrEqual(t, second.FenceTS, first.FenceTS)
	assert.NoError(t, r.server.RunMigrationOwnershipRefreshOnce(t.Context()))
	r.read(t, "n", "original-n")
	r.read(t, "y", "original-y")
	for _, tc := range []struct {
		key   string
		group uint64
		floor uint64
	}{{"n", 2, first.FenceTS}, {"y", 3, second.FenceTS}} {
		t.Run(tc.key, func(t *testing.T) {
			assert.ErrorIs(t, r.put(t, tc.group, tc.key, "too-old", tc.floor), kv.ErrRouteWriteTimestampTooLow)
			if assert.NoError(t, r.put(t, tc.group, tc.key, "second-move", second.FenceTS+100)) {
				r.read(t, tc.key, "second-move")
			}
		})
	}
	assert.Error(t, r.put(t, 2, "y", "retired-owner", second.FenceTS+200))
	_, err := r.groups[2].Store.GetAt(t.Context(), []byte("y"), ^uint64(0))
	assert.ErrorIs(t, err, store.ErrKeyNotFound)
}
