package kv

import (
	"crypto/sha256"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func installSourceOwnershipForTest(t *testing.T, f *kvFSM, floor uint64) {
	t.Helper()
	require.NoError(t, f.RecordAppliedConfiguration(9))
	guard, err := MarshalTargetStagedReadinessCommand(&pb.TargetStagedReadinessRequest{JobId: 11, MigrationJobId: 11,
		Armed: true, TrackWrites: true, RouteStart: []byte("m"), RouteEnd: []byte("z"),
		ExpectedCutoverVersion: 1, MinWriteTsExclusive: 1, RetentionPinTs: 1})
	require.NoError(t, err)
	f.SetApplyIndex(10)
	require.Nil(t, f.Apply(guard))
	route := distribution.RouteDescriptor{RouteID: 1, Start: []byte("a"), End: []byte("z"), GroupID: 1,
		State: distribution.RouteStateActive, MinWriteTSExclusive: floor}
	encoded, err := distribution.EncodeRouteDescriptorForCatalogWrite(route, true)
	require.NoError(t, err)
	digest := sha256.Sum256(encoded)
	command, err := MarshalMigrationCleanupCommand(&pb.CleanupMigrationRequest{JobId: 11,
		Mode:                      pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_SOURCE_OWNERSHIP_ACQUIRE,
		RequireConfigurationIndex: true, ExpectedConfigurationIndex: 9,
		Ownership: &pb.MigrationOwnershipProof{Generation: "0123456789abcdef0123456789abcdef", LocalGroupId: 1, OwnerGroupId: 1,
			CatalogVersion: 1, RouteId: 1, RouteStart: []byte("m"), RouteEnd: []byte("z"), WriteFloorKnown: true,
			MinWriteTsExclusive: floor, SourceTrackerVersion: 1, DescriptorHash: digest[:], Role: pb.MigrationOwnershipRole_MIGRATION_OWNERSHIP_ROLE_OWNER}})
	require.NoError(t, err)
	f.SetApplyIndex(11)
	response := f.Apply(command)
	result, ok := response.(store.CleanupVersionsResult)
	require.True(t, ok, "ownership apply returned %v", response)
	require.True(t, result.Done)
}

func TestSourceOwnershipApplyIgnoresWatcherAndBindsConfiguration(t *testing.T) {
	for _, watcher := range []RouteHistory{forbiddenApplyWatcher{}, WrapDistributionEngine(distribution.NewEngine()), WrapDistributionEngine(staleServingEngine(t, 999))} {
		f := &kvFSM{store: store.NewMVCCStore(), shardGroupID: 1, routes: watcher}
		installSourceOwnershipForTest(t, f, 0)
		req := &pb.Request{Ts: 50, ObservedRouteVersion: EncodeObservedRouteVersion(1), Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("n"), Value: []byte("value")}}}
		require.NoError(t, f.applyRequestErr(t.Context(), req))
		require.Equal(t, uint64(50), migrationTrackerMinimum(t, f))
		require.NoError(t, f.RecordAppliedConfiguration(12))
		req.Ts = 60
		require.ErrorIs(t, f.applyRequestErr(t.Context(), req), ErrRouteCutoverPending)
		reader, ok := f.store.(store.MigrationOwnershipStore)
		require.True(t, ok)
		view, err := reader.MigrationServingState(t.Context())
		require.NoError(t, err)
		refresh, err := MarshalMigrationCleanupCommand(&pb.CleanupMigrationRequest{JobId: 11,
			Mode: pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_OWNERSHIP_REFRESH, RequireConfigurationIndex: true,
			ExpectedConfigurationIndex: 12, Ownership: MigrationOwnershipProofFromState(view.Ownership[0])})
		require.NoError(t, err)
		f.SetApplyIndex(13)
		_, ok = f.Apply(refresh).(store.CleanupVersionsResult)
		require.True(t, ok)
		require.NoError(t, f.applyRequestErr(t.Context(), req))
	}
}

func TestSourceOwnershipStaleControlsCannotReopenCreation(t *testing.T) {
	f := &kvFSM{store: store.NewMVCCStore(), shardGroupID: 1, routes: forbiddenApplyWatcher{}}
	installSourceOwnershipForTest(t, f, 0)
	control := &pb.TargetStagedReadinessRequest{JobId: 11, MigrationJobId: 11, Armed: true, TrackWrites: true,
		RouteStart: []byte("m"), RouteEnd: []byte("z"), ExpectedCutoverVersion: 2, MinWriteTsExclusive: 70,
		RetentionPinTs: 1, SourceWriteFence: true, SourceReadFence: true}
	command, err := MarshalTargetStagedReadinessCommand(control)
	require.NoError(t, err)
	f.SetApplyIndex(12)
	require.Nil(t, f.Apply(command))
	control.SourceWriteFence, control.SourceReadFence, control.MinWriteTsExclusive = false, false, 1
	control.ExpectedCutoverVersion = 1
	command, err = MarshalTargetStagedReadinessCommand(control)
	require.NoError(t, err)
	f.SetApplyIndex(13)
	require.Nil(t, f.Apply(command))
	require.ErrorIs(t, f.applyRequestErr(t.Context(), &pb.Request{Ts: 80, ObservedRouteVersion: EncodeObservedRouteVersion(1),
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("n"), Value: []byte("stale")}}}), ErrRouteCutoverPending)
	control.RouteEnd = []byte("y")
	command, err = MarshalTargetStagedReadinessCommand(control)
	require.NoError(t, err)
	f.SetApplyIndex(14)
	response := f.Apply(command)
	require.NoError(t, haltApplyOf(response))
	applyErr, ok := response.(error)
	require.True(t, ok)
	require.ErrorIs(t, applyErr, store.ErrMigrationOwnership)
	require.Equal(t, uint64(70), f.store.LastCommitTS(), "rejection must not change the HLC floor")
}
