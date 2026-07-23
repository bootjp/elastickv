package adapter

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestInternalProbeMigrationStateUsesLocalFSMAndCatalog(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 9,
		Routes: []distribution.RouteDescriptor{{
			RouteID:             2,
			Start:               []byte("m"),
			GroupID:             2,
			State:               distribution.RouteStateActive,
			MinWriteTSExclusive: 88,
		}},
	}))
	tracker := kv.NewActiveTimestampTracker()
	internal := NewInternalWithEngine(nil, nil, nil, nil,
		WithInternalStore(st),
		WithInternalRouteEngine(engine),
		WithInternalActiveTimestampTracker(tracker),
	)

	state := store.TargetStagedReadinessState{
		JobID:                  7,
		RouteStart:             []byte("m"),
		ExpectedCutoverVersion: 9,
		MigrationJobID:         7,
		MinWriteTSExclusive:    88,
		Armed:                  true,
	}
	readinessWriter, ok := st.(store.MigrationTargetReadinessWriter)
	require.True(t, ok)
	require.NoError(t, readinessWriter.ApplyTargetStagedReadiness(ctx, state))
	control, err := internal.ProbeMigrationState(ctx, &pb.ProbeMigrationStateRequest{
		JobId:                  7,
		Kind:                   pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_CONTROL_APPLIED,
		RouteStart:             []byte("m"),
		ExpectedCatalogVersion: 9,
		MigrationJobId:         7,
		MinWriteTsExclusive:    88,
	})
	require.NoError(t, err)
	require.True(t, control.Ready)

	cleared, err := internal.ProbeMigrationState(ctx, &pb.ProbeMigrationStateRequest{
		JobId:                  7,
		Kind:                   pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_TARGET_DESCRIPTOR_CLEARED,
		RouteStart:             []byte("m"),
		ExpectedCatalogVersion: 9,
		ExpectedGroupId:        2,
		MinWriteTsExclusive:    88,
	})
	require.NoError(t, err)
	require.True(t, cleared.Ready)

	pin := tracker.Pin(100)
	drained, err := internal.ProbeMigrationState(ctx, &pb.ProbeMigrationStateRequest{
		JobId:                7,
		Kind:                 pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_SOURCE_READ_DRAINED,
		ReadDrainNotBeforeMs: time.Now().Add(-time.Second).UnixMilli(),
	})
	require.NoError(t, err)
	require.False(t, drained.Ready)
	pin.Release()
	drained, err = internal.ProbeMigrationState(ctx, &pb.ProbeMigrationStateRequest{
		JobId:                7,
		Kind:                 pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_SOURCE_READ_DRAINED,
		ReadDrainNotBeforeMs: time.Now().Add(-time.Second).UnixMilli(),
	})
	require.NoError(t, err)
	require.True(t, drained.Ready)

	metadata, err := internal.ProbeMigrationState(ctx, &pb.ProbeMigrationStateRequest{
		JobId: 7,
		Kind:  pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_METADATA_CLEARED,
	})
	require.NoError(t, err)
	require.False(t, metadata.Ready)
	cleaner, ok := st.(store.MigrationCleaner)
	require.True(t, ok)
	require.NoError(t, cleaner.ClearMigrationState(ctx, 7, 0))
	metadata, err = internal.ProbeMigrationState(ctx, &pb.ProbeMigrationStateRequest{
		JobId: 7,
		Kind:  pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_METADATA_CLEARED,
	})
	require.NoError(t, err)
	require.True(t, metadata.Ready)
}

func TestInternalProbeMigrationMetadataClearedWaitsForImportMetadata(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	internal := NewInternalWithEngine(nil, nil, nil, nil, WithInternalStore(st))
	_, err := st.ImportVersions(ctx, store.ImportVersionsOptions{
		JobID:     7,
		BracketID: 1,
		BatchSeq:  1,
		Cursor:    []byte("cursor-1"),
		Versions: []store.MVCCVersion{{
			Key:      []byte("m/key"),
			Value:    []byte("value"),
			CommitTS: 42,
		}},
	})
	require.NoError(t, err)

	metadata, err := internal.ProbeMigrationState(ctx, &pb.ProbeMigrationStateRequest{
		JobId: 7,
		Kind:  pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_METADATA_CLEARED,
	})
	require.NoError(t, err)
	require.False(t, metadata.Ready)

	cleaner, ok := st.(store.MigrationCleaner)
	require.True(t, ok)
	require.NoError(t, cleaner.ClearMigrationState(ctx, 7, 0))
	metadata, err = internal.ProbeMigrationState(ctx, &pb.ProbeMigrationStateRequest{
		JobId: 7,
		Kind:  pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_METADATA_CLEARED,
	})
	require.NoError(t, err)
	require.True(t, metadata.Ready)
}

func TestInternalIssueMigrationTimestampFollowsSourceLastCommit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	require.NoError(t, st.ApplyMutations(ctx, []*store.KVPairMutation{{Key: []byte("m"), Value: []byte("value")}}, nil, 50, 50))
	internal := NewInternalWithEngine(nil, mockInternalLeader{}, nil, nil, WithInternalStore(st))

	resp, err := internal.IssueMigrationTimestamp(ctx, &pb.IssueMigrationTimestampRequest{})
	require.NoError(t, err)
	require.Equal(t, uint64(50), resp.GetLastCommitTs())
	require.Greater(t, resp.GetTimestamp(), resp.GetLastCommitTs())
}
