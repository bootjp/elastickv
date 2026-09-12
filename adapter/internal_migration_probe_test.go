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

func TestInternalProbeSourceReadDrainIgnoresPostCutoverReads(t *testing.T) {
	t.Parallel()

	const cutoverTS = uint64(500)

	tests := []struct {
		name       string
		pins       []uint64
		drainMinTS uint64
		wantReady  bool
	}{
		{
			name:       "no active reads",
			drainMinTS: cutoverTS,
			wantReady:  true,
		},
		{
			name:       "pre-cutover read still in flight",
			pins:       []uint64{cutoverTS - 1},
			drainMinTS: cutoverTS,
			wantReady:  false,
		},
		{
			name:       "read pinned exactly at the cutover",
			pins:       []uint64{cutoverTS},
			drainMinTS: cutoverTS,
			wantReady:  false,
		},
		{
			name:       "unrelated post-cutover reads do not block",
			pins:       []uint64{cutoverTS + 1, cutoverTS + 9},
			drainMinTS: cutoverTS,
			wantReady:  true,
		},
		{
			name:       "one pre-cutover read among newer ones still blocks",
			pins:       []uint64{cutoverTS + 9, cutoverTS - 1},
			drainMinTS: cutoverTS,
			wantReady:  false,
		},
		{
			name:       "missing drain floor falls back to requiring an empty tracker",
			pins:       []uint64{cutoverTS + 1},
			drainMinTS: 0,
			wantReady:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			tracker := kv.NewActiveTimestampTracker()
			internal := NewInternalWithEngine(nil, nil, nil, nil,
				WithInternalStore(store.NewMVCCStore()),
				WithInternalActiveTimestampTracker(tracker),
			)
			for _, ts := range tt.pins {
				defer tracker.Pin(ts).Release()
			}

			got, err := internal.ProbeMigrationState(ctx, &pb.ProbeMigrationStateRequest{
				JobId:                7,
				Kind:                 pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_SOURCE_READ_DRAINED,
				ReadDrainNotBeforeMs: time.Now().Add(-time.Second).UnixMilli(),
				ReadDrainMinTs:       tt.drainMinTS,
			})
			require.NoError(t, err)
			require.Equal(t, tt.wantReady, got.GetReady())
		})
	}
}

func TestInternalProbeSourceReadDrainHonorsGracePeriod(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	internal := NewInternalWithEngine(nil, nil, nil, nil,
		WithInternalStore(store.NewMVCCStore()),
		WithInternalActiveTimestampTracker(kv.NewActiveTimestampTracker()),
	)

	got, err := internal.ProbeMigrationState(ctx, &pb.ProbeMigrationStateRequest{
		JobId:                7,
		Kind:                 pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_SOURCE_READ_DRAINED,
		ReadDrainNotBeforeMs: time.Now().Add(time.Hour).UnixMilli(),
		ReadDrainMinTs:       500,
	})
	require.NoError(t, err)
	require.False(t, got.GetReady())
}
