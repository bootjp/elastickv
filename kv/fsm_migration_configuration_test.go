package kv

import (
	"bytes"
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestMigrationCleanupConfigurationRejectsBeforeSideEffects(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []pb.MigrationCleanupMode{pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_VERSIONS, pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_METADATA} {
		t.Run(mode.String(), func(t *testing.T) {
			st := store.NewMVCCStore()
			fsm, ok := NewKvFSMWithHLC(st, nil).(*kvFSM)
			require.True(t, ok)
			require.NoError(t, st.PutAt(ctx, []byte("n"), []byte("customer"), 90, 0))
			state := store.TargetStagedReadinessState{JobID: 7, MigrationJobID: 7, RouteStart: []byte("a"), RouteEnd: []byte("z"),
				Armed: true, SourceWriteFence: true, SourceReadFence: true, RetentionPinTS: 1, MinWriteTSExclusive: 100}
			writer, ok := st.(store.MigrationTargetReadinessWriter)
			require.True(t, ok)
			require.NoError(t, writer.ApplyTargetStagedReadiness(ctx, state))
			require.NoError(t, fsm.RecordAppliedConfiguration(10))
			req := &pb.CleanupMigrationRequest{JobId: 7, Mode: mode, RequireConfigurationIndex: true, ExpectedConfigurationIndex: 10,
				RangeStart: []byte("a"), RangeEnd: []byte("z"), RouteStart: []byte("a"), RouteEnd: []byte("z"),
				KeyFamily: distribution.MigrationFamilyUser, MaxCommitTs: 100, MaxVersions: 10}
			cmd, err := MarshalMigrationCleanupCommand(req)
			require.NoError(t, err)
			// The cleanup was already constructed/proposed from configuration 10;
			// a ConfChange ordered before its apply invalidates that old proof.
			require.NoError(t, fsm.RecordAppliedConfiguration(11))
			resp := fsm.Apply(cmd)
			require.NoError(t, haltApplyOf(resp))
			responseErr, ok := resp.(error)
			require.True(t, ok)
			require.ErrorIs(t, responseErr, ErrMigrationConfigurationChanged)
			value, err := st.GetAt(ctx, []byte("n"), 100)
			require.NoError(t, err)
			require.Equal(t, []byte("customer"), value)
			reader, ok := st.(store.MigrationTargetReadinessReader)
			require.True(t, ok)
			states, err := reader.MigrationTargetReadinessStates(ctx)
			require.NoError(t, err)
			require.Equal(t, []store.TargetStagedReadinessState{state}, states)
			retirement, ok := st.(store.MigrationRetirementReader)
			require.True(t, ok)
			retired, err := retirement.MigrationRetired(ctx, 7)
			require.NoError(t, err)
			require.False(t, retired)
			require.Equal(t, uint64(100), st.LastCommitTS())
			req.RequireConfigurationIndex = false
			cmd, err = MarshalMigrationCleanupCommand(req)
			require.NoError(t, err)
			responseErr, ok = fsm.Apply(cmd).(error)
			require.True(t, ok)
			require.ErrorIs(t, responseErr, ErrMigrationConfigurationChanged, "an old runner cannot clear a live source guard")
			req.RequireConfigurationIndex = true
			req.ExpectedConfigurationIndex = 11
			cmd, err = MarshalMigrationCleanupCommand(req)
			require.NoError(t, err)
			resp = fsm.Apply(cmd)
			require.NoError(t, haltApplyOf(resp))
			if applyErr, ok := resp.(error); ok {
				require.NoError(t, applyErr)
			}
		})
	}
}

func TestMigrationConfigurationSurvivesSnapshotAndReopen(t *testing.T) {
	ctx := context.Background()
	st := store.NewMVCCStore()
	fsm, ok := NewKvFSMWithHLC(st, nil).(*kvFSM)
	require.True(t, ok)
	require.NoError(t, fsm.RecordAppliedConfiguration(10))
	require.NoError(t, fsm.RecordAppliedConfiguration(11))
	require.NoError(t, fsm.RecordAppliedConfiguration(5), "old ConfChange replay cannot regress the durable epoch")
	snapshot, err := st.Snapshot()
	require.NoError(t, err)
	defer snapshot.Close()
	var raw bytes.Buffer
	_, err = snapshot.WriteTo(&raw)
	require.NoError(t, err)
	dir := t.TempDir()
	pebble, err := store.NewPebbleStore(dir)
	require.NoError(t, err)
	require.NoError(t, pebble.Restore(bytes.NewReader(raw.Bytes())))
	restoredFSM, ok := NewKvFSMWithHLC(pebble, nil).(*kvFSM)
	require.True(t, ok)
	require.Equal(t, uint64(11), restoredFSM.AppliedConfigurationIndex())
	require.NoError(t, pebble.Close())
	pebble, err = store.NewPebbleStore(dir)
	require.NoError(t, err)
	defer pebble.Close()
	index, err := MigrationConfigurationIndex(ctx, pebble)
	require.NoError(t, err)
	require.Equal(t, uint64(11), index)
}

func TestMigrationConfigurationSharesDurableAppliedBoundary(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.NewPebbleStore(dir)
	require.NoError(t, err)
	fsm, ok := NewKvFSMWithHLC(st, nil).(*kvFSM)
	require.True(t, ok)
	require.NoError(t, fsm.RecordAppliedConfiguration(10))
	require.NoError(t, fsm.RecordAppliedConfiguration(11))
	reader, ok := st.(interface{ LastAppliedIndex() (uint64, bool, error) })
	require.True(t, ok)
	index, present, err := reader.LastAppliedIndex()
	require.NoError(t, err)
	require.True(t, present)
	require.Equal(t, uint64(11), index, "the skip gate cannot replay pre-epoch entries against a future epoch")
	require.NoError(t, st.Close())
	st, err = store.NewPebbleStore(dir)
	require.NoError(t, err)
	defer st.Close()
	index, err = MigrationConfigurationIndex(ctx, st)
	require.NoError(t, err)
	require.Equal(t, uint64(11), index)
}

func TestMigrationConfigurationCorruptionCannotBeOverwrittenByConfChange(t *testing.T) {
	st := store.NewMVCCStore()
	require.NoError(t, st.PutAt(t.Context(), migrationConfigurationIndexKey, []byte{1}, 100, 0))
	fsm, ok := NewKvFSMWithHLC(st, nil).(*kvFSM)
	require.True(t, ok)
	require.ErrorContains(t, fsm.RecordAppliedConfiguration(11), "invalid durable migration configuration index")
	value, err := st.GetAt(t.Context(), migrationConfigurationIndexKey, 100)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, value)
}
