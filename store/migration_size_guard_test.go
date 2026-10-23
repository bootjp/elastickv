package store

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func transferSizeReadinessForTest() TargetStagedReadinessState {
	return TargetStagedReadinessState{JobID: 11, MigrationJobID: 11, RouteStart: []byte("m"), RouteEnd: []byte("z"),
		Armed: true, TrackWrites: true, MinWriteTSExclusive: 1, RetentionPinTS: 1,
		TransferSizeGuardVersion: MigrationTransferSizeGuardVersion}
}

func TestMigrationSizeGuardCodecExplicitVersion(t *testing.T) {
	state := transferSizeReadinessForTest()
	encoded := encodeTargetStagedReadinessState(state)
	require.Equal(t, byte(3), encoded[0])
	decoded, ok := decodeTargetStagedReadinessState(encoded)
	require.True(t, ok)
	require.Equal(t, state, decoded)
	state.TransferSizeGuardVersion = 0
	// Frozen V2 bytes: legacy trackers retain their exact persisted contract.
	want, err := hex.DecodeString("020104000000000000000b0000000000000000000000000000000b000000000000000100000000000000010000000000000000016d01017a")
	require.NoError(t, err)
	require.Equal(t, want, encodeTargetStagedReadinessState(state))
	legacy, ok := decodeTargetStagedReadinessState(want)
	require.True(t, ok)
	require.Zero(t, legacy.TransferSizeGuardVersion)
	encoded[0] = 4
	_, ok = decodeTargetStagedReadinessState(encoded)
	require.False(t, ok, "unknown future guard codec must not silently lose the guard")
}

func TestMigrationSizeGuardSnapshotAndReopen(t *testing.T) {
	want := transferSizeReadinessForTest()
	runMigrationStoreSuite(t, func(t *testing.T, source MVCCStore) {
		writer, ok := source.(MigrationTargetReadinessWriter)
		require.True(t, ok)
		require.NoError(t, writer.ApplyTargetStagedReadiness(t.Context(), want))
		snapshot, err := source.Snapshot()
		require.NoError(t, err)
		raw := snapshotBytes(t, snapshot)
		require.NoError(t, snapshot.Close())
		// Native Pebble snapshots are not a memory-store input format.
		if _, memory := source.(*mvccStore); memory {
			destination := NewMVCCStore()
			t.Cleanup(func() { require.NoError(t, destination.Close()) })
			require.NoError(t, destination.Restore(bytes.NewReader(raw)))
			reader, ok := destination.(MigrationTargetReadinessReader)
			require.True(t, ok)
			states, err := reader.MigrationTargetReadinessStates(t.Context())
			require.NoError(t, err)
			require.Equal(t, []TargetStagedReadinessState{want}, states)
		}
		dir := t.TempDir()
		pebble, err := NewPebbleStore(dir)
		require.NoError(t, err)
		require.NoError(t, pebble.Restore(bytes.NewReader(raw)))
		require.NoError(t, pebble.Close())
		pebble, err = NewPebbleStore(dir)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, pebble.Close()) })
		reader, ok := pebble.(MigrationTargetReadinessReader)
		require.True(t, ok)
		states, err := reader.MigrationTargetReadinessStates(t.Context())
		require.NoError(t, err)
		require.Equal(t, []TargetStagedReadinessState{want}, states)
	})
}

func TestMigrationSizeGuardRejectsInvalidActivation(t *testing.T) {
	for _, change := range []func(*TargetStagedReadinessState){
		func(s *TargetStagedReadinessState) { s.TransferSizeGuardVersion = 2 },
		func(s *TargetStagedReadinessState) { s.Armed = false },
		func(s *TargetStagedReadinessState) { s.TrackWrites = false },
	} {
		state := transferSizeReadinessForTest()
		change(&state)
		runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
			writer, ok := st.(MigrationTargetReadinessWriter)
			require.True(t, ok)
			err := writer.ApplyTargetStagedReadiness(t.Context(), state)
			require.Error(t, err)
		})
	}
}
