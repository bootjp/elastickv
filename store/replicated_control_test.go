package store

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func controlStore(t *testing.T, s MVCCStore) ReplicatedControlStore {
	t.Helper()
	w, ok := s.(ReplicatedControlStore)
	require.True(t, ok)
	return w
}

func TestReplicatedControlStateSurvivesSnapshotWithoutTimestampAllocation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for name, factory := range absenceTestStores(t) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := factory(t)
			key := []byte(ReplicatedControlPrefix + "ttl-inline")
			applied, err := controlStore(t, s).ApplyReplicatedControlState(ctx, key, []byte("first"), 1)
			require.NoError(t, err)
			require.True(t, applied)
			require.Zero(t, s.LastCommitTS())
			applied, err = controlStore(t, s).ApplyReplicatedControlState(ctx, key, []byte("second"), 2)
			require.NoError(t, err)
			require.True(t, applied)
			snapshot, err := s.Snapshot()
			require.NoError(t, err)
			defer snapshot.Close()
			var data bytes.Buffer
			_, err = snapshot.WriteTo(&data)
			require.NoError(t, err)
			target := factory(t)
			require.NoError(t, target.Restore(&data))
			got, err := target.GetAt(ctx, key, ^uint64(0))
			require.NoError(t, err)
			require.Equal(t, []byte("second"), got)
			require.Zero(t, target.LastCommitTS())
		})
	}
}

func TestReplicatedControlStateReopenAndIndexFence(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	key := []byte(ReplicatedControlPrefix + "ttl-inline")
	w := controlStore(t, s)
	applied, err := w.ApplyReplicatedControlState(ctx, key, []byte("current"), 7)
	require.NoError(t, err)
	require.True(t, applied)
	applied, err = w.ApplyReplicatedControlState(ctx, key, []byte("stale"), 6)
	require.NoError(t, err)
	require.False(t, applied)
	require.NoError(t, s.Close())
	s, err = NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()
	got, err := s.GetAt(ctx, key, ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, []byte("current"), got)
	index, present, err := pebbleStoreApplied(t, s).LastAppliedIndex()
	require.NoError(t, err)
	require.True(t, present)
	require.Equal(t, uint64(7), index)
	require.Zero(t, s.LastCommitTS())
	_, err = controlStore(t, s).ApplyReplicatedControlState(ctx, []byte("ordinary"), []byte("invalid"), 8)
	require.Error(t, err)
}
