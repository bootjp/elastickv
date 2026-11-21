package store

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestStageRaftEntryMigrationCleanupRollsBackWithProgress(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	st, err := NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	pebble, ok := st.(*pebbleStore)
	require.True(t, ok)
	key := []byte("a")
	require.NoError(t, pebble.PutAt(ctx, key, []byte("live"), 10, 0))
	require.NoError(t, pebble.SetDurableAppliedIndex(1))
	injected := errors.New("abort cleanup entry after staging")
	err = pebble.ApplyRaftEntry(ctx, 2, func(ctx context.Context) error {
		replayed, err := StageRaftEntryProgress(ctx, 2)
		require.NoError(t, err)
		require.False(t, replayed)
		result, err := StageRaftEntryMigrationCleanup(ctx, CleanupVersionsOptions{
			JobID: 7, AppliedIndex: 2, StartKey: key, EndKey: []byte("b"), MaxVersions: 1,
		})
		require.NoError(t, err)
		require.Equal(t, uint64(1), result.DeletedRows)
		_, err = RaftEntryLatestRecord(ctx, key)
		require.ErrorIs(t, err, ErrKeyNotFound)
		return injected
	})
	require.ErrorIs(t, err, injected)
	index, present, err := pebble.LastAppliedIndex()
	require.NoError(t, err)
	require.True(t, present)
	require.Equal(t, uint64(1), index)
	row, err := pebble.GetVersionAt(ctx, key, 20)
	require.NoError(t, err)
	require.Equal(t, VersionedValue{TS: 10, Value: []byte("live")}, row)
}
