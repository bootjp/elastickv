package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

var nonEmptyBytes = rapid.SliceOf(rapid.Byte()).Filter(func(b []byte) bool { return len(b) > 0 })

func TestMVCCStore_Property_PutGet(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		key := nonEmptyBytes.Draw(t, "key")
		value := rapid.SliceOf(rapid.Byte()).Draw(t, "value")
		ts := rapid.Uint64Range(0, ^uint64(0)-100).Draw(t, "ts")

		s := NewMVCCStore()
		ctx := context.Background()

		err := s.PutAt(ctx, key, value, ts, 0)
		require.NoError(t, err)

		actualTS, ok, err := s.LatestCommitTS(ctx, key)
		require.NoError(t, err)
		require.True(t, ok)

		// 1. Get at the actual commit timestamp
		got, err := s.GetAt(ctx, key, actualTS)
		require.NoError(t, err)
		require.Equal(t, value, got)

		// 2. Get at a later timestamp
		laterTS := rapid.Uint64Range(actualTS, ^uint64(0)).Draw(t, "laterTS")
		gotLater, err := s.GetAt(ctx, key, laterTS)
		require.NoError(t, err)
		require.Equal(t, value, gotLater)

		// 3. Get at an earlier timestamp should fail
		if actualTS > 0 {
			_, err := s.GetAt(ctx, key, actualTS-1)
			require.ErrorIs(t, err, ErrKeyNotFound)
		}
	})
}

func TestMVCCStore_Property_Delete(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		key := nonEmptyBytes.Draw(t, "key")
		value := rapid.SliceOf(rapid.Byte()).Draw(t, "value")
		ts := rapid.Uint64Range(0, ^uint64(0)-100).Draw(t, "ts")

		s := NewMVCCStore()
		ctx := context.Background()

		// 1. Put
		err := s.PutAt(ctx, key, value, ts, 0)
		require.NoError(t, err)
		actualPutTS := s.LastCommitTS()

		// 2. Delete
		delTS := rapid.Uint64Range(actualPutTS, ^uint64(0)-1).Draw(t, "delTS")
		err = s.DeleteAt(ctx, key, delTS)
		require.NoError(t, err)
		actualDelTS := s.LastCommitTS()

		// 3. GetAt(actualPutTS) should still see value (Snapshot Isolation)
		got, err := s.GetAt(ctx, key, actualPutTS)
		require.NoError(t, err)
		require.Equal(t, value, got)

		// 4. GetAt(actualDelTS) should see nothing
		_, err = s.GetAt(ctx, key, actualDelTS)
		require.ErrorIs(t, err, ErrKeyNotFound)
	})
}
