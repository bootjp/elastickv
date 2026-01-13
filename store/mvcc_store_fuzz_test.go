package store

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func FuzzMVCCStore_PutGet(f *testing.F) {
	f.Add([]byte("key"), []byte("value"), uint64(100))
	f.Add([]byte(""), []byte(""), uint64(0))

	f.Fuzz(func(t *testing.T, key []byte, value []byte, ts uint64) {
		if len(key) == 0 {
			return
		}
		s := NewMVCCStore()
		ctx := context.Background()

		// 1. Put
		err := s.PutAt(ctx, key, value, ts, 0)
		require.NoError(t, err)

		// 2. Get at the same timestamp
		got, err := s.GetAt(ctx, key, ts)
		require.NoError(t, err)
		if !bytes.Equal(got, value) {
			t.Errorf("GetAt(%d) = %q, want %q", ts, got, value)
		}

		// 3. Get at a later timestamp
		gotLater, err := s.GetAt(ctx, key, ts+1)
		require.NoError(t, err)
		if !bytes.Equal(gotLater, value) {
			t.Errorf("GetAt(%d) = %q, want %q", ts+1, gotLater, value)
		}

		// 4. Get at an earlier timestamp should fail (assuming fresh store)
		if ts > 0 {
			_, err := s.GetAt(ctx, key, ts-1)
			require.ErrorIs(t, err, ErrKeyNotFound)
		}
	})
}

func FuzzMVCCStore_Delete(f *testing.F) {
	f.Add([]byte("key"), []byte("value"), uint64(100), uint64(101))

	f.Fuzz(func(t *testing.T, key []byte, value []byte, putTS uint64, delTS uint64) {
		if len(key) == 0 {
			return
		}
		// Ensure delTS > putTS for logical consistency in this test scenario,
		// though the store should handle any order of operations physically.
		// However, for logical "GetAt" assertion, we want a predictable sequence.
		if putTS >= delTS {
			delTS = putTS + 1
		}

		s := NewMVCCStore()
		ctx := context.Background()

		// 1. Put
		err := s.PutAt(ctx, key, value, putTS, 0)
		require.NoError(t, err)

		// 2. Delete
		err = s.DeleteAt(ctx, key, delTS)
		require.NoError(t, err)

		// 3. GetAt(putTS) should still see value
		got, err := s.GetAt(ctx, key, putTS)
		require.NoError(t, err)
		require.Equal(t, value, got)

		// 4. GetAt(delTS) should see nothing (ErrKeyNotFound)
		_, err = s.GetAt(ctx, key, delTS)
		require.ErrorIs(t, err, ErrKeyNotFound)
	})
}
