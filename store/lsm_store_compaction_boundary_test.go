package store

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

func TestPebbleStore_Compact_RetentionBoundary(t *testing.T) {
	for _, minTS := range []uint64{1, 25, math.MaxUint64 - 1} {
		t.Run(fmt.Sprint(minTS), func(t *testing.T) {
			s, err := NewPebbleStore(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, s.Close()) })
			ctx := context.Background()
			key := []byte("boundary")
			for _, ts := range []uint64{minTS - 1, minTS, minTS + 1} {
				require.NoError(t, s.PutAt(ctx, key, []byte(fmt.Sprint(ts)), ts, 0))
			}
			require.Equal(t, 3, countPebbleVersions(t, s))
			require.NoError(t, s.Compact(ctx, minTS))
			value, err := s.GetAt(ctx, key, minTS)
			require.NoError(t, err)
			require.Equal(t, []byte(fmt.Sprint(minTS)), value)
			value, err = s.GetAt(ctx, key, minTS+1)
			require.NoError(t, err)
			require.Equal(t, []byte(fmt.Sprint(minTS+1)), value)
			ps, ok := s.(*pebbleStore)
			require.True(t, ok)
			_, closer, err := ps.db.Get(encodeKey(key, minTS-1))
			if closer != nil {
				require.NoError(t, closer.Close())
			}
			require.ErrorIs(t, err, pebble.ErrNotFound, "old version must be physically removed")
			require.Equal(t, 2, countPebbleVersions(t, s))
		})
	}
}
