package store

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMVCCStore_ScanAt_ExclusiveEndBound verifies that ScanAt uses [start, end)
// semantics: a key exactly equal to end should NOT be returned.
func TestMVCCStore_ScanAt_ExclusiveEndBound(t *testing.T) {
	ctx := context.Background()
	st := NewMVCCStore()

	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 10, 0))
	require.NoError(t, st.PutAt(ctx, []byte("b"), []byte("vb"), 10, 0))
	require.NoError(t, st.PutAt(ctx, []byte("c"), []byte("vc"), 10, 0))

	// end="b" → should return only "a" (exclusive end)
	kvs, err := st.ScanAt(ctx, []byte("a"), []byte("b"), 10, 10)
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	assert.Equal(t, []byte("a"), kvs[0].Key)
}

// TestMVCCStore_ScanAt_EndEqualsLastKey verifies that when end equals the last
// data key, that key is excluded.
func TestMVCCStore_ScanAt_EndEqualsLastKey(t *testing.T) {
	ctx := context.Background()
	st := NewMVCCStore()

	require.NoError(t, st.PutAt(ctx, []byte("x"), []byte("vx"), 10, 0))
	require.NoError(t, st.PutAt(ctx, []byte("y"), []byte("vy"), 10, 0))
	require.NoError(t, st.PutAt(ctx, []byte("z"), []byte("vz"), 10, 0))

	// end="z" → should return "x" and "y"
	kvs, err := st.ScanAt(ctx, []byte("x"), []byte("z"), 10, 10)
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	assert.Equal(t, []byte("x"), kvs[0].Key)
	assert.Equal(t, []byte("y"), kvs[1].Key)
}

// TestMVCCStore_ScanAt_NilEnd returns all keys from start.
func TestMVCCStore_ScanAt_NilEnd(t *testing.T) {
	ctx := context.Background()
	st := NewMVCCStore()

	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 10, 0))
	require.NoError(t, st.PutAt(ctx, []byte("z"), []byte("vz"), 10, 0))

	kvs, err := st.ScanAt(ctx, []byte("a"), nil, 10, 10)
	require.NoError(t, err)
	require.Len(t, kvs, 2)
}

// TestPebbleStore_ScanAt_ExclusiveEndBound verifies [start, end) for pebble.
func TestPebbleStore_ScanAt_ExclusiveEndBound(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-scan-bound-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()
	require.NoError(t, s.PutAt(ctx, []byte("a"), []byte("va"), 10, 0))
	require.NoError(t, s.PutAt(ctx, []byte("b"), []byte("vb"), 10, 0))
	require.NoError(t, s.PutAt(ctx, []byte("c"), []byte("vc"), 10, 0))

	// end="b" → should return only "a"
	kvs, err := s.ScanAt(ctx, []byte("a"), []byte("b"), 10, 10)
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	assert.Equal(t, []byte("a"), kvs[0].Key)
}

// TestPebbleStore_ScanAt_EndEqualsLastKey verifies exclusive end for pebble.
func TestPebbleStore_ScanAt_EndEqualsLastKey(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-scan-bound2-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()
	require.NoError(t, s.PutAt(ctx, []byte("x"), []byte("vx"), 10, 0))
	require.NoError(t, s.PutAt(ctx, []byte("y"), []byte("vy"), 10, 0))
	require.NoError(t, s.PutAt(ctx, []byte("z"), []byte("vz"), 10, 0))

	// end="z" → should return "x" and "y"
	kvs, err := s.ScanAt(ctx, []byte("x"), []byte("z"), 10, 10)
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	assert.Equal(t, []byte("x"), kvs[0].Key)
	assert.Equal(t, []byte("y"), kvs[1].Key)
}
