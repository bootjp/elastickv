package store

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPebbleStore_Basic(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()
	key := []byte("key1")

	// Put at TS 10
	err = s.PutAt(ctx, key, []byte("val10"), 10, 0)
	require.NoError(t, err)

	// Get at TS 10
	val, err := s.GetAt(ctx, key, 10)
	require.NoError(t, err)
	assert.Equal(t, []byte("val10"), val)

	// Get at TS 5 (should be not found)
	_, err = s.GetAt(ctx, key, 5)
	assert.Equal(t, ErrKeyNotFound, err)

	// Put at TS 20
	err = s.PutAt(ctx, key, []byte("val20"), 20, 0)
	require.NoError(t, err)

	// Get at TS 20 -> val20
	val, err = s.GetAt(ctx, key, 20)
	require.NoError(t, err)
	assert.Equal(t, []byte("val20"), val)

	// Get at TS 15 -> val10 (latest <= 15)
	val, err = s.GetAt(ctx, key, 15)
	require.NoError(t, err)
	assert.Equal(t, []byte("val10"), val)

	// Delete at TS 30
	err = s.DeleteAt(ctx, key, 30)
	require.NoError(t, err)

	// Get at TS 35 -> NotFound (Tombstone)
	_, err = s.GetAt(ctx, key, 35)
	assert.Equal(t, ErrKeyNotFound, err)

	// Get at TS 25 -> val20
	val, err = s.GetAt(ctx, key, 25)
	require.NoError(t, err)
	assert.Equal(t, []byte("val20"), val)
}

func TestPebbleStore_Scan(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-scan-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()

	// k2: v10@10
	// k1: v10@11 (aligned), v20@20
	// k3: v30@30
	require.NoError(t, s.PutAt(ctx, []byte("k2"), []byte("v10"), 10, 0))
	require.NoError(t, s.PutAt(ctx, []byte("k1"), []byte("v10"), 10, 0))
	require.NoError(t, s.PutAt(ctx, []byte("k1"), []byte("v20"), 20, 0))
	require.NoError(t, s.PutAt(ctx, []byte("k3"), []byte("v30"), 30, 0))

	// Scan at TS 25
	// Expect: k1=v20, k2=v10 (k3 is at 30, so invisible)
	pairs, err := s.ScanAt(ctx, []byte("k"), nil, 10, 25)
	require.NoError(t, err)
	assert.Len(t, pairs, 2)
	assert.Equal(t, []byte("k1"), pairs[0].Key)
	assert.Equal(t, []byte("v20"), pairs[0].Value)
	assert.Equal(t, []byte("k2"), pairs[1].Key)
	assert.Equal(t, []byte("v10"), pairs[1].Value)

	// Scan at TS 15
	// Expect: k1=v10, k2=v10
	pairs, err = s.ScanAt(ctx, []byte("k"), nil, 10, 15)
	require.NoError(t, err)
	assert.Len(t, pairs, 2)
	assert.Equal(t, []byte("k1"), pairs[0].Key)
	assert.Equal(t, []byte("v10"), pairs[0].Value)
	
	// Scan at TS 5
	// Expect: empty
	pairs, err = s.ScanAt(ctx, []byte("k"), nil, 10, 5)
	require.NoError(t, err)
	assert.Len(t, pairs, 0)
}

func TestPebbleStore_SnapshotRestore(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-snap-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	
	ctx := context.Background()
	require.NoError(t, s.PutAt(ctx, []byte("k1"), []byte("v1"), 100, 0))
	
	// Snapshot
	buf, err := s.Snapshot()
	require.NoError(t, err)
	
	s.Close()
	
	// Restore to new dir
	dir2, err := os.MkdirTemp("", "pebble-restore-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir2)
	
	s2, err := NewPebbleStore(dir2)
	require.NoError(t, err)
	defer s2.Close()
	
	err = s2.Restore(buf)
	require.NoError(t, err)
	
	val, err := s2.GetAt(ctx, []byte("k1"), 100)
	require.NoError(t, err)
	assert.Equal(t, []byte("v1"), val)
	
	assert.Equal(t, uint64(100), s2.LastCommitTS()) // aligned 100 -> 100 (if started from 0)
}
