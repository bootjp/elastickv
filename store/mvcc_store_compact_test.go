package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMVCCStore_Compact(t *testing.T) {
	s := NewMVCCStore()
	ctx := context.Background()

	key := []byte("key1")

	// Helper to add versions
	require.NoError(t, s.PutAt(ctx, key, []byte("v10"), 10, 0))
	require.NoError(t, s.PutAt(ctx, key, []byte("v20"), 20, 0))
	require.NoError(t, s.PutAt(ctx, key, []byte("v30"), 30, 0))
	require.NoError(t, s.PutAt(ctx, key, []byte("v40"), 40, 0))

	// Verify initial state
	val, err := s.GetAt(ctx, key, 15)
	require.NoError(t, err)
	assert.Equal(t, []byte("v10"), val)

	// Compact at 25
	// Should keep: v20 (latest <= 25), v30, v40
	// Should remove: v10
	err = s.Compact(ctx, 25)
	require.NoError(t, err)

	// v10 should be gone physically, but logically checking at TS 15
	// depends on how GetAt is implemented.
	// Current GetAt implementation:
	// It iterates versions backwards. If we removed v10, and only have v20, v30, v40...
	// querying at TS 15:
	// v40 > 15 -> skip
	// v30 > 15 -> skip
	// v20 > 15 -> skip
	// No version <= 15 found -> ErrKeyNotFound
	// This is the expected behavior of Compaction: you cannot query older than minTS.

	_, err = s.GetAt(ctx, key, 15)
	assert.Equal(t, ErrKeyNotFound, err, "Should not find version older than compacted minTS")

	// Query at 25 should return v20
	val, err = s.GetAt(ctx, key, 25)
	require.NoError(t, err)
	assert.Equal(t, []byte("v20"), val)

	// Query at 35 should return v30
	val, err = s.GetAt(ctx, key, 35)
	require.NoError(t, err)
	assert.Equal(t, []byte("v30"), val)
}

func TestMVCCStore_Compact_Delete(t *testing.T) {
	s := NewMVCCStore()
	ctx := context.Background()

	key := []byte("key_del")

	require.NoError(t, s.PutAt(ctx, key, []byte("v10"), 10, 0))
	require.NoError(t, s.DeleteAt(ctx, key, 20)) // Tombstone at 20

	// Compact at 25
	// Should keep Tombstone at 20 (as it is latest <= 25)
	// Should remove v10
	err := s.Compact(ctx, 25)
	require.NoError(t, err)

	// Query at 15 -> Not found (compacted)
	_, err = s.GetAt(ctx, key, 15)
	assert.Equal(t, ErrKeyNotFound, err)

	// Query at 25 -> Not found (Tombstone at 20)
	exists, err := s.ExistsAt(ctx, key, 25)
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestMVCCStore_Compact_KeepAll(t *testing.T) {
	s := NewMVCCStore()
	ctx := context.Background()
	key := []byte("key_keep")

	require.NoError(t, s.PutAt(ctx, key, []byte("v50"), 50, 0))
	require.NoError(t, s.PutAt(ctx, key, []byte("v60"), 60, 0))

	// Compact at 40
	// All versions are > 40. None <= 40.
	// Logic says: keepIdx = -1 (not found).
	// Should return all versions.
	err := s.Compact(ctx, 40)
	require.NoError(t, err)

	val, err := s.GetAt(ctx, key, 55)
	require.NoError(t, err)
	assert.Equal(t, []byte("v50"), val)
}
