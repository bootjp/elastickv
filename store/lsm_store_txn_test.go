package store

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// ApplyMutations
// ---------------------------------------------------------------------------

func TestPebbleStore_ApplyMutations_BasicPut(t *testing.T) {
	dir := t.TempDir()
	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()

	mutations := []*KVPairMutation{
		{Op: OpTypePut, Key: []byte("k1"), Value: []byte("v1")},
		{Op: OpTypePut, Key: []byte("k2"), Value: []byte("v2")},
	}

	err = s.ApplyMutations(ctx, mutations, 0, 10)
	require.NoError(t, err)

	// Both keys should be readable at commitTS.
	val, err := s.GetAt(ctx, []byte("k1"), 10)
	require.NoError(t, err)
	assert.Equal(t, []byte("v1"), val)

	val, err = s.GetAt(ctx, []byte("k2"), 10)
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), val)

	// Before commitTS the keys should not exist.
	_, err = s.GetAt(ctx, []byte("k1"), 5)
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

func TestPebbleStore_ApplyMutations_Delete(t *testing.T) {
	dir := t.TempDir()
	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()

	// Seed a value first.
	require.NoError(t, s.PutAt(ctx, []byte("k1"), []byte("v1"), 10, 0))

	// Delete via ApplyMutations.
	mutations := []*KVPairMutation{
		{Op: OpTypeDelete, Key: []byte("k1")},
	}
	err = s.ApplyMutations(ctx, mutations, 10, 20)
	require.NoError(t, err)

	// After the delete, key should be a tombstone.
	_, err = s.GetAt(ctx, []byte("k1"), 25)
	assert.ErrorIs(t, err, ErrKeyNotFound)

	// Before the delete the value should still be visible.
	val, err := s.GetAt(ctx, []byte("k1"), 15)
	require.NoError(t, err)
	assert.Equal(t, []byte("v1"), val)
}

func TestPebbleStore_ApplyMutations_PutWithTTL(t *testing.T) {
	dir := t.TempDir()
	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()

	mutations := []*KVPairMutation{
		{Op: OpTypePut, Key: []byte("k1"), Value: []byte("v1"), ExpireAt: 50},
	}
	err = s.ApplyMutations(ctx, mutations, 0, 10)
	require.NoError(t, err)

	// Visible before expiry.
	val, err := s.GetAt(ctx, []byte("k1"), 40)
	require.NoError(t, err)
	assert.Equal(t, []byte("v1"), val)

	// Not visible at or after expiry.
	_, err = s.GetAt(ctx, []byte("k1"), 50)
	assert.ErrorIs(t, err, ErrKeyNotFound)

	_, err = s.GetAt(ctx, []byte("k1"), 60)
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

func TestPebbleStore_ApplyMutations_WriteConflict(t *testing.T) {
	dir := t.TempDir()
	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()

	// Pre-existing version at TS=20.
	require.NoError(t, s.PutAt(ctx, []byte("k1"), []byte("v1"), 20, 0))

	// Try to apply with startTS=10 (older than existing commit at 20).
	mutations := []*KVPairMutation{
		{Op: OpTypePut, Key: []byte("k1"), Value: []byte("v2")},
	}
	err = s.ApplyMutations(ctx, mutations, 10, 30)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrWriteConflict), "expected ErrWriteConflict, got %v", err)

	// The conflict key should be extractable.
	conflictKey, ok := WriteConflictKey(err)
	assert.True(t, ok)
	assert.Equal(t, []byte("k1"), conflictKey)

	// The original value should be unchanged.
	val, err := s.GetAt(ctx, []byte("k1"), 25)
	require.NoError(t, err)
	assert.Equal(t, []byte("v1"), val)
}

func TestPebbleStore_ApplyMutations_NoConflictWhenStartTSGECommit(t *testing.T) {
	dir := t.TempDir()
	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()

	// Pre-existing version at TS=10.
	require.NoError(t, s.PutAt(ctx, []byte("k1"), []byte("v1"), 10, 0))

	// Apply with startTS=10 (equal to existing commit). Should succeed.
	mutations := []*KVPairMutation{
		{Op: OpTypePut, Key: []byte("k1"), Value: []byte("v2")},
	}
	err = s.ApplyMutations(ctx, mutations, 10, 20)
	require.NoError(t, err)

	val, err := s.GetAt(ctx, []byte("k1"), 20)
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), val)
}

func TestPebbleStore_ApplyMutations_UpdatesLastCommitTS(t *testing.T) {
	dir := t.TempDir()
	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()

	assert.Equal(t, uint64(0), s.LastCommitTS())

	mutations := []*KVPairMutation{
		{Op: OpTypePut, Key: []byte("k1"), Value: []byte("v1")},
	}
	require.NoError(t, s.ApplyMutations(ctx, mutations, 0, 100))
	assert.Equal(t, uint64(100), s.LastCommitTS())

	// A second apply with a higher commitTS advances lastCommitTS.
	mutations2 := []*KVPairMutation{
		{Op: OpTypePut, Key: []byte("k2"), Value: []byte("v2")},
	}
	require.NoError(t, s.ApplyMutations(ctx, mutations2, 100, 200))
	assert.Equal(t, uint64(200), s.LastCommitTS())
}

func TestPebbleStore_ApplyMutations_Atomicity(t *testing.T) {
	dir := t.TempDir()
	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()

	// Seed k2 at TS=50 so the batch conflicts on k2.
	require.NoError(t, s.PutAt(ctx, []byte("k2"), []byte("old"), 50, 0))

	// k1 is fine, but k2 will conflict (startTS=10 < existing 50).
	mutations := []*KVPairMutation{
		{Op: OpTypePut, Key: []byte("k1"), Value: []byte("v1")},
		{Op: OpTypePut, Key: []byte("k2"), Value: []byte("v2")},
	}
	err = s.ApplyMutations(ctx, mutations, 10, 60)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrWriteConflict))

	// k1 should NOT have been written (atomic rollback).
	_, err = s.GetAt(ctx, []byte("k1"), 60)
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

// ---------------------------------------------------------------------------
// LatestCommitTS
// ---------------------------------------------------------------------------

func TestPebbleStore_LatestCommitTS_SingleVersion(t *testing.T) {
	dir := t.TempDir()
	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()

	require.NoError(t, s.PutAt(ctx, []byte("k1"), []byte("v1"), 42, 0))

	ts, found, err := s.LatestCommitTS(ctx, []byte("k1"))
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, uint64(42), ts)
}

func TestPebbleStore_LatestCommitTS_MultipleVersions(t *testing.T) {
	dir := t.TempDir()
	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()

	require.NoError(t, s.PutAt(ctx, []byte("k1"), []byte("v1"), 10, 0))
	require.NoError(t, s.PutAt(ctx, []byte("k1"), []byte("v2"), 20, 0))
	require.NoError(t, s.PutAt(ctx, []byte("k1"), []byte("v3"), 30, 0))

	ts, found, err := s.LatestCommitTS(ctx, []byte("k1"))
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, uint64(30), ts)
}

func TestPebbleStore_LatestCommitTS_NotFound(t *testing.T) {
	dir := t.TempDir()
	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()

	ts, found, err := s.LatestCommitTS(ctx, []byte("nonexistent"))
	require.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, uint64(0), ts)
}

// ---------------------------------------------------------------------------
// Compact
// ---------------------------------------------------------------------------

func TestPebbleStore_Compact_RemovesOldVersions(t *testing.T) {
	dir := t.TempDir()
	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()

	require.NoError(t, s.PutAt(ctx, []byte("k1"), []byte("v1"), 10, 0))
	require.NoError(t, s.PutAt(ctx, []byte("k1"), []byte("v2"), 20, 0))
	require.NoError(t, s.PutAt(ctx, []byte("k1"), []byte("v3"), 30, 0))

	// Compact with minTS=25. The first version <= 25 is TS=20 (kept);
	// TS=10 is older and should be removed. TS=30 is above minTS, kept.
	require.NoError(t, s.Compact(ctx, 25))

	// TS=30 still visible.
	val, err := s.GetAt(ctx, []byte("k1"), 30)
	require.NoError(t, err)
	assert.Equal(t, []byte("v3"), val)

	// TS=25 returns the anchor version (v2 at TS=20).
	val, err = s.GetAt(ctx, []byte("k1"), 25)
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), val)

	// Reads below minTS=25 are rejected as compacted.
	_, err = s.GetAt(ctx, []byte("k1"), 15)
	assert.ErrorIs(t, err, ErrReadTSCompacted)
}

func TestPebbleStore_Compact_KeepsNewestVersion(t *testing.T) {
	dir := t.TempDir()
	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()

	require.NoError(t, s.PutAt(ctx, []byte("k1"), []byte("v1"), 10, 0))

	// Compact with minTS=100 (well above the only version).
	// The single version at TS=10 should be kept as the anchor.
	require.NoError(t, s.Compact(ctx, 100))

	// Read at minTS=100 — the anchor (v1@10) should still be visible.
	val, err := s.GetAt(ctx, []byte("k1"), 100)
	require.NoError(t, err)
	assert.Equal(t, []byte("v1"), val)

	// Reads below minTS are rejected.
	_, err = s.GetAt(ctx, []byte("k1"), 10)
	assert.ErrorIs(t, err, ErrReadTSCompacted)
}

func TestPebbleStore_Compact_TombstoneCleanup(t *testing.T) {
	dir := t.TempDir()
	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()

	require.NoError(t, s.PutAt(ctx, []byte("k1"), []byte("v1"), 10, 0))
	require.NoError(t, s.DeleteAt(ctx, []byte("k1"), 20))

	// Compact with minTS=25. The tombstone at TS=20 is kept as anchor;
	// the put at TS=10 is older and should be removed.
	require.NoError(t, s.Compact(ctx, 25))

	// Read at minTS=25 — the tombstone anchor makes the key "not found".
	_, err = s.GetAt(ctx, []byte("k1"), 25)
	assert.ErrorIs(t, err, ErrKeyNotFound)

	// Reads below minTS are rejected as compacted.
	_, err = s.GetAt(ctx, []byte("k1"), 15)
	assert.ErrorIs(t, err, ErrReadTSCompacted)
}

func TestPebbleStore_Compact_MetaKeyNotAffected(t *testing.T) {
	dir := t.TempDir()
	s, err := NewPebbleStore(dir)
	require.NoError(t, err)

	ctx := context.Background()

	// Write some data to ensure the meta key exists.
	require.NoError(t, s.PutAt(ctx, []byte("k1"), []byte("v1"), 10, 0))
	require.NoError(t, s.PutAt(ctx, []byte("k1"), []byte("v2"), 20, 0))

	lastBefore := s.LastCommitTS()

	// Compact should skip the meta key.
	require.NoError(t, s.Compact(ctx, 15))

	assert.Equal(t, lastBefore, s.LastCommitTS())

	// Reopen to ensure persisted meta key was not corrupted.
	require.NoError(t, s.Close())

	s2, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s2.Close()

	assert.Equal(t, lastBefore, s2.LastCommitTS())
}

func TestPebbleStore_Compact_TxnInternalKeysSkipped(t *testing.T) {
	dir := t.TempDir()
	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()

	// Write a transaction internal key directly (simulating lock resolution).
	txnKey := append([]byte(nil), txnInternalKeyPrefix...)
	txnKey = append(txnKey, []byte("lock:k1")...)
	require.NoError(t, s.PutAt(ctx, txnKey, []byte("lock-data"), 5, 0))

	// Also write a regular key.
	require.NoError(t, s.PutAt(ctx, []byte("k1"), []byte("v1"), 10, 0))
	require.NoError(t, s.PutAt(ctx, []byte("k1"), []byte("v2"), 20, 0))

	// Compact. The txn internal key should be untouched.
	require.NoError(t, s.Compact(ctx, 15))

	// The txn internal key should still be readable at/above minTS.
	val, err := s.GetAt(ctx, txnKey, 15)
	require.NoError(t, err)
	assert.Equal(t, []byte("lock-data"), val)

	// Regular key: TS=20 is above minTS, kept. TS=10 is the anchor, kept.
	val, err = s.GetAt(ctx, []byte("k1"), 20)
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), val)
}

func TestPebbleStore_Compact_MultipleKeys(t *testing.T) {
	dir := t.TempDir()
	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()

	// k1: versions at 5, 10, 20
	require.NoError(t, s.PutAt(ctx, []byte("k1"), []byte("k1v5"), 5, 0))
	require.NoError(t, s.PutAt(ctx, []byte("k1"), []byte("k1v10"), 10, 0))
	require.NoError(t, s.PutAt(ctx, []byte("k1"), []byte("k1v20"), 20, 0))

	// k2: versions at 8, 15
	require.NoError(t, s.PutAt(ctx, []byte("k2"), []byte("k2v8"), 8, 0))
	require.NoError(t, s.PutAt(ctx, []byte("k2"), []byte("k2v15"), 15, 0))

	// Compact with minTS=12.
	// k1: TS=20 > 12 kept. TS=10 <= 12, first anchor kept. TS=5 older, deleted.
	// k2: TS=15 > 12 kept. TS=8 <= 12, anchor kept. Nothing older.
	require.NoError(t, s.Compact(ctx, 12))

	// k1@20 visible.
	val, err := s.GetAt(ctx, []byte("k1"), 20)
	require.NoError(t, err)
	assert.Equal(t, []byte("k1v20"), val)

	// k1@12 returns anchor (k1v10).
	val, err = s.GetAt(ctx, []byte("k1"), 12)
	require.NoError(t, err)
	assert.Equal(t, []byte("k1v10"), val)

	// Reads below minTS=12 are rejected.
	_, err = s.GetAt(ctx, []byte("k1"), 7)
	assert.ErrorIs(t, err, ErrReadTSCompacted)

	// k2@15 visible.
	val, err = s.GetAt(ctx, []byte("k2"), 15)
	require.NoError(t, err)
	assert.Equal(t, []byte("k2v15"), val)

	// k2@12 returns anchor (k2v8).
	val, err = s.GetAt(ctx, []byte("k2"), 12)
	require.NoError(t, err)
	assert.Equal(t, []byte("k2v8"), val)
}
