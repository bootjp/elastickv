package store

import (
	"bytes"
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

func TestPebbleStore_LastCommitTSPersistedAcrossRestart(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-last-ts-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	ctx := context.Background()

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	require.NoError(t, s.PutAt(ctx, []byte("k"), []byte("v"), 42, 0))
	require.Equal(t, uint64(42), s.LastCommitTS())
	require.NoError(t, s.Close())

	reopened, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer reopened.Close()
	require.Equal(t, uint64(42), reopened.LastCommitTS())
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

func TestPebbleStore_Scan_TombstoneMasksOlderVersions(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-scan-tombstone-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()
	require.NoError(t, s.PutAt(ctx, []byte("k1"), []byte("v1"), 10, 0))
	require.NoError(t, s.DeleteAt(ctx, []byte("k1"), 20))

	pairs, err := s.ScanAt(ctx, []byte("k"), nil, 10, 25)
	require.NoError(t, err)
	assert.Len(t, pairs, 0)
}

func TestPebbleStore_Scan_SkipsKeyWithOnlyFutureVersions(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-scan-future-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()
	require.NoError(t, s.PutAt(ctx, []byte("k1"), []byte("v100"), 100, 0))
	require.NoError(t, s.PutAt(ctx, []byte("k1"), []byte("v110"), 110, 0))
	require.NoError(t, s.PutAt(ctx, []byte("k2"), []byte("v80"), 80, 0))

	pairs, err := s.ScanAt(ctx, []byte("k"), nil, 10, 85)
	require.NoError(t, err)
	require.Len(t, pairs, 1)
	assert.Equal(t, []byte("k2"), pairs[0].Key)
	assert.Equal(t, []byte("v80"), pairs[0].Value)
}

func TestPebbleStore_Scan_IgnoresInternalMetaKey(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-scan-meta-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()
	require.NoError(t, s.PutAt(ctx, []byte("k1"), []byte("v1"), 10, 0))
	require.NoError(t, s.PutAt(ctx, []byte("k2"), []byte("v2"), 20, 0))

	// Full-range scan should not decode/expose the internal _meta_last_commit_ts key.
	pairs, err := s.ScanAt(ctx, []byte(""), nil, 10, 30)
	require.NoError(t, err)
	require.Len(t, pairs, 2)
	assert.Equal(t, []byte("k1"), pairs[0].Key)
	assert.Equal(t, []byte("k2"), pairs[1].Key)
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
	defer buf.Close()

	var raw bytes.Buffer
	_, err = buf.WriteTo(&raw)
	require.NoError(t, err)

	s.Close()

	// Restore to new dir
	dir2, err := os.MkdirTemp("", "pebble-restore-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir2)

	s2, err := NewPebbleStore(dir2)
	require.NoError(t, err)
	defer s2.Close()

	err = s2.Restore(bytes.NewReader(raw.Bytes()))
	require.NoError(t, err)

	val, err := s2.GetAt(ctx, []byte("k1"), 100)
	require.NoError(t, err)
	assert.Equal(t, []byte("v1"), val)

	assert.Equal(t, uint64(100), s2.LastCommitTS()) // aligned 100 -> 100 (if started from 0)
}

// TestPebbleStore_RestoreFromStreamingMVCC verifies that a pebbleStore can
// restore from a snapshot created by the in-memory mvccStore (streaming
// "EKVMVCC2" format). This is the migration path when upgrading from the
// in-memory FSM to the Pebble-backed FSM.
func TestPebbleStore_RestoreFromStreamingMVCC(t *testing.T) {
	ctx := context.Background()

	// Build a snapshot using the in-memory mvccStore.
	src := NewMVCCStore()
	require.NoError(t, src.PutAt(ctx, []byte("key1"), []byte("val1"), 10, 0))
	require.NoError(t, src.PutAt(ctx, []byte("key1"), []byte("val1-updated"), 20, 0))
	require.NoError(t, src.DeleteAt(ctx, []byte("key2"), 15))
	require.NoError(t, src.PutWithTTLAt(ctx, []byte("key3"), []byte("val3"), 30, 9999))

	snap, err := src.Snapshot()
	require.NoError(t, err)
	defer snap.Close()

	var buf bytes.Buffer
	_, err = snap.WriteTo(&buf)
	require.NoError(t, err)

	// Restore into a fresh pebbleStore.
	dir, err := os.MkdirTemp("", "pebble-migrate-streaming-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	dst, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer dst.Close()

	require.NoError(t, dst.Restore(bytes.NewReader(buf.Bytes())))

	// Verify data was migrated correctly.
	val, err := dst.GetAt(ctx, []byte("key1"), 20)
	require.NoError(t, err)
	assert.Equal(t, []byte("val1-updated"), val)

	val, err = dst.GetAt(ctx, []byte("key1"), 10)
	require.NoError(t, err)
	assert.Equal(t, []byte("val1"), val)

	_, err = dst.GetAt(ctx, []byte("key2"), 15)
	assert.ErrorIs(t, err, ErrKeyNotFound)

	val, err = dst.GetAt(ctx, []byte("key3"), 30)
	require.NoError(t, err)
	assert.Equal(t, []byte("val3"), val)

	assert.Equal(t, src.LastCommitTS(), dst.LastCommitTS())
}

// TestPebbleStore_RestoreFromLegacyGob verifies that a pebbleStore can
// restore from the oldest gob-based mvccStore snapshot format.
func TestPebbleStore_RestoreFromLegacyGob(t *testing.T) {
	ctx := context.Background()

	// Build a legacy gob snapshot manually.
	src := NewMVCCStore()
	require.NoError(t, src.PutAt(ctx, []byte("a"), []byte("1"), 5, 0))
	require.NoError(t, src.PutAt(ctx, []byte("b"), []byte("2"), 10, 0))

	srcImpl, ok := src.(*mvccStore)
	require.True(t, ok, "expected *mvccStore")
	var buf bytes.Buffer
	require.NoError(t, srcImpl.writeLegacyGobSnapshot(&buf))

	// Restore into pebbleStore.
	dir, err := os.MkdirTemp("", "pebble-migrate-gob-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	dst, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer dst.Close()

	require.NoError(t, dst.Restore(bytes.NewReader(buf.Bytes())))

	val, err := dst.GetAt(ctx, []byte("a"), 5)
	require.NoError(t, err)
	assert.Equal(t, []byte("1"), val)

	val, err = dst.GetAt(ctx, []byte("b"), 10)
	require.NoError(t, err)
	assert.Equal(t, []byte("2"), val)

	assert.Equal(t, src.LastCommitTS(), dst.LastCommitTS())
}
