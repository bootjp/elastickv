package store

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- mvccStore: txn internal key compaction ---

// TestMVCCStore_Compact_TxnLockSingleVersionPreserved verifies that an active
// lock (single version) is not removed by compaction.
func TestMVCCStore_Compact_TxnLockSingleVersionPreserved(t *testing.T) {
	ctx := context.Background()
	st := NewMVCCStore()

	txnKey := append([]byte(nil), txnInternalKeyPrefix...)
	txnKey = append(txnKey, []byte("lock|k1")...)

	require.NoError(t, st.PutAt(ctx, txnKey, []byte("lock-data"), 10, 0))

	require.NoError(t, st.Compact(ctx, 20))

	val, err := st.GetAt(ctx, txnKey, 20)
	require.NoError(t, err)
	assert.Equal(t, []byte("lock-data"), val)
}

// TestMVCCStore_Compact_TxnLockOldVersionRemoved verifies that after a lock is
// resolved (tombstoned), compaction removes the old lock data version.
func TestMVCCStore_Compact_TxnLockOldVersionRemoved(t *testing.T) {
	ctx := context.Background()
	st := NewMVCCStore()

	txnKey := append([]byte(nil), txnInternalKeyPrefix...)
	txnKey = append(txnKey, []byte("lock|k1")...)

	// PREPARE writes lock at TS=10, COMMIT writes tombstone at TS=20.
	require.NoError(t, st.PutAt(ctx, txnKey, []byte("lock-data"), 10, 0))
	require.NoError(t, st.DeleteAt(ctx, txnKey, 20))

	// Before compaction: 2 versions
	require.NoError(t, st.Compact(ctx, 25))

	// After compaction: old version (TS=10) removed, tombstone (TS=20) kept.
	// Reading the key should return not-found (tombstone).
	_, err := st.GetAt(ctx, txnKey, 25)
	require.ErrorIs(t, err, ErrKeyNotFound)
}

// TestMVCCStore_Compact_TxnCommitRecordPreserved verifies that a commit record
// (single version) is not removed by compaction.
func TestMVCCStore_Compact_TxnCommitRecordPreserved(t *testing.T) {
	ctx := context.Background()
	st := NewMVCCStore()

	cmtKey := append([]byte(nil), txnInternalKeyPrefix...)
	cmtKey = append(cmtKey, []byte("cmt|pk1\x00\x00\x00\x00\x00\x00\x00\x0a")...)

	require.NoError(t, st.PutAt(ctx, cmtKey, []byte("commit-record"), 20, 0))

	require.NoError(t, st.Compact(ctx, 30))

	val, err := st.GetAt(ctx, cmtKey, 30)
	require.NoError(t, err)
	assert.Equal(t, []byte("commit-record"), val)
}

// --- mvccStore: minRetainedTS advancement ---

// TestMVCCStore_Compact_AdvancesMinRetainedTSWithNoPending verifies that
// minRetainedTS advances even when there are no keys to compact.
func TestMVCCStore_Compact_AdvancesMinRetainedTSWithNoPending(t *testing.T) {
	ctx := context.Background()
	st := NewMVCCStore()

	// Only a single version per key — nothing to compact.
	require.NoError(t, st.PutAt(ctx, []byte("k"), []byte("v"), 50, 0))

	require.NoError(t, st.Compact(ctx, 100))

	// Reads below minRetainedTS should be rejected.
	_, err := st.GetAt(ctx, []byte("k"), 99)
	require.ErrorIs(t, err, ErrReadTSCompacted)

	// Read at minRetainedTS should work.
	val, err := st.GetAt(ctx, []byte("k"), 100)
	require.NoError(t, err)
	assert.Equal(t, []byte("v"), val)
}

// TestMVCCStore_Compact_AdvancesMinRetainedTSEmptyStore verifies that
// minRetainedTS advances even with an empty store.
func TestMVCCStore_Compact_AdvancesMinRetainedTSEmptyStore(t *testing.T) {
	ctx := context.Background()
	st := NewMVCCStore()

	require.NoError(t, st.Compact(ctx, 100))

	// Any read below 100 should be rejected.
	_, err := st.GetAt(ctx, []byte("k"), 99)
	require.ErrorIs(t, err, ErrReadTSCompacted)
}

// --- pebbleStore: txn internal key compaction ---

// TestPebbleStore_Compact_TxnLockLifecycle verifies the full lock lifecycle:
// PREPARE creates a version, COMMIT tombstones it, compaction cleans up.
func TestPebbleStore_Compact_TxnLockLifecycle(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-txn-lifecycle-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()
	txnKey := append([]byte(nil), txnInternalKeyPrefix...)
	txnKey = append(txnKey, []byte("lock|k1")...)

	// PREPARE: lock at TS=10
	require.NoError(t, s.PutAt(ctx, txnKey, []byte("lock-data"), 10, 0))
	// COMMIT: tombstone at TS=20
	require.NoError(t, s.DeleteAt(ctx, txnKey, 20))

	initialVersions := countPebbleVersions(t, s)

	// Compact above commitTS.
	require.NoError(t, s.Compact(ctx, 25))

	afterVersions := countPebbleVersions(t, s)
	// Old lock version at TS=10 should be removed.
	assert.Less(t, afterVersions, initialVersions,
		"compaction should remove old txn lock version")

	// Lock should not be readable (tombstone).
	_, err = s.GetAt(ctx, txnKey, 25)
	require.ErrorIs(t, err, ErrKeyNotFound)
}

// TestPebbleStore_Compact_TxnIntentLifecycle verifies that intent key versions
// are cleaned up after resolution.
func TestPebbleStore_Compact_TxnIntentLifecycle(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-txn-intent-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()
	intKey := append([]byte(nil), txnInternalKeyPrefix...)
	intKey = append(intKey, []byte("int|k1")...)

	// PREPARE: intent at TS=10
	require.NoError(t, s.PutAt(ctx, intKey, []byte("intent-data"), 10, 0))
	// COMMIT: tombstone at TS=20
	require.NoError(t, s.DeleteAt(ctx, intKey, 20))

	before := countPebbleVersions(t, s)
	require.NoError(t, s.Compact(ctx, 25))
	after := countPebbleVersions(t, s)

	assert.Less(t, after, before, "compaction should remove old intent version")
}

// TestPebbleStore_Compact_TxnActiveLockPreserved verifies that an active lock
// (not yet committed/aborted) is preserved during compaction.
func TestPebbleStore_Compact_TxnActiveLockPreserved(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-txn-active-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()
	txnKey := append([]byte(nil), txnInternalKeyPrefix...)
	txnKey = append(txnKey, []byte("lock|k1")...)

	// Only PREPARE, no COMMIT — single version.
	require.NoError(t, s.PutAt(ctx, txnKey, []byte("active-lock"), 10, 0))

	before := countPebbleVersions(t, s)
	require.NoError(t, s.Compact(ctx, 20))
	after := countPebbleVersions(t, s)

	assert.Equal(t, before, after, "active lock should not be compacted")

	val, err := s.GetAt(ctx, txnKey, 20)
	require.NoError(t, err)
	assert.Equal(t, []byte("active-lock"), val)
}

// --- pebbleStore: minRetainedTS advancement ---

// TestPebbleStore_Compact_AdvancesMinRetainedTSWithNoPending verifies that
// minRetainedTS advances even when no versions need trimming.
func TestPebbleStore_Compact_AdvancesMinRetainedTSWithNoPending(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-compact-nopending-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()
	// Single version — nothing to compact.
	require.NoError(t, s.PutAt(ctx, []byte("k"), []byte("v"), 50, 0))

	require.NoError(t, s.Compact(ctx, 100))

	retention := requirePebbleRetentionController(t, s)
	assert.Equal(t, uint64(100), retention.MinRetainedTS())

	_, err = s.GetAt(ctx, []byte("k"), 99)
	require.ErrorIs(t, err, ErrReadTSCompacted)
}

// --- pebbleStore: ApplyMutations serialization ---

// TestPebbleStore_ApplyMutations_ConcurrentConflictDetection verifies that
// applyMu serializes conflict check + commit so concurrent ApplyMutations
// sharing the same startTS cannot all succeed — at least (goroutines-1) must
// conflict.
func TestPebbleStore_ApplyMutations_ConcurrentConflictDetection(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-apply-concurrent-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()
	key := []byte("contended")

	// Seed key at TS=1 so startTS=1 sees an existing version.
	require.NoError(t, s.PutAt(ctx, key, []byte("seed"), 1, 0))

	const goroutines = 10

	// All goroutines share the same startTS and commitTS so that after the first
	// one commits, all remaining goroutines must observe a conflict.
	startTS := s.LastCommitTS() // == 1
	require.Equal(t, uint64(1), startTS, "expected LastCommitTS to equal the seed TS")
	commitTS := startTS + 1 // == 2

	var ready sync.WaitGroup
	ready.Add(goroutines)

	type result struct {
		success  int
		conflict int
	}
	results := make(chan result, goroutines)

	for g := 0; g < goroutines; g++ {
		go func() {
			// Signal ready and wait for all goroutines to be ready before
			// starting, maximizing contention.
			ready.Done()
			ready.Wait()

			err := s.ApplyMutations(ctx, []*KVPairMutation{
				{Op: OpTypePut, Key: key, Value: []byte("val")},
			}, startTS, commitTS)
			r := result{}
			if err == nil {
				r.success++
			} else {
				r.conflict++
			}
			results <- r
		}()
	}

	totalSuccess := 0
	totalConflict := 0
	for g := 0; g < goroutines; g++ {
		r := <-results
		totalSuccess += r.success
		totalConflict += r.conflict
	}

	assert.Equal(t, goroutines, totalSuccess+totalConflict)
	assert.Greater(t, totalSuccess, 0, "at least one must succeed")
	// All goroutines share the same startTS: after the first commits, the rest
	// must conflict because the key's latest TS will exceed startTS.
	assert.Greater(t, totalConflict, 0, "at least one must conflict")
}
