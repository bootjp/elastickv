package adapter

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

// newDeltaCompactorTestFixture creates a store, a coordinator, and a DeltaCompactor
// configured with a low maxCount threshold (2) so tests can trigger compaction easily.
func newDeltaCompactorTestFixture(t *testing.T) (store.MVCCStore, *DeltaCompactor) {
	t.Helper()
	st := store.NewMVCCStore()
	coord := newLocalAdapterCoordinator(st)
	c := NewDeltaCompactor(st, coord, WithDeltaCompactorMaxDeltaCount(2))
	return st, c
}

func TestDeltaCompactor_ListDeltaFoldedIntoBaseMeta(t *testing.T) {
	t.Parallel()

	st, c := newDeltaCompactorTestFixture(t)
	ctx := context.Background()
	userKey := []byte("mylist")

	// Write a base meta: Head=0, Len=10.
	baseMeta := store.ListMeta{Head: 0, Len: 10}
	metaBytes, err := store.MarshalListMeta(baseMeta)
	require.NoError(t, err)
	require.NoError(t, st.PutAt(ctx, store.ListMetaKey(userKey), metaBytes, 1, 0))

	// Write 3 delta keys (HeadDelta=1, LenDelta=-1 each).
	delta := store.MarshalListMetaDelta(store.ListMetaDelta{HeadDelta: 1, LenDelta: -1})
	d1Key := store.ListMetaDeltaKey(userKey, 10, 0)
	d2Key := store.ListMetaDeltaKey(userKey, 11, 0)
	d3Key := store.ListMetaDeltaKey(userKey, 12, 0)
	require.NoError(t, st.PutAt(ctx, d1Key, delta, 10, 0))
	require.NoError(t, st.PutAt(ctx, d2Key, delta, 11, 0))
	require.NoError(t, st.PutAt(ctx, d3Key, delta, 12, 0))

	require.NoError(t, c.SyncOnce(ctx))

	readTS := st.LastCommitTS()

	// Base meta should reflect Head=3, Len=7.
	raw, err := st.GetAt(ctx, store.ListMetaKey(userKey), readTS)
	require.NoError(t, err)
	got, err := store.UnmarshalListMeta(raw)
	require.NoError(t, err)
	require.Equal(t, int64(3), got.Head)
	require.Equal(t, int64(7), got.Len)

	// All 3 delta keys should be deleted.
	for _, dk := range [][]byte{d1Key, d2Key, d3Key} {
		_, getErr := st.GetAt(ctx, dk, readTS)
		require.ErrorIs(t, getErr, store.ErrKeyNotFound, "delta key should be deleted after compaction: %s", dk)
	}
}

func TestDeltaCompactor_ListBelowThresholdNotCompacted(t *testing.T) {
	t.Parallel()

	st, c := newDeltaCompactorTestFixture(t) // threshold = 2
	ctx := context.Background()
	userKey := []byte("shortlist")

	baseMeta := store.ListMeta{Head: 0, Len: 5}
	metaBytes, err := store.MarshalListMeta(baseMeta)
	require.NoError(t, err)
	require.NoError(t, st.PutAt(ctx, store.ListMetaKey(userKey), metaBytes, 1, 0))

	// Only 1 delta key — below the threshold of 2.
	delta := store.MarshalListMetaDelta(store.ListMetaDelta{HeadDelta: 1, LenDelta: -1})
	d1Key := store.ListMetaDeltaKey(userKey, 10, 0)
	require.NoError(t, st.PutAt(ctx, d1Key, delta, 10, 0))

	require.NoError(t, c.SyncOnce(ctx))

	readTS := st.LastCommitTS()

	// Delta key should still exist (not compacted).
	_, getErr := st.GetAt(ctx, d1Key, readTS)
	require.NoError(t, getErr, "delta key below threshold should not be deleted")

	// Base meta should be unchanged (Head=0, Len=5).
	raw, err := st.GetAt(ctx, store.ListMetaKey(userKey), readTS)
	require.NoError(t, err)
	got, err := store.UnmarshalListMeta(raw)
	require.NoError(t, err)
	require.Equal(t, int64(0), got.Head)
	require.Equal(t, int64(5), got.Len)
}

func TestDeltaCompactor_HashDeltaFoldedIntoBaseMeta(t *testing.T) {
	t.Parallel()

	st, c := newDeltaCompactorTestFixture(t)
	ctx := context.Background()
	userKey := []byte("myhash")

	// Write base meta: Len=10.
	require.NoError(t, st.PutAt(ctx, store.HashMetaKey(userKey), store.MarshalHashMeta(store.HashMeta{Len: 10}), 1, 0))

	// Write 2 delta keys with LenDelta=+1 each.
	delta := store.MarshalHashMetaDelta(store.HashMetaDelta{LenDelta: 1})
	d1Key := store.HashMetaDeltaKey(userKey, 10, 0)
	d2Key := store.HashMetaDeltaKey(userKey, 11, 0)
	require.NoError(t, st.PutAt(ctx, d1Key, delta, 10, 0))
	require.NoError(t, st.PutAt(ctx, d2Key, delta, 11, 0))

	require.NoError(t, c.SyncOnce(ctx))

	readTS := st.LastCommitTS()

	raw, err := st.GetAt(ctx, store.HashMetaKey(userKey), readTS)
	require.NoError(t, err)
	got, err := store.UnmarshalHashMeta(raw)
	require.NoError(t, err)
	require.Equal(t, int64(12), got.Len)

	for _, dk := range [][]byte{d1Key, d2Key} {
		_, getErr := st.GetAt(ctx, dk, readTS)
		require.ErrorIs(t, getErr, store.ErrKeyNotFound)
	}
}

func TestDeltaCompactor_SetDeltaFoldedIntoBaseMeta(t *testing.T) {
	t.Parallel()

	st, c := newDeltaCompactorTestFixture(t)
	ctx := context.Background()
	userKey := []byte("myset")

	require.NoError(t, st.PutAt(ctx, store.SetMetaKey(userKey), store.MarshalSetMeta(store.SetMeta{Len: 5}), 1, 0))

	delta := store.MarshalSetMetaDelta(store.SetMetaDelta{LenDelta: 2})
	d1Key := store.SetMetaDeltaKey(userKey, 10, 0)
	d2Key := store.SetMetaDeltaKey(userKey, 11, 0)
	require.NoError(t, st.PutAt(ctx, d1Key, delta, 10, 0))
	require.NoError(t, st.PutAt(ctx, d2Key, delta, 11, 0))

	require.NoError(t, c.SyncOnce(ctx))

	readTS := st.LastCommitTS()

	raw, err := st.GetAt(ctx, store.SetMetaKey(userKey), readTS)
	require.NoError(t, err)
	got, err := store.UnmarshalSetMeta(raw)
	require.NoError(t, err)
	require.Equal(t, int64(9), got.Len)

	for _, dk := range [][]byte{d1Key, d2Key} {
		_, getErr := st.GetAt(ctx, dk, readTS)
		require.ErrorIs(t, getErr, store.ErrKeyNotFound)
	}
}

func TestDeltaCompactor_ZSetDeltaFoldedIntoBaseMeta(t *testing.T) {
	t.Parallel()

	st, c := newDeltaCompactorTestFixture(t)
	ctx := context.Background()
	userKey := []byte("myzset")

	require.NoError(t, st.PutAt(ctx, store.ZSetMetaKey(userKey), store.MarshalZSetMeta(store.ZSetMeta{Len: 3}), 1, 0))

	delta := store.MarshalZSetMetaDelta(store.ZSetMetaDelta{LenDelta: 1})
	d1Key := store.ZSetMetaDeltaKey(userKey, 10, 0)
	d2Key := store.ZSetMetaDeltaKey(userKey, 11, 0)
	require.NoError(t, st.PutAt(ctx, d1Key, delta, 10, 0))
	require.NoError(t, st.PutAt(ctx, d2Key, delta, 11, 0))

	require.NoError(t, c.SyncOnce(ctx))

	readTS := st.LastCommitTS()

	raw, err := st.GetAt(ctx, store.ZSetMetaKey(userKey), readTS)
	require.NoError(t, err)
	got, err := store.UnmarshalZSetMeta(raw)
	require.NoError(t, err)
	require.Equal(t, int64(5), got.Len)

	for _, dk := range [][]byte{d1Key, d2Key} {
		_, getErr := st.GetAt(ctx, dk, readTS)
		require.ErrorIs(t, getErr, store.ErrKeyNotFound)
	}
}

func TestDeltaCompactor_NonLeaderSkips(t *testing.T) {
	t.Parallel()

	// Use a real (writing) coordinator so that if compaction were incorrectly
	// dispatched the delta keys would actually be deleted, making the assertion
	// meaningful. The stub's IsLeaderForKey returns false to simulate a follower.
	st := store.NewMVCCStore()
	coord := newLocalAdapterCoordinator(st)
	coord.leaderSet = true
	coord.leader = false
	c := NewDeltaCompactor(st, coord, WithDeltaCompactorMaxDeltaCount(1))
	ctx := context.Background()

	userKey := []byte("nonleaderlist")
	delta := store.MarshalListMetaDelta(store.ListMetaDelta{HeadDelta: 1, LenDelta: -1})
	d1Key := store.ListMetaDeltaKey(userKey, 10, 0)
	d2Key := store.ListMetaDeltaKey(userKey, 11, 0)
	require.NoError(t, st.PutAt(ctx, d1Key, delta, 10, 0))
	require.NoError(t, st.PutAt(ctx, d2Key, delta, 11, 0))

	require.NoError(t, c.SyncOnce(ctx))

	readTS := st.LastCommitTS()
	// Delta keys must NOT be touched since this node is not the per-key leader.
	_, getErr := st.GetAt(ctx, d1Key, readTS)
	require.NoError(t, getErr, "non-leader should not compact delta keys")
}

func TestDeltaCompactor_ListNoBaseMeta(t *testing.T) {
	t.Parallel()

	// Compactor should work even when the base meta key doesn't exist yet
	// (all state is in deltas). This can happen during migration.
	st, c := newDeltaCompactorTestFixture(t)
	ctx := context.Background()
	userKey := []byte("newlist")

	delta := store.MarshalListMetaDelta(store.ListMetaDelta{HeadDelta: 1, LenDelta: -1})
	d1Key := store.ListMetaDeltaKey(userKey, 10, 0)
	d2Key := store.ListMetaDeltaKey(userKey, 11, 0)
	require.NoError(t, st.PutAt(ctx, d1Key, delta, 10, 0))
	require.NoError(t, st.PutAt(ctx, d2Key, delta, 11, 0))

	require.NoError(t, c.SyncOnce(ctx))

	readTS := st.LastCommitTS()

	// When the accumulated deltas produce Len=0 (clamped from -2), the metadata key
	// must be deleted rather than written with Len=0. Redis semantics: an empty list
	// is non-existent, so EXISTS/TYPE must not return a stale entry.
	_, err := st.GetAt(ctx, store.ListMetaKey(userKey), readTS)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	// Delta keys must also be deleted by the compaction.
	_, err = st.GetAt(ctx, d1Key, readTS)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	_, err = st.GetAt(ctx, d2Key, readTS)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

// TestDeltaCompactor_UrgentCompactionTriggeredByChannel verifies that a request
// queued via TriggerUrgentCompaction is processed by the Run loop, compacting
// the targeted key without waiting for the next regular tick.
//
// maxCount is set high so the regular SyncOnce pass skips the key, ensuring it
// is the urgent path that performs the actual compaction.
func TestDeltaCompactor_UrgentCompactionTriggeredByChannel(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	coord := newLocalAdapterCoordinator(st)
	// High maxCount ensures SyncOnce does not compact our key; only the urgent path will.
	const hugeMaxCount = 1<<31 - 1
	c := NewDeltaCompactor(st, coord, WithDeltaCompactorMaxDeltaCount(hugeMaxCount))
	ctx := context.Background()
	userKey := []byte("urgent-hash-key")

	// Write base meta: Len=5.
	require.NoError(t, st.PutAt(ctx, store.HashMetaKey(userKey), store.MarshalHashMeta(store.HashMeta{Len: 5}), 1, 0))

	// Write 3 delta keys (LenDelta=+1 each). These are below hugeMaxCount so SyncOnce
	// skips them; TriggerUrgentCompaction must be what kicks off compaction.
	delta := store.MarshalHashMetaDelta(store.HashMetaDelta{LenDelta: 1})
	for i := uint64(0); i < 3; i++ {
		dKey := store.HashMetaDeltaKey(userKey, 10+i, 0)
		require.NoError(t, st.PutAt(ctx, dKey, delta, 10+i, 0))
	}

	// Queue the urgent compaction request.
	c.TriggerUrgentCompaction("hash", userKey)

	// Start the Run loop; it runs SyncOnce (skips the key) then drains the urgent channel.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = c.Run(runCtx) }()

	// Wait until the base meta reflects the compacted value (5 + 3 = 8).
	require.Eventually(t, func() bool {
		readTS := st.LastCommitTS()
		raw, err := st.GetAt(ctx, store.HashMetaKey(userKey), readTS)
		if err != nil {
			return false
		}
		got, err := store.UnmarshalHashMeta(raw)
		return err == nil && got.Len == 8
	}, 2*time.Second, 10*time.Millisecond, "urgent compaction should have updated the hash meta to Len=8")
}

// TestDeltaCompactor_UrgentCompactionPagination verifies that when a key has
// more than MaxDeltaScanLimit delta keys, compactUrgentKey loops until all
// batches are compacted, leaving the key readable.
//
// maxCount is set to MaxInt so the regular SyncOnce pass skips the key
// (threshold not met), ensuring only the urgent path exercises the pagination loop.
func TestDeltaCompactor_UrgentCompactionPagination(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	coord := newLocalAdapterCoordinator(st)
	// Use a very high maxCount so SyncOnce never compacts our test key, forcing
	// the urgent path to handle all batches via the pagination loop.
	const hugeMaxCount = 1<<31 - 1
	c := NewDeltaCompactor(st, coord, WithDeltaCompactorMaxDeltaCount(hugeMaxCount))
	ctx := context.Background()
	userKey := []byte("overflow-hash-key")

	// Write more than MaxDeltaScanLimit delta keys so that a single-pass scan
	// (capped at MaxDeltaScanLimit+1) leaves some unconsumed, requiring pagination.
	// Use a typed constant to avoid int->uint64 overflow warnings.
	const totalDeltasU64 = uint64(store.MaxDeltaScanLimit + 10) // 1034
	delta := store.MarshalHashMetaDelta(store.HashMetaDelta{LenDelta: 1})
	for i := uint64(0); i < totalDeltasU64; i++ {
		dKey := store.HashMetaDeltaKey(userKey, i+1, 0)
		require.NoError(t, st.PutAt(ctx, dKey, delta, i+1, 0))
	}

	// Queue and process the urgent compaction.
	c.TriggerUrgentCompaction("hash", userKey)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = c.Run(runCtx) }()

	// Wait until the base meta holds the accumulated total.
	// The pagination loop should take two passes: first 1025, then 9.
	require.Eventually(t, func() bool {
		readTS := st.LastCommitTS()
		raw, err := st.GetAt(ctx, store.HashMetaKey(userKey), readTS)
		if err != nil {
			return false
		}
		got, err := store.UnmarshalHashMeta(raw)
		return err == nil && got.Len == int64(totalDeltasU64)
	}, 5*time.Second, 20*time.Millisecond, "all %d delta keys should be compacted into base meta", totalDeltasU64)

	// No delta keys should remain after pagination compaction.
	readTS := st.LastCommitTS()
	prefix := store.HashMetaDeltaScanPrefix(userKey)
	end := store.PrefixScanEnd(prefix)
	remaining, err := st.ScanAt(ctx, prefix, end, int(totalDeltasU64)+1, readTS)
	require.NoError(t, err)
	require.Empty(t, remaining, "all delta keys must be deleted after urgent compaction")
}
