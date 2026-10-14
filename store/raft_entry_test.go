package store

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func raftEntryPut(key, value string) []*KVPairMutation {
	return []*KVPairMutation{{Op: OpTypePut, Key: []byte(key), Value: []byte(value)}}
}

func raftEntryExists(t *testing.T, st MVCCStore, key string) bool {
	t.Helper()
	ok, err := st.ExistsAt(context.Background(), []byte(key), ^uint64(0))
	require.NoError(t, err)
	return ok
}

func raftEntryAppliedIndex(t *testing.T, st MVCCStore) (uint64, bool) {
	t.Helper()
	idx, present, err := pebbleStoreApplied(t, st).LastAppliedIndex()
	require.NoError(t, err)
	return idx, present
}

func raftEntryApplier(t *testing.T, st MVCCStore) RaftEntryApplier {
	t.Helper()
	applier, ok := st.(RaftEntryApplier)
	require.True(t, ok, "%T must implement RaftEntryApplier", st)
	return applier
}

// TestApplyRaftEntry_NothingIsDurableUntilTheEntryCommits is the store-level
// shape of audit gap G17: inside the scope neither the first request's write
// nor the entry's applied index is durable, so a crash between two requests
// cannot leave a replica whose durable applied index names a half-applied
// entry; after the scope both are durable together.
func TestApplyRaftEntry_NothingIsDurableUntilTheEntryCommits(t *testing.T) {
	ctx := context.Background()
	st := newApplyIndexPebbleStore(t)
	applier := raftEntryApplier(t, st)
	require.NoError(t, st.ApplyMutationsRaftAt(ctx, raftEntryPut("k0", "v0"), nil, 5, 5, 3))

	var (
		midIndex  uint64
		midK1     bool
		midLastTS uint64
	)
	require.NoError(t, applier.ApplyRaftEntry(ctx, 4, func(ctx context.Context) error {
		if err := st.ApplyMutationsRaftAt(ctx, raftEntryPut("k1", "v1"), nil, 10, 10, 4); err != nil {
			return err
		}
		midIndex, _ = raftEntryAppliedIndex(t, st)
		midK1 = raftEntryExists(t, st, "k1")
		midLastTS = st.LastCommitTS()
		return st.ApplyMutationsRaftAt(ctx, raftEntryPut("k2", "v2"), nil, 11, 11, 4)
	}))

	require.Equal(t, uint64(3), midIndex, "the applied index must not name the entry before it commits")
	require.False(t, midK1, "the first request must not be durable before the entry commits")
	require.Equal(t, uint64(5), midLastTS, "lastCommitTS must not advance before the entry commits")

	idx, present := raftEntryAppliedIndex(t, st)
	require.True(t, present)
	require.Equal(t, uint64(4), idx)
	require.True(t, raftEntryExists(t, st, "k1"))
	require.True(t, raftEntryExists(t, st, "k2"))
	require.Equal(t, uint64(11), st.LastCommitTS())

	ps := pebbleStoreApplied(t, st)
	persisted, err := readPebbleUint64(ps.db, metaLastCommitTSBytes)
	require.NoError(t, err)
	require.Equal(t, uint64(11), persisted, "the persisted lastCommitTS travels in the entry's batch")
}

func TestApplyRaftEntry_CallbackErrorDiscardsEveryRequest(t *testing.T) {
	ctx := context.Background()
	st := newApplyIndexPebbleStore(t)
	applier := raftEntryApplier(t, st)
	require.NoError(t, st.ApplyMutationsRaftAt(ctx, raftEntryPut("k0", "v0"), nil, 5, 5, 3))

	errBoom := errors.New("boom")
	err := applier.ApplyRaftEntry(ctx, 4, func(ctx context.Context) error {
		require.NoError(t, st.ApplyMutationsRaftAt(ctx, raftEntryPut("k1", "v1"), nil, 10, 10, 4))
		return errBoom
	})
	require.ErrorIs(t, err, errBoom)

	require.False(t, raftEntryExists(t, st, "k1"))
	idx, _ := raftEntryAppliedIndex(t, st)
	require.Equal(t, uint64(3), idx)
	require.Equal(t, uint64(5), st.LastCommitTS())

	// The store stays usable: the same entry applies in full afterwards.
	require.NoError(t, applier.ApplyRaftEntry(ctx, 4, func(ctx context.Context) error {
		return st.ApplyMutationsRaftAt(ctx, raftEntryPut("k1", "v1"), nil, 10, 10, 4)
	}))
	require.True(t, raftEntryExists(t, st, "k1"))
	idx, _ = raftEntryAppliedIndex(t, st)
	require.Equal(t, uint64(4), idx)
}

// TestApplyRaftEntry_LaterRequestSeesEarlierRequest pins that a request
// inside the scope observes the requests staged before it exactly as it
// would after a per-request commit: a prefix delete tombstones a key the
// previous request wrote, and a write below an earlier request's commit
// timestamp on the same key is a write conflict.
func TestApplyRaftEntry_LaterRequestSeesEarlierRequest(t *testing.T) {
	ctx := context.Background()
	st := newApplyIndexPebbleStore(t)
	applier := raftEntryApplier(t, st)

	require.NoError(t, applier.ApplyRaftEntry(ctx, 7, func(ctx context.Context) error {
		require.NoError(t, st.ApplyMutationsRaftAt(ctx, raftEntryPut("h/f", "1"), nil, 20, 20, 7))
		require.NoError(t, st.DeletePrefixesAtRaftAt(ctx, []PrefixDelete{{Prefix: []byte("h/")}}, 21, 7))
		require.NoError(t, st.ApplyMutationsRaftAt(ctx, raftEntryPut("x", "hi"), nil, 30, 30, 7))
		err := st.ApplyMutationsRaftAt(ctx, raftEntryPut("x", "stale"), nil, 29, 29, 7)
		require.ErrorIs(t, err, ErrWriteConflict)
		return nil
	}))

	require.False(t, raftEntryExists(t, st, "h/f"), "the prefix delete must see the write staged before it")
	got, err := st.GetAt(ctx, []byte("x"), ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, []byte("hi"), got)
	idx, _ := raftEntryAppliedIndex(t, st)
	require.Equal(t, uint64(7), idx)
	require.Equal(t, uint64(30), st.LastCommitTS())
}

// TestApplyRaftEntry_RepeatedRequestInsideEntryIsAReplay pins parity with the
// per-request path's stale-apply fast path: the same key at the same commit
// timestamp twice in one entry is treated as already landed the second time.
func TestApplyRaftEntry_RepeatedRequestInsideEntryIsAReplay(t *testing.T) {
	ctx := context.Background()
	st := newApplyIndexPebbleStore(t)
	applier := raftEntryApplier(t, st)

	require.NoError(t, applier.ApplyRaftEntry(ctx, 9, func(ctx context.Context) error {
		require.NoError(t, st.ApplyMutationsRaftAt(ctx, raftEntryPut("k", "a"), nil, 50, 50, 9))
		return st.ApplyMutationsRaftAt(ctx, raftEntryPut("k", "b"), nil, 50, 50, 9)
	}))

	got, err := st.GetAt(ctx, []byte("k"), ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, []byte("a"), got)
	idx, _ := raftEntryAppliedIndex(t, st)
	require.Equal(t, uint64(9), idx)
}

// TestApplyRaftEntry_ValidationFailureLeavesBatchUntouched pins that a
// request rejected before staging neither taints the entry nor leaks a
// partial write: the requests around it still land.
func TestApplyRaftEntry_ValidationFailureLeavesBatchUntouched(t *testing.T) {
	ctx := context.Background()
	st := newApplyIndexPebbleStore(t)
	applier := raftEntryApplier(t, st)

	require.NoError(t, applier.ApplyRaftEntry(ctx, 8, func(ctx context.Context) error {
		require.NoError(t, st.ApplyMutationsRaftAt(ctx, raftEntryPut("k1", "v1"), nil, 10, 10, 8))
		bad := []*KVPairMutation{
			{Op: OpTypePut, Key: []byte("good-in-bad"), Value: []byte("v")},
			{Op: OpType(99), Key: []byte("bad"), Value: []byte("v")},
		}
		require.ErrorIs(t, st.ApplyMutationsRaftAt(ctx, bad, nil, 11, 11, 8), ErrUnknownOp)
		return st.ApplyMutationsRaftAt(ctx, raftEntryPut("k2", "v2"), nil, 12, 12, 8)
	}))

	require.True(t, raftEntryExists(t, st, "k1"))
	require.True(t, raftEntryExists(t, st, "k2"))
	require.False(t, raftEntryExists(t, st, "good-in-bad"), "a rejected request must stage nothing")
	idx, _ := raftEntryAppliedIndex(t, st)
	require.Equal(t, uint64(8), idx)
	require.Equal(t, uint64(12), st.LastCommitTS())
}

func TestApplyRaftEntry_EmptyEntryCommitsNothing(t *testing.T) {
	ctx := context.Background()
	st := newApplyIndexPebbleStore(t)
	applier := raftEntryApplier(t, st)
	require.NoError(t, st.ApplyMutationsRaftAt(ctx, raftEntryPut("k0", "v0"), nil, 5, 5, 3))

	require.NoError(t, applier.ApplyRaftEntry(ctx, 4, func(context.Context) error { return nil }))

	idx, _ := raftEntryAppliedIndex(t, st)
	require.Equal(t, uint64(3), idx, "an entry whose every request failed validation writes nothing, as on the per-request path")
}

func TestApplyRaftEntry_IndexMismatchFailsTheEntry(t *testing.T) {
	ctx := context.Background()
	st := newApplyIndexPebbleStore(t)
	applier := raftEntryApplier(t, st)

	err := applier.ApplyRaftEntry(ctx, 4, func(ctx context.Context) error {
		require.NoError(t, st.ApplyMutationsRaftAt(ctx, raftEntryPut("k1", "v1"), nil, 10, 10, 4))
		require.ErrorIs(t, st.ApplyMutationsRaftAt(ctx, raftEntryPut("k2", "v2"), nil, 11, 11, 5), ErrRaftEntryIndexMismatch)
		// Every later staging call reports the same failure.
		require.ErrorIs(t, st.ApplyMutationsRaftAt(ctx, raftEntryPut("k3", "v3"), nil, 12, 12, 4), ErrRaftEntryIndexMismatch)
		return nil
	})
	require.ErrorIs(t, err, ErrRaftEntryIndexMismatch)
	require.False(t, raftEntryExists(t, st, "k1"))
	_, present := raftEntryAppliedIndex(t, st)
	require.False(t, present)
}

func TestApplyRaftEntry_NestedScopeReusesTheOuterEntry(t *testing.T) {
	ctx := context.Background()
	st := newApplyIndexPebbleStore(t)
	applier := raftEntryApplier(t, st)

	var midK1 bool
	require.NoError(t, applier.ApplyRaftEntry(ctx, 4, func(ctx context.Context) error {
		if err := applier.ApplyRaftEntry(ctx, 4, func(ctx context.Context) error {
			return st.ApplyMutationsRaftAt(ctx, raftEntryPut("k1", "v1"), nil, 10, 10, 4)
		}); err != nil {
			return err
		}
		midK1 = raftEntryExists(t, st, "k1")
		return nil
	}))
	require.False(t, midK1, "a nested scope must not commit on its own")
	require.True(t, raftEntryExists(t, st, "k1"))
}

// TestApplyRaftEntry_SnapshotBetweenRequestsDoesNotDeadlock pins the lock
// order of the entry scope against newSSTIngestSnapshot, which takes dbMu
// exclusively and then applyMu. An entry that held applyMu across its
// callback and re-acquired dbMu for each staging call inverted that order:
// a snapshot taken between two requests held dbMu waiting for applyMu
// while the second request waited for dbMu, and Raft apply stopped for
// good (found in review).
func TestApplyRaftEntry_SnapshotBetweenRequestsDoesNotDeadlock(t *testing.T) {
	ctx := context.Background()
	st := newApplyIndexPebbleStore(t)
	ps := pebbleStoreApplied(t, st)
	applier := raftEntryApplier(t, st)

	const wait = 10 * time.Second
	snapshotDone := make(chan error, 1)
	entryDone := make(chan error, 1)
	go func() {
		entryDone <- applier.ApplyRaftEntry(ctx, 4, func(ctx context.Context) error {
			if err := st.ApplyMutationsRaftAt(ctx, raftEntryPut("k1", "v1"), nil, 10, 10, 4); err != nil {
				return err
			}
			go func() {
				snap, err := ps.newSSTIngestSnapshot()
				if err == nil {
					err = snap.Close()
				}
				snapshotDone <- err
			}()
			// Give the snapshot time to reach its lock acquisition before the
			// second request stages; the result must not depend on it either way.
			time.Sleep(100 * time.Millisecond)
			return st.ApplyMutationsRaftAt(ctx, raftEntryPut("k2", "v2"), nil, 11, 11, 4)
		})
	}()

	select {
	case err := <-entryDone:
		require.NoError(t, err)
	case <-time.After(wait):
		t.Fatal("ApplyRaftEntry deadlocked against a snapshot taken between two requests of the entry")
	}
	select {
	case err := <-snapshotDone:
		require.NoError(t, err)
	case <-time.After(wait):
		t.Fatal("the snapshot never completed")
	}
	require.True(t, raftEntryExists(t, st, "k1"))
	require.True(t, raftEntryExists(t, st, "k2"))
	idx, _ := raftEntryAppliedIndex(t, st)
	require.Equal(t, uint64(4), idx)
}

func TestApplyRaftEntry_InMemoryStoreRunsTheCallback(t *testing.T) {
	ctx := context.Background()
	st := NewMVCCStore()
	applier := raftEntryApplier(t, st)
	require.NoError(t, applier.ApplyRaftEntry(ctx, 4, func(ctx context.Context) error {
		return st.ApplyMutationsRaftAt(ctx, raftEntryPut("k1", "v1"), nil, 10, 10, 4)
	}))
	require.True(t, raftEntryExists(t, st, "k1"))
}
