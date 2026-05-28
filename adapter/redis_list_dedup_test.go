package adapter

import (
	"bytes"
	"context"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

// dedupTestCoordinator drives the option-2 one-phase idempotency path
// end-to-end. It layers two things on the OCC-aware coordinator:
//
//   - The FSM's exact-ts dedup probe (mimicking handleOnePhaseTxnRequest):
//     when a request carries prev_commit_ts, it checks whether the primary
//     key already has a committed version at exactly that timestamp and, if
//     so, no-ops the apply.
//   - An injected ambiguous commit: a chosen dispatch either lands-then-errors
//     (ambiguousLands) or errors-without-applying, reproducing the
//     leadership-churn window where attempt 1 may or may not have committed.
//
// Because the OCC layer is real (store.ApplyMutations against StartTS), a
// reuse attempt whose probe did NOT fire would conflict against attempt 1's
// own version — exactly the trap the probe must avoid. So a passing test
// proves the probe is load-bearing, not cosmetic.
type dedupTestCoordinator struct {
	*occAdapterCoordinator
	ambiguousDispatch int  // 1-based dispatch number to make ambiguous
	ambiguousLands    bool // true: apply then error; false: error without applying
	dispatches        int
	probeNoOps        int
	// beforeDispatch, if set, runs at the start of each Dispatch with the
	// 1-based dispatch number — lets a test inject a concurrent commit
	// between the adapter's attempts.
	beforeDispatch func(n int)
}

func newDedupTestCoordinator(st store.MVCCStore, ambiguousDispatch int, lands bool) *dedupTestCoordinator {
	return &dedupTestCoordinator{
		occAdapterCoordinator: newOCCAdapterCoordinator(st),
		ambiguousDispatch:     ambiguousDispatch,
		ambiguousLands:        lands,
	}
}

func (c *dedupTestCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	c.dispatches++
	n := c.dispatches
	if c.beforeDispatch != nil {
		c.beforeDispatch(n)
	}
	if handled, resp, err := c.maybeProbe(ctx, req); handled {
		return resp, err
	}
	if n == c.ambiguousDispatch && !c.ambiguousLands {
		// OCC-style pre-reject: nothing is written, definitely did not land.
		return nil, store.ErrWriteConflict
	}
	resp, err := c.occAdapterCoordinator.Dispatch(ctx, req)
	if err != nil {
		return nil, err
	}
	if n == c.ambiguousDispatch && c.ambiguousLands {
		// The apply LANDED, but the adapter sees an ambiguous retryable error.
		return nil, store.ErrWriteConflict
	}
	return resp, nil
}

// maybeProbe mimics handleOnePhaseTxnRequest's exact-ts dedup check. It
// returns handled=true when the probe owns the response (hit → no-op success
// or probe error), and handled=false to fall through to the normal apply.
// Extracted from Dispatch to keep the dispatcher under the cyclop limit.
func (c *dedupTestCoordinator) maybeProbe(ctx context.Context, req *kv.OperationGroup[kv.OP]) (bool, *kv.CoordinateResponse, error) {
	if req == nil || !req.IsTxn || req.PrevCommitTS == 0 {
		return false, nil, nil
	}
	primary := minElemKey(req.Elems)
	landed, err := c.store.CommittedVersionAt(ctx, primary, req.PrevCommitTS)
	if err != nil {
		return true, nil, err
	}
	if landed {
		c.probeNoOps++
		return true, &kv.CoordinateResponse{}, nil
	}
	return false, nil, nil
}

// minElemKey mirrors kv.primaryKeyForElems: the minimum non-empty key among
// the elements. The FSM probes this key for the prior attempt's version.
func minElemKey(elems []*kv.Elem[kv.OP]) []byte {
	var primary []byte
	for _, e := range elems {
		if e == nil || len(e.Key) == 0 {
			continue
		}
		if primary == nil || bytes.Compare(e.Key, primary) < 0 {
			primary = e.Key
		}
	}
	return primary
}

// TestListPushDedup_LandedPriorAttempt_NoDuplicate is the option-2 headline:
// attempt 1 commits the RPUSH but returns an ambiguous error; the retry
// reuses the same write set with prev_commit_ts, the probe finds the landed
// version and no-ops, and the client gets the correct length with NO
// duplicate element. Without the probe, the reuse would OCC-conflict against
// attempt 1's own version, recompute at a new seq, and duplicate.
func TestListPushDedup_LandedPriorAttempt_NoDuplicate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := newDedupTestCoordinator(st, 1, true)
	srv := &RedisServer{store: st, coordinator: coord, scriptCache: map[string]string{}, onePhaseTxnDedup: true}

	key := []byte("mylist")
	n, err := srv.listRPush(ctx, key, [][]byte{[]byte("v")})
	require.NoError(t, err)
	require.Equal(t, int64(1), n, "RPUSH must return the post-push length reconstructed from the prior attempt")
	require.Equal(t, 2, coord.dispatches, "one failed attempt + one reuse")
	require.Equal(t, 1, coord.probeNoOps, "the reuse must dedup via the exact-ts probe")

	// No duplicate: the list has exactly one element.
	readTS := snapshotTS(coord.Clock(), st)
	meta, _, err := srv.resolveListMeta(ctx, key, readTS)
	require.NoError(t, err)
	require.Equal(t, int64(1), meta.Len, "the delta must be counted once — no double-count")

	val, err := st.GetAt(ctx, listItemKey(key, meta.Head), readTS)
	require.NoError(t, err)
	require.Equal(t, []byte("v"), val)
}

// TestListPushDedup_PriorAttemptDidNotLand_Applies covers the truncated case:
// attempt 1 errored without committing, so the probe misses and the reuse
// applies the same write set at a fresh commit_ts. The element lands exactly
// once and the length is correct.
func TestListPushDedup_PriorAttemptDidNotLand_Applies(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := newDedupTestCoordinator(st, 1, false)
	srv := &RedisServer{store: st, coordinator: coord, scriptCache: map[string]string{}, onePhaseTxnDedup: true}

	key := []byte("mylist")
	n, err := srv.listRPush(ctx, key, [][]byte{[]byte("v")})
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
	require.Equal(t, 2, coord.dispatches)
	require.Equal(t, 0, coord.probeNoOps, "nothing landed, so the probe must miss and the reuse applies")

	readTS := snapshotTS(coord.Clock(), st)
	meta, _, err := srv.resolveListMeta(ctx, key, readTS)
	require.NoError(t, err)
	require.Equal(t, int64(1), meta.Len)

	val, err := st.GetAt(ctx, listItemKey(key, meta.Head), readTS)
	require.NoError(t, err)
	require.Equal(t, []byte("v"), val)
}

// TestListPushDedup_GenuineConflictRecomputes covers outcome 3: the prior
// attempt did NOT land, and a concurrent txn took the target seq between
// attempts. The reuse's probe misses, the OCC apply conflicts against the
// other txn, so the adapter drops the reusable attempt and recomputes from a
// fresh meta — appending our value at a NEW seq. Two distinct elements result
// (correct), not a duplicate.
func TestListPushDedup_GenuineConflictRecomputes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := newDedupTestCoordinator(st, 1, false) // attempt 1 errors without landing
	key := []byte("mylist")
	// Before the reuse (dispatch 2), simulate another client's RPUSH landing
	// at seq 0 so the reuse conflicts and must recompute.
	coord.beforeDispatch = func(n int) {
		if n != 2 {
			return
		}
		ts := coord.Clock().Next()
		require.NoError(t, st.PutAt(ctx, listItemKey(key, 0), []byte("other"), ts, 0))
		delta := store.MarshalListMetaDelta(store.ListMetaDelta{HeadDelta: 0, LenDelta: 1})
		require.NoError(t, st.PutAt(ctx, store.ListMetaDeltaKey(key, ts, 0), delta, ts, 0))
	}
	srv := &RedisServer{store: st, coordinator: coord, scriptCache: map[string]string{}, onePhaseTxnDedup: true}

	n, err := srv.listRPush(ctx, key, [][]byte{[]byte("v")})
	require.NoError(t, err)
	require.Equal(t, int64(2), n, "after recompute, our value is the 2nd element")
	require.Equal(t, 3, coord.dispatches, "attempt 1 + conflicting reuse + recompute")
	require.Equal(t, 0, coord.probeNoOps, "the reuse probe must miss (prior did not land)")

	readTS := snapshotTS(coord.Clock(), st)
	meta, _, err := srv.resolveListMeta(ctx, key, readTS)
	require.NoError(t, err)
	require.Equal(t, int64(2), meta.Len, "two distinct appends, not a duplicate")

	other, err := st.GetAt(ctx, listItemKey(key, meta.Head), readTS)
	require.NoError(t, err)
	require.Equal(t, []byte("other"), other)
	ours, err := st.GetAt(ctx, listItemKey(key, meta.Head+1), readTS)
	require.NoError(t, err)
	require.Equal(t, []byte("v"), ours)
}

// TestListPushDedup_DisabledKeepsLegacyPath confirms the gate is off by
// default: without onePhaseTxnDedup, listPushCore never emits prev_commit_ts,
// so the coordinator's probe is never exercised (dedup count stays zero) and
// the legacy recompute-on-retry path runs.
func TestListPushDedup_DisabledKeepsLegacyPath(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	// ambiguousDispatch=0 → no injected error; a clean single dispatch.
	coord := newDedupTestCoordinator(st, 0, false)
	srv := &RedisServer{store: st, coordinator: coord, scriptCache: map[string]string{}} // onePhaseTxnDedup defaults false

	key := []byte("mylist")
	n, err := srv.listRPush(ctx, key, [][]byte{[]byte("v")})
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
	require.Equal(t, 1, coord.dispatches)
	require.Equal(t, 0, coord.probeNoOps)
}
