package adapter

import (
	"bytes"
	"context"
	"encoding/binary"
<<<<<<< HEAD
=======
	"errors"
>>>>>>> origin/design/hotspot-split-m2-promotion-complete
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	// landThenWriteConflictAtDispatch reproduces the self-inflicted-conflict
	// scenario (codex P1): the named dispatch applies the elems THEN returns
	// store.ErrWriteConflict — modelling leadership churn that surfaces a
	// committed entry as a write conflict.
	landThenWriteConflictAtDispatch int
	// txnLockedAtDispatch makes the named dispatch return kv.ErrTxnLocked
	// WITHOUT applying — an ambiguous retryable error distinct from
	// WriteConflict, exercising the "advance pending.commitTS and retry" branch.
	txnLockedAtDispatch int
<<<<<<< HEAD
=======
	// wireWriteConflicts converts every typed write conflict this test
	// coordinator returns into the keyed gRPC status produced by leader
	// forwarding. It exercises adapter-side type restoration without changing
	// the underlying landed-vs-not-landed scenario.
	wireWriteConflicts bool
>>>>>>> origin/design/hotspot-split-m2-promotion-complete
	// routeFenceAtDispatch makes the named dispatch return ErrRouteWriteFenced
	// before the FSM dedup probe or apply. It is retryable but cannot be
	// treated as an ambiguous landing.
	routeFenceAtDispatch int
	dispatches           int
	probeNoOps           int
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

func TestResolveListMetaReadsLegacyDeltaPrefix(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	srv := &RedisServer{store: st}
	key := []byte("legacy-list")
	base, err := store.MarshalListMeta(store.ListMeta{Head: 10, Tail: 12, Len: 2})
	require.NoError(t, err)
	require.NoError(t, st.PutAt(ctx, store.ListMetaKey(key), base, 1, 0))
	delta := store.MarshalListMetaDelta(store.ListMetaDelta{HeadDelta: -1, LenDelta: 3})
	require.NoError(t, st.PutAt(ctx, legacyListMetaDeltaKey(key, 2), delta, 2, 0))

	meta, exists, err := srv.resolveListMeta(ctx, key, 3)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, int64(9), meta.Head)
	require.Equal(t, int64(5), meta.Len)
	require.Equal(t, int64(14), meta.Tail)
}

func TestResolveListMetaEnforcesDeltaLimitAcrossCurrentAndLegacyPrefixes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	srv := &RedisServer{store: st}
	key := []byte("list-delta-cap")
	delta := store.MarshalListMetaDelta(store.ListMetaDelta{LenDelta: 1})
	for i := uint64(1); i <= uint64(store.MaxDeltaScanLimit); i++ {
		require.NoError(t, st.PutAt(ctx, store.ListMetaDeltaKey(key, i, 0), delta, i, 0))
	}
	legacyTS := uint64(store.MaxDeltaScanLimit + 1)
	require.NoError(t, st.PutAt(ctx, legacyListMetaDeltaKey(key, legacyTS), delta, legacyTS, 0))

	_, _, err := srv.resolveListMeta(ctx, key, legacyTS)
	require.ErrorIs(t, err, ErrDeltaScanTruncated)
}

func TestResolveListMetaDoesNotTreatDeltaLookingMetaValueAsLegacyDelta(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	srv := &RedisServer{store: st}
	key := deltaLookingListMetaUserKey([]byte("embedded"))
	base, err := store.MarshalListMeta(store.ListMeta{Head: 4, Tail: 6, Len: 2})
	require.NoError(t, err)
	require.NoError(t, st.PutAt(ctx, store.ListMetaKey(key), base, 1, 0))

	meta, exists, err := srv.resolveListMeta(ctx, key, 2)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, int64(4), meta.Head)
	require.Equal(t, int64(2), meta.Len)
	require.Equal(t, int64(6), meta.Tail)
}

func TestResolveListMetaIgnoresLegacyDeltaPrefixCollisionForMissingKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	srv := &RedisServer{store: st}
	key := []byte("legacy-collision")
	collidingUserKey := deltaLookingListMetaUserKey(key)
	base, err := store.MarshalListMeta(store.ListMeta{Head: 4, Tail: 6, Len: 2})
	require.NoError(t, err)
	require.NoError(t, st.PutAt(ctx, store.ListMetaKey(collidingUserKey), base, 1, 0))

	_, exists, err := srv.resolveListMeta(ctx, key, 2)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestResolveListMetaCountsOnlyAcceptedLegacyDeltasForTruncation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	srv := &RedisServer{store: st}
	key := []byte("legacy-collision-window")
	base, err := store.MarshalListMeta(store.ListMeta{Head: 0, Tail: 1, Len: 1})
	require.NoError(t, err)
	require.NoError(t, st.PutAt(ctx, store.ListMetaKey(key), base, 1, 0))

	collidingMeta, err := store.MarshalListMeta(store.ListMeta{Head: 4, Tail: 6, Len: 2})
	require.NoError(t, err)
	for i := uint64(2); i < uint64(store.MaxDeltaScanLimit+2); i++ {
		collidingUserKey := deltaLookingListMetaUserKeyAt(key, i, 0)
		require.NoError(t, st.PutAt(ctx, store.ListMetaKey(collidingUserKey), collidingMeta, i, 0))
	}
	delta := store.MarshalListMetaDelta(store.ListMetaDelta{HeadDelta: 0, LenDelta: 1})
	deltaTS := uint64(store.MaxDeltaScanLimit + 2)
	require.NoError(t, st.PutAt(ctx, legacyListMetaDeltaKey(key, deltaTS), delta, deltaTS, 0))

	meta, exists, err := srv.resolveListMeta(ctx, key, deltaTS+1)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, int64(2), meta.Len)
}

func TestProbeListTypeReadsLegacyDeltaPrefix(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	srv := &RedisServer{store: st}
	key := []byte("legacy-list-type")
	delta := store.MarshalListMetaDelta(store.ListMetaDelta{HeadDelta: 0, LenDelta: 1})
	require.NoError(t, st.PutAt(ctx, legacyListMetaDeltaKey(key, 2), delta, 2, 0))

	typ, found, err := srv.probeListType(ctx, key, 3)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, redisTypeList, typ)
}

func TestProbeListTypeIgnoresLegacyDeltaPrefixCollision(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	srv := &RedisServer{store: st}
	key := []byte("legacy-list-type-collision")
	collidingUserKey := deltaLookingListMetaUserKey(key)
	base, err := store.MarshalListMeta(store.ListMeta{Head: 4, Tail: 6, Len: 2})
	require.NoError(t, err)
	require.NoError(t, st.PutAt(ctx, store.ListMetaKey(collidingUserKey), base, 1, 0))

	_, found, err := srv.probeListType(ctx, key, 2)
	require.NoError(t, err)
	require.False(t, found)
}

func TestProbeListTypePagesPastLegacyDeltaPrefixCollisions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	srv := &RedisServer{store: st}
	key := []byte("legacy-list-type-page")
	collidingMeta, err := store.MarshalListMeta(store.ListMeta{Head: 4, Tail: 6, Len: 2})
	require.NoError(t, err)
	for i := uint64(1); i <= uint64(store.MaxDeltaScanLimit); i++ {
		collidingUserKey := deltaLookingListMetaUserKeyAt(key, i, 0)
		require.NoError(t, st.PutAt(ctx, store.ListMetaKey(collidingUserKey), collidingMeta, i, 0))
	}
	deltaTS := uint64(store.MaxDeltaScanLimit + 1)
	delta := store.MarshalListMetaDelta(store.ListMetaDelta{HeadDelta: 0, LenDelta: 1})
	require.NoError(t, st.PutAt(ctx, legacyListMetaDeltaKey(key, deltaTS), delta, deltaTS, 0))

	typ, found, err := srv.probeListType(ctx, key, deltaTS+1)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, redisTypeList, typ)
}

func TestDeleteListElemsFiltersLegacyDeltaPrefixCollisions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	srv := &RedisServer{store: st}
	key := []byte("legacy-list-delete")
	collidingUserKey := deltaLookingListMetaUserKey(key)
	base, err := store.MarshalListMeta(store.ListMeta{Head: 4, Tail: 6, Len: 2})
	require.NoError(t, err)
	collidingMetaKey := store.ListMetaKey(collidingUserKey)
	require.NoError(t, st.PutAt(ctx, collidingMetaKey, base, 1, 0))
	deltaKey := legacyListMetaDeltaKey(key, 2)
	delta := store.MarshalListMetaDelta(store.ListMetaDelta{HeadDelta: 0, LenDelta: 1})
	require.NoError(t, st.PutAt(ctx, deltaKey, delta, 2, 0))

	elems, err := srv.deleteListElems(ctx, key, 3)
	require.NoError(t, err)
	deleted := make(map[string]struct{}, len(elems))
	for _, elem := range elems {
		if elem.Op == kv.Del {
			deleted[string(elem.Key)] = struct{}{}
		}
	}
	require.Contains(t, deleted, string(deltaKey))
	require.NotContains(t, deleted, string(collidingMetaKey))
}

func legacyListMetaDeltaKey(userKey []byte, commitTS uint64) []byte {
	key := store.LegacyListMetaDeltaScanPrefix(userKey)
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], commitTS)
	key = append(key, ts[:]...)
	var seq [4]byte
	binary.BigEndian.PutUint32(seq[:], 0)
	return append(key, seq[:]...)
}

func deltaLookingListMetaUserKey(fakeUserKey []byte) []byte {
	return deltaLookingListMetaUserKeyAt(fakeUserKey, 7, 1)
}

func deltaLookingListMetaUserKeyAt(fakeUserKey []byte, commitTS uint64, seqInTxn uint32) []byte {
	key := make([]byte, 0, len("d|")+4+len(fakeUserKey)+8+4)
	key = append(key, "d|"...)
	var lenPrefix [4]byte
	binary.BigEndian.PutUint32(lenPrefix[:], uint32(len(fakeUserKey))) //nolint:gosec // test data is small.
	key = append(key, lenPrefix[:]...)
	key = append(key, fakeUserKey...)
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], commitTS)
	key = append(key, ts[:]...)
	var seq [4]byte
	binary.BigEndian.PutUint32(seq[:], seqInTxn)
	return append(key, seq[:]...)
}

func (c *dedupTestCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	c.dispatches++
	n := c.dispatches
	if c.beforeDispatch != nil {
		c.beforeDispatch(n)
	}
	if c.shouldRouteFence(n) {
		return nil, kv.ErrRouteWriteFenced
	}
	if handled, resp, err := c.maybeProbe(ctx, req); handled {
		return resp, err
	}
	if err := c.preApplyError(n); err != nil {
<<<<<<< HEAD
		return nil, err
=======
		return nil, c.maybeWireWriteConflict(req, err)
>>>>>>> origin/design/hotspot-split-m2-promotion-complete
	}
	resp, err := c.occAdapterCoordinator.Dispatch(ctx, req)
	if err != nil {
		return nil, c.maybeWireWriteConflict(req, err)
	}
	if n == c.ambiguousDispatch && c.ambiguousLands {
		// The apply LANDED, but the adapter sees an ambiguous retryable error.
		return nil, c.maybeWireWriteConflict(req, store.ErrWriteConflict)
	}
	if n == c.landThenWriteConflictAtDispatch {
		// The apply LANDED, but leadership churn surfaces the commit as
		// store.ErrWriteConflict — the original bug class. The adapter's
		// self-inflicted-conflict guard must probe our just-committed
		// commit_ts before treating this as a third-party conflict.
		return nil, c.maybeWireWriteConflict(req, store.ErrWriteConflict)
	}
	return resp, nil
}

<<<<<<< HEAD
=======
func (c *dedupTestCoordinator) maybeWireWriteConflict(req *kv.OperationGroup[kv.OP], err error) error {
	if !c.wireWriteConflicts || !errors.Is(err, store.ErrWriteConflict) {
		return err
	}
	key, ok := store.WriteConflictKey(err)
	if !ok || len(key) == 0 {
		key = minElemKey(req.Elems)
	}
	return status.Error(codes.Unknown, store.NewWriteConflictError(key).Error())
}

>>>>>>> origin/design/hotspot-split-m2-promotion-complete
func (c *dedupTestCoordinator) shouldRouteFence(dispatch int) bool {
	return dispatch == c.routeFenceAtDispatch
}

func (c *dedupTestCoordinator) preApplyError(dispatch int) error {
	switch {
	case dispatch == c.ambiguousDispatch && !c.ambiguousLands:
		return store.ErrWriteConflict
	case dispatch == c.txnLockedAtDispatch:
		return kv.ErrTxnLocked
	default:
		return nil
	}
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

func TestListPushBoundaryReadKeysIncludeWideFence(t *testing.T) {
	t.Parallel()

	key := []byte("list:fence")
	require.Equal(t,
		[][]byte{redisTxnWideListFenceKey(key)},
		listPushBoundaryReadKeys(key, store.ListMeta{}))
	require.Equal(t,
		[][]byte{redisTxnWideListFenceKey(key), listItemKey(key, 7)},
		listPushBoundaryReadKeys(key, store.ListMeta{Head: 7, Len: 1, Tail: 8}))
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

func TestListPushDedup_RouteFenceRetryPreservesPriorProbe(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := newDedupTestCoordinator(st, 1, true)
	coord.routeFenceAtDispatch = 2
	srv := &RedisServer{store: st, coordinator: coord, scriptCache: map[string]string{}, onePhaseTxnDedup: true}

	key := []byte("mylist")
	n, err := srv.listRPush(ctx, key, [][]byte{[]byte("v")})
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
	require.Equal(t, 3, coord.dispatches, "attempt 1 landed, route-fenced reuse, then dedup probe retry")
	require.Equal(t, 1, coord.probeNoOps, "route-fenced reuse must not replace the prior landed probe")

	readTS := snapshotTS(coord.Clock(), st)
	meta, _, err := srv.resolveListMeta(ctx, key, readTS)
	require.NoError(t, err)
	require.Equal(t, int64(1), meta.Len, "route-fence retry must not append a duplicate")
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
	coord.wireWriteConflicts = true                // forwarded conflict loses its typed chain
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

// TestListPushDedup_InterveningRPopRefusesStaleReuse is the codex P1
// regression. From a one-element list [A]: attempt 1's RPUSH "v" plans seq 1
// then fails without landing; an intervening RPOP shrinks the list to []
// (Head→1, Len→0); the retry must NOT silently re-apply the stale write set
// (which would place "v" at seq 1, past the new Tail=1, unreachable to
// LRANGE). The boundary-position read keys (listItemKey(Head),
// listItemKey(Tail-1)) carried in the dispatched OperationGroup.ReadKeys
// catch the RPOP atomically via OCC; the adapter drops pending and recomputes
// from the fresh (empty) meta so the new RPUSH plans seq 1 (Head+Len=1+0)
// and lands at the correct, reachable position.
func TestListPushDedup_InterveningRPopRefusesStaleReuse(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	key := []byte("mylist")

	// Seed the list with one element at seq 0 (Head=0, Len=1, Tail=1).
	require.NoError(t, st.PutAt(ctx, listItemKey(key, 0), []byte("A"), 1, 0))
	delta := store.MarshalListMetaDelta(store.ListMetaDelta{HeadDelta: 0, LenDelta: 1})
	require.NoError(t, st.PutAt(ctx, store.ListMetaDeltaKey(key, 1, 0), delta, 1, 0))

	coord := newDedupTestCoordinator(st, 1, false) // attempt 1 errors without landing
	coord.clock.Observe(1)
	// Between attempts, an RPOP commits: it tombstones seq 0 and writes a
	// (HeadDelta=+1, LenDelta=-1) meta delta — shrinking the list to empty.
	coord.beforeDispatch = func(n int) {
		if n != 2 {
			return
		}
		ts := coord.Clock().Next()
		require.NoError(t, st.DeleteAt(ctx, listItemKey(key, 0), ts))
		popDelta := store.MarshalListMetaDelta(store.ListMetaDelta{HeadDelta: 1, LenDelta: -1})
		require.NoError(t, st.PutAt(ctx, store.ListMetaDeltaKey(key, ts, 0), popDelta, ts, 0))
	}
	srv := &RedisServer{store: st, coordinator: coord, scriptCache: map[string]string{}, onePhaseTxnDedup: true}

	n, err := srv.listRPush(ctx, key, [][]byte{[]byte("v")})
	require.NoError(t, err)
	// After the RPOP shrank the list to [] and we recomputed onto the new
	// state, the post-push length is 1 (just "v") and seq is the new Tail
	// (Head+Len = 1+0 = 1) — but more importantly, the element MUST be
	// reachable: meta.Head <= seq < meta.Tail.
	require.Equal(t, int64(1), n)
	// Pin the exact dispatch sequence: attempt 1 pre-rejects, attempt 2's
	// reuse OCC-conflicts on the boundary read key (RPOP's tombstone),
	// attempt 3 recomputes from fresh meta and lands. A future regression
	// that piles on extra dispatches before success would slip past a >2
	// check.
	require.Equal(t, 3, coord.dispatches)

	readTS := snapshotTS(coord.Clock(), st)
	meta, _, err := srv.resolveListMeta(ctx, key, readTS)
	require.NoError(t, err)
	require.Equal(t, int64(1), meta.Len, "list has exactly one element")

	val, err := st.GetAt(ctx, listItemKey(key, meta.Head), readTS)
	require.NoError(t, err)
	require.Equal(t, []byte("v"), val, "the pushed element must be reachable at meta.Head")
}

// TestListPushDedup_InterveningNonConflictingLPush_RecomputesLength is the
// codex P2 regression. Attempt 1's RPUSH plans seq Tail and fails without
// landing; an intervening LPUSH commits non-conflictingly (writes at
// Head-1, doesn't touch our boundary read keys at Head / Tail-1). The
// reuse's probe misses (attempt 1 didn't land), the boundary OCC passes
// (LPUSH didn't touch our boundary), the write-key OCC passes (different
// keys), and the apply succeeds at the fresh commit_ts. Pre-fix, the
// adapter returned pending.length (= attempt-1-snapshot, missing LPUSH's
// element). Post-fix, the adapter detects the apply-not-no-op case via
// CommittedVersionAt(probeKey, pending.commitTS) miss, re-reads the
// current meta, and returns the correct post-push length.
func TestListPushDedup_InterveningNonConflictingLPush_RecomputesLength(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	key := []byte("mylist")

	// Seed [A] at seq 0 (Head=0, Len=1, Tail=1).
	require.NoError(t, st.PutAt(ctx, listItemKey(key, 0), []byte("A"), 1, 0))
	seedDelta := store.MarshalListMetaDelta(store.ListMetaDelta{HeadDelta: 0, LenDelta: 1})
	require.NoError(t, st.PutAt(ctx, store.ListMetaDeltaKey(key, 1, 0), seedDelta, 1, 0))

	coord := newDedupTestCoordinator(st, 1, false) // attempt 1 errors without landing
	coord.clock.Observe(1)
	// Between attempts, simulate an LPUSH committing: writes listItemKey(-1)
	// (Head-1) and a delta with HeadDelta=-1, LenDelta=+1. Crucially this
	// does NOT touch the boundary read keys (Head=0, Tail-1=0) or our
	// RPUSH's write keys (listItemKey(1), ListMetaDeltaKey(key, T1, 0)),
	// so neither the read-set OCC nor the write-set OCC catches it.
	coord.beforeDispatch = func(n int) {
		if n != 2 {
			return
		}
		ts := coord.Clock().Next()
		require.NoError(t, st.PutAt(ctx, listItemKey(key, -1), []byte("x"), ts, 0))
		lpushDelta := store.MarshalListMetaDelta(store.ListMetaDelta{HeadDelta: -1, LenDelta: 1})
		require.NoError(t, st.PutAt(ctx, store.ListMetaDeltaKey(key, ts, 0), lpushDelta, ts, 0))
	}
	srv := &RedisServer{store: st, coordinator: coord, scriptCache: map[string]string{}, onePhaseTxnDedup: true}

	n, err := srv.listRPush(ctx, key, [][]byte{[]byte("v")})
	require.NoError(t, err)
	// Three elements after the dust settles: LPUSH's "x" at seq -1, the
	// seed "A" at seq 0, our "v" at seq 1. A stale snapshot return would
	// be 2 (attempt 1's view: 1 seed + 1 push, no LPUSH).
	require.Equal(t, int64(3), n, "return must reflect intervening non-conflicting writes, not the attempt-1 snapshot")
	// Only two dispatches: attempt 1 (pre-rejects), attempt 2 (reuse applies).
	// No recompute needed because the boundary and write-key OCC both pass.
	require.Equal(t, 2, coord.dispatches)
	require.Equal(t, 0, coord.probeNoOps, "prior did not land; the probe must miss")

	readTS := snapshotTS(coord.Clock(), st)
	meta, _, err := srv.resolveListMeta(ctx, key, readTS)
	require.NoError(t, err)
	require.Equal(t, int64(3), meta.Len)
	require.Equal(t, int64(-1), meta.Head)
}

// TestListPushDedup_SelfInflictedReuseConflict_ReturnsSuccess is the codex
// P1 regression for the "self-inflicted conflict" leadership-churn case:
//
//  1. attempt 1 errors without landing (pending stored, pending.commitTS=T1)
//  2. reuse dispatched at fresh T2 with PrevCommitTS=T1; the FSM probes T1,
//     misses (T1 didn't land), and APPLIES at T2 — but leadership churn
//     surfaces the commit as ErrWriteConflict to the adapter.
//
// Pre-fix the adapter treated this as a genuine third-party conflict, dropped
// pending, recomputed on a fresh meta read (which now reflects T2's effect)
// and APPENDED THE SAME ELEMENT A SECOND TIME at a new seq — exactly the
// duplicate-elements anomaly this feature exists to prevent. The fix is to
// probe our just-attempted commit_ts on a reuse WriteConflict; if it landed
// the conflict is against our own commit, return success and keep pending
// pointing at THIS commit_ts.
func TestListPushDedup_SelfInflictedReuseConflict_ReturnsSuccess(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := newDedupTestCoordinator(st, 1, false) // attempt 1 pre-rejects without landing
	coord.landThenWriteConflictAtDispatch = 2      // reuse applies, then returns WriteConflict
	srv := &RedisServer{store: st, coordinator: coord, scriptCache: map[string]string{}, onePhaseTxnDedup: true}

	key := []byte("mylist")
	n, err := srv.listRPush(ctx, key, [][]byte{[]byte("v")})
	require.NoError(t, err)
	require.Equal(t, int64(1), n, "self-inflicted conflict must not duplicate the element")
	require.Equal(t, 2, coord.dispatches, "no recompute should fire; the reuse landed")
	require.Equal(t, 0, coord.probeNoOps, "the FSM probe at T1 misses; the no-op count stays zero")

	readTS := snapshotTS(coord.Clock(), st)
	meta, _, err := srv.resolveListMeta(ctx, key, readTS)
	require.NoError(t, err)
	require.Equal(t, int64(1), meta.Len, "exactly one element after self-inflicted conflict")
	val, err := st.GetAt(ctx, listItemKey(key, meta.Head), readTS)
	require.NoError(t, err)
	require.Equal(t, []byte("v"), val)
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
