package adapter

import (
	"bytes"
	"context"
	"math"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/tidwall/redcon"
)

func listMetaKey(userKey []byte) []byte {
	return store.ListMetaKey(userKey)
}

func listItemKey(userKey []byte, seq int64) []byte {
	return store.ListItemKey(userKey, seq)
}

const listPushFenceAndDeltaElems = 2

func clampRange(start, end, length int) (int, int) {
	if start < 0 {
		start = length + start
	}
	if end < 0 {
		end = length + end
	}
	if start < 0 {
		start = 0
	}
	if end >= length {
		end = length - 1
	}
	if end < start {
		return 0, -1
	}
	return start, end
}

func (r *RedisServer) loadListMetaAt(ctx context.Context, key []byte, readTS uint64) (store.ListMeta, bool, error) {
	val, err := r.store.GetAt(ctx, store.ListMetaKey(key), readTS)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return store.ListMeta{}, false, nil
		}
		return store.ListMeta{}, false, errors.WithStack(err)
	}
	meta, err := store.UnmarshalListMeta(val)
	if err != nil {
		return store.ListMeta{}, false, errors.WithStack(err)
	}
	return meta, true, nil
}

// buildRPushOps creates operations to append values to the tail of a list using
// the Delta pattern. Instead of writing to the base metadata key (causing OCC
// conflicts), it emits a single ListMetaDelta key with LenDelta = len(values).
// commitTS must be pre-allocated via dispatchElemsWithCommitTS; seqInTxn
// disambiguates multiple push operations in the same transaction.
func (r *RedisServer) buildRPushOps(meta store.ListMeta, key []byte, values [][]byte, commitTS uint64, seqInTxn uint32) ([]*kv.Elem[kv.OP], store.ListMeta, error) {
	if len(values) == 0 {
		return nil, meta, nil
	}

	elems := make([]*kv.Elem[kv.OP], 0, len(values)+listPushFenceAndDeltaElems)
	seq := meta.Head + meta.Len
	for _, v := range values {
		vCopy := bytes.Clone(v)
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: listItemKey(key, seq), Value: vCopy})
		seq++
	}

	// Emit a Delta key instead of writing the base meta key.
	delta := store.MarshalListMetaDelta(store.ListMetaDelta{HeadDelta: 0, LenDelta: int64(len(values))})
	elems = append(elems,
		redisTxnWideListFenceElem(key),
		&kv.Elem[kv.OP]{Op: kv.Put, Key: store.ListMetaDeltaKey(key, commitTS, seqInTxn), Value: delta},
	)

	meta.Len += int64(len(values))
	meta.Tail = meta.Head + meta.Len
	return elems, meta, nil
}

// listPushBuildFn is the type for functions that build list push operations.
type listPushBuildFn func(meta store.ListMeta, key []byte, values [][]byte, commitTS uint64, seqInTxn uint32) ([]*kv.Elem[kv.OP], store.ListMeta, error)

// listPushCore is the shared retry loop for RPUSH and LPUSH. The caller supplies
// a buildFn that assembles the specific operations (RPUSH appends to tail, LPUSH
// prepends to head). When onePhaseTxnDedup is enabled it uses the write-set-reuse
// retry path (option 2); otherwise it keeps the original recompute-on-retry loop.
func (r *RedisServer) listPushCore(ctx context.Context, key []byte, values [][]byte, buildFn listPushBuildFn) (int64, error) {
	if r.onePhaseTxnDedup {
		return r.listPushCoreWithDedup(ctx, key, values, buildFn)
	}

	var newLen int64
	err := r.retryRedisWrite(ctx, func() error {
		readTS := r.readTS()
		meta, metaExists, typ, cleanupElems, err := r.listPushSnapshot(ctx, key, readTS)
		if err != nil {
			return err
		}

		// Pre-allocate commitTS so we can embed it in the Delta key.
		startTS := normalizeStartTS(readTS)
		commitTS, err := r.nextCommitTSAfter(ctx, startTS, "listPushCore: allocate commitTS")
		if err != nil {
			return errors.WithStack(err)
		}
		ops, updatedMeta, err := buildFn(meta, key, values, commitTS, 0)
		if err != nil {
			return err
		}
		ops = append(cleanupElems, ops...)
		if len(ops) == 0 {
			newLen = updatedMeta.Len
			return nil
		}

		// Dispatch with the pre-allocated commitTS.
		_, dispErr := r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
			IsTxn:    true,
			StartTS:  startTS,
			CommitTS: commitTS,
			ReadKeys: listPushReadKeys(key, meta, typ, metaExists),
			Elems:    ops,
		})
		if dispErr != nil {
			return errors.WithStack(dispErr)
		}
		newLen = updatedMeta.Len
		return nil
	})
	return newLen, err
}

func (r *RedisServer) listPushSnapshot(ctx context.Context, key []byte, readTS uint64) (store.ListMeta, bool, redisValueType, []*kv.Elem[kv.OP], error) {
	typ, err := r.keyTypeOrEmptyAt(ctx, key, readTS, redisTypeList)
	if err != nil {
		return store.ListMeta{}, false, redisTypeNone, nil, err
	}
	cleanup, expired, err := r.expiredCollectionCleanupForRecreate(ctx, key, readTS, typ, redisTypeList)
	if err != nil {
		return store.ListMeta{}, false, redisTypeNone, nil, err
	}
	if expired {
		return store.ListMeta{}, false, redisTypeNone, cleanup, nil
	}
	meta, exists, err := r.resolveListMeta(ctx, key, readTS)
	if err != nil {
		return store.ListMeta{}, false, redisTypeNone, nil, err
	}
	if len(cleanup) > 0 && exists {
		typ = redisTypeList
	}
	return meta, exists, typ, cleanup, nil
}

// reusableListPush captures a dispatched list-push attempt so a subsequent
// retry can reuse its exact write set (same seq, same item/delta keys) and
// probe whether it already landed, instead of recomputing seq from a fresh
// meta read. Recomputing is what duplicates the element under leadership
// churn: attempt 1 commits at T1 but returns an ambiguous error, the retry
// reads the now-larger list and appends at a NEW seq. Reuse + the FSM's
// exact-ts dedup probe close that. See option 2 in
// docs/design/2026_05_21_proposed_txn_secondary_idempotency.md.
type reusableListPush struct {
	ops     []*kv.Elem[kv.OP]
	startTS uint64
	// commitTS is the most recent dispatched commit_ts for this write set;
	// the next retry passes it as prev_commit_ts so the FSM probes exactly
	// the attempt that might have landed.
	commitTS uint64
	// length is the client-visible post-push length. It is invariant across
	// reuse — the write set was built once from attempt 1's meta — so it is
	// also the correct value to return when the FSM dedup no-ops the apply
	// (R1 result reconstruction: no store re-read needed).
	length int64
	// readKeys is the boundary read set captured at attempt 1's meta read:
	// listItemKey(Head) and (when Len > 1) listItemKey(Tail-1). It is the
	// load-bearing fence against the codex P1 scenario where an intervening
	// pop/trim shrinks the list before the retry — without it, the reused
	// seq would land past the new Tail and be unreachable to LRANGE. OCC
	// validates these atomically against startTS at FSM apply, so any
	// boundary-touching commit fires WriteConflict and the adapter drops
	// pending → recomputes. Empty when attempt 1 read an empty list (no
	// boundary to fence; the OCC on the write key suffices for that case).
	readKeys [][]byte
}

// dispatchListPushReuse runs one iteration of the option-2 reuse path:
// dispatches the captured write set under a fresh commit_ts (carrying
// pending.commitTS as PrevCommitTS so the FSM probes whether the prior
// attempt landed) and returns the post-push length on success. The drop
// return signals the caller to clear pending — set on a genuine
// WriteConflict from another txn so the next iteration recomputes from
// fresh meta. Extracted from listPushCoreWithDedup to keep that closure
// under the cyclop / gocognit / nestif limits.
func (r *RedisServer) dispatchListPushReuse(ctx context.Context, key []byte, pending *reusableListPush) (newLen int64, drop bool, err error) {
	// HLC-4 parity: persistence-grade commit_ts allocation must honor
	// the physical-ceiling fence so a stale-leader window cannot mint a
	// timestamp that collides with the successor's. The error path
	// returns ErrCeilingExpired which isRetryableRedisTxnErr classifies
	// as non-retryable, so it exits retryRedisWrite directly to the
	// client — same shape as the other persistence-grade Next call
	// sites in this file.
	commitTS, allocErr := r.nextCommitTSAfter(ctx, pending.startTS, "redis list-push reuse: allocate commitTS")
	if allocErr != nil {
		return 0, false, errors.WithStack(allocErr)
	}
	_, dispErr := r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		IsTxn:        true,
		StartTS:      pending.startTS,
		CommitTS:     commitTS,
		PrevCommitTS: pending.commitTS,
		ReadKeys:     pending.readKeys,
		Elems:        pending.ops,
	})
	if dispErr == nil {
		return r.resolveReuseLength(ctx, key, pending), false, nil
	}
	// This path owns an exact reusable write set plus PrevCommitTS, so it can
	// safely opt in to retrying an otherwise-ambiguous forwarded conflict.
	// Normalize before the typed conflict branch; generic Redis writes keep
	// raw wire write conflicts fail-closed.
	dispErr = normalizeRetryableRedisTxnErr(dispErr)
	if errors.Is(dispErr, store.ErrWriteConflict) {
		// Self-inflicted-conflict guard (codex P1): the apply might have
		// landed at this fresh commitTS but bubbled up as WriteConflict due
		// to leadership churn (the original bug class the doc's "Resolved"
		// section identifies). Without this probe, dropping pending here
		// would recompute and append a second copy. Ask the store: did
		// our just-attempted commit_ts land? If yes, this conflict is
		// against our own commit — return success and keep pending pointing
		// at THIS commit_ts so any subsequent retry probes the right point.
		//
		// Length resolution (codex P2 round-11): pending.length was computed
		// during the prior attempt and is stale w.r.t. any non-conflicting
		// list-modifying writes that landed between attempt 1 and this fresh
		// apply. Probing pending.commitTS would hit for the fresh apply and
		// (under the old resolveReuseLength shortcut) silently return the
		// prior-attempt length — understating the count. Always re-read meta
		// in the self-conflict path. resolveListMeta failure falls back to
		// pending.length to honor codex P2 round-10 ("avoid failing after a
		// reuse apply").
		if probeKey := firstWriteKey(pending.ops); len(probeKey) > 0 {
			landed, perr := r.store.CommittedVersionAt(ctx, probeKey, commitTS)
			if perr == nil && landed {
				pending.commitTS = commitTS
				return r.resolveLengthAfterFreshApply(ctx, key, pending), false, nil
			}
		}
		// Our attempt did not land at commitTS and the target seq is taken
		// by another txn — a genuine conflict. Drop pending; the next
		// iteration recomputes from a fresh meta read.
		return 0, true, errors.WithStack(dispErr)
	}
	// Still ambiguous (lock / other retryable): this reuse may itself have
	// landed, so the next retry must probe THIS commit_ts. Route-fence
	// rejections are retryable but pre-apply, so keep the older witness.
	if shouldPreserveRedisTxnAttempt(dispErr) {
		pending.commitTS = commitTS
	}
	return 0, false, errors.WithStack(dispErr)
}

// resolveReuseLength returns the client-visible post-push length after a
// successful reuse dispatch. If our prior attempt's exact commit_ts
// version exists, the FSM no-op'd (probe hit) and pending.length is the
// correct length we computed at that attempt. Otherwise the FSM applied
// the reused write set at a fresh commit_ts and we must re-read meta to
// capture any non-conflicting list-modifying writes that committed
// between attempts (codex P2) — without this, the return value would
// silently understate the count when the boundary OCC fence and
// write-key OCC both pass but the list length changed.
//
// Failure modes are converted to a degraded return (pending.length) rather
// than surfaced as an error, because the dispatch already committed. Per
// codex P2 round-10 ("avoid failing after a reuse apply"), reporting a
// write error after the apply landed drives the client into a retry that
// has no pending state and would re-append the element — the very anomaly
// this feature prevents. Specifically:
//   - probe error of any kind: prefer pending.length over failure.
//   - resolveListMeta failure (e.g. delta scan over MaxDeltaScanLimit
//     under churn): fall back to pending.length.
//
// Returns int64 directly (no error) so callers do not have to invent
// caller-side fallback logic; the degraded-return contract is fixed here
// (golangci unparam / nilerr fix on the prior error-returning shape).
func (r *RedisServer) resolveReuseLength(ctx context.Context, key []byte, pending *reusableListPush) int64 {
	if probeKey := firstWriteKey(pending.ops); len(probeKey) > 0 {
		hit, perr := r.store.CommittedVersionAt(ctx, probeKey, pending.commitTS)
		if perr == nil && hit {
			return pending.length
		}
		if perr != nil {
			// Probe failed; the dispatch already committed so degrade
			// gracefully rather than propagate the read error.
			return pending.length
		}
		// perr == nil && !hit: prior attempt didn't land at this ts; the
		// FSM applied fresh writes, fall through to re-read meta.
	}
	return r.resolveLengthAfterFreshApply(ctx, key, pending)
}

// resolveLengthAfterFreshApply re-reads list meta to capture the post-apply
// length when we know the fresh commitTS applied (no probe shortcut), with
// the same fall-back-to-pending.length contract as resolveReuseLength. Used
// by the self-conflict path (codex P2 round-11): there pending.length is
// stale w.r.t. intervening non-conflicting writes, so the probe-hit
// shortcut would silently understate the count.
func (r *RedisServer) resolveLengthAfterFreshApply(ctx context.Context, key []byte, pending *reusableListPush) int64 {
	currentMeta, _, mErr := r.resolveListMeta(ctx, key, r.readTS())
	if mErr != nil {
		return pending.length
	}
	return currentMeta.Len
}

// firstWriteKey returns the first non-empty Put element key from ops, or nil
// when there is none. Used after a successful reuse dispatch to probe
// whether our prior attempt's commit_ts actually landed: attempt 1 writes
// all its elem keys atomically at the same commit_ts, so any one of them
// answers the question.
func firstWriteKey(ops []*kv.Elem[kv.OP]) []byte {
	for _, e := range ops {
		if e != nil && e.Op == kv.Put && len(e.Key) > 0 {
			return e.Key
		}
	}
	return nil
}

// listPushBoundaryReadKeys returns the collection fence plus the boundary
// positions of the list as read keys for OCC. Including these in the
// dispatched OperationGroup makes
// FSM apply atomically reject the retry when any pop/trim has touched the
// boundary between attempts (codex P1 fix: prevents a reused seq from
// landing past a shrunk Tail). The wide fence also catches SET/DEL
// replacements that commit before this append. The keys are deduped: a
// single-element list has Head == Tail-1, so we emit it once.
func listPushBoundaryReadKeys(key []byte, meta store.ListMeta) [][]byte {
	fence := redisTxnWideListFenceKey(key)
	if meta.Len <= 0 {
		return [][]byte{fence}
	}
	tailIdx := meta.Tail - 1
	if tailIdx == meta.Head {
		return [][]byte{fence, listItemKey(key, meta.Head)}
	}
	return [][]byte{
		fence,
		listItemKey(key, meta.Head),
		listItemKey(key, tailIdx),
	}
}

func listPushReadKeys(key []byte, meta store.ListMeta, typ redisValueType, metaExists bool) [][]byte {
	if typ == redisTypeNone || !metaExists {
		return redisTxnWideCollectionFenceKeys(key)
	}
	return listPushBoundaryReadKeys(key, meta)
}

// listPushCoreWithDedup is the option-2 retry loop. The first attempt computes
// the write set from the current meta; any retryable failure makes the next
// iteration REUSE that write set under a fresh commit_ts with prev_commit_ts
// set, so the FSM no-ops if the prior attempt already landed. A WriteConflict
// on a reuse attempt means the probe ruled out our own prior attempt and the
// seq is genuinely taken by another txn, so we fall back to a full recompute.
func (r *RedisServer) listPushCoreWithDedup(ctx context.Context, key []byte, values [][]byte, buildFn listPushBuildFn) (int64, error) {
	var newLen int64
	var pending *reusableListPush
	err := r.retryRedisWrite(ctx, func() error {
		if pending != nil {
			length, drop, dispErr := r.dispatchListPushReuse(ctx, key, pending)
			if drop {
				pending = nil
			}
			if dispErr != nil {
				return dispErr
			}
			newLen = length
			return nil
		}

		readTS := r.readTS()
		meta, metaExists, typ, cleanupElems, err := r.listPushSnapshot(ctx, key, readTS)
		if err != nil {
			return err
		}

		// HLC-4 parity with prepareDispatch / dispatchExecReuse —
		// see dispatchListPushReuse above for the rationale.
		startTS := normalizeStartTS(readTS)
		commitTS, allocErr := r.nextCommitTSAfter(ctx, startTS, "redis list-push first-attempt: allocate commitTS")
		if allocErr != nil {
			return errors.WithStack(allocErr)
		}
		ops, updatedMeta, err := buildFn(meta, key, values, commitTS, 0)
		if err != nil {
			return err
		}
		ops = append(cleanupElems, ops...)
		if len(ops) == 0 {
			newLen = updatedMeta.Len
			return nil
		}

		boundaryReads := listPushReadKeys(key, meta, typ, metaExists)
		_, dispErr := r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
			IsTxn:    true,
			StartTS:  startTS,
			CommitTS: commitTS,
			ReadKeys: boundaryReads,
			Elems:    ops,
		})
		if dispErr == nil {
			newLen = updatedMeta.Len
			return nil
		}
		// Preserve the exact attempt for a forwarded conflict only after
		// restoring its typed form. The next iteration can then reuse these
		// operations instead of recomputing a second list append.
		dispErr = normalizeRetryableRedisTxnErr(dispErr)
		// Only remember the attempt for reuse if retryRedisWrite will actually
		// loop and the attempt may have landed. Route-fence rejections are
		// retryable but happen before this write set can apply, so preserving
		// that commitTS would overwrite an older ambiguous witness. For
		// errors that escape the loop (transient-leader, context deadline,
		// FSM apply error, etc.), `pending` would be discarded with the
		// goroutine, and recording it would mislead a future reader about
		// what state was preserved. The dedup window is therefore bounded by
		// retryRedisWrite's retry predicate; ambiguous errors that escape
		// to the client are a separate problem space (cross-request
		// idempotency cache) and out of scope for this design.
		if shouldPreserveRedisTxnAttempt(dispErr) {
			pending = &reusableListPush{
				ops:      ops,
				startTS:  startTS,
				commitTS: commitTS,
				length:   updatedMeta.Len,
				readKeys: boundaryReads,
			}
		}
		return errors.WithStack(dispErr)
	})
	return newLen, err
}

func (r *RedisServer) listRPush(ctx context.Context, key []byte, values [][]byte) (int64, error) {
	return r.listPushCore(ctx, key, values, r.buildRPushOps)
}

// buildLPushOps creates operations to prepend values to the head of a list using
// the Delta pattern. LPUSH reverses the order of arguments:
// LPUSH key a b c → [c, b, a, ...existing].
func (r *RedisServer) buildLPushOps(meta store.ListMeta, key []byte, values [][]byte, commitTS uint64, seqInTxn uint32) ([]*kv.Elem[kv.OP], store.ListMeta, error) {
	if len(values) == 0 {
		return nil, meta, nil
	}

	n := int64(len(values))
	if meta.Head < math.MinInt64+n {
		return nil, meta, errors.WithStack(errors.New("LPUSH would underflow list Head sequence number"))
	}
	elems := make([]*kv.Elem[kv.OP], 0, len(values)+listPushFenceAndDeltaElems)
	// LPUSH reverses args, so last arg gets the lowest sequence number.
	newHead := meta.Head - n
	for i, v := range values {
		// values[0]=a, values[1]=b, values[2]=c → seq ordering: c(newHead), b(newHead+1), a(newHead+2)
		seq := newHead + n - 1 - int64(i)
		vCopy := bytes.Clone(v)
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: listItemKey(key, seq), Value: vCopy})
	}

	// Emit a Delta key instead of writing the base meta key.
	delta := store.MarshalListMetaDelta(store.ListMetaDelta{HeadDelta: -n, LenDelta: n})
	elems = append(elems,
		redisTxnWideListFenceElem(key),
		&kv.Elem[kv.OP]{Op: kv.Put, Key: store.ListMetaDeltaKey(key, commitTS, seqInTxn), Value: delta},
	)

	meta.Head = newHead
	meta.Len += n
	return elems, meta, nil
}

func (r *RedisServer) listLPush(ctx context.Context, key []byte, values [][]byte) (int64, error) {
	return r.listPushCore(ctx, key, values, r.buildLPushOps)
}

// clampPopCount clamps count to [1, min(listLen, maxWideColumnItems)].
// An error is returned when the effective count would exceed maxWideColumnItems,
// which guards against OOM from enormous claim-key allocations.
func clampPopCount(count int, listLen int64) (int64, error) {
	n := int64(count)
	if n > listLen {
		n = listLen
	}
	if n > int64(maxWideColumnItems) {
		return 0, errors.Wrapf(ErrCollectionTooLarge, "LPOP/RPOP count %d exceeds maximum %d", n, maxWideColumnItems)
	}
	return n, nil
}

// listPopClaim implements LPOP (left=true) or RPOP (left=false) using the
// Claim pattern to avoid write-write conflicts on the list metadata key.
// For each item popped it emits:
//   - Del(listItemKey) — removes the item value
//   - Put(listClaimKey, empty) — uniqueness guard; conflicts if another txn
//     claims the same sequence number concurrently
//
// A single ListMetaDelta with {HeadDelta, LenDelta} is emitted for the whole batch.
//
// Returns the popped values (len ≤ count) or nil if the list does not exist.
func (r *RedisServer) buildListPopElems(ctx context.Context, key []byte, meta store.ListMeta, n int64, left bool, readTS uint64) ([]string, []*kv.Elem[kv.OP], error) {
	// Build the [start, end) scan range covering exactly the n items to pop.
	// n is already clamped to meta.Len by the caller, so no overflow is possible.
	var startKey, endKey []byte
	if left {
		startKey = listItemKey(key, meta.Head)
		endKey = listItemKey(key, meta.Head+n)
	} else {
		startKey = listItemKey(key, meta.Tail-n)
		endKey = listItemKey(key, meta.Tail)
	}

	var kvps []*store.KVPair
	var scanErr error
	if left {
		kvps, scanErr = r.store.ScanAt(ctx, startKey, endKey, int(n), readTS)
	} else {
		kvps, scanErr = r.store.ReverseScanAt(ctx, startKey, endKey, int(n), readTS)
	}
	if scanErr != nil {
		return nil, nil, errors.WithStack(scanErr)
	}

	// Emit claim keys for every sequence position in the claimed range, including
	// holes. This ensures that two concurrent pops over the same hole produce a
	// write conflict rather than both silently advancing HeadDelta over the same
	// empty position, which would otherwise orphan later items.
	var claimStart, claimEnd int64
	if left {
		claimStart = meta.Head
		claimEnd = meta.Head + n
	} else {
		claimStart = meta.Tail - n
		claimEnd = meta.Tail
	}
	// Capacity: n claim keys + n Del(item) for found items + 1 for the delta key appended by caller.
	// n is bounded by maxWideColumnItems (100_000) so the int conversion is safe.
	elems := make([]*kv.Elem[kv.OP], 0, int(n)+len(kvps)+listPopDeltaOverhead)
	for seq := claimStart; seq < claimEnd; seq++ {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: store.ListClaimKey(key, seq), Value: []byte{}})
	}

	values := make([]string, 0, len(kvps))
	for _, pair := range kvps {
		_, ok := store.ExtractListItemSeq(pair.Key, key)
		if !ok {
			continue
		}
		values = append(values, string(pair.Value))
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: bytes.Clone(pair.Key)})
	}
	return values, elems, nil
}

// checkListKeyType verifies the key is a list. Returns (keyFound, error).
// Writes wrongTypeError if the key exists but is not a list.
func (r *RedisServer) checkListKeyType(ctx context.Context, key []byte, readTS uint64) (found bool, err error) {
	typ, typErr := r.keyTypeAt(ctx, key, readTS)
	if typErr != nil {
		return false, typErr
	}
	if typ == redisTypeNone {
		return false, nil
	}
	if typ != redisTypeList {
		return false, wrongTypeError()
	}
	return true, nil
}

// listPopClaimOnce executes one attempt of a pop-with-claim transaction.
// Returns (nil, nil) for a missing key or an empty list, and the popped
// values otherwise.
func (r *RedisServer) listPopClaimOnce(ctx context.Context, key []byte, count int, left bool, readTS uint64) ([]string, error) {
	found, typeErr := r.checkListKeyType(ctx, key, readTS)
	if typeErr != nil || !found {
		return nil, typeErr
	}

	meta, exists, metaErr := r.resolveListMeta(ctx, key, readTS)
	if metaErr != nil {
		return nil, metaErr
	}
	if !exists || meta.Len == 0 {
		// count >= 1 on an empty list: Redis returns nil (same as missing key).
		return nil, nil
	}

	n, err := clampPopCount(count, meta.Len)
	if err != nil {
		return nil, err
	}

	values, elems, buildErr := r.buildListPopElems(ctx, key, meta, n, left, readTS)
	if buildErr != nil {
		return nil, buildErr
	}

	if err := r.commitListPop(ctx, key, elems, n, left, readTS); err != nil {
		return nil, err
	}
	return values, nil
}

// commitListPop allocates commitTS, appends the ListMetaDelta entry,
// and dispatches the pop transaction. Extracted from listPopClaimOnce
// so that function stays under the cyclop ceiling after the HLC-4
// (iii) NextFenced fence added a new error branch (PR #867 Phase 2b).
func (r *RedisServer) commitListPop(ctx context.Context, key []byte, elems []*kv.Elem[kv.OP], n int64, left bool, readTS uint64) error {
	// n is the number of sequence positions claimed (including any holes).
	// HeadDelta and LenDelta must use n, not len(values), so that Head
	// advances past holes and the metadata stays consistent with Tail.
	startTS := normalizeStartTS(readTS)
	commitTS, err := r.nextCommitTSAfter(ctx, startTS, "commitListPop: allocate commitTS")
	if err != nil {
		return errors.WithStack(err)
	}
	var headDelta int64
	if left {
		headDelta = n // head advances by n positions for LPOP
	}
	delta := store.MarshalListMetaDelta(store.ListMetaDelta{HeadDelta: headDelta, LenDelta: -n})
	elems = append(elems,
		redisTxnWideListFenceElem(key),
		&kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.ListMetaDeltaKey(key, commitTS, 0),
			Value: delta,
		},
	)

	if _, dispErr := r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  startTS,
		CommitTS: commitTS,
		Elems:    elems,
	}); dispErr != nil {
		return errors.WithStack(dispErr)
	}
	return nil
}

func (r *RedisServer) listPopClaim(ctx context.Context, key []byte, count int, left bool) ([]string, error) {
	// count=0: Redis returns an empty array if the key exists as a list, nil otherwise.
	if count <= 0 {
		readTS := r.readTS()
		found, err := r.checkListKeyType(ctx, key, readTS)
		if err != nil || !found {
			return nil, err
		}
		return []string{}, nil
	}

	var popped []string
	err := r.retryRedisWrite(ctx, func() error {
		result, popErr := r.listPopClaimOnce(ctx, key, count, left, r.readTS())
		if popErr != nil {
			return popErr
		}
		popped = result
		return nil
	})
	return popped, err
}

func (r *RedisServer) fetchListRange(ctx context.Context, key []byte, meta store.ListMeta, startIdx, endIdx int64, readTS uint64) ([]string, error) {
	if endIdx < startIdx {
		return []string{}, nil
	}

	startSeq := meta.Head + startIdx
	endSeq := meta.Head + endIdx

	startKey := listItemKey(key, startSeq)
	endKey := listItemKey(key, endSeq+1) // exclusive

	kvs, err := r.store.ScanAt(ctx, startKey, endKey, int(endIdx-startIdx+1), readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	out := make([]string, 0, len(kvs))
	for _, kvp := range kvs {
		out = append(out, string(kvp.Value))
	}
	return out, nil
}

func (r *RedisServer) rangeList(ctx context.Context, key []byte, startRaw, endRaw []byte) ([]string, error) {
	if !r.coordinator.IsLeaderForKey(key) {
		return r.proxyLRange(key, startRaw, endRaw)
	}

	readTS := r.readTS()
	typ, err := r.keyTypeAt(ctx, key, readTS)
	if err != nil {
		return nil, err
	}
	if typ == redisTypeNone {
		return []string{}, nil
	}
	if typ != redisTypeList {
		return nil, wrongTypeError()
	}

	// PR #749 follow-up: pass the per-call dispatch ctx so a stalled
	// VerifyLeaderForKey honours the caller's deadline rather than the
	// long-lived handlerContext + verifyLeaderEngineCtx fallback. Same
	// shape as keys() / FLUSHDB.
	if err := r.coordinator.VerifyLeaderForKey(ctx, key); err != nil {
		return nil, errors.WithStack(err)
	}

	meta, exists, err := r.resolveListMeta(ctx, key, readTS)
	if err != nil {
		return nil, err
	}
	if !exists || meta.Len == 0 {
		return []string{}, nil
	}

	s, e, err := parseRangeBounds(startRaw, endRaw, int(meta.Len))
	if err != nil {
		return nil, err
	}

	return r.fetchListRange(ctx, key, meta, int64(s), int64(e), readTS)
}

type listPushFunc func(ctx context.Context, key []byte, values [][]byte) (int64, error)
type listProxyFunc func(key []byte, values [][]byte) (int64, error)

func (r *RedisServer) listPushCmd(conn redcon.Conn, cmd redcon.Command, pushFn listPushFunc, proxyFn listProxyFunc) {
	key := cmd.Args[1]
	if !r.coordinator.IsLeaderForKey(key) {
		length, err := proxyFn(key, cmd.Args[2:])
		if err != nil {
			writeRedisError(conn, err)
			return
		}
		conn.WriteInt64(length)
		return
	}

	ctx, cancel := context.WithTimeout(r.handlerContext(), redisDispatchTimeout)
	defer cancel()
	readTS := r.readTS()
	typ, err := r.keyTypeAt(ctx, key, readTS)
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	if typ != redisTypeNone && typ != redisTypeList {
		conn.WriteError(wrongTypeMessage)
		return
	}

	length, err := pushFn(ctx, key, cmd.Args[2:])

	if err != nil {
		writeRedisError(conn, err)
		return
	}
	conn.WriteInt64(length)
}

func (r *RedisServer) rpush(conn redcon.Conn, cmd redcon.Command) {
	r.listPushCmd(conn, cmd, r.listRPush, r.proxyRPush)
}

func (r *RedisServer) lrange(conn redcon.Conn, cmd redcon.Command) {
	ctx, cancel := context.WithTimeout(r.handlerContext(), redisDispatchTimeout)
	defer cancel()
	items, err := r.rangeList(ctx, cmd.Args[1], cmd.Args[2], cmd.Args[3])
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	conn.WriteArray(len(items))
	for _, it := range items {
		conn.WriteBulkString(it)
	}
}

func (r *RedisServer) lpush(conn redcon.Conn, cmd redcon.Command) {
	r.listPushCmd(conn, cmd, r.listLPush, r.proxyLPush)
}

func (r *RedisServer) ltrim(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	start, err := parseInt(cmd.Args[2])
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	stop, err := parseInt(cmd.Args[3])
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	if err := r.retryRedisWrite(ctx, func() error {
		readTS := r.readTS()
		typ, err := r.keyTypeAt(ctx, cmd.Args[1], readTS)
		if err != nil {
			return err
		}
		if typ == redisTypeNone {
			return nil
		}
		if typ != redisTypeList {
			return wrongTypeError()
		}
		current, err := r.listValuesAt(ctx, cmd.Args[1], readTS)
		if err != nil {
			return err
		}
		s, e := normalizeRankRange(start, stop, len(current))
		trimmed := []string{}
		if e >= s {
			trimmed = append(trimmed, current[s:e+1]...)
		}
		return r.rewriteListTxn(ctx, cmd.Args[1], readTS, trimmed)
	}); err != nil {
		writeRedisError(conn, err)
		return
	}
	conn.WriteString("OK")
}

func (r *RedisServer) lindex(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	index, err := parseInt(cmd.Args[2])
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.handlerContext(), redisDispatchTimeout)
	defer cancel()
	readTS := r.readTS()
	typ, err := r.keyTypeAt(ctx, cmd.Args[1], readTS)
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	if typ == redisTypeNone {
		conn.WriteNull()
		return
	}
	if typ != redisTypeList {
		conn.WriteError(wrongTypeMessage)
		return
	}
	values, err := r.listValuesAt(ctx, cmd.Args[1], readTS)
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	idx := normalizeIndex(index, len(values))
	if idx < 0 {
		conn.WriteNull()
		return
	}
	conn.WriteBulkString(values[idx])
}
