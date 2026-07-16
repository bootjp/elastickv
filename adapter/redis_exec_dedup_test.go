package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

// TestExecDedup_LandedPriorAttempt_ReturnsCachedResults is the option-2
// headline for MULTI/EXEC (M3 R1): attempt 1 commits the transaction body
// but bubbles up an ambiguous error, the retry reuses the same write set
// with prev_commit_ts, the FSM probe finds the landed version and no-ops,
// and the client gets the same results array attempt 1 computed.
//
// Without the probe, the reuse would OCC-conflict against attempt 1's own
// version, the adapter would drop pending and recompute from a fresh
// snapshot — which for SET is harmless (idempotent overwrite) but for
// INCR/RPUSH would produce a different (and wrong, since attempt 1 already
// landed) result. This test pins that the reuse path returns attempt 1's
// cached results without re-executing the command body.
func TestExecDedup_LandedPriorAttempt_ReturnsCachedResults(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := newDedupTestCoordinator(st, 1, true) // attempt 1 lands then errors
	srv := &RedisServer{store: st, coordinator: coord, scriptCache: map[string]string{}, onePhaseTxnDedup: true}

	// Single-mop EXEC: one SET command.
	queue := []redcon.Command{
		{Args: [][]byte{[]byte(cmdSet), []byte("k"), []byte("v1")}},
	}
	results, err := srv.runTransaction(queue)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "OK", results[0].str, "SET must return the cached OK from attempt 1, not be re-executed")
	require.Equal(t, 2, coord.dispatches, "one failed (ambiguous-land) attempt + one reuse")
	require.Equal(t, 1, coord.probeNoOps, "the reuse must dedup via the exact-ts probe")

	// And the value is exactly the one attempt 1 wrote.
	rawVal, err := st.GetAt(ctx, redisStrKey([]byte("k")), snapshotTS(coord.Clock(), st))
	require.NoError(t, err)
	val, _, err := decodeRedisStr(rawVal)
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), val)
}

func TestExecDedup_RouteFenceRetryPreservesPriorProbe(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := newDedupTestCoordinator(st, 1, true)
	coord.routeFenceAtDispatch = 2
	srv := &RedisServer{store: st, coordinator: coord, scriptCache: map[string]string{}, onePhaseTxnDedup: true}

	queue := []redcon.Command{
		{Args: [][]byte{[]byte(cmdSet), []byte("k"), []byte("v1")}},
	}
	results, err := srv.runTransaction(queue)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "OK", results[0].str)
	require.Equal(t, 3, coord.dispatches)
	require.Equal(t, 1, coord.probeNoOps, "route-fenced reuse must not replace the prior landed probe")

	rawVal, err := st.GetAt(ctx, redisStrKey([]byte("k")), snapshotTS(coord.Clock(), st))
	require.NoError(t, err)
	val, _, err := decodeRedisStr(rawVal)
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), val)
}

// TestExecDedup_PriorAttemptDidNotLand_Applies covers the truncated case for
// MULTI/EXEC: attempt 1 errored without committing (OCC-style pre-reject),
// so the probe misses and the reuse applies the same write set at a fresh
// commit_ts. The cached results are still returned (they describe the
// EXEC body's intent against attempt 1's snapshot, which is what the client
// sees regardless of which physical commit_ts the bytes hit MVCC at).
func TestExecDedup_PriorAttemptDidNotLand_Applies(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := newDedupTestCoordinator(st, 1, false) // attempt 1 errors without landing
	srv := &RedisServer{store: st, coordinator: coord, scriptCache: map[string]string{}, onePhaseTxnDedup: true}

	queue := []redcon.Command{
		{Args: [][]byte{[]byte(cmdSet), []byte("k"), []byte("v1")}},
	}
	results, err := srv.runTransaction(queue)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "OK", results[0].str)
	require.Equal(t, 2, coord.dispatches)
	require.Equal(t, 0, coord.probeNoOps, "nothing landed, so the probe must miss and the reuse applies")

	rawVal, err := st.GetAt(ctx, redisStrKey([]byte("k")), snapshotTS(coord.Clock(), st))
	require.NoError(t, err)
	val, _, err := decodeRedisStr(rawVal)
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), val)
}

// TestExecDedup_GenuineConflictRebuildsAndApplies covers outcome 3 for
// MULTI/EXEC: attempt 1 did not land; a concurrent client wrote the same
// key between attempts; the reuse OCC-conflicts (the foreign write
// advances the key's commit_ts past pending.startTS), the self-conflict
// probe rules out our own landing, the adapter drops pending and rebuilds
// the txn from a fresh snapshot — the new attempt then succeeds.
//
// This test pins the discriminator: probe-miss + OCC-conflict ⇒ recompute.
// If the adapter incorrectly reused on a foreign conflict, the cached
// results from attempt 1 would be returned alongside a value written by
// the concurrent client (an inconsistent view).
func TestExecDedup_GenuineConflictRebuildsAndApplies(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := newDedupTestCoordinator(st, 1, false) // attempt 1 errors without landing
	key := []byte("k")

	// Before dispatch 2 (the reuse), inject a concurrent SET so the reuse
	// OCC-conflicts on the write key.
	coord.beforeDispatch = func(n int) {
		if n != 2 {
			return
		}
		ts := coord.Clock().Next()
		require.NoError(t, st.PutAt(ctx, redisStrKey(key), encodeRedisStr([]byte("other"), nil), ts, 0))
	}

	srv := &RedisServer{store: st, coordinator: coord, scriptCache: map[string]string{}, onePhaseTxnDedup: true}
	queue := []redcon.Command{
		{Args: [][]byte{[]byte(cmdSet), key, []byte("v1")}},
	}
	results, err := srv.runTransaction(queue)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "OK", results[0].str)
	// Exactly three dispatches: attempt 1 (pre-reject, no land), reuse
	// (OCC-conflict on the foreign-committed key), fresh-snapshot retry
	// (success). The single injected pre-reject + single concurrent SET
	// fully determine the retry topology — Equal pins it so any future
	// regression that adds an extra dispatch is caught immediately
	// (coderabbitai PR #887 minor).
	require.Equal(t, 3, coord.dispatches)
	require.Equal(t, 0, coord.probeNoOps, "nothing landed at attempt 1's ts; probe must not fire as a hit")

	// Our final write wins (it commits AFTER the concurrent SET because we
	// rebuilt at a fresh startTS that observed the foreign commit).
	rawVal, err := st.GetAt(ctx, redisStrKey(key), snapshotTS(coord.Clock(), st))
	require.NoError(t, err)
	val, _, err := decodeRedisStr(rawVal)
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), val)
}

// TestExecDedup_SelfInflictedReuseConflict_ReturnsSuccess mirrors the
// listPush self-inflicted-conflict regression: the reuse dispatch APPLIES
// the elems at the fresh commitTS but bubbles up store.ErrWriteConflict
// (leadership churn surfacing a committed entry as a conflict). The
// adapter probes the just-attempted commit_ts; the probe hits; cached
// results are returned. Without the guard, the adapter would drop pending
// and recompute, double-applying the EXEC body.
func TestExecDedup_SelfInflictedReuseConflict_ReturnsSuccess(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := newDedupTestCoordinator(st, 1, false) // attempt 1 pre-rejects (didn't land)
	coord.landThenWriteConflictAtDispatch = 2      // reuse lands then surfaces WriteConflict
	srv := &RedisServer{store: st, coordinator: coord, scriptCache: map[string]string{}, onePhaseTxnDedup: true}

	queue := []redcon.Command{
		{Args: [][]byte{[]byte(cmdSet), []byte("k"), []byte("v1")}},
	}
	results, err := srv.runTransaction(queue)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "OK", results[0].str)
	require.Equal(t, 2, coord.dispatches, "attempt 1 pre-reject + reuse land-then-conflict; no third attempt")
	require.Equal(t, 0, coord.probeNoOps,
		"the FSM probe at attempt 1's ts must NOT hit (attempt 1 did not land); "+
			"the success comes from the adapter's self-conflict guard probing the fresh commitTS")

	rawVal, err := st.GetAt(ctx, redisStrKey([]byte("k")), snapshotTS(coord.Clock(), st))
	require.NoError(t, err)
	val, _, err := decodeRedisStr(rawVal)
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), val)
}

// TestExecDedup_MultiMopLandedPriorAttempt_ReturnsCachedResults extends the
// single-mop dedup coverage to a 3-command MULTI/EXEC body: SET + SET + DEL
// on three different keys. The PR #884 design doc scopes the first PR to
// "single-mop EXEC" out of conservatism — the mechanism (cache results
// array; OCC fence on readKeys) works identically for multi-mop because:
//
//   - The cached results array captures one entry per command, fixed at
//     attempt 1's startTS snapshot. Reuse returns the array as-is; the FSM
//     no-op / apply-fresh decision is invisible to per-command return
//     values (SET="OK", DEL=count).
//   - readKeys aggregates reads from every command in the body via
//     txn.readKeys; OCC at the FSM apply checks the union, so a stale read
//     anywhere in the body triggers WriteConflict → drop pending →
//     recompute. This is the same fence single-mop relies on, just over a
//     larger key set.
//
// This test pins the multi-mop guarantee: a 3-mop EXEC where attempt 1
// lands then errors must reuse the cached results array (returning OK, OK,
// 1) and MUST NOT re-execute any of the three commands. Without dedup the
// SET overwrites would be harmless (idempotent) but DEL would re-execute
// and return 0 on the second pass (key already deleted) — observably
// wrong. This regression-test closes the design doc's "multi-mop EXEC …
// validation is a follow-up" open item.
func TestExecDedup_MultiMopLandedPriorAttempt_ReturnsCachedResults(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()

	// Seed key c so the DEL on it returns 1 (matching attempt 1's view).
	require.NoError(t, st.PutAt(ctx, redisStrKey([]byte("c")), encodeRedisStr([]byte("seed"), nil), 5, 0))

	coord := newDedupTestCoordinator(st, 1, true) // attempt 1 lands then errors
	srv := &RedisServer{store: st, coordinator: coord, scriptCache: map[string]string{}, onePhaseTxnDedup: true}

	queue := []redcon.Command{
		{Args: [][]byte{[]byte(cmdSet), []byte("a"), []byte("va")}},
		{Args: [][]byte{[]byte(cmdSet), []byte("b"), []byte("vb")}},
		{Args: [][]byte{[]byte(cmdDel), []byte("c")}},
	}
	results, err := srv.runTransaction(queue)
	require.NoError(t, err)
	require.Len(t, results, 3, "all three command results must be returned, not re-executed")
	require.Equal(t, "OK", results[0].str, "SET a — cached OK from attempt 1")
	require.Equal(t, "OK", results[1].str, "SET b — cached OK from attempt 1")
	require.Equal(t, int64(1), results[2].integer,
		"DEL c — must be the cached count from attempt 1, NOT a re-executed 0")

	require.Equal(t, 2, coord.dispatches, "one ambiguous-land attempt + one reuse")
	require.Equal(t, 1, coord.probeNoOps, "reuse dedup via the exact-ts probe across the full multi-mop write set")

	// Stored state matches: a, b set; c deleted.
	rawA, err := st.GetAt(ctx, redisStrKey([]byte("a")), snapshotTS(coord.Clock(), st))
	require.NoError(t, err)
	valA, _, err := decodeRedisStr(rawA)
	require.NoError(t, err)
	require.Equal(t, []byte("va"), valA)

	rawB, err := st.GetAt(ctx, redisStrKey([]byte("b")), snapshotTS(coord.Clock(), st))
	require.NoError(t, err)
	valB, _, err := decodeRedisStr(rawB)
	require.NoError(t, err)
	require.Equal(t, []byte("vb"), valB)

	_, getCerr := st.GetAt(ctx, redisStrKey([]byte("c")), snapshotTS(coord.Clock(), st))
	require.ErrorIs(t, getCerr, store.ErrKeyNotFound, "c must be deleted by the single landed apply, not double-applied")
}

// TestExecDedup_DisabledKeepsLegacyPath verifies the dedup gate is honored:
// when onePhaseTxnDedup is off, runTransaction takes the legacy path
// (recompute on every retry, no prev_commit_ts) — byte-identical to today.
// Pins that the new code is strictly opt-in.
func TestExecDedup_DisabledKeepsLegacyPath(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := newDedupTestCoordinator(st, 1, false) // attempt 1 errors without landing
	srv := &RedisServer{store: st, coordinator: coord, scriptCache: map[string]string{} /* gate left false */}

	queue := []redcon.Command{
		{Args: [][]byte{[]byte(cmdSet), []byte("k"), []byte("v1")}},
	}
	results, err := srv.runTransaction(queue)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "OK", results[0].str)
	// Legacy path runs no probe.
	require.Equal(t, 0, coord.probeNoOps)

	rawVal, err := st.GetAt(ctx, redisStrKey([]byte("k")), snapshotTS(coord.Clock(), st))
	require.NoError(t, err)
	val, _, err := decodeRedisStr(rawVal)
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), val)
}
