package adapter

import (
	"bytes"
	"context"
	"slices"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

const delRPushReproListKey = "g7-list"

// TestDel_ListEmptyingDoesNotFenceConcurrentPush checks that a standalone DEL
// of a list and a concurrent RPUSH to it serialize.
//
// Variant: interleaving test over the read-set reproduction harness in
// redis_lua_read_skew_repro_test.go (hold at a read, not at Propose; see the
// harness comment for why).
//
// The list starts as [v1]. Client A runs DEL; it is held right after its scan
// of the list's item keys returned (it saw only v1). Client B then runs
// RPUSH v2, which commits: it writes item seq 1, a length delta and the list
// fence key. A is released, allocates its commit timestamp (above B's) and
// commits Del of the item, base meta and delta keys it scanned. A neither
// reads nor writes the list fence, and none of the keys it deletes were
// written by B, so nothing rejects it. B's item and delta survive A's delete:
// the list's metadata says length 1 while seq 0 is gone -- a list with a hole.
//
// Serial outcomes: RPUSH then DEL leaves no list; DEL then RPUSH leaves [v2]
// with RPUSH replying 1. B replies 2 here (it saw v1), so the only acceptable
// final state with that reply is an empty list, and LLEN must agree with
// LRANGE either way.
func TestDel_ListEmptyingDoesNotFenceConcurrentPush(t *testing.T) {
	t.Parallel()

	h := newReadSetReproHarness(t, "g7")
	ctx := h.ctx
	listKey := []byte(delRPushReproListKey)

	seeded, err := h.clientB.RPush(ctx, delRPushReproListKey, "v1").Result()
	require.NoError(t, err)
	require.EqualValues(t, 1, seeded)
	h.log.record("B  RPUSH %s v1 -> %d (seed)", delRPushReproListKey, seeded)

	itemPrefix := append([]byte(store.ListItemPrefix), listKey...)
	park := h.reads.addPark("A: DEL's list-item scan returned", func(op string, key []byte) bool {
		return op == readSetReproOpScanAt && bytes.Equal(key, itemPrefix)
	})

	// (1) A runs DEL and is held after it scanned the list items.
	delDone := h.goCommand(func() (any, error) {
		return h.clientA.Del(ctx, delRPushReproListKey).Result()
	})
	waitReadSetReproParked(t, park, delDone, "A (DEL)")

	// (2) B appends v2; it commits while A is held.
	pushed, pushErr := h.clientB.RPush(ctx, delRPushReproListKey, "v2").Result()
	h.log.record("B  RPUSH %s v2 -> %s", delRPushReproListKey, readSetReproReply{val: pushed, err: pushErr})

	// (3) Release A.
	park.releaseNow()
	delRes := waitReadSetReproDone(t, delDone, "A (DEL)")
	h.log.record("A  DEL %s -> %s", delRPushReproListKey, delRes)

	llen, llenErr := h.clientB.LLen(ctx, delRPushReproListKey).Result()
	lrange, lrangeErr := h.clientB.LRange(ctx, delRPushReproListKey, 0, -1).Result()
	finalTS := h.st.LastCommitTS()
	meta, metaExists, metaErr := h.server.resolveListMeta(ctx, listKey, finalTS)

	t.Logf("event log:\n%s", h.log)
	t.Logf("final: LLEN=%s LRANGE=%s resolved meta at ts=%d: exists=%v %+v (err=%v)",
		readSetReproReply{val: llen, err: llenErr}, readSetReproReply{val: lrange, err: lrangeErr},
		finalTS, metaExists, meta, metaErr)
	t.Logf("live raw keys containing %q:\n%s", delRPushReproListKey, h.rawKeysContaining(t, listKey))
	require.NoError(t, pushErr)
	require.NoError(t, llenErr)
	require.NoError(t, lrangeErr)

	if delRes.err != nil {
		t.Logf("DEL surfaced an error (a conflict reported to the client is acceptable): %v", delRes.err)
		return
	}
	delCount, ok := delRes.val.(int64)
	require.True(t, ok, "DEL reply %T is not an integer", delRes.val)

	consistent := llen == int64(len(lrange))
	serializable := len(lrange) == 0 || (pushed == 1 && slices.Equal(lrange, []string{"v2"}))
	if delCount == 1 && (!consistent || !serializable) {
		t.Errorf("DEL %s reported 1 deleted and a concurrent RPUSH v2 (reply %d) committed, but the list is neither empty nor a consistently readable [v2] "+
			"(LLEN=%d LRANGE=%q): DEL deleted only the items it scanned and did not fence the list, so the later push survived",
			delRPushReproListKey, pushed, llen, lrange)
	}
}

func TestLTrim_EmptyingConflictsWithConcurrentPush(t *testing.T) {
	t.Parallel()

	h := newReadSetReproHarness(t, "g7-ltrim")
	ctx := h.ctx
	key := "g7-ltrim-list"
	require.NoError(t, h.clientB.RPush(ctx, key, "v1").Err())
	itemPrefix := append([]byte(store.ListItemPrefix), []byte(key)...)
	park := h.reads.addPark("A: LTRIM list-item scan", func(op string, readKey []byte) bool {
		return op == readSetReproOpScanAt && bytes.HasPrefix(readKey, itemPrefix)
	})
	trimDone := h.goCommand(func() (any, error) {
		return h.clientA.LTrim(ctx, key, 1, 0).Result()
	})
	waitReadSetReproParked(t, park, trimDone, "A (LTRIM)")
	length, err := h.clientB.RPush(ctx, key, "v2").Result()
	require.NoError(t, err)
	require.EqualValues(t, 2, length)
	park.releaseNow()
	trimRes := waitReadSetReproDone(t, trimDone, "A (LTRIM)")
	if trimRes.err != nil {
		return
	}
	got, err := h.clientB.LRange(ctx, key, 0, -1).Result()
	require.NoError(t, err)
	require.Empty(t, got, "LTRIM succeeded after observing v1; the concurrent RPUSH replied 2")
}

// TestDeleteLogicalKeyElems_PutsCollectionFences pins that the logical-delete
// helpers Put the txn-wide fence of every collection type they clear, so an
// add that commits after the delete's read timestamp (it Puts the same fence)
// conflicts with the delete. Strings and streams have no fence: every write to
// them rewrites the anchor the delete removes.
func TestDeleteLogicalKeyElems_PutsCollectionFences(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	key := []byte("fence-delete")
	const readTS = uint64(10)
	st := store.NewMVCCStore()
	srv := NewRedisServer(nil, "", st, newLocalAdapterCoordinator(st), nil, nil)

	forType := func(typ redisValueType) func(*testing.T) []*kv.Elem[kv.OP] {
		return func(t *testing.T) []*kv.Elem[kv.OP] {
			t.Helper()
			elems, existed, err := srv.deleteLogicalKeyElemsForType(ctx, key, readTS, typ)
			require.NoError(t, err)
			require.True(t, existed, "a concrete start type reports the key as existing")
			return elems
		}
	}
	cases := []struct {
		name  string
		elems func(*testing.T) []*kv.Elem[kv.OP]
		want  [][]byte
	}{
		{
			name: "any type",
			elems: func(t *testing.T) []*kv.Elem[kv.OP] {
				t.Helper()
				elems, _, err := srv.deleteLogicalKeyElems(ctx, key, readTS)
				require.NoError(t, err)
				return elems
			},
			want: redisTxnWideCollectionFenceKeys(key),
		},
		{
			name: "list rows",
			elems: func(t *testing.T) []*kv.Elem[kv.OP] {
				t.Helper()
				elems, err := srv.deleteListElems(ctx, key, readTS)
				require.NoError(t, err)
				return elems
			},
			want: [][]byte{redisTxnWideListFenceKey(key)},
		},
		{name: "list", elems: forType(redisTypeList), want: [][]byte{redisTxnWideListFenceKey(key)}},
		{name: "hash", elems: forType(redisTypeHash), want: [][]byte{redisTxnWideHashFenceKey(key)}},
		{name: "set", elems: forType(redisTypeSet), want: [][]byte{redisTxnWideSetFenceKey(key)}},
		{name: "zset", elems: forType(redisTypeZSet), want: [][]byte{redisTxnWideZSetFenceKey(key)}},
		{name: "string", elems: forType(redisTypeString), want: nil},
		{name: "stream", elems: forType(redisTypeStream), want: nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var fencePuts [][]byte
			for _, elem := range tc.elems(t) {
				if redisTxnWideFenceUserKey(elem.Key) == nil {
					continue
				}
				require.Equal(t, kv.Put, elem.Op, "fence key %q must be Put, not deleted", elem.Key)
				fencePuts = append(fencePuts, elem.Key)
			}
			requireReadKeysMatch(t, fencePuts, tc.want)
		})
	}
}
