package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestRedisLua_RPopLPushMissingTailItemReturnsNil(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r := newListPopTestServer(t)
	src := []byte("bull:test:wait")
	dst := []byte("bull:test:active")
	seedListMeta(t, r, src, store.ListMeta{Head: 0, Len: 1, Tail: 1})

	scriptCtx, err := newLuaScriptContext(ctx, r)
	require.NoError(t, err)
	defer scriptCtx.Close()

	reply, err := scriptCtx.cmdRPopLPush([]string{string(src), string(dst)})
	require.NoError(t, err)
	require.Equal(t, luaReplyNil, reply.kind)
	require.NoError(t, scriptCtx.commit())

	readTS := r.readTS()
	typ, err := r.keyTypeAt(ctx, src, readTS)
	require.NoError(t, err)
	require.Equal(t, redisTypeNone, typ)
	dstValues, err := r.listValuesAt(ctx, dst, readTS)
	require.NoError(t, err)
	require.Empty(t, dstValues)
}

func TestRedisLua_RPopLPushSkipsTailHole(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r := newListPopTestServer(t)
	src := []byte("bull:test:wait:hole")
	dst := []byte("bull:test:active:hole")
	seedListMeta(t, r, src, store.ListMeta{Head: 0, Len: 2, Tail: 2})
	require.NoError(t, r.store.PutAt(ctx, listItemKey(src, 0), []byte("job-1"), 2, 0))

	scriptCtx, err := newLuaScriptContext(ctx, r)
	require.NoError(t, err)
	defer scriptCtx.Close()

	reply, err := scriptCtx.cmdRPopLPush([]string{string(src), string(dst)})
	require.NoError(t, err)
	require.Equal(t, luaReplyString, reply.kind)
	require.Equal(t, "job-1", reply.text)
	require.NoError(t, scriptCtx.commit())

	readTS := r.readTS()
	srcValues, err := r.listValuesAt(ctx, src, readTS)
	require.NoError(t, err)
	require.Empty(t, srcValues)
	dstValues, err := r.listValuesAt(ctx, dst, readTS)
	require.NoError(t, err)
	require.Equal(t, []string{"job-1"}, dstValues)
}

func TestRedisLua_RPopLPushSparseTailEmitsRightTrimDelta(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r := newListPopTestServer(t)
	src := []byte("bull:test:wait:tail-trim")
	dst := []byte("bull:test:active:tail-trim")
	seedListMeta(t, r, src, store.ListMeta{Head: 0, Len: 3, Tail: 3})
	require.NoError(t, r.store.PutAt(ctx, listItemKey(src, 0), []byte("job-1"), 2, 0))
	require.NoError(t, r.store.PutAt(ctx, listItemKey(src, 1), []byte("job-2"), 2, 0))

	scriptCtx, err := newLuaScriptContext(ctx, r)
	require.NoError(t, err)
	defer scriptCtx.Close()

	reply, err := scriptCtx.cmdRPopLPush([]string{string(src), string(dst)})
	require.NoError(t, err)
	require.Equal(t, luaReplyString, reply.kind)
	require.Equal(t, "job-2", reply.text)
	require.NoError(t, scriptCtx.commit())

	readTS := r.readTS()
	meta, exists, err := r.resolveListMeta(ctx, src, readTS)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, int64(1), meta.Len)
	srcValues, err := r.listValuesAt(ctx, src, readTS)
	require.NoError(t, err)
	require.Equal(t, []string{"job-1"}, srcValues)
	dstValues, err := r.listValuesAt(ctx, dst, readTS)
	require.NoError(t, err)
	require.Equal(t, []string{"job-2"}, dstValues)
}

func TestRedisLua_RPopLPushSkipsPrefixCollisionItem(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r := newListPopTestServer(t)
	src := []byte("bull:test:wait:prefix")
	dst := []byte("bull:test:active:prefix")
	seedListMeta(t, r, src, store.ListMeta{Head: 0, Len: 1, Tail: 1})
	require.NoError(t, r.store.PutAt(ctx, listItemKey(src, 0), []byte("job-1"), 2, 0))
	collider := append([]byte{}, src...)
	collider = append(collider, listItemKey(src, 0)[len(store.ListItemPrefix)+len(src):]...)
	collider = append(collider, 'x')
	require.NoError(t, r.store.PutAt(ctx, listItemKey(collider, 0), []byte("other-list-job"), 2, 0))

	scriptCtx, err := newLuaScriptContext(ctx, r)
	require.NoError(t, err)
	defer scriptCtx.Close()

	reply, err := scriptCtx.cmdRPopLPush([]string{string(src), string(dst)})
	require.NoError(t, err)
	require.Equal(t, luaReplyString, reply.kind)
	require.Equal(t, "job-1", reply.text)
	require.NoError(t, scriptCtx.commit())
}

func TestRedisLua_RPopLPushMissingTailThenRecreateRewritesMeta(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r := newListPopTestServer(t)
	src := []byte("bull:test:wait:recreate")
	dst := []byte("bull:test:active:recreate")
	seedListMeta(t, r, src, store.ListMeta{Head: 0, Len: 1, Tail: 1})

	scriptCtx, err := newLuaScriptContext(ctx, r)
	require.NoError(t, err)
	defer scriptCtx.Close()

	reply, err := scriptCtx.cmdRPopLPush([]string{string(src), string(dst)})
	require.NoError(t, err)
	require.Equal(t, luaReplyNil, reply.kind)

	llen, err := scriptCtx.cmdRPush([]string{string(src), "job-new"})
	require.NoError(t, err)
	require.Equal(t, luaReplyInt, llen.kind)
	require.Equal(t, int64(1), llen.integer)
	require.NoError(t, scriptCtx.commit())

	readTS := r.readTS()
	meta, exists, err := r.resolveListMeta(ctx, src, readTS)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, int64(1), meta.Len)
	srcValues, err := r.listValuesAt(ctx, src, readTS)
	require.NoError(t, err)
	require.Equal(t, []string{"job-new"}, srcValues)
}

func TestRedisLua_RPopLPushScansPastLargeTailHole(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r := newListPopTestServer(t)
	src := []byte("bull:test:wait:long-hole")
	dst := []byte("bull:test:active:long-hole")
	largeLen := int64(luaSparseListPopScanLimit) + 2
	seedListMeta(t, r, src, store.ListMeta{Head: 0, Len: largeLen, Tail: largeLen})
	require.NoError(t, r.store.PutAt(ctx, listItemKey(src, 0), []byte("job-1"), 2, 0))

	scriptCtx, err := newLuaScriptContext(ctx, r)
	require.NoError(t, err)
	defer scriptCtx.Close()

	reply, err := scriptCtx.cmdRPopLPush([]string{string(src), string(dst)})
	require.NoError(t, err)
	require.Equal(t, luaReplyString, reply.kind)
	require.Equal(t, "job-1", reply.text)
	require.NoError(t, scriptCtx.commit())

	readTS := r.readTS()
	typ, err := r.keyTypeAt(ctx, src, readTS)
	require.NoError(t, err)
	require.Equal(t, redisTypeNone, typ)
	dstValues, err := r.listValuesAt(ctx, dst, readTS)
	require.NoError(t, err)
	require.Equal(t, []string{"job-1"}, dstValues)
}

func TestRedisLua_RPopLPushKeepsRemainingSparseHeadItem(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r := newListPopTestServer(t)
	src := []byte("bull:test:wait:remaining-head")
	dst := []byte("bull:test:active:remaining-head")
	largeLen := int64(luaSparseListPopScanLimit) + 2
	seedListMeta(t, r, src, store.ListMeta{Head: 0, Len: largeLen, Tail: largeLen})
	require.NoError(t, r.store.PutAt(ctx, listItemKey(src, 0), []byte("job-1"), 2, 0))
	require.NoError(t, r.store.PutAt(ctx, listItemKey(src, 1), []byte("job-2"), 3, 0))

	scriptCtx, err := newLuaScriptContext(ctx, r)
	require.NoError(t, err)
	defer scriptCtx.Close()

	reply, err := scriptCtx.cmdRPopLPush([]string{string(src), string(dst)})
	require.NoError(t, err)
	require.Equal(t, luaReplyString, reply.kind)
	require.Equal(t, "job-2", reply.text)
	require.NoError(t, scriptCtx.commit())

	readTS := r.readTS()
	srcValues, err := r.listValuesAt(ctx, src, readTS)
	require.NoError(t, err)
	require.Equal(t, []string{"job-1"}, srcValues)
	dstValues, err := r.listValuesAt(ctx, dst, readTS)
	require.NoError(t, err)
	require.Equal(t, []string{"job-2"}, dstValues)
}

func TestRedisLua_RPopLPushScansPastPhysicalTailTombstones(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r := newListPopTestServer(t)
	src := []byte("bull:test:wait:tombstone-tail")
	dst := []byte("bull:test:active:tombstone-tail")
	largeLen := int64(luaSparseListPopScanLimit) + 2
	seedListMeta(t, r, src, store.ListMeta{Head: 0, Len: largeLen, Tail: largeLen})
	require.NoError(t, r.store.PutAt(ctx, listItemKey(src, 0), []byte("job-1"), 2, 0))
	seedListPrefixCollider(t, r, ctx, src, 0)
	commitTS := uint64(3)
	for seq := int64(1); seq <= int64(luaSparseListPopScanLimit)+1; seq++ {
		require.NoError(t, r.store.DeleteAt(ctx, listItemKey(src, seq), commitTS))
		commitTS++
	}

	scriptCtx, err := newLuaScriptContext(ctx, r)
	require.NoError(t, err)
	defer scriptCtx.Close()

	reply, err := scriptCtx.cmdRPopLPush([]string{string(src), string(dst)})
	require.NoError(t, err)
	require.Equal(t, luaReplyString, reply.kind)
	require.Equal(t, "job-1", reply.text)
	require.NoError(t, scriptCtx.commit())

	readTS := r.readTS()
	typ, err := r.keyTypeAt(ctx, src, readTS)
	require.NoError(t, err)
	require.Equal(t, redisTypeNone, typ)
	dstValues, err := r.listValuesAt(ctx, dst, readTS)
	require.NoError(t, err)
	require.Equal(t, []string{"job-1"}, dstValues)
}

func TestRedisLua_LPopScansPastPhysicalHeadTombstones(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r := newListPopTestServer(t)
	src := []byte("bull:test:wait:tombstone-head")
	largeLen := int64(luaSparseListPopScanLimit) + 2
	seedListMeta(t, r, src, store.ListMeta{Head: 0, Len: largeLen, Tail: largeLen})
	require.NoError(t, r.store.PutAt(ctx, listItemKey(src, largeLen-1), []byte("job-1"), 2, 0))
	seedListPrefixCollider(t, r, ctx, src, 0)
	commitTS := uint64(3)
	for seq := int64(0); seq <= int64(luaSparseListPopScanLimit); seq++ {
		require.NoError(t, r.store.DeleteAt(ctx, listItemKey(src, seq), commitTS))
		commitTS++
	}

	scriptCtx, err := newLuaScriptContext(ctx, r)
	require.NoError(t, err)
	defer scriptCtx.Close()

	reply, err := scriptCtx.cmdLPop([]string{string(src)})
	require.NoError(t, err)
	require.Equal(t, luaReplyString, reply.kind)
	require.Equal(t, "job-1", reply.text)
	require.NoError(t, scriptCtx.commit())

	readTS := r.readTS()
	typ, err := r.keyTypeAt(ctx, src, readTS)
	require.NoError(t, err)
	require.Equal(t, redisTypeNone, typ)
}

func TestRedisLua_RPopLPushDeletesPhysicalTombstoneOnlyList(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r := newListPopTestServer(t)
	src := []byte("bull:test:wait:tombstone-only")
	dst := []byte("bull:test:active:tombstone-only")
	largeLen := int64(luaSparseListPopScanLimit) + 2
	seedListMeta(t, r, src, store.ListMeta{Head: 0, Len: largeLen, Tail: largeLen})
	commitTS := uint64(2)
	for seq := int64(0); seq <= int64(luaSparseListPopScanLimit)+1; seq++ {
		require.NoError(t, r.store.DeleteAt(ctx, listItemKey(src, seq), commitTS))
		commitTS++
	}

	scriptCtx, err := newLuaScriptContext(ctx, r)
	require.NoError(t, err)
	defer scriptCtx.Close()

	reply, err := scriptCtx.cmdRPopLPush([]string{string(src), string(dst)})
	require.NoError(t, err)
	require.Equal(t, luaReplyNil, reply.kind)
	require.NoError(t, scriptCtx.commit())

	readTS := r.readTS()
	typ, err := r.keyTypeAt(ctx, src, readTS)
	require.NoError(t, err)
	require.Equal(t, redisTypeNone, typ)
	dstValues, err := r.listValuesAt(ctx, dst, readTS)
	require.NoError(t, err)
	require.Empty(t, dstValues)
}

func TestRedisLua_RPopLPushSyntheticPhysicalLimitFailsClosed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	base := store.NewMVCCStore()
	st := noExactFallbackPhysicalLimitStore{MVCCStore: base}
	coord := newLocalAdapterCoordinator(st)
	r := NewRedisServer(nil, "", st, coord, nil, nil)
	src := []byte("bull:test:wait:synthetic-limit")
	dst := []byte("bull:test:active:synthetic-limit")
	seedListMeta(t, r, src, store.ListMeta{Head: 0, Len: 1, Tail: 1})
	require.NoError(t, r.store.PutAt(ctx, listItemKey(src, 0), []byte("job-1"), 2, 0))

	scriptCtx, err := newLuaScriptContext(ctx, r)
	require.NoError(t, err)
	defer scriptCtx.Close()

	_, err = scriptCtx.cmdRPopLPush([]string{string(src), string(dst)})
	require.ErrorIs(t, err, ErrCollectionTooLarge)
}

func TestRedisLua_RPopLPushDeletesLargeSparseListWithoutItems(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r := newListPopTestServer(t)
	src := []byte("bull:test:wait:large-empty")
	dst := []byte("bull:test:active:large-empty")
	largeLen := int64(luaSparseListPopScanLimit) + 2
	seedListMeta(t, r, src, store.ListMeta{Head: 0, Len: largeLen, Tail: largeLen})

	scriptCtx, err := newLuaScriptContext(ctx, r)
	require.NoError(t, err)
	defer scriptCtx.Close()

	reply, err := scriptCtx.cmdRPopLPush([]string{string(src), string(dst)})
	require.NoError(t, err)
	require.Equal(t, luaReplyNil, reply.kind)
	require.NoError(t, scriptCtx.commit())

	readTS := r.readTS()
	typ, err := r.keyTypeAt(ctx, src, readTS)
	require.NoError(t, err)
	require.Equal(t, redisTypeNone, typ)
	dstValues, err := r.listValuesAt(ctx, dst, readTS)
	require.NoError(t, err)
	require.Empty(t, dstValues)
}

func seedListMeta(t *testing.T, r *RedisServer, key []byte, meta store.ListMeta) {
	t.Helper()

	raw, err := store.MarshalListMeta(meta)
	require.NoError(t, err)
	require.NoError(t, r.store.PutAt(context.Background(), store.ListMetaKey(key), raw, 1, 0))
}

func seedListPrefixCollider(t *testing.T, r *RedisServer, ctx context.Context, key []byte, seq int64) {
	t.Helper()

	collider := append([]byte{}, key...)
	collider = append(collider, listItemKey(key, seq)[len(store.ListItemPrefix)+len(key):]...)
	collider = append(collider, 'x')
	require.NoError(t, r.store.PutAt(ctx, listItemKey(collider, 0), []byte("other-list-job"), 2, 0))
}

type noExactFallbackPhysicalLimitStore struct {
	store.MVCCStore
}

func (s noExactFallbackPhysicalLimitStore) ScanAtPhysicalLimit(context.Context, []byte, []byte, int, int, uint64) ([]*store.KVPair, bool, error) {
	return nil, true, nil
}

func (s noExactFallbackPhysicalLimitStore) ReverseScanAtPhysicalLimit(context.Context, []byte, []byte, int, int, uint64) ([]*store.KVPair, bool, error) {
	return nil, true, nil
}

func (s noExactFallbackPhysicalLimitStore) AllowExactScanFallbackAfterPhysicalLimit(context.Context, []byte, []byte, int, int, uint64, bool) bool {
	return false
}
