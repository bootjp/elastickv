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
