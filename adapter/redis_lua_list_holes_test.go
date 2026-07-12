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
	seedListMeta(t, r, src, store.ListMeta{Head: 0, Len: 1, Tail: 1}, 1)

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
	seedListMeta(t, r, src, store.ListMeta{Head: 0, Len: 2, Tail: 2}, 1)
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

func seedListMeta(t *testing.T, r *RedisServer, key []byte, meta store.ListMeta, commitTS uint64) {
	t.Helper()

	raw, err := store.MarshalListMeta(meta)
	require.NoError(t, err)
	require.NoError(t, r.store.PutAt(context.Background(), store.ListMetaKey(key), raw, commitTS, 0))
}
