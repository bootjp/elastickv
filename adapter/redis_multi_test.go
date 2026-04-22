package adapter

import (
	"context"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/bootjp/elastickv/store"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// waitForListState polls until this node has applied raft commits such that
// the list stored under key resolves to expectedLen items whose values match
// expectedValues (when non-nil) via both resolveListMeta and a direct scan at
// the node-local readTS.
//
// Why this is necessary: the go-redis client may be connected to a follower
// (or a leader that momentarily lags in applying its own commit). The adapter's
// client-facing read path (LRANGE) uses LeaseRead/LinearizableRead so it
// blocks until the local apply catches up, which is why LRANGE observes the
// new state. However readTS() returns store.LastCommitTS() directly, and a
// direct ScanAt(readTS) bypasses that wait. When the client and the direct
// scan target the same node, there is still a window between the client
// receiving the EXEC reply (driven by the wait-apply inside read handlers) and
// the raft-applied commit updating LastCommitTS on this node's store — in
// particular when this node was not the proposer and applies strictly after
// the response was delivered via a different code path. Polling resolves the
// gap deterministically without a timing-based sleep.
func waitForListState(t *testing.T, n Node, key []byte, expectedLen int, expectedValues []string) {
	t.Helper()
	ctx := context.Background()
	require.Eventually(t, func() bool {
		readTS := n.redisServer.readTS()
		meta, exists, err := n.redisServer.resolveListMeta(ctx, key, readTS)
		if err != nil {
			return false
		}
		// Redis represents an empty / deleted list as the absence of
		// the meta key. When the caller is verifying deletion
		// (expectedLen == 0), !exists is the success signal;
		// requiring exists==true would make this helper unusable for
		// post-DEL waits. For non-empty expectations a missing meta
		// still means "not yet applied" and should keep polling.
		if expectedLen == 0 {
			if exists && meta.Len != 0 {
				return false
			}
		} else {
			if !exists || meta.Len != int64(expectedLen) {
				return false
			}
		}
		kvs, err := n.redisServer.store.ScanAt(
			ctx,
			store.ListItemKey(key, math.MinInt64),
			store.ListItemKey(key, math.MaxInt64),
			expectedLen+1,
			readTS,
		)
		if err != nil || len(kvs) != expectedLen {
			return false
		}
		if expectedValues == nil {
			return true
		}
		for i, kvp := range kvs {
			if string(kvp.Value) != expectedValues[i] {
				return false
			}
		}
		return true
	}, 5*time.Second, 250*time.Millisecond,
		"node did not catch up to expected list state for key %q (len=%d)", string(key), expectedLen)
}

func TestRedis_MultiExecAtomic(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	rdb := redis.NewClient(&redis.Options{Addr: nodes[1].redisAddress})
	ctx := context.Background()

	// start txn
	res := rdb.Do(ctx, "MULTI")
	require.NoError(t, res.Err())
	require.Equal(t, "OK", res.Val())

	// queue writes
	require.Equal(t, "QUEUED", rdb.Do(ctx, "SET", "mk1", "v1").Val())
	require.Equal(t, "QUEUED", rdb.Do(ctx, "SET", "mk2", "v2").Val())

	// commit
	execRes, err := rdb.Do(ctx, "EXEC").Result()
	require.NoError(t, err)
	vals, ok := execRes.([]any)
	require.True(t, ok, "exec result should be []interface{}")
	require.Len(t, vals, 2)
	require.Equal(t, "OK", vals[0])
	require.Equal(t, "OK", vals[1])

	// ensure writes visible
	got1, err := rdb.Get(ctx, "mk1").Result()
	require.NoError(t, err)
	require.Equal(t, "v1", got1)
	got2, err := rdb.Get(ctx, "mk2").Result()
	require.NoError(t, err)
	require.Equal(t, "v2", got2)
}

func TestRedis_RPushLRange(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	ctx := context.Background()

	pushRes, err := rdb.Do(ctx, "RPUSH", "listk", "a", "b", "c").Result()
	require.NoError(t, err)
	length, ok := pushRes.(int64)
	require.True(t, ok, "RPUSH result should be int64")
	require.Equal(t, int64(3), length)

	rangeRes, err := rdb.Do(ctx, "LRANGE", "listk", 0, -1).Result()
	require.NoError(t, err)
	require.Equal(t, []any{"a", "b", "c"}, rangeRes)

	rangeEmpty, err := rdb.Do(ctx, "LRANGE", "listk", 5, 10).Result()
	require.NoError(t, err)
	require.Equal(t, []any{}, rangeEmpty)
}

func TestRedis_DiscardClearsTxn(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	rdb := redis.NewClient(&redis.Options{Addr: nodes[2].redisAddress})
	ctx := context.Background()

	require.NoError(t, rdb.Set(ctx, "dc-key", "before", 0).Err())

	require.Equal(t, "OK", rdb.Do(ctx, "MULTI").Val())
	require.Equal(t, "QUEUED", rdb.Do(ctx, "SET", "dc-key", "after").Val())

	require.Equal(t, "OK", rdb.Do(ctx, "DISCARD").Val())

	// exec without MULTI should error
	exec := rdb.Do(ctx, "EXEC")
	require.Error(t, exec.Err())

	// value should remain unchanged
	got, err := rdb.Get(ctx, "dc-key").Result()
	require.NoError(t, err)
	require.Equal(t, "before", got)
}

func TestRedis_DelList_RemovesLargeListAndInternalKeys(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	ctx := context.Background()

	args := make([]any, 0, 1302)
	args = append(args, "RPUSH", "list-big-del")
	for i := range 1300 {
		args = append(args, "v"+strconv.Itoa(i))
	}
	_, err := rdb.Do(ctx, args...).Result()
	require.NoError(t, err)

	delCount, err := rdb.Del(ctx, "list-big-del").Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), delCount)

	rangeRes, err := rdb.Do(ctx, "LRANGE", "list-big-del", 0, -1).Result()
	require.NoError(t, err)
	require.Equal(t, []any{}, rangeRes)

	readTS := nodes[0].redisServer.readTS()
	_, err = nodes[0].redisServer.store.GetAt(ctx, store.ListMetaKey([]byte("list-big-del")), readTS)
	require.ErrorIs(t, err, store.ErrKeyNotFound)

	kvs, err := nodes[0].redisServer.store.ScanAt(
		ctx,
		store.ListItemKey([]byte("list-big-del"), math.MinInt64),
		store.ListItemKey([]byte("list-big-del"), math.MaxInt64),
		1,
		readTS,
	)
	require.NoError(t, err)
	assert.Len(t, kvs, 0)
}

func TestRedis_DelList_EmptyAfterDeleteHasNoResidualInternalKeys(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	ctx := context.Background()

	_, err := rdb.Do(ctx, "RPUSH", "list-empty-del", "a").Result()
	require.NoError(t, err)

	_, err = rdb.Del(ctx, "list-empty-del").Result()
	require.NoError(t, err)

	// Second DEL should be a no-op for list internals.
	_, err = rdb.Del(ctx, "list-empty-del").Result()
	require.NoError(t, err)

	readTS := nodes[0].redisServer.readTS()
	_, err = nodes[0].redisServer.store.GetAt(ctx, store.ListMetaKey([]byte("list-empty-del")), readTS)
	require.ErrorIs(t, err, store.ErrKeyNotFound)

	kvs, err := nodes[0].redisServer.store.ScanAt(
		ctx,
		store.ListItemKey([]byte("list-empty-del"), math.MinInt64),
		store.ListItemKey([]byte("list-empty-del"), math.MaxInt64),
		1,
		readTS,
	)
	require.NoError(t, err)
	assert.Len(t, kvs, 0)
}

func TestRedis_MultiExec_DelThenRPushRecreatesList(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	rdb := redis.NewClient(&redis.Options{Addr: nodes[1].redisAddress})
	ctx := context.Background()

	_, err := rdb.Do(ctx, "RPUSH", "list-del-rpush", "old1", "old2").Result()
	require.NoError(t, err)

	require.Equal(t, "OK", rdb.Do(ctx, "MULTI").Val())
	require.Equal(t, "QUEUED", rdb.Do(ctx, "DEL", "list-del-rpush").Val())
	require.Equal(t, "QUEUED", rdb.Do(ctx, "RPUSH", "list-del-rpush", "new1", "new2").Val())

	execRes, err := rdb.Do(ctx, "EXEC").Result()
	require.NoError(t, err)
	vals, ok := execRes.([]any)
	require.True(t, ok)
	require.Len(t, vals, 2)
	require.Equal(t, int64(1), vals[0])
	require.Equal(t, int64(2), vals[1])

	rangeRes, err := rdb.Do(ctx, "LRANGE", "list-del-rpush", 0, -1).Result()
	require.NoError(t, err)
	require.Equal(t, []any{"new1", "new2"}, rangeRes)

	// With the Delta pattern, RPUSH inside a MULTI/EXEC emits a delta key
	// rather than updating the base metadata key directly. Additionally, the
	// node the client is connected to may apply the EXEC commit slightly
	// after the client receives its response, so poll until this node's
	// store has caught up before asserting on the raw scan.
	waitForListState(t, nodes[1], []byte("list-del-rpush"), 2, []string{"new1", "new2"})
}

func TestRedis_MultiExec_SetGetAfterDeleteReturnsNilOldValue(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	ctx := context.Background()

	require.NoError(t, rdb.Set(ctx, "sg:del", "v1", 0).Err())
	require.Equal(t, "OK", rdb.Do(ctx, "MULTI").Val())
	require.Equal(t, "QUEUED", rdb.Do(ctx, "DEL", "sg:del").Val())
	require.Equal(t, "QUEUED", rdb.Do(ctx, "SET", "sg:del", "v2", "GET").Val())

	execRes, err := rdb.Do(ctx, "EXEC").Result()
	require.NoError(t, err)
	vals, ok := execRes.([]any)
	require.True(t, ok)
	require.Len(t, vals, 2)
	require.Equal(t, int64(1), vals[0])
	require.Nil(t, vals[1], "SET GET should see key as missing after staged DEL")

	got, err := rdb.Get(ctx, "sg:del").Result()
	require.NoError(t, err)
	require.Equal(t, "v2", got)
}
