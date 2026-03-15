package adapter

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestRedis_MisskeyConnectionCompatibility(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	ctx := context.Background()

	info, err := rdb.Do(ctx, "INFO").Text()
	require.NoError(t, err)
	require.Contains(t, info, "loading:0")

	require.Equal(t, "OK", rdb.Do(ctx, "CLIENT", "SETINFO", "LIB-NAME", "ioredis").Val())
	require.Equal(t, "OK", rdb.Do(ctx, "CLIENT", "SETINFO", "LIB-VER", "5.10.0").Val())
	require.Equal(t, "OK", rdb.Do(ctx, "SELECT", "0").Val())

	res := rdb.Do(ctx, "SET", "lock:ap-object", "v1", "PX", "200", "NX")
	require.NoError(t, res.Err())
	require.Equal(t, "OK", res.Val())

	res = rdb.Do(ctx, "SET", "lock:ap-object", "v2", "PX", "200", "NX")
	require.ErrorIs(t, res.Err(), redis.Nil)

	getRes := rdb.Do(ctx, "SET", "lock:ap-object", "v3", "EX", "1", "GET")
	require.NoError(t, getRes.Err())
	require.Equal(t, "v1", getRes.Val())

	pttl, err := rdb.PTTL(ctx, "lock:ap-object").Result()
	require.NoError(t, err)
	require.Greater(t, pttl, time.Duration(0))

	time.Sleep(1100 * time.Millisecond)
	require.ErrorIs(t, rdb.Get(ctx, "lock:ap-object").Err(), redis.Nil)
}

func TestRedis_MisskeyPubSubCompatibility(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	pub := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	sub := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() {
		_ = pub.Close()
		_ = sub.Close()
	}()

	ctx := context.Background()
	ps := sub.Subscribe(ctx, "misskey.example.test")
	defer func() { _ = ps.Close() }()

	_, err := ps.ReceiveTimeout(ctx, 2*time.Second)
	require.NoError(t, err)

	count, err := pub.Publish(ctx, "misskey.example.test", `{"channel":"internal","message":{"type":"metaUpdated"}}`).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	msg, err := ps.ReceiveMessage(ctx)
	require.NoError(t, err)
	require.Equal(t, "misskey.example.test", msg.Channel)
	require.Equal(t, `{"channel":"internal","message":{"type":"metaUpdated"}}`, msg.Payload)
}

func TestRedis_MisskeyPubSubClusterFanout(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	pub := redis.NewClient(&redis.Options{Addr: nodes[2].redisAddress})
	sub1 := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	sub2 := redis.NewClient(&redis.Options{Addr: nodes[1].redisAddress})
	defer func() {
		_ = pub.Close()
		_ = sub1.Close()
		_ = sub2.Close()
	}()

	ctx := context.Background()
	ps1 := sub1.Subscribe(ctx, "misskey.cluster.test")
	ps2 := sub2.Subscribe(ctx, "misskey.cluster.test")
	defer func() {
		_ = ps1.Close()
		_ = ps2.Close()
	}()

	_, err := ps1.ReceiveTimeout(ctx, 2*time.Second)
	require.NoError(t, err)
	_, err = ps2.ReceiveTimeout(ctx, 2*time.Second)
	require.NoError(t, err)

	count, err := pub.Publish(ctx, "misskey.cluster.test", `{"type":"cluster"}`).Result()
	require.NoError(t, err)
	require.Equal(t, int64(2), count)

	msg1, err := ps1.ReceiveMessage(ctx)
	require.NoError(t, err)
	require.Equal(t, "misskey.cluster.test", msg1.Channel)
	require.Equal(t, `{"type":"cluster"}`, msg1.Payload)

	msg2, err := ps2.ReceiveMessage(ctx)
	require.NoError(t, err)
	require.Equal(t, "misskey.cluster.test", msg2.Channel)
	require.Equal(t, `{"type":"cluster"}`, msg2.Payload)
}

func TestRedis_MisskeyAdminCompatibility(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	sub := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() {
		_ = rdb.Close()
		_ = sub.Close()
	}()

	ctx := context.Background()

	require.NoError(t, rdb.Set(ctx, "cache:one", "1", 0).Err())
	require.NoError(t, rdb.HSet(ctx, "cache:two", "field", "2").Err())

	size, err := rdb.DBSize(ctx).Result()
	require.NoError(t, err)
	require.Equal(t, int64(2), size)

	ps := sub.Subscribe(ctx, "misskey.pubsub.one", "misskey.pubsub.two")
	defer func() { _ = ps.Close() }()
	for i := 0; i < 2; i++ {
		_, err = ps.ReceiveTimeout(ctx, 2*time.Second)
		require.NoError(t, err)
	}

	channels, err := rdb.Do(ctx, "PUBSUB", "CHANNELS", "misskey.pubsub.*").StringSlice()
	require.NoError(t, err)
	require.Equal(t, []string{"misskey.pubsub.one", "misskey.pubsub.two"}, channels)

	numsub, err := rdb.Do(ctx, "PUBSUB", "NUMSUB", "misskey.pubsub.one", "missing").Result()
	require.NoError(t, err)
	values, ok := numsub.([]any)
	require.True(t, ok)
	require.Equal(t, []any{"misskey.pubsub.one", int64(1), "missing", int64(0)}, values)

	numpat, err := rdb.Do(ctx, "PUBSUB", "NUMPAT").Int64()
	require.NoError(t, err)
	require.Equal(t, int64(0), numpat)

	require.Equal(t, "OK", rdb.FlushDB(ctx).Val())

	size, err = rdb.DBSize(ctx).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), size)
}

func TestRedis_MisskeyDirectDataStructures(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	ctx := context.Background()

	lpushLen, err := rdb.LPush(ctx, "list:homeTimeline:u1", "n1", "n2").Result()
	require.NoError(t, err)
	require.Equal(t, int64(2), lpushLen)
	require.NoError(t, rdb.LTrim(ctx, "list:homeTimeline:u1", 0, 1).Err())
	require.NoError(t, rdb.RPush(ctx, "list:homeTimeline:u1", "n3").Err())
	last, err := rdb.LIndex(ctx, "list:homeTimeline:u1", -1).Result()
	require.NoError(t, err)
	require.Equal(t, "n3", last)
	all, err := rdb.LRange(ctx, "list:homeTimeline:u1", 0, -1).Result()
	require.NoError(t, err)
	require.Equal(t, []string{"n2", "n1", "n3"}, all)

	added, err := rdb.SAdd(ctx, "newChatMessagesExists:u1", "user:u2", "room:r1").Result()
	require.NoError(t, err)
	require.Equal(t, int64(2), added)
	isMember, err := rdb.SIsMember(ctx, "newChatMessagesExists:u1", "user:u2").Result()
	require.NoError(t, err)
	require.True(t, isMember)
	removed, err := rdb.SRem(ctx, "newChatMessagesExists:u1", "room:r1").Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), removed)

	hval, err := rdb.HIncrBy(ctx, "reactionsBufferDeltas:note1", "like", 2).Result()
	require.NoError(t, err)
	require.Equal(t, int64(2), hval)
	hall, err := rdb.HGetAll(ctx, "reactionsBufferDeltas:note1").Result()
	require.NoError(t, err)
	require.Equal(t, map[string]string{"like": "2"}, hall)

	_, err = rdb.ZAdd(ctx, "featuredGlobalNotesRanking:1", redis.Z{Score: 1, Member: "note1"}, redis.Z{Score: 3, Member: "note2"}).Result()
	require.NoError(t, err)
	zvals, err := rdb.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{
		Key:   "featuredGlobalNotesRanking:1",
		Start: 0,
		Stop:  1,
		Rev:   true,
	}).Result()
	require.NoError(t, err)
	require.Len(t, zvals, 2)
	require.Equal(t, "note2", zvals[0].Member)
	require.Equal(t, 3.0, zvals[0].Score)

	pfAdded, err := rdb.PFAdd(ctx, "hashtagUsers:golang:20260314", "u1", "u2", "u2").Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), pfAdded)
	pfCount, err := rdb.PFCount(ctx, "hashtagUsers:golang:20260314").Result()
	require.NoError(t, err)
	require.Equal(t, int64(2), pfCount)

	var cursor uint64
	var keys []string
	for {
		var batch []string
		batch, cursor, err = rdb.Scan(ctx, cursor, "reactionsBufferDeltas:*", 100).Result()
		require.NoError(t, err)
		keys = append(keys, batch...)
		if cursor == 0 {
			break
		}
	}
	require.Contains(t, keys, "reactionsBufferDeltas:note1")

	id1, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "notificationTimeline:u1",
		MaxLen: 2,
		Approx: true,
		ID:     "1000-0",
		Values: []string{"data", `{"id":"n1"}`},
	}).Result()
	require.NoError(t, err)
	require.Equal(t, "1000-0", id1)
	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "notificationTimeline:u1",
		MaxLen: 2,
		Approx: true,
		ID:     "1001-0",
		Values: []string{"data", `{"id":"n2"}`},
	}).Result()
	require.NoError(t, err)
	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "notificationTimeline:u1",
		MaxLen: 2,
		Approx: true,
		ID:     "1002-0",
		Values: []string{"data", `{"id":"n3"}`},
	}).Result()
	require.NoError(t, err)

	xlen, err := rdb.XLen(ctx, "notificationTimeline:u1").Result()
	require.NoError(t, err)
	require.Equal(t, int64(2), xlen)

	xrev, err := rdb.XRevRangeN(ctx, "notificationTimeline:u1", "+", "-", 1).Result()
	require.NoError(t, err)
	require.Len(t, xrev, 1)
	require.Equal(t, "1002-0", xrev[0].ID)
	require.Equal(t, map[string]interface{}{"data": `{"id":"n3"}`}, xrev[0].Values)

	xrange, err := rdb.XRange(ctx, "notificationTimeline:u1", "1001-0", "+").Result()
	require.NoError(t, err)
	require.Len(t, xrange, 2)
	require.Equal(t, "1001-0", xrange[0].ID)
	require.Equal(t, "1002-0", xrange[1].ID)
}

func TestRedis_MisskeyFeaturedRankingTransaction(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	ctx := context.Background()

	require.Equal(t, "OK", rdb.Do(ctx, "MULTI").Val())
	require.Equal(t, "QUEUED", rdb.Do(ctx, "ZINCRBY", "featuredHashtagsRanking:1", 1, "golang").Val())
	require.Equal(t, "QUEUED", rdb.Do(ctx, "EXPIRE", "featuredHashtagsRanking:1", 30, "NX").Val())

	execRes, err := rdb.Do(ctx, "EXEC").Result()
	require.NoError(t, err)
	vals, ok := execRes.([]any)
	require.True(t, ok)
	require.Len(t, vals, 2)
	require.Equal(t, "1", vals[0])
	require.Equal(t, int64(1), vals[1])

	zvals, err := rdb.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{
		Key:   "featuredHashtagsRanking:1",
		Start: 0,
		Stop:  0,
		Rev:   true,
	}).Result()
	require.NoError(t, err)
	require.Len(t, zvals, 1)
	require.Equal(t, "golang", zvals[0].Member)
	require.Equal(t, 1.0, zvals[0].Score)

	ttl, err := rdb.TTL(ctx, "featuredHashtagsRanking:1").Result()
	require.NoError(t, err)
	require.Greater(t, ttl, time.Duration(0))
}

func TestRedis_MisskeySETEX(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	ctx := context.Background()

	// SETEX key seconds value
	res := rdb.Do(ctx, "SETEX", "webauthn:challenge:u1", "120", "challenge-data")
	require.NoError(t, res.Err())
	require.Equal(t, "OK", res.Val())

	// Verify value was stored
	val, err := rdb.Get(ctx, "webauthn:challenge:u1").Result()
	require.NoError(t, err)
	require.Equal(t, "challenge-data", val)

	// Verify TTL was set
	ttl, err := rdb.TTL(ctx, "webauthn:challenge:u1").Result()
	require.NoError(t, err)
	require.Greater(t, ttl, time.Duration(0))
	require.LessOrEqual(t, ttl, 120*time.Second)

	// SETEX overwrites existing value
	res = rdb.Do(ctx, "SETEX", "webauthn:challenge:u1", "60", "new-challenge")
	require.NoError(t, res.Err())
	val, err = rdb.Get(ctx, "webauthn:challenge:u1").Result()
	require.NoError(t, err)
	require.Equal(t, "new-challenge", val)
}

func TestRedis_MisskeyGETDEL(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	ctx := context.Background()

	// Set a value, then GETDEL
	require.NoError(t, rdb.Set(ctx, "webauthn:reg:u1", "reg-data", 0).Err())

	val := rdb.Do(ctx, "GETDEL", "webauthn:reg:u1")
	require.NoError(t, val.Err())
	require.Equal(t, "reg-data", val.Val())

	// Key should be deleted now
	require.ErrorIs(t, rdb.Get(ctx, "webauthn:reg:u1").Err(), redis.Nil)

	// GETDEL on non-existent key returns nil
	val = rdb.Do(ctx, "GETDEL", "nonexistent")
	require.ErrorIs(t, val.Err(), redis.Nil)
}

func TestRedis_MisskeySETNX(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	ctx := context.Background()

	// SETNX on non-existent key succeeds (returns 1)
	res := rdb.Do(ctx, "SETNX", "lock:distributed:1", "owner1")
	require.NoError(t, res.Err())
	require.Equal(t, int64(1), res.Val())

	// Verify value was stored
	val, err := rdb.Get(ctx, "lock:distributed:1").Result()
	require.NoError(t, err)
	require.Equal(t, "owner1", val)

	// SETNX on existing key fails (returns 0)
	res = rdb.Do(ctx, "SETNX", "lock:distributed:1", "owner2")
	require.NoError(t, res.Err())
	require.Equal(t, int64(0), res.Val())

	// Value should not have changed
	val, err = rdb.Get(ctx, "lock:distributed:1").Result()
	require.NoError(t, err)
	require.Equal(t, "owner1", val)
}
