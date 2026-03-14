package adapter

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestRedis_BullMQDirectCommands(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	reader := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	writer := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() {
		_ = rdb.Close()
		_ = reader.Close()
		_ = writer.Close()
	}()

	ok, err := rdb.HMSet(ctx, "bull:test:meta", "opts.maxLenEvents", "1000", "version", "bullmq").Result()
	require.NoError(t, err)
	require.True(t, ok)

	added, err := rdb.HSet(ctx, "bull:test:meta", "concurrency", 1, "max", 10).Result()
	require.NoError(t, err)
	require.Equal(t, int64(2), added)

	metaVersion, err := rdb.HGet(ctx, "bull:test:meta", "version").Result()
	require.NoError(t, err)
	require.Equal(t, "bullmq", metaVersion)

	metaValues, err := rdb.HMGet(ctx, "bull:test:meta", "concurrency", "max", "missing").Result()
	require.NoError(t, err)
	require.Equal(t, []any{"1", "10", nil}, metaValues)

	id, err := rdb.Incr(ctx, "bull:test:id").Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), id)

	streamCh := make(chan []redis.XStream, 1)
	streamErrCh := make(chan error, 1)
	go func() {
		streams, err := reader.XRead(ctx, &redis.XReadArgs{
			Streams: []string{"bull:test:events", "$"},
			Count:   10,
			Block:   time.Second,
		}).Result()
		if err != nil {
			streamErrCh <- err
			return
		}
		streamCh <- streams
	}()

	time.Sleep(100 * time.Millisecond)

	firstID, err := writer.XAdd(ctx, &redis.XAddArgs{
		Stream: "bull:test:events",
		ID:     "1000-0",
		Values: []string{"event", "waiting", "jobId", "job-1"},
	}).Result()
	require.NoError(t, err)
	require.Equal(t, "1000-0", firstID)

	select {
	case err := <-streamErrCh:
		require.NoError(t, err)
	case streams := <-streamCh:
		require.Len(t, streams, 1)
		require.Equal(t, "bull:test:events", streams[0].Stream)
		require.Len(t, streams[0].Messages, 1)
		require.Equal(t, "1000-0", streams[0].Messages[0].ID)
		require.Equal(t, map[string]any{"event": "waiting", "jobId": "job-1"}, streams[0].Messages[0].Values)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for XREAD result")
	}

	_, err = writer.XAdd(ctx, &redis.XAddArgs{
		Stream: "bull:test:events",
		ID:     "1001-0",
		Values: []string{"event", "completed", "jobId", "job-1"},
	}).Result()
	require.NoError(t, err)

	trimmed, err := writer.Do(ctx, "XTRIM", "bull:test:events", "MAXLEN", "~", "1").Int64()
	require.NoError(t, err)
	require.Equal(t, int64(1), trimmed)

	xlen, err := writer.XLen(ctx, "bull:test:events").Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), xlen)

	popCh := make(chan *redis.ZWithKey, 1)
	popErrCh := make(chan error, 1)
	go func() {
		res, err := reader.BZPopMin(ctx, time.Second, "bull:test:marker").Result()
		if err != nil {
			popErrCh <- err
			return
		}
		popCh <- res
	}()

	time.Sleep(100 * time.Millisecond)

	_, err = writer.ZAdd(ctx, "bull:test:marker",
		redis.Z{Score: 20, Member: "20"},
		redis.Z{Score: 10, Member: "10"},
	).Result()
	require.NoError(t, err)

	select {
	case err := <-popErrCh:
		require.NoError(t, err)
	case popped := <-popCh:
		require.Equal(t, "bull:test:marker", popped.Key)
		require.Equal(t, "10", popped.Member)
		require.Equal(t, 10.0, popped.Score)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for BZPOPMIN result")
	}

	zvals, err := writer.ZRangeWithScores(ctx, "bull:test:marker", 0, -1).Result()
	require.NoError(t, err)
	require.Len(t, zvals, 1)
	require.Equal(t, "20", zvals[0].Member)
	require.Equal(t, 20.0, zvals[0].Score)
}
