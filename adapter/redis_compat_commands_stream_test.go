package adapter

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bootjp/elastickv/monitoring"
	"github.com/cockroachdb/errors"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestRedis_StreamXAddXReadRoundTrip(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()
	ctx := context.Background()

	for i := range 5 {
		id := fmt.Sprintf("%d-0", 1_000_000+i)
		_, err := rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: "stream-rt",
			ID:     id,
			Values: []string{"i", fmt.Sprint(i)},
		}).Result()
		require.NoError(t, err)
	}

	streams, err := rdb.XRead(ctx, &redis.XReadArgs{
		Streams: []string{"stream-rt", "0"},
		Count:   100,
	}).Result()
	require.NoError(t, err)
	require.Len(t, streams, 1)
	require.Len(t, streams[0].Messages, 5)
	for i, msg := range streams[0].Messages {
		require.Equal(t, fmt.Sprintf("%d-0", 1_000_000+i), msg.ID)
		require.Equal(t, map[string]any{"i": fmt.Sprint(i)}, msg.Values)
	}
}

// TestRedis_StreamXReadLatencyIsConstant guards the O(new) property: after
// 10k entries, the 100th XREAD from "$" must run in roughly the same time
// as the 1st. The crude 2x ceiling tolerates GC / scheduler jitter.
func TestRedis_StreamXReadLatencyIsConstant(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 10k-entry stream test in -short mode")
	}
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()
	ctx := context.Background()

	const (
		total  = 10_000
		probes = 100
	)
	for i := range total {
		_, err := rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: "stream-lat",
			ID:     fmt.Sprintf("%d-0", 1_000_000+i),
			Values: []string{"i", fmt.Sprint(i)},
		}).Result()
		require.NoError(t, err)
	}

	afterID := fmt.Sprintf("%d-0", 1_000_000+total-1)
	measure := func() time.Duration {
		start := time.Now()
		streams, err := rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{"stream-lat", afterID},
			Count:   10,
			Block:   10 * time.Millisecond,
		}).Result()
		elapsed := time.Since(start)
		require.True(t, errors.Is(err, redis.Nil) || err == nil)
		require.Empty(t, streams)
		return elapsed
	}

	first := measure()
	var longest time.Duration
	for range probes {
		if d := measure(); d > longest {
			longest = d
		}
	}
	// Threshold: 2x first sample plus a small floor to absorb single-digit
	// millisecond jitter. The old blob implementation grows linearly with
	// the entry count, so for 10k entries the 100th probe was routinely
	// dozens of times slower than the first.
	ceiling := 2*first + 10*time.Millisecond
	require.LessOrEqualf(t, longest, ceiling,
		"XREAD latency should not grow with stream size: first=%s longest=%s", first, longest)
}

func TestRedis_StreamXTrimMaxLen(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()
	ctx := context.Background()

	for i := range 100 {
		_, err := rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: "stream-trim",
			ID:     fmt.Sprintf("%d-0", 1_000_000+i),
			Values: []string{"i", fmt.Sprint(i)},
		}).Result()
		require.NoError(t, err)
	}

	trimmed, err := rdb.Do(ctx, "XTRIM", "stream-trim", "MAXLEN", "10").Int64()
	require.NoError(t, err)
	require.Equal(t, int64(90), trimmed)

	xlen, err := rdb.XLen(ctx, "stream-trim").Result()
	require.NoError(t, err)
	require.Equal(t, int64(10), xlen)

	entries, err := rdb.XRange(ctx, "stream-trim", "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, entries, 10)
	for i, msg := range entries {
		require.Equal(t, fmt.Sprintf("%d-0", 1_000_000+90+i), msg.ID)
	}
}

func TestRedis_StreamXRangeBounds(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()
	ctx := context.Background()

	ids := []string{"1000-0", "1001-0", "1002-0", "1003-0"}
	for _, id := range ids {
		_, err := rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: "stream-range",
			ID:     id,
			Values: []string{"v", id},
		}).Result()
		require.NoError(t, err)
	}

	all, err := rdb.XRange(ctx, "stream-range", "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, all, 4)

	inclusive, err := rdb.XRange(ctx, "stream-range", "1001-0", "1002-0").Result()
	require.NoError(t, err)
	require.Len(t, inclusive, 2)
	require.Equal(t, "1001-0", inclusive[0].ID)
	require.Equal(t, "1002-0", inclusive[1].ID)

	exclusiveStart, err := rdb.Do(ctx, "XRANGE", "stream-range", "(1001-0", "+").Slice()
	require.NoError(t, err)
	require.Len(t, exclusiveStart, 2)

	exclusiveEnd, err := rdb.Do(ctx, "XRANGE", "stream-range", "-", "(1002-0").Slice()
	require.NoError(t, err)
	require.Len(t, exclusiveEnd, 2)

	rev, err := rdb.XRevRange(ctx, "stream-range", "+", "-").Result()
	require.NoError(t, err)
	require.Len(t, rev, 4)
	require.Equal(t, "1003-0", rev[0].ID)
	require.Equal(t, "1000-0", rev[3].ID)
}

func TestRedis_StreamMigrationFromLegacyBlob(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	// Replace the registry-less zero observer with a local counter-only
	// registry so we can assert the legacy-read counter moves.
	registry := monitoring.NewRegistry("n1", "127.0.0.1:0")
	nodes[0].redisServer.streamLegacyReadObserver = registry.StreamLegacyFormatReadObserver()

	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()
	ctx := context.Background()

	// Seed a legacy blob directly via the store, bypassing the adapter.
	key := []byte("legacy-stream")
	legacy := redisStreamValue{Entries: []redisStreamEntry{
		newRedisStreamEntry("1700000000000-0", []string{"event", "a"}),
		newRedisStreamEntry("1700000000000-5", []string{"event", "b"}),
	}}
	payload, err := marshalStreamValue(legacy)
	require.NoError(t, err)
	require.NoError(t, nodes[0].redisServer.store.PutAt(ctx, redisStreamKey(key), payload, uint64(time.Now().UnixNano()), 0))

	// XREAD from a legacy stream serves via the legacy path.
	streams, err := rdb.XRead(ctx, &redis.XReadArgs{
		Streams: []string{"legacy-stream", "0"},
		Count:   10,
	}).Result()
	require.NoError(t, err)
	require.Len(t, streams, 1)
	require.Len(t, streams[0].Messages, 2)
	require.Equal(t, "1700000000000-0", streams[0].Messages[0].ID)
	require.Equal(t, int64(1), gatherLegacyReads(t, registry))

	// XADD converts to the new layout in the same transaction.
	newID, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "legacy-stream",
		ID:     "1700000000001-0",
		Values: []string{"event", "c"},
	}).Result()
	require.NoError(t, err)
	require.Equal(t, "1700000000001-0", newID)

	// The legacy blob must be gone post-migration.
	readTS := uint64(time.Now().UnixNano()) << 1
	_, getErr := nodes[0].redisServer.store.GetAt(ctx, redisStreamKey(key), readTS)
	require.Error(t, getErr)

	// Subsequent XREAD serves from the new layout; counter does not move.
	streams, err = rdb.XRead(ctx, &redis.XReadArgs{
		Streams: []string{"legacy-stream", "0"},
		Count:   10,
	}).Result()
	require.NoError(t, err)
	require.Len(t, streams[0].Messages, 3)
	require.Equal(t, int64(1), gatherLegacyReads(t, registry))

	// XLEN reports 3, not 5 (no double count from the migration).
	xlen, err := rdb.XLen(ctx, "legacy-stream").Result()
	require.NoError(t, err)
	require.Equal(t, int64(3), xlen)

	// Auto-ID remains strictly monotonic: XADD '*' must produce an ID
	// greater than the pre-migration last ID.
	autoID, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "legacy-stream",
		ID:     "*",
		Values: []string{"event", "d"},
	}).Result()
	require.NoError(t, err)
	require.Greater(t, autoID, "1700000000001-0")
}

// TestRedis_StreamAutoIDMonotonicAfterTrim verifies that XTRIM removing the
// current tail does not reset XADD '*' — the LastMs/LastSeq in the meta
// record must preserve the highest ID ever assigned.
func TestRedis_StreamAutoIDMonotonicAfterTrim(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()
	ctx := context.Background()

	ceiling := "9999999999999-0"
	_, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "stream-auto",
		ID:     ceiling,
		Values: []string{"k", "v"},
	}).Result()
	require.NoError(t, err)

	trimmed, err := rdb.Do(ctx, "XTRIM", "stream-auto", "MAXLEN", "0").Int64()
	require.NoError(t, err)
	require.Equal(t, int64(1), trimmed)

	// With the tail trimmed and length==0, `*` must still produce an ID
	// strictly greater than the previous ceiling.
	id, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "stream-auto",
		ID:     "*",
		Values: []string{"k", "v2"},
	}).Result()
	require.NoError(t, err)
	require.Greater(t, id, ceiling)
}

func gatherLegacyReads(t *testing.T, registry *monitoring.Registry) int64 {
	t.Helper()
	mfs, err := registry.Gatherer().Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != "elastickv_stream_legacy_format_reads_total" {
			continue
		}
		var total float64
		for _, m := range mf.GetMetric() {
			if c := m.GetCounter(); c != nil {
				total += c.GetValue()
			}
		}
		return int64(total)
	}
	return 0
}

