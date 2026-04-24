package adapter

import (
	"context"
	"fmt"
	"sort"
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

	// Warm up: the first few XREADs pay cold-path costs (gRPC conn setup,
	// allocator page faults, JIT-of-sorts). We use the median of a warm
	// window as the baseline so single-ms noise on the *first* sample
	// doesn't become the whole budget.
	const warmup = 8
	warmSamples := make([]time.Duration, 0, warmup)
	for range warmup {
		warmSamples = append(warmSamples, measure())
	}
	sort.Slice(warmSamples, func(i, j int) bool { return warmSamples[i] < warmSamples[j] })
	baseline := warmSamples[len(warmSamples)/2]

	// Collect the measured window, compare the *median*, not the max —
	// max-of-100 under -race on a shared CI runner is dominated by
	// scheduler tail latency and has nothing to do with O(new) vs O(N).
	samples := make([]time.Duration, 0, probes)
	for range probes {
		samples = append(samples, measure())
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	median := samples[len(samples)/2]
	p95 := samples[(len(samples)*95)/100]

	// Threshold: median must stay within 3x baseline plus an absolute
	// floor; p95 is allowed more headroom because -race on CI runners
	// routinely shows double-digit-ms GC pauses unrelated to XREAD's
	// algorithmic class. The old blob implementation grows linearly
	// with the entry count, so for 10k entries *every* probe was 10x+
	// slower than the baseline — 3x/6x ceilings still catch that
	// regression cleanly.
	medianCeiling := 3*baseline + 20*time.Millisecond
	p95Ceiling := 6*baseline + 40*time.Millisecond
	require.LessOrEqualf(t, median, medianCeiling,
		"XREAD median latency should not grow with stream size: baseline=%s median=%s p95=%s",
		baseline, median, p95)
	require.LessOrEqualf(t, p95, p95Ceiling,
		"XREAD p95 latency should not grow with stream size: baseline=%s median=%s p95=%s",
		baseline, median, p95)
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

	// Shorthand ms-only bounds (Codex P2 regression guard).
	// `XRANGE k 0 +` and `XRANGE k 1001 1002` must work without
	// returning "ERR Invalid stream ID"; the legacy blob path accepted
	// shorthand via string-compare fallback, so migrating streams must
	// keep that contract. parseStreamBoundID expands "ms" to ms-0
	// (lower inclusive / upper exclusive) or ms-MaxUint64 (lower
	// exclusive / upper inclusive) so the half-open scan covers the
	// whole ms row.
	shortAll, err := rdb.XRange(ctx, "stream-range", "0", "+").Result()
	require.NoError(t, err, "XRANGE with shorthand lower bound 0 must succeed after migration")
	require.Len(t, shortAll, 4)

	shortRow, err := rdb.XRange(ctx, "stream-range", "1001", "1002").Result()
	require.NoError(t, err, "XRANGE with shorthand bounds ms-only must succeed")
	require.Len(t, shortRow, 2)
	require.Equal(t, "1001-0", shortRow[0].ID)
	require.Equal(t, "1002-0", shortRow[1].ID)

	shortExclusiveUpper, err := rdb.Do(ctx, "XRANGE", "stream-range", "-", "(1002").Slice()
	require.NoError(t, err, "XRANGE with shorthand exclusive upper bound must succeed")
	require.Len(t, shortExclusiveUpper, 2, "(1002 shorthand excludes all ms=1002 entries")
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
	seedTS := nowNanos(t)
	require.NoError(t, nodes[0].redisServer.store.PutAt(ctx, redisStreamKey(key), payload, seedTS, 0))

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

	// The legacy blob must be gone post-migration. Pick a readTS that is
	// clearly in the future of any commit performed above so MVCC visibility
	// never hides a still-living blob.
	readTS := nowNanos(t) + uint64(time.Minute)
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

// TestRedis_StreamMigrationWithMaxLenTrim seeds a legacy blob and issues an
// XADD with MAXLEN small enough to drop some migrated entries in the same
// transaction. The coordinator applies operations sequentially so the
// trim-path Del tombstones the migration-path Put at the same commitTS,
// and the end state matches what Redis would produce running XADD+trim on
// a native entry-per-key stream.
func TestRedis_StreamMigrationWithMaxLenTrim(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	registry := monitoring.NewRegistry("n1", "127.0.0.1:0")
	nodes[0].redisServer.streamLegacyReadObserver = registry.StreamLegacyFormatReadObserver()

	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()
	ctx := context.Background()

	key := []byte("legacy-maxlen")
	legacy := redisStreamValue{Entries: []redisStreamEntry{
		newRedisStreamEntry("1700000000000-0", []string{"i", "0"}),
		newRedisStreamEntry("1700000000000-1", []string{"i", "1"}),
		newRedisStreamEntry("1700000000000-2", []string{"i", "2"}),
		newRedisStreamEntry("1700000000000-3", []string{"i", "3"}),
		newRedisStreamEntry("1700000000000-4", []string{"i", "4"}),
	}}
	payload, err := marshalStreamValue(legacy)
	require.NoError(t, err)
	require.NoError(t, nodes[0].redisServer.store.PutAt(ctx, redisStreamKey(key), payload, nowNanos(t), 0))

	// XADD MAXLEN=2 migrates the 5 legacy entries and trims down to the
	// two newest, plus the freshly-added entry == 2 entries remain.
	// Using rdb.Do() so the MAXLEN clause lands in the exact position the
	// server-side parser expects (`XADD key MAXLEN N id field value`).
	_, err = rdb.Do(ctx, "XADD", "legacy-maxlen", "MAXLEN", "2", "1700000000000-5", "i", "5").Result()
	require.NoError(t, err)

	// The legacy blob is gone.
	_, getErr := nodes[0].redisServer.store.GetAt(ctx, redisStreamKey(key), nowNanos(t)+uint64(time.Minute))
	require.Error(t, getErr)

	xlen, err := rdb.XLen(ctx, "legacy-maxlen").Result()
	require.NoError(t, err)
	require.Equal(t, int64(2), xlen)

	entries, err := rdb.XRange(ctx, "legacy-maxlen", "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Equal(t, "1700000000000-4", entries[0].ID)
	require.Equal(t, "1700000000000-5", entries[1].ID)
}

// TestRedis_StreamMultiExecDelRemovesWideColumnLayout verifies that a
// MULTI/EXEC DEL on a migrated stream drops the wide-column meta and every
// entry row, not just the (already-empty) legacy blob key. Regression
// guard for the CodeRabbit-flagged leak where DEL reported success while
// !stream|meta|... and !stream|entry|... survived.
func TestRedis_StreamMultiExecDelRemovesWideColumnLayout(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()
	ctx := context.Background()

	key := "multi-stream-del"
	for i := range 5 {
		_, err := rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: key,
			ID:     fmt.Sprintf("%d-0", 1_700_000_000_000+i),
			Values: []string{"i", fmt.Sprint(i)},
		}).Result()
		require.NoError(t, err)
	}

	// Run the delete inside MULTI/EXEC so stageKeyDeletion is exercised.
	pipe := rdb.TxPipeline()
	pipe.Del(ctx, key)
	_, err := pipe.Exec(ctx)
	require.NoError(t, err)

	xlen, err := rdb.XLen(ctx, key).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), xlen)

	// A subsequent XADD should succeed and see an empty stream, not
	// inherit any leftover meta / entries.
	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: key,
		ID:     "1800000000000-0",
		Values: []string{"k", "v"},
	}).Result()
	require.NoError(t, err)

	entries, err := rdb.XRange(ctx, key, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, entries, 1)
}

// nowNanos returns the current UnixNano timestamp as uint64, failing the
// test if the reading is non-positive. Centralising the bounds check here
// keeps the int64->uint64 conversion safe and the individual test sites
// free of gosec waivers.
func nowNanos(t *testing.T) uint64 {
	t.Helper()
	ns := time.Now().UnixNano()
	require.Positive(t, ns)
	if ns < 0 {
		// Unreachable after require.Positive, but lets gosec see the bound.
		return 0
	}
	return uint64(ns)
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

// TestXAddEnforceMaxWideColumn is a pure-function regression guard: the
// maxWideColumnItems cap must reject unbounded XADDs on a stream that is
// already at the ceiling, but must NOT reject when the caller supplied a
// MAXLEN clause that keeps the committed length bounded.
func TestXAddEnforceMaxWideColumn(t *testing.T) {
	t.Parallel()
	key := []byte("s")
	ceiling := int64(maxWideColumnItems)

	cases := []struct {
		name     string
		length   int64
		maxLen   int
		wantFail bool
	}{
		{"below-cap-no-maxlen", ceiling - 1, -1, false},
		{"at-cap-no-maxlen", ceiling, -1, true},
		{"above-cap-no-maxlen", ceiling + 5, -1, true},
		{"at-cap-bounded-maxlen", ceiling, 10, false},
		{"at-cap-maxlen-zero", ceiling, 0, false},
		{"above-cap-bounded-maxlen", ceiling + 5, maxWideColumnItems, false},
		{"at-cap-maxlen-too-large", ceiling, maxWideColumnItems + 1, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := xaddEnforceMaxWideColumn(key, tc.length, tc.maxLen)
			if tc.wantFail {
				require.Error(t, err)
				require.ErrorIs(t, err, ErrCollectionTooLarge)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
