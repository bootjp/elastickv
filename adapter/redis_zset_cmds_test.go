package adapter

import (
	"context"
	"math"
	"testing"

	"github.com/bootjp/elastickv/monitoring"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func seedZSetScoreRowsForTest(t *testing.T, st store.MVCCStore, key []byte, commitTS uint64, entries []redisZSetEntry) {
	t.Helper()
	ctx := context.Background()
	for _, entry := range entries {
		member := []byte(entry.Member)
		require.NoError(t, st.PutAt(ctx, store.ZSetMemberKey(key, member), store.MarshalZSetScore(entry.Score), commitTS, 0))
		require.NoError(t, st.PutAt(ctx, store.ZSetScoreKey(key, entry.Score, member), []byte{}, commitTS, 0))
	}
	require.NoError(t, st.PutAt(
		ctx,
		store.ZSetMetaKey(key),
		store.MarshalZSetMeta(store.ZSetMeta{Len: int64(len(entries))}),
		commitTS,
		0,
	))
}

func TestZSetRangeByScoreFastSortsSameScoreMembers(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	srv := &RedisServer{store: st}
	key := []byte("zfast:same-score")
	seedZSetScoreRowsForTest(t, st, key, 10, []redisZSetEntry{
		{Member: "m14", Score: -3},
		{Member: "m1", Score: -3},
		{Member: "m6", Score: -3},
	})

	scorePrefix := store.ZSetScoreRangeScanPrefix(key, -3)
	got, hit, reason, err := srv.zsetRangeByScoreFast(
		ctx, key, scorePrefix, store.PrefixScanEnd(scorePrefix),
		false, 0, -1, nil, 20,
	)
	require.NoError(t, err)
	require.True(t, hit)
	require.Equal(t, monitoring.LuaFastPathFallbackNone, reason)
	require.Equal(t, []redisZSetEntry{
		{Member: "m1", Score: -3},
		{Member: "m14", Score: -3},
		{Member: "m6", Score: -3},
	}, got)

	got, hit, reason, err = srv.zsetRangeByScoreFast(
		ctx, key, scorePrefix, store.PrefixScanEnd(scorePrefix),
		true, 0, -1, nil, 20,
	)
	require.NoError(t, err)
	require.True(t, hit)
	require.Equal(t, monitoring.LuaFastPathFallbackNone, reason)
	require.Equal(t, []redisZSetEntry{
		{Member: "m6", Score: -3},
		{Member: "m14", Score: -3},
		{Member: "m1", Score: -3},
	}, got)
}

func TestZSetRangeByScoreFastFallsBackForBoundedScoreTies(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	srv := &RedisServer{store: st}
	key := []byte("zfast:bounded-tie")
	seedZSetScoreRowsForTest(t, st, key, 10, []redisZSetEntry{
		{Member: "m14", Score: 44},
		{Member: "m1", Score: 44},
		{Member: "m6", Score: 44},
	})

	scorePrefix := store.ZSetScoreRangeScanPrefix(key, 44)
	got, hit, reason, err := srv.zsetRangeByScoreFast(
		ctx, key, scorePrefix, store.PrefixScanEnd(scorePrefix),
		false, 0, 1, nil, 20,
	)
	require.NoError(t, err)
	require.False(t, hit)
	require.Equal(t, monitoring.LuaFastPathFallbackTruncated, reason)
	require.Nil(t, got)
}

func TestZSetRangeByScoreFastAppliesBoundedWindowForUniqueScores(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	srv := &RedisServer{store: st}
	key := []byte("zfast:bounded-unique")
	seedZSetScoreRowsForTest(t, st, key, 10, []redisZSetEntry{
		{Member: "m14", Score: 1},
		{Member: "m1", Score: 2},
		{Member: "m6", Score: 3},
	})

	prefix := store.ZSetScoreScanPrefix(key)
	got, hit, reason, err := srv.zsetRangeByScoreFast(
		ctx, key, prefix, store.PrefixScanEnd(prefix),
		false, 1, 2, nil, 20,
	)
	require.NoError(t, err)
	require.True(t, hit)
	require.Equal(t, monitoring.LuaFastPathFallbackNone, reason)
	require.Equal(t, []redisZSetEntry{
		{Member: "m1", Score: 2},
		{Member: "m6", Score: 3},
	}, got)
}

func TestZSetFastPathTruncatedFallsBackWhenRequestedWindowExceedsScanCap(t *testing.T) {
	t.Parallel()

	require.True(t, zsetFastPathTruncated(
		maxWideScanLimit,
		maxWideScanLimit,
		maxWideScanLimit,
		maxWideScanLimit-10,
		20,
	))
	require.False(t, zsetFastPathTruncated(
		maxWideScanLimit-1,
		maxWideScanLimit,
		maxWideScanLimit-1,
		maxWideScanLimit-10,
		20,
	))
}

func TestScoreInRangeHonorsInfinityBounds(t *testing.T) {
	t.Parallel()

	negInf := zScoreBound{kind: zBoundNegInf, inclusive: true}
	posInf := zScoreBound{kind: zBoundPosInf, inclusive: true}
	all := []redisZSetEntry{
		{Member: "finite", Score: 1},
		{Member: "neg-inf", Score: math.Inf(-1)},
		{Member: "pos-inf", Score: math.Inf(+1)},
	}

	require.Equal(t, []redisZSetEntry{
		{Member: "pos-inf", Score: math.Inf(+1)},
	}, filterZRangeByScore(all, posInf, posInf))
	require.Equal(t, []redisZSetEntry{
		{Member: "neg-inf", Score: math.Inf(-1)},
	}, filterZRangeByScore(all, negInf, negInf))
	require.False(t, scoreInRange(1, posInf, posInf))
	require.False(t, scoreInRange(1, negInf, negInf))
}
