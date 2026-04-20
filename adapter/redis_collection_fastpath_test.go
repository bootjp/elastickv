package adapter

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// These tests pin the semantics of the wide-column fast paths for
// HGET, HEXISTS, and SISMEMBER introduced in PR #565. Each command
// now probes the specific field / member key directly and falls back
// to the keyTypeAt + loadHash/SetAt slow path on miss, so we need
// end-to-end coverage for:
//
//   - fast-path hit (wide-column present, TTL alive)
//   - TTL-expired hit (wide-column row physically present but the
//     hash/set has a past EXPIRE -- must return nil / 0)
//   - miss (nonexistent user key -- must return nil / 0)
//   - WRONGTYPE (user key exists but as a string, not the expected
//     collection type)
//
// The tests deliberately speak Redis RESP rather than calling
// hashFieldFastLookup / setMemberFastExists directly, so they also
// cover the handler wiring above the helpers (WriteBulk vs
// WriteBulkString path for HGET, integer protocol for HEXISTS /
// SISMEMBER, etc.).

const collectionFastPathTTL = 80 * time.Millisecond

// waitForTTLExpiry sleeps past a short TTL so the next read sees the
// key as expired. A small headroom accounts for wall-clock jitter on
// CI; tests that are sensitive to this use short TTLs (~80 ms) so the
// delay stays well under a second.
func waitForTTLExpiry() {
	time.Sleep(collectionFastPathTTL + 50*time.Millisecond)
}

func TestRedis_HGET_FastPathHit(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.HSet(ctx, "h:fast", "field", "value").Err())

	got, err := rdb.HGet(ctx, "h:fast", "field").Result()
	require.NoError(t, err)
	require.Equal(t, "value", got)
}

func TestRedis_HGET_Miss(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	_, err := rdb.HGet(ctx, "h:missing", "field").Result()
	require.ErrorIs(t, err, redis.Nil, "HGET on a nonexistent key must return nil")
}

func TestRedis_HGET_UnknownField(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.HSet(ctx, "h:known", "a", "1").Err())
	_, err := rdb.HGet(ctx, "h:known", "b").Result()
	require.ErrorIs(t, err, redis.Nil, "HGET on an existing hash but unknown field must return nil")
}

func TestRedis_HGET_WRONGTYPE(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.Set(ctx, "h:str", "not-a-hash", 0).Err())
	_, err := rdb.HGet(ctx, "h:str", "field").Result()
	require.Error(t, err, "HGET on a string key must return WRONGTYPE")
	require.Contains(t, err.Error(), "WRONGTYPE")
}

func TestRedis_HGET_TTLExpired(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.HSet(ctx, "h:ttl", "field", "value").Err())
	require.NoError(t, rdb.PExpire(ctx, "h:ttl", collectionFastPathTTL).Err())
	waitForTTLExpiry()

	_, err := rdb.HGet(ctx, "h:ttl", "field").Result()
	require.ErrorIs(t, err, redis.Nil, "HGET on an expired hash must return nil")
}

func TestRedis_HEXISTS_FastPathHit(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.HSet(ctx, "he:fast", "field", "v").Err())

	ok, err := rdb.HExists(ctx, "he:fast", "field").Result()
	require.NoError(t, err)
	require.True(t, ok)
}

func TestRedis_HEXISTS_Miss(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	ok, err := rdb.HExists(ctx, "he:missing", "field").Result()
	require.NoError(t, err)
	require.False(t, ok, "HEXISTS on a nonexistent key must return 0")
}

func TestRedis_HEXISTS_UnknownField(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.HSet(ctx, "he:known", "a", "1").Err())
	ok, err := rdb.HExists(ctx, "he:known", "b").Result()
	require.NoError(t, err)
	require.False(t, ok, "HEXISTS on an existing hash but unknown field must return 0")
}

func TestRedis_HEXISTS_WRONGTYPE(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.Set(ctx, "he:str", "x", 0).Err())
	_, err := rdb.HExists(ctx, "he:str", "field").Result()
	require.Error(t, err)
	require.Contains(t, err.Error(), "WRONGTYPE")
}

func TestRedis_HEXISTS_TTLExpired(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.HSet(ctx, "he:ttl", "field", "v").Err())
	require.NoError(t, rdb.PExpire(ctx, "he:ttl", collectionFastPathTTL).Err())
	waitForTTLExpiry()

	ok, err := rdb.HExists(ctx, "he:ttl", "field").Result()
	require.NoError(t, err)
	require.False(t, ok, "HEXISTS on an expired hash must return 0")
}

func TestRedis_SISMEMBER_FastPathHit(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.SAdd(ctx, "s:fast", "member").Err())

	ok, err := rdb.SIsMember(ctx, "s:fast", "member").Result()
	require.NoError(t, err)
	require.True(t, ok)
}

func TestRedis_SISMEMBER_Miss(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	ok, err := rdb.SIsMember(ctx, "s:missing", "member").Result()
	require.NoError(t, err)
	require.False(t, ok, "SISMEMBER on a nonexistent set must return 0")
}

func TestRedis_SISMEMBER_UnknownMember(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.SAdd(ctx, "s:known", "a").Err())
	ok, err := rdb.SIsMember(ctx, "s:known", "b").Result()
	require.NoError(t, err)
	require.False(t, ok, "SISMEMBER on an existing set but unknown member must return 0")
}

func TestRedis_SISMEMBER_WRONGTYPE(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.Set(ctx, "s:str", "x", 0).Err())
	_, err := rdb.SIsMember(ctx, "s:str", "member").Result()
	require.Error(t, err)
	require.Contains(t, err.Error(), "WRONGTYPE")
}

func TestRedis_SISMEMBER_TTLExpired(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.SAdd(ctx, "s:ttl", "member").Err())
	require.NoError(t, rdb.PExpire(ctx, "s:ttl", collectionFastPathTTL).Err())
	waitForTTLExpiry()

	ok, err := rdb.SIsMember(ctx, "s:ttl", "member").Result()
	require.NoError(t, err)
	require.False(t, ok, "SISMEMBER on an expired set must return 0")
}

// TestRedis_HGET_FastPathGuardDualEncoding exercises the string-
// priority guard (hasHigherPriorityStringEncoding) at the HGET
// wire-protocol level: a key holding both a hash wide-column row AND
// a string must report WRONGTYPE, not the hash value.
//
// The scenario is only reachable under data-corruption recovery; we
// drive it here by overwriting a pre-existing hash with a SET, which
// replaceWithStringTxn normally cleans the hash keys for, but by the
// time SET commits the hash field row may still be observable at the
// readTS for this HGET. The test tolerates either "WRONGTYPE" or the
// old hash value being returned, since the exact race window is tiny,
// but it must NOT panic or silently leak wide-column data as a string
// hit: the guard is what keeps that outcome consistent with
// keyTypeAt semantics.
func TestRedis_HGET_FastPathGuardDualEncodingSmoke(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.HSet(ctx, "dual:key", "field", "hashVal").Err())
	require.NoError(t, rdb.Set(ctx, "dual:key", "stringVal", 0).Err())

	// After the SET, HGET must either see WRONGTYPE (string-priority
	// guard fires, slow path reports the key as string) or nil (the
	// hash keys were cleaned up). It must NOT return "hashVal".
	got, err := rdb.HGet(ctx, "dual:key", "field").Result()
	if err != nil {
		require.Contains(t, err.Error(), "WRONGTYPE")
		return
	}
	require.NotEqual(t, "hashVal", got, "fast path must not leak collection data when a string has taken over the key")
}
