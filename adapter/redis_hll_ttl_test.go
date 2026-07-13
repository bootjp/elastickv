package adapter

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestRedisHLLTTLAtReadsEmbeddedAfterScanIndexDeleted(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	server := &RedisServer{store: st}
	key := []byte("hll:inline-ttl")
	expireAt := time.Now().Add(time.Hour)

	payload, err := encodeRedisHLL(redisSetValue{Members: []string{"a"}}, &expireAt)
	require.NoError(t, err)
	require.NoError(t, st.PutAt(ctx, redisHLLKey(key), payload, 10, 0))
	require.NoError(t, st.PutAt(ctx, redisTTLKey(key), encodeRedisTTL(expireAt), 11, 0))
	require.NoError(t, st.DeleteAt(ctx, redisTTLKey(key), 12))

	got, err := server.ttlAt(ctx, key, 12)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, redisExpireAtMillis(expireAt), redisExpireAtMillis(*got))
}

func TestRedisHLLTTLAtIgnoresStaleCollectionTTL(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	server := &RedisServer{store: st}
	key := []byte("hll:stale-collection")
	expiredAt := time.Now().Add(-time.Minute)

	require.NoError(t, st.PutAt(ctx, store.HashMetaKey(key), store.MarshalHashMeta(store.HashMeta{
		Len:      1,
		ExpireAt: redisExpireAtMillis(expiredAt),
	}), 10, 0))
	payload, err := encodeRedisHLL(redisSetValue{Members: []string{"fresh"}}, nil)
	require.NoError(t, err)
	require.NoError(t, st.PutAt(ctx, redisHLLKey(key), payload, 11, 0))

	got, err := server.ttlAt(ctx, key, 11)
	require.NoError(t, err)
	require.Nil(t, got)

	expired, err := server.hasExpired(ctx, key, 11, false)
	require.NoError(t, err)
	require.False(t, expired)

	value, err := server.loadSetAt(ctx, hllKind, key, 11)
	require.NoError(t, err)
	require.Equal(t, []string{"fresh"}, value.Members)
}

func TestRedisLegacyHLLTTLAtIgnoresStaleCollectionTTL(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	expiredAt := time.Now().Add(-time.Minute)
	liveAt := time.Now().Add(time.Hour)

	cases := []struct {
		name           string
		writeLegacyTTL bool
		wantTTL        *time.Time
	}{
		{name: "no-ttl"},
		{name: "legacy-ttl", writeLegacyTTL: true, wantTTL: &liveAt},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := store.NewMVCCStore()
			server := &RedisServer{store: st}
			key := []byte("hll:legacy-stale-collection:" + tc.name)

			require.NoError(t, st.PutAt(ctx, store.HashMetaKey(key), store.MarshalHashMeta(store.HashMeta{
				Len:      1,
				ExpireAt: redisExpireAtMillis(expiredAt),
			}), 10, 0))
			payload, err := marshalSetValue(redisSetValue{Members: []string{"fresh"}})
			require.NoError(t, err)
			require.NoError(t, st.PutAt(ctx, redisHLLKey(key), payload, 11, 0))
			if tc.writeLegacyTTL {
				require.NoError(t, st.PutAt(ctx, redisTTLKey(key), encodeRedisTTL(*tc.wantTTL), 12, 0))
			}
			readTS := st.LastCommitTS()

			got, err := server.ttlAt(ctx, key, readTS)
			require.NoError(t, err)
			if tc.wantTTL == nil {
				require.Nil(t, got)
			} else {
				require.NotNil(t, got)
				require.Equal(t, redisExpireAtMillis(*tc.wantTTL), redisExpireAtMillis(*got))
			}

			expired, err := server.hasExpired(ctx, key, readTS, false)
			require.NoError(t, err)
			require.False(t, expired)
		})
	}
}

func TestRedisSetWideMutationBaseCleansExpiredHLLAnchor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	server := &RedisServer{store: st}
	key := []byte("hll:expired:set-recreate")
	expiredAt := time.Now().Add(-time.Minute)
	payload, err := encodeRedisHLL(redisSetValue{Members: []string{"stale"}}, &expiredAt)
	require.NoError(t, err)
	require.NoError(t, st.PutAt(ctx, redisHLLKey(key), payload, 10, 0))

	cleanup, migration, legacyBase, expiredRecreate, err := server.setWideMutationBase(ctx, key, 10, redisTypeNone)
	require.NoError(t, err)
	require.True(t, expiredRecreate)
	require.Empty(t, migration)
	require.Nil(t, legacyBase)

	var deletesHLL bool
	for _, elem := range cleanup {
		if elem.Op == kv.Del && bytes.Equal(elem.Key, redisHLLKey(key)) {
			deletesHLL = true
			break
		}
	}
	require.True(t, deletesHLL)
}

func TestRedisDispatchHLLExpireWritesInlineAnchorAndScanIndex(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := newLocalAdapterCoordinator(st)
	server := &RedisServer{store: st, coordinator: coord}
	key := []byte("hll:expire")
	legacyPayload, err := marshalSetValue(redisSetValue{Members: []string{"a", "b"}})
	require.NoError(t, err)
	require.NoError(t, st.PutAt(ctx, redisHLLKey(key), legacyPayload, 1, 0))

	expireAt := time.Now().Add(time.Hour)
	applied, err := server.dispatchHLLExpire(ctx, key, 10, expireAt)
	require.NoError(t, err)
	require.True(t, applied)

	readTS := coord.Clock().Next()
	raw, err := st.GetAt(ctx, redisHLLKey(key), readTS)
	require.NoError(t, err)
	decoded, gotTTL, embedded, err := decodeRedisHLL(raw)
	require.NoError(t, err)
	require.True(t, embedded)
	require.Equal(t, redisSetValue{Members: []string{"a", "b"}}, decoded)
	require.NotNil(t, gotTTL)
	require.Equal(t, redisExpireAtMillis(expireAt), redisExpireAtMillis(*gotTTL))

	rawTTL, err := st.GetAt(ctx, redisTTLKey(key), readTS)
	require.NoError(t, err)
	indexTTL, err := decodeRedisTTL(rawTTL)
	require.NoError(t, err)
	require.Equal(t, redisExpireAtMillis(expireAt), redisExpireAtMillis(indexTTL))
}

func TestRedisLoadHLLAtTreatsExpiredInlineTTLAsEmpty(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	server := &RedisServer{store: st}
	key := []byte("hll:expired")
	expireAt := time.Now().Add(-time.Minute)
	payload, err := encodeRedisHLL(redisSetValue{Members: []string{"stale"}}, &expireAt)
	require.NoError(t, err)
	require.NoError(t, st.PutAt(ctx, redisHLLKey(key), payload, 10, 0))

	value, err := server.loadSetAt(ctx, hllKind, key, 10)
	require.NoError(t, err)
	require.Empty(t, value.Members)
}

func TestRedisLoadHLLAtTreatsExpiredLegacyTTLAsEmpty(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	server := &RedisServer{store: st}
	key := []byte("hll:expired-legacy")
	payload, err := marshalSetValue(redisSetValue{Members: []string{"stale"}})
	require.NoError(t, err)
	require.NoError(t, st.PutAt(ctx, redisHLLKey(key), payload, 10, 0))
	require.NoError(t, st.PutAt(ctx, redisTTLKey(key), encodeRedisTTL(time.Now().Add(-time.Minute)), 11, 0))

	value, err := server.loadSetAt(ctx, hllKind, key, 11)
	require.NoError(t, err)
	require.Empty(t, value.Members)

	live, err := server.hllExistsAt(key, 11)
	require.NoError(t, err)
	require.False(t, live)
}

func TestRedisLoadHLLAtSkipsLegacyTTLWhenFallbackDisabled(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	server := &RedisServer{store: st, disableLegacyTTLReadFallback: true}
	key := []byte("hll:expired-legacy-disabled")
	payload, err := marshalSetValue(redisSetValue{Members: []string{"live"}})
	require.NoError(t, err)
	require.NoError(t, st.PutAt(ctx, redisHLLKey(key), payload, 10, 0))
	require.NoError(t, st.PutAt(ctx, redisTTLKey(key), encodeRedisTTL(time.Now().Add(-time.Minute)), 11, 0))

	value, err := server.loadSetAt(ctx, hllKind, key, 11)
	require.NoError(t, err)
	require.Equal(t, []string{"live"}, value.Members)
}
