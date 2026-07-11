package adapter

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

func TestRedisCollectionExpireWritesInlineMetaTTL(t *testing.T) {
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	server := nodes[0].redisServer
	ttl := 30 * time.Second
	cases := []struct {
		name     string
		keyBase  string
		create   func(string) error
		metaKey  func([]byte) []byte
		expireAt func([]byte) (uint64, error)
	}{
		{
			name:    "hash",
			keyBase: "ttl:inline:hash",
			create:  func(key string) error { return rdb.HSet(ctx, key, "field", "value").Err() },
			metaKey: store.HashMetaKey,
			expireAt: func(raw []byte) (uint64, error) {
				meta, err := store.UnmarshalHashMeta(raw)
				return meta.ExpireAt, err
			},
		},
		{
			name:    "set",
			keyBase: "ttl:inline:set",
			create:  func(key string) error { return rdb.SAdd(ctx, key, "member").Err() },
			metaKey: store.SetMetaKey,
			expireAt: func(raw []byte) (uint64, error) {
				meta, err := store.UnmarshalSetMeta(raw)
				return meta.ExpireAt, err
			},
		},
		{
			name:    "zset",
			keyBase: "ttl:inline:zset",
			create: func(key string) error {
				return rdb.ZAdd(ctx, key, redis.Z{Score: 1, Member: "member"}).Err()
			},
			metaKey: store.ZSetMetaKey,
			expireAt: func(raw []byte) (uint64, error) {
				meta, err := store.UnmarshalZSetMeta(raw)
				return meta.ExpireAt, err
			},
		},
		{
			name:    "list",
			keyBase: "ttl:inline:list",
			create:  func(key string) error { return rdb.RPush(ctx, key, "item").Err() },
			metaKey: store.ListMetaKey,
			expireAt: func(raw []byte) (uint64, error) {
				meta, err := store.UnmarshalListMeta(raw)
				return meta.ExpireAt, err
			},
		},
		{
			name:    "stream",
			keyBase: "ttl:inline:stream",
			create: func(key string) error {
				return rdb.XAdd(ctx, &redis.XAddArgs{
					Stream: key,
					Values: map[string]any{"field": "value"},
				}).Err()
			},
			metaKey: store.StreamMetaKey,
			expireAt: func(raw []byte) (uint64, error) {
				meta, err := store.UnmarshalStreamMeta(raw)
				return meta.ExpireAt, err
			},
		},
	}
	modes := []struct {
		name   string
		expire func(string) error
	}{
		{
			name: "direct",
			expire: func(key string) error {
				return rdb.PExpire(ctx, key, ttl).Err()
			},
		},
		{
			name: "multi",
			expire: func(key string) error {
				pipe := rdb.TxPipeline()
				pipe.PExpire(ctx, key, ttl)
				_, err := pipe.Exec(ctx)
				return err
			},
		},
		{
			name: "lua",
			expire: func(key string) error {
				_, err := rdb.Eval(ctx, `return redis.call("pexpire", KEYS[1], ARGV[1])`, []string{key}, int(ttl/time.Millisecond)).Result()
				return err
			},
		},
	}

	for _, tc := range cases {
		for _, mode := range modes {
			t.Run(tc.name+"/"+mode.name, func(t *testing.T) {
				key := tc.keyBase + ":" + mode.name
				require.NoError(t, tc.create(key))
				require.NoError(t, mode.expire(key))

				readTS := server.readTS()
				raw, err := server.store.GetAt(ctx, tc.metaKey([]byte(key)), readTS)
				require.NoError(t, err)
				expireAtMs, err := tc.expireAt(raw)
				require.NoError(t, err)
				require.Greater(t, expireAtMs, redisExpireAtMillis(time.Now().Add(ttl/2)))

				commitTS, err := server.coordinator.Clock().NextFenced()
				require.NoError(t, err)
				require.NoError(t, server.store.DeleteAt(ctx, redisTTLKey([]byte(key)), commitTS))

				got, err := server.ttlAt(ctx, []byte(key), commitTS)
				require.NoError(t, err)
				require.NotNil(t, got, "ttlAt should read inline metadata after the scan index is deleted")
				require.Equal(t, expireAtMs, redisExpireAtMillis(*got))
			})
		}
	}
}

func TestRedisCollectionExpireHandlesLegacyBlobs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	expireAt := time.Now().Add(time.Hour)
	cases := []struct {
		name      string
		typ       redisValueType
		legacyKey func([]byte) []byte
		payload   func(t *testing.T) []byte
	}{
		{
			name:      "hash",
			typ:       redisTypeHash,
			legacyKey: redisHashKey,
			payload: func(t *testing.T) []byte {
				t.Helper()
				raw, err := marshalHashValue(redisHashValue{"field": "value"})
				require.NoError(t, err)
				return raw
			},
		},
		{
			name:      "set",
			typ:       redisTypeSet,
			legacyKey: redisSetKey,
			payload: func(t *testing.T) []byte {
				t.Helper()
				raw, err := marshalSetValue(redisSetValue{Members: []string{"member"}})
				require.NoError(t, err)
				return raw
			},
		},
		{
			name:      "zset",
			typ:       redisTypeZSet,
			legacyKey: redisZSetKey,
			payload: func(t *testing.T) []byte {
				t.Helper()
				raw, err := marshalZSetValue(redisZSetValue{Entries: []redisZSetEntry{{Member: "member", Score: 1}}})
				require.NoError(t, err)
				return raw
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := store.NewMVCCStore()
			server := NewRedisServer(nil, "", st, newLocalAdapterCoordinator(st), nil, nil)
			key := []byte("ttl:legacy:" + tc.name)
			require.NoError(t, st.PutAt(ctx, tc.legacyKey(key), tc.payload(t), 1, 0))

			typ, err := server.rawKeyTypeAt(ctx, key, 1)
			require.NoError(t, err)
			require.Equal(t, tc.typ, typ)
			applied, err := server.dispatchExpireForType(ctx, key, 1, typ, expireAt)
			require.NoError(t, err)
			require.True(t, applied)

			readTS := st.LastCommitTS()
			rawTTL, err := st.GetAt(ctx, redisTTLKey(key), readTS)
			require.NoError(t, err)
			ttl, err := decodeRedisTTL(rawTTL)
			require.NoError(t, err)
			require.Equal(t, redisExpireAtMillis(expireAt), redisExpireAtMillis(ttl))
			got, err := server.ttlAt(ctx, key, readTS)
			require.NoError(t, err)
			require.NotNil(t, got)
			require.Equal(t, redisExpireAtMillis(expireAt), redisExpireAtMillis(*got))
		})
	}
}

func TestRedisTTLAtInlineNoTTLCollectionMetaSkipsLegacyFallback(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	expiredAt := time.Now().Add(-time.Hour)
	cases := []struct {
		name    string
		metaKey func([]byte) []byte
		value   func(t *testing.T) []byte
	}{
		{
			name:    "list",
			metaKey: store.ListMetaKey,
			value: func(t *testing.T) []byte {
				t.Helper()
				raw, err := store.MarshalListMeta(store.ListMeta{Len: 1})
				require.NoError(t, err)
				return raw
			},
		},
		{
			name:    "hash",
			metaKey: store.HashMetaKey,
			value: func(t *testing.T) []byte {
				t.Helper()
				return store.MarshalHashMeta(store.HashMeta{Len: 1})
			},
		},
		{
			name:    "set",
			metaKey: store.SetMetaKey,
			value: func(t *testing.T) []byte {
				t.Helper()
				return store.MarshalSetMeta(store.SetMeta{Len: 1})
			},
		},
		{
			name:    "zset",
			metaKey: store.ZSetMetaKey,
			value: func(t *testing.T) []byte {
				t.Helper()
				return store.MarshalZSetMeta(store.ZSetMeta{Len: 1})
			},
		},
		{
			name:    "stream",
			metaKey: store.StreamMetaKey,
			value: func(t *testing.T) []byte {
				t.Helper()
				raw, err := store.MarshalStreamMeta(store.StreamMeta{Length: 1})
				require.NoError(t, err)
				return raw
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := store.NewMVCCStore()
			server := &RedisServer{store: st}
			key := []byte("ttl:no-ttl-inline:" + tc.name)
			require.NoError(t, st.PutAt(ctx, tc.metaKey(key), tc.value(t), 1, 0))
			require.NoError(t, st.PutAt(ctx, redisTTLKey(key), encodeRedisTTL(expiredAt), 2, 0))

			got, err := server.ttlAt(ctx, key, 2)
			require.NoError(t, err)
			require.Nil(t, got)
			expired, err := server.hasExpired(ctx, key, 2, true)
			require.NoError(t, err)
			require.False(t, expired)
		})
	}
}

func TestRedisCollectionExpireAllowsDeltaHeavyCollections(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	expireAt := time.Now().Add(time.Hour)
	cases := []struct {
		name         string
		typ          redisValueType
		metaKey      func([]byte) []byte
		baseValue    func(t *testing.T) []byte
		deltaKey     func([]byte, uint64) []byte
		deltaValue   []byte
		metaExpireAt func([]byte) (uint64, error)
	}{
		{
			name:    "list",
			typ:     redisTypeList,
			metaKey: store.ListMetaKey,
			baseValue: func(t *testing.T) []byte {
				t.Helper()
				raw, err := store.MarshalListMeta(store.ListMeta{Head: 0, Len: 1})
				require.NoError(t, err)
				return raw
			},
			deltaKey: func(key []byte, ts uint64) []byte {
				return store.ListMetaDeltaKey(key, ts, 0)
			},
			deltaValue: store.MarshalListMetaDelta(store.ListMetaDelta{HeadDelta: 0, LenDelta: 1}),
			metaExpireAt: func(raw []byte) (uint64, error) {
				meta, err := store.UnmarshalListMeta(raw)
				return meta.ExpireAt, err
			},
		},
		{
			name:    "hash",
			typ:     redisTypeHash,
			metaKey: store.HashMetaKey,
			baseValue: func(t *testing.T) []byte {
				t.Helper()
				return store.MarshalHashMeta(store.HashMeta{Len: 1})
			},
			deltaKey: func(key []byte, ts uint64) []byte {
				return store.HashMetaDeltaKey(key, ts, 0)
			},
			deltaValue: store.MarshalHashMetaDelta(store.HashMetaDelta{LenDelta: 1}),
			metaExpireAt: func(raw []byte) (uint64, error) {
				meta, err := store.UnmarshalHashMeta(raw)
				return meta.ExpireAt, err
			},
		},
		{
			name:    "set",
			typ:     redisTypeSet,
			metaKey: store.SetMetaKey,
			baseValue: func(t *testing.T) []byte {
				t.Helper()
				return store.MarshalSetMeta(store.SetMeta{Len: 1})
			},
			deltaKey: func(key []byte, ts uint64) []byte {
				return store.SetMetaDeltaKey(key, ts, 0)
			},
			deltaValue: store.MarshalSetMetaDelta(store.SetMetaDelta{LenDelta: 1}),
			metaExpireAt: func(raw []byte) (uint64, error) {
				meta, err := store.UnmarshalSetMeta(raw)
				return meta.ExpireAt, err
			},
		},
		{
			name:    "zset",
			typ:     redisTypeZSet,
			metaKey: store.ZSetMetaKey,
			baseValue: func(t *testing.T) []byte {
				t.Helper()
				return store.MarshalZSetMeta(store.ZSetMeta{Len: 1})
			},
			deltaKey: func(key []byte, ts uint64) []byte {
				return store.ZSetMetaDeltaKey(key, ts, 0)
			},
			deltaValue: store.MarshalZSetMetaDelta(store.ZSetMetaDelta{LenDelta: 1}),
			metaExpireAt: func(raw []byte) (uint64, error) {
				meta, err := store.UnmarshalZSetMeta(raw)
				return meta.ExpireAt, err
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := store.NewMVCCStore()
			coord := newLocalAdapterCoordinator(st)
			compactor := NewDeltaCompactor(st, coord)
			server := &RedisServer{store: st, coordinator: coord, compactor: compactor}
			key := []byte("ttl:delta-heavy:" + tc.name)
			require.NoError(t, st.PutAt(ctx, tc.metaKey(key), tc.baseValue(t), 1, 0))
			const firstDeltaTS uint64 = 2
			const lastDeltaTS uint64 = store.MaxDeltaScanLimit + firstDeltaTS
			for ts := firstDeltaTS; ts <= lastDeltaTS; ts++ {
				require.NoError(t, st.PutAt(ctx, tc.deltaKey(key, ts), tc.deltaValue, ts, 0))
			}

			readTS := st.LastCommitTS()
			applied, err := server.dispatchCollectionExpire(ctx, key, readTS, tc.typ, expireAt)
			require.NoError(t, err)
			require.True(t, applied)

			readTS = st.LastCommitTS()
			raw, err := st.GetAt(ctx, tc.metaKey(key), readTS)
			require.NoError(t, err)
			expireAtMs, err := tc.metaExpireAt(raw)
			require.NoError(t, err)
			require.Equal(t, redisExpireAtMillis(expireAt), expireAtMs)
			rawTTL, err := st.GetAt(ctx, redisTTLKey(key), readTS)
			require.NoError(t, err)
			ttl, err := decodeRedisTTL(rawTTL)
			require.NoError(t, err)
			require.Equal(t, redisExpireAtMillis(expireAt), redisExpireAtMillis(ttl))
			select {
			case req := <-compactor.urgentCh:
				require.Equal(t, tc.name, req.typeName)
				require.Equal(t, key, req.userKey)
			default:
				t.Fatalf("expected urgent compaction request for %s", tc.name)
			}
		})
	}
}

func TestRedisCollectionExpireTriggersCompactionForDeltaOnlyTruncatedCollections(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	expireAt := time.Now().Add(time.Hour)
	cases := []struct {
		name       string
		typ        redisValueType
		deltaKey   func([]byte, uint64) []byte
		deltaValue []byte
	}{
		{
			name:       "list",
			typ:        redisTypeList,
			deltaKey:   func(key []byte, ts uint64) []byte { return store.ListMetaDeltaKey(key, ts, 0) },
			deltaValue: store.MarshalListMetaDelta(store.ListMetaDelta{LenDelta: 1}),
		},
		{
			name:       "hash",
			typ:        redisTypeHash,
			deltaKey:   func(key []byte, ts uint64) []byte { return store.HashMetaDeltaKey(key, ts, 0) },
			deltaValue: store.MarshalHashMetaDelta(store.HashMetaDelta{LenDelta: 1}),
		},
		{
			name:       "set",
			typ:        redisTypeSet,
			deltaKey:   func(key []byte, ts uint64) []byte { return store.SetMetaDeltaKey(key, ts, 0) },
			deltaValue: store.MarshalSetMetaDelta(store.SetMetaDelta{LenDelta: 1}),
		},
		{
			name:       "zset",
			typ:        redisTypeZSet,
			deltaKey:   func(key []byte, ts uint64) []byte { return store.ZSetMetaDeltaKey(key, ts, 0) },
			deltaValue: store.MarshalZSetMetaDelta(store.ZSetMetaDelta{LenDelta: 1}),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := store.NewMVCCStore()
			coord := newLocalAdapterCoordinator(st)
			compactor := NewDeltaCompactor(st, coord)
			server := &RedisServer{store: st, coordinator: coord, compactor: compactor}
			key := []byte("ttl:delta-only-truncated:" + tc.name)
			for ts := uint64(1); ts <= store.MaxDeltaScanLimit+1; ts++ {
				require.NoError(t, st.PutAt(ctx, tc.deltaKey(key, ts), tc.deltaValue, ts, 0))
			}

			applied, err := server.dispatchCollectionExpire(ctx, key, st.LastCommitTS(), tc.typ, expireAt)
			require.ErrorIs(t, err, ErrDeltaScanTruncated)
			require.False(t, applied)
			_, err = st.GetAt(ctx, redisTTLKey(key), st.LastCommitTS())
			require.ErrorIs(t, err, store.ErrKeyNotFound)

			select {
			case req := <-compactor.urgentCh:
				require.Equal(t, tc.name, req.typeName)
				require.Equal(t, key, req.userKey)
			default:
				t.Fatalf("expected urgent compaction request for %s", tc.name)
			}
		})
	}
}

func TestRedisMultiExecStagedCollectionCreateWritesInlineTTL(t *testing.T) {
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	server := nodes[0].redisServer
	ttl := 50 * time.Second
	cases := []struct {
		name         string
		key          string
		commands     [][]any
		metaKey      func([]byte) []byte
		metaExpireAt func([]byte) (uint64, error)
	}{
		{
			name:     "list",
			key:      "ttl:multi-create:list",
			commands: [][]any{{"RPUSH", "ttl:multi-create:list", "value"}},
			metaKey:  store.ListMetaKey,
			metaExpireAt: func(raw []byte) (uint64, error) {
				meta, err := store.UnmarshalListMeta(raw)
				return meta.ExpireAt, err
			},
		},
		{
			name:     "hash",
			key:      "ttl:multi-create:hash",
			commands: [][]any{{"HSET", "ttl:multi-create:hash", "field", "value"}},
			metaKey:  store.HashMetaKey,
			metaExpireAt: func(raw []byte) (uint64, error) {
				meta, err := store.UnmarshalHashMeta(raw)
				return meta.ExpireAt, err
			},
		},
		{
			name:     "zset",
			key:      "ttl:multi-create:zset",
			commands: [][]any{{"ZINCRBY", "ttl:multi-create:zset", "1", "member"}},
			metaKey:  store.ZSetMetaKey,
			metaExpireAt: func(raw []byte) (uint64, error) {
				meta, err := store.UnmarshalZSetMeta(raw)
				return meta.ExpireAt, err
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, rdb.Do(ctx, "MULTI").Err())
			for _, cmd := range tc.commands {
				require.Equal(t, "QUEUED", rdb.Do(ctx, cmd...).Val())
			}
			require.Equal(t, "QUEUED", rdb.Do(ctx, "PEXPIRE", tc.key, int(ttl/time.Millisecond)).Val())
			_, err := rdb.Do(ctx, "EXEC").Result()
			require.NoError(t, err)

			key := []byte(tc.key)
			var expireAtMs uint64
			require.Eventually(t, func() bool {
				readTS := server.readTS()
				raw, err := server.store.GetAt(ctx, tc.metaKey(key), readTS)
				if err != nil {
					return false
				}
				got, err := tc.metaExpireAt(raw)
				if err != nil {
					return false
				}
				expireAtMs = got
				return got > redisExpireAtMillis(time.Now().Add(ttl/2))
			}, 5*time.Second, 20*time.Millisecond)

			commitTS, err := server.coordinator.Clock().NextFenced()
			require.NoError(t, err)
			require.NoError(t, server.store.DeleteAt(ctx, redisTTLKey(key), commitTS))
			got, err := server.ttlAt(ctx, key, commitTS)
			require.NoError(t, err)
			require.NotNil(t, got)
			require.Equal(t, expireAtMs, redisExpireAtMillis(*got))
		})
	}
}

func TestRedisXAddPreservesStreamInlineTTL(t *testing.T) {
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	server := nodes[0].redisServer
	key := "ttl:stream:xadd-preserve"
	ttl := 50 * time.Second
	require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: key,
		Values: map[string]any{"field": "one"},
	}).Err())
	require.NoError(t, rdb.PExpire(ctx, key, ttl).Err())

	readTS := server.readTS()
	raw, err := server.store.GetAt(ctx, store.StreamMetaKey([]byte(key)), readTS)
	require.NoError(t, err)
	before, err := store.UnmarshalStreamMeta(raw)
	require.NoError(t, err)
	require.Greater(t, before.ExpireAt, redisExpireAtMillis(time.Now().Add(ttl/2)))

	require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: key,
		Values: map[string]any{"field": "two"},
	}).Err())

	readTS = server.readTS()
	raw, err = server.store.GetAt(ctx, store.StreamMetaKey([]byte(key)), readTS)
	require.NoError(t, err)
	after, err := store.UnmarshalStreamMeta(raw)
	require.NoError(t, err)
	require.Equal(t, int64(2), after.Length)
	require.Equal(t, before.ExpireAt, after.ExpireAt)

	commitTS, err := server.coordinator.Clock().NextFenced()
	require.NoError(t, err)
	require.NoError(t, server.store.DeleteAt(ctx, redisTTLKey([]byte(key)), commitTS))
	got, err := server.ttlAt(ctx, []byte(key), commitTS)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, after.ExpireAt, redisExpireAtMillis(*got))
}

func TestRedisLegacyCollectionMigrationPreservesLegacyTTL(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	expireAt := time.Now().Add(time.Hour)
	cases := []struct {
		name     string
		legacy   func([]byte) []byte
		payload  func(t *testing.T) []byte
		build    func(*RedisServer, context.Context, []byte, uint64) ([]*kv.Elem[kv.OP], error)
		metaKey  func([]byte) []byte
		expireAt func([]byte) (uint64, error)
	}{
		{
			name:   "hash",
			legacy: redisHashKey,
			payload: func(t *testing.T) []byte {
				t.Helper()
				raw, err := marshalHashValue(redisHashValue{"field": "value"})
				require.NoError(t, err)
				return raw
			},
			build: func(server *RedisServer, ctx context.Context, key []byte, readTS uint64) ([]*kv.Elem[kv.OP], error) {
				return server.buildHashLegacyMigrationElems(ctx, key, readTS)
			},
			metaKey: store.HashMetaKey,
			expireAt: func(raw []byte) (uint64, error) {
				meta, err := store.UnmarshalHashMeta(raw)
				return meta.ExpireAt, err
			},
		},
		{
			name:   "set",
			legacy: redisSetKey,
			payload: func(t *testing.T) []byte {
				t.Helper()
				raw, err := marshalSetValue(redisSetValue{Members: []string{"member"}})
				require.NoError(t, err)
				return raw
			},
			build: func(server *RedisServer, ctx context.Context, key []byte, readTS uint64) ([]*kv.Elem[kv.OP], error) {
				return server.buildSetLegacyMigrationElems(ctx, key, readTS)
			},
			metaKey: store.SetMetaKey,
			expireAt: func(raw []byte) (uint64, error) {
				meta, err := store.UnmarshalSetMeta(raw)
				return meta.ExpireAt, err
			},
		},
		{
			name:   "zset",
			legacy: redisZSetKey,
			payload: func(t *testing.T) []byte {
				t.Helper()
				raw, err := marshalZSetValue(redisZSetValue{Entries: []redisZSetEntry{{Member: "member", Score: 1}}})
				require.NoError(t, err)
				return raw
			},
			build: func(server *RedisServer, ctx context.Context, key []byte, readTS uint64) ([]*kv.Elem[kv.OP], error) {
				return server.buildZSetLegacyMigrationElems(ctx, key, readTS)
			},
			metaKey: store.ZSetMetaKey,
			expireAt: func(raw []byte) (uint64, error) {
				meta, err := store.UnmarshalZSetMeta(raw)
				return meta.ExpireAt, err
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := store.NewMVCCStore()
			server := NewRedisServer(nil, "", st, newLocalAdapterCoordinator(st), nil, nil)
			key := []byte("ttl:legacy-migrate:" + tc.name)
			require.NoError(t, st.PutAt(ctx, tc.legacy(key), tc.payload(t), 10, 0))
			require.NoError(t, st.PutAt(ctx, redisTTLKey(key), encodeRedisTTL(expireAt), 10, 0))

			elems, err := tc.build(server, ctx, key, 10)
			require.NoError(t, err)
			raw := elemValueForKey(t, elems, tc.metaKey(key))
			got, err := tc.expireAt(raw)
			require.NoError(t, err)
			require.Equal(t, redisExpireAtMillis(expireAt), got)
		})
	}
}

func TestRedisLegacyCollectionMigrationCleansExpiredLegacyBlob(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	expireAt := time.Now().Add(-time.Hour)
	cases := []struct {
		name    string
		legacy  func([]byte) []byte
		payload func(t *testing.T) []byte
		build   func(*RedisServer, context.Context, []byte, uint64) ([]*kv.Elem[kv.OP], error)
		metaKey func([]byte) []byte
	}{
		{
			name:   "hash",
			legacy: redisHashKey,
			payload: func(t *testing.T) []byte {
				t.Helper()
				raw, err := marshalHashValue(redisHashValue{"field": "value"})
				require.NoError(t, err)
				return raw
			},
			build: func(server *RedisServer, ctx context.Context, key []byte, readTS uint64) ([]*kv.Elem[kv.OP], error) {
				return server.buildHashLegacyMigrationElems(ctx, key, readTS)
			},
			metaKey: store.HashMetaKey,
		},
		{
			name:   "set",
			legacy: redisSetKey,
			payload: func(t *testing.T) []byte {
				t.Helper()
				raw, err := marshalSetValue(redisSetValue{Members: []string{"member"}})
				require.NoError(t, err)
				return raw
			},
			build: func(server *RedisServer, ctx context.Context, key []byte, readTS uint64) ([]*kv.Elem[kv.OP], error) {
				return server.buildSetLegacyMigrationElems(ctx, key, readTS)
			},
			metaKey: store.SetMetaKey,
		},
		{
			name:   "zset",
			legacy: redisZSetKey,
			payload: func(t *testing.T) []byte {
				t.Helper()
				raw, err := marshalZSetValue(redisZSetValue{Entries: []redisZSetEntry{{Member: "member", Score: 1}}})
				require.NoError(t, err)
				return raw
			},
			build: func(server *RedisServer, ctx context.Context, key []byte, readTS uint64) ([]*kv.Elem[kv.OP], error) {
				return server.buildZSetLegacyMigrationElems(ctx, key, readTS)
			},
			metaKey: store.ZSetMetaKey,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := store.NewMVCCStore()
			server := NewRedisServer(nil, "", st, newLocalAdapterCoordinator(st), nil, nil)
			key := []byte("ttl:legacy-expired-migrate:" + tc.name)
			require.NoError(t, st.PutAt(ctx, tc.legacy(key), tc.payload(t), 10, 0))
			require.NoError(t, st.PutAt(ctx, redisTTLKey(key), encodeRedisTTL(expireAt), 10, 0))

			elems, err := tc.build(server, ctx, key, 10)
			require.NoError(t, err)
			requireNoPutElemByKey(t, elems, tc.metaKey(key))
			require.True(t, elemDelKeysContain(elems, tc.legacy(key)))
			require.True(t, elemDelKeysContain(elems, redisTTLKey(key)))
		})
	}
}

func TestRedisHSetExpiredLegacyHashRecreatesWithoutStaleFields(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	server := NewRedisServer(nil, "", st, newLocalAdapterCoordinator(st), nil, nil)
	key := []byte("ttl:legacy-expired-hset")
	raw, err := marshalHashValue(redisHashValue{"stale": "old"})
	require.NoError(t, err)
	require.NoError(t, st.PutAt(ctx, redisHashKey(key), raw, 10, 0))
	require.NoError(t, st.PutAt(ctx, redisTTLKey(key), encodeRedisTTL(time.Now().Add(-time.Hour)), 10, 0))

	added, err := server.applyHashFieldPairs(key, [][]byte{[]byte("fresh"), []byte("new")})
	require.NoError(t, err)
	require.Equal(t, 1, added)

	readTS := st.LastCommitTS()
	value, err := server.loadHashAt(ctx, key, readTS)
	require.NoError(t, err)
	require.Equal(t, redisHashValue{"fresh": "new"}, value)
	ttl, err := server.ttlAt(ctx, key, readTS)
	require.NoError(t, err)
	require.Nil(t, ttl)
	_, err = st.GetAt(ctx, redisHashKey(key), readTS)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	_, err = st.GetAt(ctx, redisTTLKey(key), readTS)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestRedisExpiredInlineCollectionRecreateCleansPhysicalRows(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	expiredAt := time.Now().Add(-time.Hour)
	expiredMs := redisExpireAtMillis(expiredAt)

	t.Run("list", func(t *testing.T) {
		t.Parallel()

		st := store.NewMVCCStore()
		server := NewRedisServer(nil, "", st, newLocalAdapterCoordinator(st), nil, nil)
		key := []byte("ttl:inline-expired-recreate:list")
		metaRaw, err := store.MarshalListMeta(store.ListMeta{Head: 0, Tail: 1, Len: 1, ExpireAt: expiredMs})
		require.NoError(t, err)
		require.NoError(t, st.PutAt(ctx, store.ListMetaKey(key), metaRaw, 10, 0))
		require.NoError(t, st.PutAt(ctx, store.ListItemKey(key, 0), []byte("old"), 10, 0))
		require.NoError(t, st.PutAt(ctx, redisTTLKey(key), encodeRedisTTL(expiredAt), 10, 0))

		n, err := server.listRPush(ctx, key, [][]byte{[]byte("new")})
		require.NoError(t, err)
		require.Equal(t, int64(1), n)

		readTS := st.LastCommitTS()
		values, err := server.listValuesAt(ctx, key, readTS)
		require.NoError(t, err)
		require.Equal(t, []string{"new"}, values)
		requireNoTTLAt(t, server, st, key, readTS)
	})

	t.Run("hash", func(t *testing.T) {
		t.Parallel()

		st := store.NewMVCCStore()
		server := NewRedisServer(nil, "", st, newLocalAdapterCoordinator(st), nil, nil)
		key := []byte("ttl:inline-expired-recreate:hash")
		require.NoError(t, st.PutAt(ctx, store.HashMetaKey(key), store.MarshalHashMeta(store.HashMeta{Len: 1, ExpireAt: expiredMs}), 10, 0))
		require.NoError(t, st.PutAt(ctx, store.HashFieldKey(key, []byte("old")), []byte("stale"), 10, 0))
		require.NoError(t, st.PutAt(ctx, redisTTLKey(key), encodeRedisTTL(expiredAt), 10, 0))

		added, err := server.applyHashFieldPairs(key, [][]byte{[]byte("fresh"), []byte("new")})
		require.NoError(t, err)
		require.Equal(t, 1, added)

		readTS := st.LastCommitTS()
		value, err := server.loadHashAt(ctx, key, readTS)
		require.NoError(t, err)
		require.Equal(t, redisHashValue{"fresh": "new"}, value)
		requireMissingAt(t, st, store.HashFieldKey(key, []byte("old")), readTS)
		requireNoTTLAt(t, server, st, key, readTS)
	})

	t.Run("set", func(t *testing.T) {
		t.Parallel()

		st := store.NewMVCCStore()
		server := NewRedisServer(nil, "", st, newLocalAdapterCoordinator(st), nil, nil)
		key := []byte("ttl:inline-expired-recreate:set")
		require.NoError(t, st.PutAt(ctx, store.SetMetaKey(key), store.MarshalSetMeta(store.SetMeta{Len: 1, ExpireAt: expiredMs}), 10, 0))
		require.NoError(t, st.PutAt(ctx, store.SetMemberKey(key, []byte("old")), []byte{}, 10, 0))
		require.NoError(t, st.PutAt(ctx, redisTTLKey(key), encodeRedisTTL(expiredAt), 10, 0))

		conn := &recordingConn{}
		server.sadd(conn, redcon.Command{Args: [][]byte{[]byte(cmdSAdd), key, []byte("new")}})
		require.Empty(t, conn.err)
		require.Equal(t, int64(1), conn.int)

		readTS := st.LastCommitTS()
		requireMissingAt(t, st, store.SetMemberKey(key, []byte("old")), readTS)
		requireExistsAt(t, st, store.SetMemberKey(key, []byte("new")), readTS)
		requireNoTTLAt(t, server, st, key, readTS)
	})

	t.Run("zset", func(t *testing.T) {
		t.Parallel()

		st := store.NewMVCCStore()
		server := NewRedisServer(nil, "", st, newLocalAdapterCoordinator(st), nil, nil)
		key := []byte("ttl:inline-expired-recreate:zset")
		require.NoError(t, st.PutAt(ctx, store.ZSetMetaKey(key), store.MarshalZSetMeta(store.ZSetMeta{Len: 1, ExpireAt: expiredMs}), 10, 0))
		require.NoError(t, st.PutAt(ctx, store.ZSetMemberKey(key, []byte("old")), store.MarshalZSetScore(1), 10, 0))
		require.NoError(t, st.PutAt(ctx, store.ZSetScoreKey(key, 1, []byte("old")), []byte{}, 10, 0))
		require.NoError(t, st.PutAt(ctx, redisTTLKey(key), encodeRedisTTL(expiredAt), 10, 0))

		conn := &recordingConn{}
		server.zadd(conn, redcon.Command{Args: [][]byte{[]byte(cmdZAdd), key, []byte("2"), []byte("new")}})
		require.Empty(t, conn.err)
		require.Equal(t, int64(1), conn.int)

		readTS := st.LastCommitTS()
		requireMissingAt(t, st, store.ZSetMemberKey(key, []byte("old")), readTS)
		requireMissingAt(t, st, store.ZSetScoreKey(key, 1, []byte("old")), readTS)
		requireExistsAt(t, st, store.ZSetMemberKey(key, []byte("new")), readTS)
		requireNoTTLAt(t, server, st, key, readTS)
	})
}

func TestRedisHIncrByExpiredLegacyHashStartsFromZero(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	server := NewRedisServer(nil, "", st, newLocalAdapterCoordinator(st), nil, nil)
	key := []byte("ttl:legacy-expired-hincrby")
	raw, err := marshalHashValue(redisHashValue{"counter": "not-an-int"})
	require.NoError(t, err)
	require.NoError(t, st.PutAt(ctx, redisHashKey(key), raw, 10, 0))
	require.NoError(t, st.PutAt(ctx, redisTTLKey(key), encodeRedisTTL(time.Now().Add(-time.Hour)), 10, 0))

	conn := &recordingConn{}
	server.hincrby(conn, redcon.Command{Args: [][]byte{[]byte(cmdHIncrBy), key, []byte("counter"), []byte("1")}})
	require.Empty(t, conn.err)
	require.Equal(t, int64(1), conn.int)

	readTS := st.LastCommitTS()
	value, err := server.loadHashAt(ctx, key, readTS)
	require.NoError(t, err)
	require.Equal(t, redisHashValue{"counter": "1"}, value)
	requireMissingAt(t, st, redisHashKey(key), readTS)
	requireNoTTLAt(t, server, st, key, readTS)
}

func TestRedisTTLInlineMigratorCleansExpiredLegacyTTLIndex(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	expireAt := time.Now().Add(-time.Hour)
	cases := []struct {
		name    string
		legacy  func([]byte) []byte
		payload func(t *testing.T) []byte
		metaKey func([]byte) []byte
	}{
		{
			name:   "hash",
			legacy: redisHashKey,
			payload: func(t *testing.T) []byte {
				t.Helper()
				raw, err := marshalHashValue(redisHashValue{"field": "value"})
				require.NoError(t, err)
				return raw
			},
			metaKey: store.HashMetaKey,
		},
		{
			name:   "set",
			legacy: redisSetKey,
			payload: func(t *testing.T) []byte {
				t.Helper()
				raw, err := marshalSetValue(redisSetValue{Members: []string{"member"}})
				require.NoError(t, err)
				return raw
			},
			metaKey: store.SetMetaKey,
		},
		{
			name:   "zset",
			legacy: redisZSetKey,
			payload: func(t *testing.T) []byte {
				t.Helper()
				raw, err := marshalZSetValue(redisZSetValue{Entries: []redisZSetEntry{{Member: "member", Score: 1}}})
				require.NoError(t, err)
				return raw
			},
			metaKey: store.ZSetMetaKey,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := store.NewMVCCStore()
			compactor := NewDeltaCompactor(st, newLocalAdapterCoordinator(st))
			key := []byte("ttl:migrator-expired-legacy:" + tc.name)
			ttlKey := redisTTLKey(key)
			ttlPayload := encodeRedisTTL(expireAt)
			require.NoError(t, st.PutAt(ctx, tc.legacy(key), tc.payload(t), 10, 0))
			require.NoError(t, st.PutAt(ctx, ttlKey, ttlPayload, 10, 0))

			elems, err := compactor.migrateTTLIndexedCollectionElems(ctx, &store.KVPair{Key: ttlKey, Value: ttlPayload}, 10)
			require.NoError(t, err)
			requireNoPutElemByKey(t, elems, tc.metaKey(key))
			require.True(t, elemDelKeysContain(elems, tc.legacy(key)))
			require.True(t, elemDelKeysContain(elems, ttlKey))
		})
	}
}

func TestRedisLegacyStreamRecreateClearsExpiredLegacyTTL(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	server := NewRedisServer(nil, "", st, newLocalAdapterCoordinator(st), nil, nil)
	key := []byte("ttl:legacy-expired-migrate:stream")
	legacy := redisStreamValue{Entries: []redisStreamEntry{
		newRedisStreamEntry("1700000000000-0", []string{"field", "value"}),
	}}
	payload, err := marshalStreamValue(legacy)
	require.NoError(t, err)
	require.NoError(t, st.PutAt(ctx, redisStreamKey(key), payload, 10, 0))
	require.NoError(t, st.PutAt(ctx, redisTTLKey(key), encodeRedisTTL(time.Now().Add(-time.Hour)), 10, 0))

	cleanup, meta, metaFound, err := server.streamWriteBase(ctx, key, 10)
	require.NoError(t, err)
	require.NotEmpty(t, cleanup)
	require.False(t, metaFound)
	require.Zero(t, meta.ExpireAt)
}

func elemValueForKey(t *testing.T, elems []*kv.Elem[kv.OP], want []byte) []byte {
	t.Helper()
	for _, elem := range elems {
		if elem.Op == kv.Put && string(elem.Key) == string(want) {
			return elem.Value
		}
	}
	t.Fatalf("missing Put elem for key %q", want)
	return nil
}

func elemDelKeysContain(elems []*kv.Elem[kv.OP], want []byte) bool {
	for _, elem := range elems {
		if elem.Op == kv.Del && string(elem.Key) == string(want) {
			return true
		}
	}
	return false
}

func requireMissingAt(t *testing.T, st store.MVCCStore, key []byte, readTS uint64) {
	t.Helper()
	_, err := st.GetAt(context.Background(), key, readTS)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func requireExistsAt(t *testing.T, st store.MVCCStore, key []byte, readTS uint64) {
	t.Helper()
	exists, err := st.ExistsAt(context.Background(), key, readTS)
	require.NoError(t, err)
	require.True(t, exists)
}

func requireNoTTLAt(t *testing.T, server *RedisServer, st store.MVCCStore, key []byte, readTS uint64) {
	t.Helper()
	ttl, err := server.ttlAt(context.Background(), key, readTS)
	require.NoError(t, err)
	require.Nil(t, ttl)
	requireMissingAt(t, st, redisTTLKey(key), readTS)
}

func TestRedisHDelLegacyRewritePreservesLegacyTTL(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	server := &RedisServer{store: st, coordinator: newLocalAdapterCoordinator(st)}
	key := []byte("ttl:hdel:legacy-rewrite")
	raw, err := marshalHashValue(redisHashValue{"drop": "old", "keep": "value"})
	require.NoError(t, err)
	expireAt := time.Now().Add(time.Hour)
	require.NoError(t, st.PutAt(ctx, redisHashKey(key), raw, 10, 0))
	require.NoError(t, st.PutAt(ctx, redisTTLKey(key), encodeRedisTTL(expireAt), 11, 0))

	require.NoError(t, server.persistHashTxn(ctx, key, st.LastCommitTS(), redisHashValue{"keep": "value"}))

	readTS := st.LastCommitTS()
	metaRaw, err := st.GetAt(ctx, store.HashMetaKey(key), readTS)
	require.NoError(t, err)
	meta, err := store.UnmarshalHashMeta(metaRaw)
	require.NoError(t, err)
	require.Equal(t, int64(1), meta.Len)
	require.Equal(t, redisExpireAtMillis(expireAt), meta.ExpireAt)

	require.NoError(t, st.DeleteAt(ctx, redisTTLKey(key), readTS+1))
	got, err := server.ttlAt(ctx, key, readTS+1)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, meta.ExpireAt, redisExpireAtMillis(*got))
}

func TestRedisLuaZSetDeltaTTLCompactionPreservesCardinality(t *testing.T) {
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	server := nodes[0].redisServer
	keyString := "ttl:lua-zset-delta-compaction"
	key := []byte(keyString)
	require.NoError(t, server.store.PutAt(ctx, store.ZSetMetaKey(key), store.MarshalZSetMeta(store.ZSetMeta{Len: 1}), 10, 0))
	for i := uint64(0); i < store.MaxDeltaScanLimit; i++ {
		require.NoError(t, server.store.PutAt(ctx,
			store.ZSetMetaDeltaKey(key, 11+i, 0),
			store.MarshalZSetMetaDelta(store.ZSetMetaDelta{LenDelta: 1}),
			11+i,
			0,
		))
	}

	ttl := 50 * time.Second
	got, err := rdb.Eval(ctx, `
redis.call("zadd", KEYS[1], "2", "new-member")
return redis.call("pexpire", KEYS[1], ARGV[1])
`, []string{keyString}, int(ttl/time.Millisecond)).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), got)

	readTS := server.readTS()
	raw, err := server.store.GetAt(ctx, store.ZSetMetaKey(key), readTS)
	require.NoError(t, err)
	meta, err := store.UnmarshalZSetMeta(raw)
	require.NoError(t, err)
	require.Equal(t, int64(store.MaxDeltaScanLimit+2), meta.Len)
	require.Greater(t, meta.ExpireAt, redisExpireAtMillis(time.Now().Add(ttl/2)))

	commitTS, err := server.coordinator.Clock().NextFenced()
	require.NoError(t, err)
	require.NoError(t, server.store.DeleteAt(ctx, redisTTLKey(key), commitTS))
	ttlValue, err := server.ttlAt(ctx, key, commitTS)
	require.NoError(t, err)
	require.NotNil(t, ttlValue)
	require.Equal(t, meta.ExpireAt, redisExpireAtMillis(*ttlValue))
}

func TestRedisLuaStagedCollectionCreateWritesInlineTTL(t *testing.T) {
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	server := nodes[0].redisServer
	ttl := 50 * time.Second
	cases := []struct {
		name         string
		key          string
		script       string
		metaKey      func([]byte) []byte
		metaExpireAt func([]byte) (uint64, error)
	}{
		{
			name:    "list",
			key:     "ttl:lua-create:list",
			script:  `redis.call("rpush", KEYS[1], "value"); return redis.call("pexpire", KEYS[1], ARGV[1])`,
			metaKey: store.ListMetaKey,
			metaExpireAt: func(raw []byte) (uint64, error) {
				meta, err := store.UnmarshalListMeta(raw)
				return meta.ExpireAt, err
			},
		},
		{
			name:    "hash",
			key:     "ttl:lua-create:hash",
			script:  `redis.call("hset", KEYS[1], "field", "value"); return redis.call("pexpire", KEYS[1], ARGV[1])`,
			metaKey: store.HashMetaKey,
			metaExpireAt: func(raw []byte) (uint64, error) {
				meta, err := store.UnmarshalHashMeta(raw)
				return meta.ExpireAt, err
			},
		},
		{
			name:    "set",
			key:     "ttl:lua-create:set",
			script:  `redis.call("sadd", KEYS[1], "member"); return redis.call("pexpire", KEYS[1], ARGV[1])`,
			metaKey: store.SetMetaKey,
			metaExpireAt: func(raw []byte) (uint64, error) {
				meta, err := store.UnmarshalSetMeta(raw)
				return meta.ExpireAt, err
			},
		},
		{
			name:    "zset",
			key:     "ttl:lua-create:zset",
			script:  `redis.call("zadd", KEYS[1], "1", "member"); return redis.call("pexpire", KEYS[1], ARGV[1])`,
			metaKey: store.ZSetMetaKey,
			metaExpireAt: func(raw []byte) (uint64, error) {
				meta, err := store.UnmarshalZSetMeta(raw)
				return meta.ExpireAt, err
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := rdb.Eval(ctx, tc.script, []string{tc.key}, int(ttl/time.Millisecond)).Result()
			require.NoError(t, err)
			require.Equal(t, int64(1), got)

			key := []byte(tc.key)
			var expireAtMs uint64
			require.Eventually(t, func() bool {
				readTS := server.readTS()
				raw, err := server.store.GetAt(ctx, tc.metaKey(key), readTS)
				if err != nil {
					return false
				}
				got, err := tc.metaExpireAt(raw)
				if err != nil {
					return false
				}
				expireAtMs = got
				return got > redisExpireAtMillis(time.Now().Add(ttl/2))
			}, 5*time.Second, 20*time.Millisecond)

			commitTS, err := server.coordinator.Clock().NextFenced()
			require.NoError(t, err)
			require.NoError(t, server.store.DeleteAt(ctx, redisTTLKey(key), commitTS))
			ttlValue, err := server.ttlAt(ctx, key, commitTS)
			require.NoError(t, err)
			require.NotNil(t, ttlValue)
			require.Equal(t, expireAtMs, redisExpireAtMillis(*ttlValue))
		})
	}
}
