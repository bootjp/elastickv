package adapter

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/keyviz"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/monitoring"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// newDeltaCompactorTestFixture creates a store, a coordinator, and a DeltaCompactor
// configured with a low maxCount threshold (2) so tests can trigger compaction easily.
func newDeltaCompactorTestFixture(t *testing.T) (store.MVCCStore, *DeltaCompactor) {
	t.Helper()
	st := store.NewMVCCStore()
	coord := newLocalAdapterCoordinator(st)
	c := NewDeltaCompactor(st, coord, WithDeltaCompactorMaxDeltaCount(2))
	return st, c
}

func TestDeltaCompactor_TTLInlineMigratesLegacyString(t *testing.T) {
	t.Parallel()

	st, c := newDeltaCompactorTestFixture(t)
	ctx := context.Background()
	key := []byte("ttl:migrate:string")
	expireAt := time.Now().Add(time.Hour)
	require.NoError(t, st.PutAt(ctx, redisStrKey(key), []byte("value"), 1, 0))
	require.NoError(t, st.PutAt(ctx, redisTTLKey(key), encodeRedisTTL(expireAt), 2, 0))

	require.NoError(t, c.SyncOnce(ctx))

	readTS := st.LastCommitTS()
	raw, err := st.GetAt(ctx, redisStrKey(key), readTS)
	require.NoError(t, err)
	value, ttl, err := decodeRedisStr(raw)
	require.NoError(t, err)
	require.Equal(t, []byte("value"), value)
	require.NotNil(t, ttl)
	require.Equal(t, redisExpireAtMillis(expireAt), redisExpireAtMillis(*ttl))
}

func TestDeltaCompactor_TTLInlineMigratesLegacyHLL(t *testing.T) {
	t.Parallel()

	st, c := newDeltaCompactorTestFixture(t)
	ctx := context.Background()
	key := []byte("ttl:migrate:hll")
	expireAt := time.Now().Add(time.Hour)
	legacy, err := marshalSetValue(redisSetValue{Members: []string{"b", "a"}})
	require.NoError(t, err)
	require.NoError(t, st.PutAt(ctx, redisHLLKey(key), legacy, 1, 0))
	require.NoError(t, st.PutAt(ctx, redisTTLKey(key), encodeRedisTTL(expireAt), 2, 0))

	require.NoError(t, c.SyncOnce(ctx))

	readTS := st.LastCommitTS()
	raw, err := st.GetAt(ctx, redisHLLKey(key), readTS)
	require.NoError(t, err)
	value, ttl, embedded, err := decodeRedisHLL(raw)
	require.NoError(t, err)
	require.True(t, embedded)
	require.Equal(t, redisSetValue{Members: []string{"a", "b"}}, value)
	require.NotNil(t, ttl)
	require.Equal(t, redisExpireAtMillis(expireAt), redisExpireAtMillis(*ttl))
}

func TestDeltaCompactor_TTLInlineMigratesLegacyHashMeta(t *testing.T) {
	t.Parallel()

	st, c := newDeltaCompactorTestFixture(t)
	ctx := context.Background()
	key := []byte("ttl:migrate:hash")
	expireAt := time.Now().Add(time.Hour)
	legacyMeta := make([]byte, redisSimpleMetaLegacySizeBytes)
	binary.BigEndian.PutUint64(legacyMeta, 3)
	require.NoError(t, st.PutAt(ctx, store.HashMetaKey(key), legacyMeta, 1, 0))
	require.NoError(t, st.PutAt(ctx, redisTTLKey(key), encodeRedisTTL(expireAt), 2, 0))

	require.NoError(t, c.SyncOnce(ctx))

	readTS := st.LastCommitTS()
	raw, err := st.GetAt(ctx, store.HashMetaKey(key), readTS)
	require.NoError(t, err)
	require.Len(t, raw, redisSimpleMetaInlineSizeBytes)
	meta, err := store.UnmarshalHashMeta(raw)
	require.NoError(t, err)
	require.Equal(t, int64(3), meta.Len)
	require.Equal(t, redisExpireAtMillis(expireAt), meta.ExpireAt)
}

func TestDeltaCompactor_TTLInlineMigratesListUserKeyStartingWithDeltaPrefix(t *testing.T) {
	t.Parallel()

	st, c := newDeltaCompactorTestFixture(t)
	ctx := context.Background()
	key := []byte("d|ttl:migrate:list")
	expireAt := time.Now().Add(time.Hour)
	legacyMeta := make([]byte, redisWideMetaLegacySizeBytes)
	binary.BigEndian.PutUint64(legacyMeta[0:8], 0)
	binary.BigEndian.PutUint64(legacyMeta[8:16], 1)
	binary.BigEndian.PutUint64(legacyMeta[16:24], 1)
	require.NoError(t, st.PutAt(ctx, store.ListMetaKey(key), legacyMeta, 1, 0))
	require.NoError(t, st.PutAt(ctx, redisTTLKey(key), encodeRedisTTL(expireAt), 2, 0))

	deltaKey := store.ListMetaDeltaKey(key, 3, 0)
	delta := store.MarshalListMetaDelta(store.ListMetaDelta{HeadDelta: 0, LenDelta: 1})
	require.NoError(t, st.PutAt(ctx, deltaKey, delta, 3, 0))

	require.NoError(t, c.SyncOnce(ctx))

	readTS := st.LastCommitTS()
	raw, err := st.GetAt(ctx, store.ListMetaKey(key), readTS)
	require.NoError(t, err)
	require.Len(t, raw, redisWideMetaInlineSizeBytes)
	meta, err := store.UnmarshalListMeta(raw)
	require.NoError(t, err)
	require.Equal(t, int64(1), meta.Len)
	require.Equal(t, redisExpireAtMillis(expireAt), meta.ExpireAt)

	rawDelta, err := st.GetAt(ctx, deltaKey, readTS)
	require.NoError(t, err)
	require.Equal(t, delta, rawDelta)
}

func TestIsListMetaMigrationDeltaRecognizesLegacyRowsByValue(t *testing.T) {
	t.Parallel()

	userKey := []byte("list")
	delta := store.MarshalListMetaDelta(store.ListMetaDelta{LenDelta: 1})
	legacyKey := legacyListMetaDeltaKey(userKey, 2)

	require.True(t, isListMetaMigrationDelta(&store.KVPair{Key: legacyKey, Value: delta}))
	require.True(t, isListMetaMigrationDelta(&store.KVPair{
		Key:   store.ListMetaDeltaKey(userKey, 2, 0),
		Value: delta,
	}))
	require.False(t, isListMetaMigrationDelta(&store.KVPair{
		Key:   store.ListMetaKey(append([]byte("d|"), userKey...)),
		Value: make([]byte, redisWideMetaLegacySizeBytes),
	}))
}

func TestDeltaCompactor_TTLInlineMigratesLegacyStreamTTL(t *testing.T) {
	t.Parallel()

	st, c := newDeltaCompactorTestFixture(t)
	ctx := context.Background()
	key := []byte("ttl:migrate:stream")
	expireAt := time.Now().Add(time.Hour)
	legacy := redisStreamValue{Entries: []redisStreamEntry{
		newRedisStreamEntry("1700000000000-0", []string{"event", "a"}),
		newRedisStreamEntry("1700000000000-5", []string{"event", "b"}),
	}}
	payload, err := marshalStreamValue(legacy)
	require.NoError(t, err)
	require.NoError(t, st.PutAt(ctx, redisStreamKey(key), payload, 1, 0))
	require.NoError(t, st.PutAt(ctx, redisTTLKey(key), encodeRedisTTL(expireAt), 2, 0))

	require.NoError(t, c.SyncOnce(ctx))

	readTS := st.LastCommitTS()
	_, err = st.GetAt(ctx, redisStreamKey(key), readTS)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	raw, err := st.GetAt(ctx, store.StreamMetaKey(key), readTS)
	require.NoError(t, err)
	require.Len(t, raw, store.StreamMetaTrimBinarySize)
	meta, err := store.UnmarshalStreamMeta(raw)
	require.NoError(t, err)
	require.Zero(t, meta.Length)
	require.Zero(t, meta.LastMs)
	require.Zero(t, meta.LastSeq)
	require.Equal(t, redisExpireAtMillis(expireAt), meta.ExpireAt)
	require.Zero(t, meta.TrimmedMs)
	require.Zero(t, meta.TrimmedSeq)
	_, err = st.GetAt(ctx, store.StreamEntryKey(key, 1700000000000, 5), readTS)
	require.ErrorIs(t, err, store.ErrKeyNotFound)

	server := &RedisServer{store: st}
	got, err := server.loadStreamAt(ctx, key, readTS)
	require.NoError(t, err)
	require.Empty(t, got.Entries)
}

func TestRedisTTLAt_LegacyFallbackCanBeDisabled(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	key := []byte("ttl:fallback:disabled")
	expireAt := time.Now().Add(time.Hour)
	require.NoError(t, st.PutAt(ctx, redisStrKey(key), []byte("legacy"), 1, 0))
	require.NoError(t, st.PutAt(ctx, redisTTLKey(key), encodeRedisTTL(expireAt), 2, 0))

	withFallback := &RedisServer{store: st}
	got, err := withFallback.ttlAt(ctx, key, 2)
	require.NoError(t, err)
	require.NotNil(t, got)

	withoutFallback := &RedisServer{store: st, disableLegacyTTLReadFallback: true}
	got, err = withoutFallback.ttlAt(ctx, key, 2)
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestRedisHasExpiredSkipsLegacyTTLWhenFallbackDisabled(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	key := []byte("ttl:fallback:non-string")
	legacyMeta := make([]byte, redisSimpleMetaLegacySizeBytes)
	binary.BigEndian.PutUint64(legacyMeta, 1)
	require.NoError(t, st.PutAt(ctx, store.HashMetaKey(key), legacyMeta, 1, 0))
	require.NoError(t, st.PutAt(ctx, redisTTLKey(key), encodeRedisTTL(time.Now().Add(-time.Minute)), 2, 0))

	withFallback := &RedisServer{store: st}
	expired, err := withFallback.hasExpired(ctx, key, 2, true)
	require.NoError(t, err)
	require.True(t, expired)

	withoutFallback := &RedisServer{store: st, disableLegacyTTLReadFallback: true}
	expired, err = withoutFallback.hasExpired(ctx, key, 2, true)
	require.NoError(t, err)
	require.False(t, expired)
}

func TestDeltaCompactor_TTLInlineDoesNotOverwriteCurrentMetaFromStaleLegacyIndex(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	currentExpireAt := time.Now().Add(2 * time.Hour)
	staleExpireAt := time.Now().Add(time.Hour)
	currentMs := redisExpireAtMillis(currentExpireAt)
	cases := []struct {
		name string
		key  []byte
		seed func(store.MVCCStore, []byte) error
		read func(store.MVCCStore, []byte, uint64) (int64, uint64, error)
	}{
		{
			name: "hash",
			key:  []byte("ttl:migrate:current-hash"),
			seed: func(st store.MVCCStore, key []byte) error {
				return st.PutAt(ctx, store.HashMetaKey(key),
					store.MarshalHashMeta(store.HashMeta{Len: 3, ExpireAt: currentMs}), 1, 0)
			},
			read: func(st store.MVCCStore, key []byte, readTS uint64) (int64, uint64, error) {
				raw, err := st.GetAt(ctx, store.HashMetaKey(key), readTS)
				if err != nil {
					return 0, 0, err
				}
				meta, err := store.UnmarshalHashMeta(raw)
				return meta.Len, meta.ExpireAt, err
			},
		},
		{
			name: "set",
			key:  []byte("ttl:migrate:current-set"),
			seed: func(st store.MVCCStore, key []byte) error {
				return st.PutAt(ctx, store.SetMetaKey(key),
					store.MarshalSetMeta(store.SetMeta{Len: 3, ExpireAt: currentMs}), 1, 0)
			},
			read: func(st store.MVCCStore, key []byte, readTS uint64) (int64, uint64, error) {
				raw, err := st.GetAt(ctx, store.SetMetaKey(key), readTS)
				if err != nil {
					return 0, 0, err
				}
				meta, err := store.UnmarshalSetMeta(raw)
				return meta.Len, meta.ExpireAt, err
			},
		},
		{
			name: "zset",
			key:  []byte("ttl:migrate:current-zset"),
			seed: func(st store.MVCCStore, key []byte) error {
				return st.PutAt(ctx, store.ZSetMetaKey(key),
					store.MarshalZSetMeta(store.ZSetMeta{Len: 3, ExpireAt: currentMs}), 1, 0)
			},
			read: func(st store.MVCCStore, key []byte, readTS uint64) (int64, uint64, error) {
				raw, err := st.GetAt(ctx, store.ZSetMetaKey(key), readTS)
				if err != nil {
					return 0, 0, err
				}
				meta, err := store.UnmarshalZSetMeta(raw)
				return meta.Len, meta.ExpireAt, err
			},
		},
		{
			name: "list",
			key:  []byte("ttl:migrate:current-list"),
			seed: func(st store.MVCCStore, key []byte) error {
				meta, err := store.MarshalListMeta(store.ListMeta{Head: 1, Len: 3, ExpireAt: currentMs})
				if err != nil {
					return err
				}
				return st.PutAt(ctx, store.ListMetaKey(key), meta, 1, 0)
			},
			read: func(st store.MVCCStore, key []byte, readTS uint64) (int64, uint64, error) {
				raw, err := st.GetAt(ctx, store.ListMetaKey(key), readTS)
				if err != nil {
					return 0, 0, err
				}
				meta, err := store.UnmarshalListMeta(raw)
				return meta.Len, meta.ExpireAt, err
			},
		},
		{
			name: "stream",
			key:  []byte("ttl:migrate:current-stream"),
			seed: func(st store.MVCCStore, key []byte) error {
				meta, err := store.MarshalStreamMeta(store.StreamMeta{Length: 3, ExpireAt: currentMs})
				if err != nil {
					return err
				}
				return st.PutAt(ctx, store.StreamMetaKey(key), meta, 1, 0)
			},
			read: func(st store.MVCCStore, key []byte, readTS uint64) (int64, uint64, error) {
				raw, err := st.GetAt(ctx, store.StreamMetaKey(key), readTS)
				if err != nil {
					return 0, 0, err
				}
				meta, err := store.UnmarshalStreamMeta(raw)
				return meta.Length, meta.ExpireAt, err
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, c := newDeltaCompactorTestFixture(t)
			require.NoError(t, tc.seed(st, tc.key))
			require.NoError(t, st.PutAt(ctx, redisTTLKey(tc.key), encodeRedisTTL(staleExpireAt), 2, 0))

			require.NoError(t, c.SyncOnce(ctx))

			readTS := st.LastCommitTS()
			n, ttlMs, err := tc.read(st, tc.key, readTS)
			require.NoError(t, err)
			require.Equal(t, int64(3), n)
			require.Equal(t, currentMs, ttlMs)
			rawTTL, err := st.GetAt(ctx, redisTTLKey(tc.key), readTS)
			require.NoError(t, err)
			indexTTL, err := decodeRedisTTL(rawTTL)
			require.NoError(t, err)
			require.Equal(t, currentMs, redisExpireAtMillis(indexTTL))
		})
	}
}

func TestDeltaCompactor_TTLInlineKeepsCurrentNoTTLMetaFromStaleLegacyIndex(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	staleExpireAt := time.Now().Add(time.Hour)
	cases := []struct {
		name string
		key  []byte
		seed func(store.MVCCStore, []byte) error
		read func(store.MVCCStore, []byte, uint64) (int64, uint64, error)
	}{
		{
			name: "hash",
			key:  []byte("ttl:migrate:no-ttl-hash"),
			seed: func(st store.MVCCStore, key []byte) error {
				return st.PutAt(ctx, store.HashMetaKey(key),
					store.MarshalHashMeta(store.HashMeta{Len: 3}), 1, 0)
			},
			read: func(st store.MVCCStore, key []byte, readTS uint64) (int64, uint64, error) {
				raw, err := st.GetAt(ctx, store.HashMetaKey(key), readTS)
				if err != nil {
					return 0, 0, err
				}
				meta, err := store.UnmarshalHashMeta(raw)
				return meta.Len, meta.ExpireAt, err
			},
		},
		{
			name: "set",
			key:  []byte("ttl:migrate:no-ttl-set"),
			seed: func(st store.MVCCStore, key []byte) error {
				return st.PutAt(ctx, store.SetMetaKey(key),
					store.MarshalSetMeta(store.SetMeta{Len: 3}), 1, 0)
			},
			read: func(st store.MVCCStore, key []byte, readTS uint64) (int64, uint64, error) {
				raw, err := st.GetAt(ctx, store.SetMetaKey(key), readTS)
				if err != nil {
					return 0, 0, err
				}
				meta, err := store.UnmarshalSetMeta(raw)
				return meta.Len, meta.ExpireAt, err
			},
		},
		{
			name: "zset",
			key:  []byte("ttl:migrate:no-ttl-zset"),
			seed: func(st store.MVCCStore, key []byte) error {
				return st.PutAt(ctx, store.ZSetMetaKey(key),
					store.MarshalZSetMeta(store.ZSetMeta{Len: 3}), 1, 0)
			},
			read: func(st store.MVCCStore, key []byte, readTS uint64) (int64, uint64, error) {
				raw, err := st.GetAt(ctx, store.ZSetMetaKey(key), readTS)
				if err != nil {
					return 0, 0, err
				}
				meta, err := store.UnmarshalZSetMeta(raw)
				return meta.Len, meta.ExpireAt, err
			},
		},
		{
			name: "list",
			key:  []byte("ttl:migrate:no-ttl-list"),
			seed: func(st store.MVCCStore, key []byte) error {
				meta, err := store.MarshalListMeta(store.ListMeta{Head: 1, Len: 3})
				if err != nil {
					return err
				}
				return st.PutAt(ctx, store.ListMetaKey(key), meta, 1, 0)
			},
			read: func(st store.MVCCStore, key []byte, readTS uint64) (int64, uint64, error) {
				raw, err := st.GetAt(ctx, store.ListMetaKey(key), readTS)
				if err != nil {
					return 0, 0, err
				}
				meta, err := store.UnmarshalListMeta(raw)
				return meta.Len, meta.ExpireAt, err
			},
		},
		{
			name: "stream",
			key:  []byte("ttl:migrate:no-ttl-stream"),
			seed: func(st store.MVCCStore, key []byte) error {
				meta, err := store.MarshalStreamMeta(store.StreamMeta{Length: 3})
				if err != nil {
					return err
				}
				return st.PutAt(ctx, store.StreamMetaKey(key), meta, 1, 0)
			},
			read: func(st store.MVCCStore, key []byte, readTS uint64) (int64, uint64, error) {
				raw, err := st.GetAt(ctx, store.StreamMetaKey(key), readTS)
				if err != nil {
					return 0, 0, err
				}
				meta, err := store.UnmarshalStreamMeta(raw)
				return meta.Length, meta.ExpireAt, err
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, c := newDeltaCompactorTestFixture(t)
			require.NoError(t, tc.seed(st, tc.key))
			require.NoError(t, st.PutAt(ctx, redisTTLKey(tc.key), encodeRedisTTL(staleExpireAt), 2, 0))

			require.NoError(t, c.SyncOnce(ctx))

			readTS := st.LastCommitTS()
			n, ttlMs, err := tc.read(st, tc.key, readTS)
			require.NoError(t, err)
			require.Equal(t, int64(3), n)
			require.Zero(t, ttlMs)
			_, err = st.GetAt(ctx, redisTTLKey(tc.key), readTS)
			require.ErrorIs(t, err, store.ErrKeyNotFound)
		})
	}
}

type trackingCompactionCoordinator struct {
	*localAdapterCoordinator
	mu        sync.Mutex
	elemCount []int
}

func (c *trackingCompactionCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	if req != nil && len(req.Elems) > 0 {
		c.mu.Lock()
		c.elemCount = append(c.elemCount, len(req.Elems))
		c.mu.Unlock()
	}
	return c.localAdapterCoordinator.Dispatch(ctx, req)
}

func (c *trackingCompactionCoordinator) counts() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]int(nil), c.elemCount...)
}

func TestDeltaCompactor_TTLInlineMigrationDispatchesSmallBatches(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := &trackingCompactionCoordinator{localAdapterCoordinator: newLocalAdapterCoordinator(st)}
	c := NewDeltaCompactor(st, coord, WithDeltaCompactorMaxDeltaCount(2))

	const totalKeys uint64 = ttlInlineMigrationBatchKeyLimit + 5
	expireAt := time.Now().Add(time.Hour)
	for i := uint64(0); i < totalKeys; i++ {
		key := []byte("ttl:migrate:batch:" + strconv.FormatUint(i, 10))
		require.NoError(t, st.PutAt(ctx, redisStrKey(key), []byte("value"), 1+i*2, 0))
		require.NoError(t, st.PutAt(ctx, redisTTLKey(key), encodeRedisTTL(expireAt), 2+i*2, 0))
	}

	require.NoError(t, c.SyncOnce(ctx))

	counts := coord.counts()
	require.GreaterOrEqual(t, len(counts), 2)
	for _, n := range counts {
		require.LessOrEqual(t, n, ttlInlineMigrationBatchKeyLimit*redisPairWidth)
	}
}

type shardOnlyLeaderCompactionCoordinator struct {
	*localAdapterCoordinator
}

func (c *shardOnlyLeaderCompactionCoordinator) IsLeader() bool {
	return false
}

func (c *shardOnlyLeaderCompactionCoordinator) IsLeaderForKey([]byte) bool {
	return true
}

func TestDeltaCompactor_TTLInlineMigratesOnNonDefaultShardLeader(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := &shardOnlyLeaderCompactionCoordinator{localAdapterCoordinator: newLocalAdapterCoordinator(st)}
	c := NewDeltaCompactor(st, coord, WithDeltaCompactorMaxDeltaCount(2))

	key := []byte("ttl:migrate:shard-only")
	expireAt := time.Now().Add(time.Hour)
	legacyMeta := make([]byte, redisSimpleMetaLegacySizeBytes)
	binary.BigEndian.PutUint64(legacyMeta, 1)
	require.NoError(t, st.PutAt(ctx, store.HashMetaKey(key), legacyMeta, 1, 0))
	require.NoError(t, st.PutAt(ctx, redisTTLKey(key), encodeRedisTTL(expireAt), 2, 0))

	require.NoError(t, c.SyncOnce(ctx))

	raw, err := st.GetAt(ctx, store.HashMetaKey(key), st.LastCommitTS())
	require.NoError(t, err)
	meta, err := store.UnmarshalHashMeta(raw)
	require.NoError(t, err)
	require.Equal(t, int64(1), meta.Len)
	require.Equal(t, redisExpireAtMillis(expireAt), meta.ExpireAt)
}

type localLeaderGroupCompactionCoordinator struct {
	*shardOnlyLeaderCompactionCoordinator
	groupIDs []uint64
}

func (c *localLeaderGroupCompactionCoordinator) LocalLeaderGroupIDs() ([]uint64, bool) {
	return append([]uint64(nil), c.groupIDs...), true
}

type recordingTTLInlineGroupScanStore struct {
	store.MVCCStore
	fullStarts  []string
	groupScans  []uint64
	groupStarts []string
}

func (s *recordingTTLInlineGroupScanStore) ScanAt(ctx context.Context, start []byte, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	s.fullStarts = append(s.fullStarts, string(start))
	return s.MVCCStore.ScanAt(ctx, start, end, limit, ts)
}

func (s *recordingTTLInlineGroupScanStore) ScanGroupOwnedAt(ctx context.Context, groupID uint64, start []byte, end []byte, limit int, ts uint64) ([]*store.KVPair, []byte, error) {
	s.groupScans = append(s.groupScans, groupID)
	s.groupStarts = append(s.groupStarts, string(start))
	return scanPageWithResume(ctx, s.MVCCStore, start, end, limit, ts)
}

// scanPageWithResume reads one page the way ShardStore.ScanGroupOwnedAt does
// and reports its last key when the page was full.
func scanPageWithResume(ctx context.Context, st store.MVCCStore, start []byte, end []byte, limit int, ts uint64) ([]*store.KVPair, []byte, error) {
	kvs, err := st.ScanAt(ctx, start, end, limit, ts)
	if err != nil || len(kvs) < limit {
		return kvs, nil, err
	}
	return kvs, kvs[len(kvs)-1].Key, nil
}

func TestDeltaCompactor_TTLInlineUsesLocalLeaderGroupScan(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := &recordingTTLInlineGroupScanStore{MVCCStore: store.NewMVCCStore()}
	coord := &localLeaderGroupCompactionCoordinator{
		shardOnlyLeaderCompactionCoordinator: &shardOnlyLeaderCompactionCoordinator{
			localAdapterCoordinator: newLocalAdapterCoordinator(st),
		},
		groupIDs: []uint64{2},
	}
	c := NewDeltaCompactor(st, coord, WithDeltaCompactorMaxDeltaCount(2))

	key := []byte("ttl:migrate:group-scan")
	expireAt := time.Now().Add(time.Hour)
	legacyMeta := make([]byte, redisSimpleMetaLegacySizeBytes)
	binary.BigEndian.PutUint64(legacyMeta, 1)
	require.NoError(t, st.PutAt(ctx, store.HashMetaKey(key), legacyMeta, 1, 0))
	require.NoError(t, st.PutAt(ctx, redisTTLKey(key), encodeRedisTTL(expireAt), 2, 0))

	require.NoError(t, c.SyncOnce(ctx))

	require.Contains(t, st.groupScans, uint64(2))
	require.Contains(t, st.groupStarts, store.HashMetaPrefix)
	require.NotContains(t, st.fullStarts, store.HashMetaPrefix,
		"sharded ttl migration should scan candidate metadata with ScanGroupOwnedAt")
	raw, err := st.GetAt(ctx, store.HashMetaKey(key), st.LastCommitTS())
	require.NoError(t, err)
	meta, err := store.UnmarshalHashMeta(raw)
	require.NoError(t, err)
	require.Equal(t, redisExpireAtMillis(expireAt), meta.ExpireAt)
}

// budgetedTTLInlineScanStore ends the tick during its first group scan, the
// way a tick whose first scan uses up the per-tick budget ends, and records
// which group and prefix each tick reached.
type budgetedTTLInlineScanStore struct {
	store.MVCCStore

	endTick context.CancelFunc
	scanned map[string]int
}

func (s *budgetedTTLInlineScanStore) ScanGroupOwnedAt(_ context.Context, groupID uint64, start []byte, _ []byte, _ int, _ uint64) ([]*store.KVPair, []byte, error) {
	s.scanned[string(start)+"#"+strconv.FormatUint(groupID, 10)]++
	s.endTick()
	return nil, nil, nil
}

// Every TTL-inline scan plan shares one per-tick budget. When the budget runs
// out partway through the plans, the next tick must start where this one
// stopped; starting every tick from the first handler's first group starved
// the later handlers and groups for as long as the earlier ones used the whole
// budget.
func TestDeltaCompactor_TTLInlineRotatesPlansAcrossTicksThatRunOutOfBudget(t *testing.T) {
	t.Parallel()

	st := &budgetedTTLInlineScanStore{MVCCStore: store.NewMVCCStore(), scanned: map[string]int{}}
	groupIDs := []uint64{1, 2, 3}
	coord := &localLeaderGroupCompactionCoordinator{
		shardOnlyLeaderCompactionCoordinator: &shardOnlyLeaderCompactionCoordinator{
			localAdapterCoordinator: newLocalAdapterCoordinator(st),
		},
		groupIDs: groupIDs,
	}
	c := NewDeltaCompactor(st, coord)
	handlers := c.ttlInlineMigrationHandlers()
	plans := len(handlers) * len(groupIDs)

	for range plans {
		ctx, cancel := context.WithCancel(context.Background())
		st.endTick = cancel
		require.ErrorIs(t, c.migrateTTLInlineOnce(ctx, 10, time.Now()), context.Canceled)
		cancel()
	}

	want := make(map[string]int, plans)
	for _, h := range handlers {
		for _, groupID := range groupIDs {
			want[string(h.prefix)+"#"+strconv.FormatUint(groupID, 10)] = 1
		}
	}
	require.Equal(t, want, st.scanned, "each tick must reach the plan after the one the previous tick reached")
}

// newSplitRedisShardStore returns a ShardStore whose catalog sends user keys
// below "m" to group 1 and the rest to group 2, the way a multi-shard
// deployment splits the Redis keyspace.
func newSplitRedisShardStore() *kv.ShardStore {
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)
	return kv.NewShardStore(engine, map[uint64]*kv.ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	})
}

// A node scans each data group it leads for legacy TTL anchors. The bare anchor
// prefixes ("!redis|str|", "!hs|meta|", ...) resolve to the first route only, so
// a route-checked group scan refused every other group: anchors outside the
// first route were never migrated and every tick failed. Run the migrator over a
// real split ShardStore and require the anchors of both groups to migrate.
func TestDeltaCompactor_TTLInlineMigratesEveryLocalLeaderShard(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := newSplitRedisShardStore()
	coord := &localLeaderGroupCompactionCoordinator{
		shardOnlyLeaderCompactionCoordinator: &shardOnlyLeaderCompactionCoordinator{
			localAdapterCoordinator: newLocalAdapterCoordinator(st),
		},
		groupIDs: []uint64{1, 2},
	}
	c := NewDeltaCompactor(st, coord, WithDeltaCompactorMaxDeltaCount(2))

	expireAt := time.Now().Add(time.Hour)
	// One key on each side of the split per anchor type.
	stringKeys := [][]byte{[]byte("apple"), []byte("zebra")}
	hashKeys := [][]byte{[]byte("aardvark"), []byte("yak")}
	legacyMeta := make([]byte, redisSimpleMetaLegacySizeBytes)
	binary.BigEndian.PutUint64(legacyMeta, 1)
	ts := uint64(0)
	seed := func(key, value []byte) {
		ts++
		require.NoError(t, st.PutAt(ctx, key, value, ts, 0))
	}
	for _, key := range stringKeys {
		seed(redisStrKey(key), []byte("legacy-value"))
		seed(redisTTLKey(key), encodeRedisTTL(expireAt))
	}
	for _, key := range hashKeys {
		seed(store.HashMetaKey(key), legacyMeta)
		seed(redisTTLKey(key), encodeRedisTTL(expireAt))
	}

	require.NoError(t, c.SyncOnce(ctx))

	readTS := st.LastCommitTS()
	for _, key := range stringKeys {
		raw, err := st.GetAt(ctx, redisStrKey(key), readTS)
		require.NoError(t, err)
		require.Truef(t, isNewRedisStrFormat(raw), "string %q was not migrated", key)
		value, ttl, err := decodeRedisStr(raw)
		require.NoError(t, err)
		require.Equal(t, []byte("legacy-value"), value)
		require.NotNil(t, ttl)
		require.Equal(t, redisExpireAtMillis(expireAt), redisExpireAtMillis(*ttl))
	}
	for _, key := range hashKeys {
		raw, err := st.GetAt(ctx, store.HashMetaKey(key), readTS)
		require.NoError(t, err)
		meta, err := store.UnmarshalHashMeta(raw)
		require.NoError(t, err)
		require.Equalf(t, redisExpireAtMillis(expireAt), meta.ExpireAt, "hash %q was not migrated", key)
	}
}

// A group can still hold a full page of copies of ranges it no longer owns. A
// tick must read one page per plan and move the cursor past it, so the group's
// own anchors behind such a page are reached on the next tick; reading on until
// a page of owned rows turned a large stale backlog into a call that never
// finished within the tick budget and never advanced.
func TestDeltaCompactor_TTLInlineAdvancesPastAFullPageOfStaleRows(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 2)
	engine.UpdateRoute([]byte("m"), nil, 1)
	group1 := store.NewMVCCStore()
	st := kv.NewShardStore(engine, map[uint64]*kv.ShardGroup{
		1: {Store: group1},
		2: {Store: store.NewMVCCStore()},
	})
	coord := &localLeaderGroupCompactionCoordinator{
		shardOnlyLeaderCompactionCoordinator: &shardOnlyLeaderCompactionCoordinator{
			localAdapterCoordinator: newLocalAdapterCoordinator(st),
		},
		groupIDs: []uint64{1},
	}
	c := NewDeltaCompactor(st, coord, WithDeltaCompactorMaxDeltaCount(2))

	// A full page of stale copies of group 2's keys sorts before group 1's key.
	for i := range ttlInlineMigrationTickScanLimit {
		key := []byte("a" + strconv.Itoa(1000+i))
		require.NoError(t, group1.PutAt(ctx, redisStrKey(key), []byte("stale"), 1, 0))
	}
	owned := []byte("zebra")
	expireAt := time.Now().Add(time.Hour)
	require.NoError(t, st.PutAt(ctx, redisStrKey(owned), []byte("legacy-value"), 2, 0))
	require.NoError(t, st.PutAt(ctx, redisTTLKey(owned), encodeRedisTTL(expireAt), 3, 0))
	migrated := func() bool {
		raw, err := st.GetAt(ctx, redisStrKey(owned), st.LastCommitTS())
		require.NoError(t, err)
		return isNewRedisStrFormat(raw)
	}

	require.NoError(t, c.SyncOnce(ctx))
	require.False(t, migrated(), "a tick reads one page per plan")
	require.NoError(t, c.SyncOnce(ctx))
	require.True(t, migrated(), "the next tick resumes after the stale page")
}

// Hash, set and zset metadata written before wide-column routing can still sit
// at the route of its raw key while its user key belongs to another group, and
// reads fall back to that row. The group holding it must hand it to the
// migrator, which rewrites it into the user key's group when this node leads
// that group.
func TestDeltaCompactor_TTLInlineMigratesWideColumnRowsAtTheirLegacyRoute(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)
	group1 := store.NewMVCCStore()
	st := kv.NewShardStore(engine, map[uint64]*kv.ShardGroup{
		1: {Store: group1},
		2: {Store: store.NewMVCCStore()},
	})
	coord := &localLeaderGroupCompactionCoordinator{
		shardOnlyLeaderCompactionCoordinator: &shardOnlyLeaderCompactionCoordinator{
			localAdapterCoordinator: newLocalAdapterCoordinator(st),
		},
		groupIDs: []uint64{1, 2},
	}
	c := NewDeltaCompactor(st, coord, WithDeltaCompactorMaxDeltaCount(2))

	key := []byte("zebra")
	expireAt := time.Now().Add(time.Hour)
	legacyMeta := make([]byte, redisSimpleMetaLegacySizeBytes)
	binary.BigEndian.PutUint64(legacyMeta, 1)
	// The raw meta key starts with "!", so its legacy route is group 1.
	require.NoError(t, group1.PutAt(ctx, store.HashMetaKey(key), legacyMeta, 1, 0))
	require.NoError(t, st.PutAt(ctx, redisTTLKey(key), encodeRedisTTL(expireAt), 2, 0))

	require.NoError(t, c.SyncOnce(ctx))

	raw, err := st.GetAt(ctx, store.HashMetaKey(key), st.LastCommitTS())
	require.NoError(t, err)
	meta, err := store.UnmarshalHashMeta(raw)
	require.NoError(t, err)
	require.Equal(t, int64(1), meta.Len)
	require.Equal(t, redisExpireAtMillis(expireAt), meta.ExpireAt)
}

// Stream metadata written before streams were routed by user key can sit at
// the route of its raw key while the user key belongs to another group, and
// point reads fall back to it while that group holds nothing. The migrator must
// rewrite it into the user key's group with its TTL inline, as it does for
// hash, set and zset metadata.
func TestDeltaCompactor_TTLInlineMigratesStreamMetaAtItsLegacyRoute(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)
	group1 := store.NewMVCCStore()
	st := kv.NewShardStore(engine, map[uint64]*kv.ShardGroup{
		1: {Store: group1},
		2: {Store: store.NewMVCCStore()},
	})
	coord := &localLeaderGroupCompactionCoordinator{
		shardOnlyLeaderCompactionCoordinator: &shardOnlyLeaderCompactionCoordinator{
			localAdapterCoordinator: newLocalAdapterCoordinator(st),
		},
		groupIDs: []uint64{1, 2},
	}
	c := NewDeltaCompactor(st, coord, WithDeltaCompactorMaxDeltaCount(2))

	key := []byte("zebra")
	expireAt := time.Now().Add(time.Hour)
	legacyMeta := make([]byte, redisWideMetaLegacySizeBytes)
	binary.BigEndian.PutUint64(legacyMeta[0:8], 3)
	binary.BigEndian.PutUint64(legacyMeta[8:16], 100)
	binary.BigEndian.PutUint64(legacyMeta[16:24], 7)
	// The raw meta key starts with "!", so its legacy route is group 1.
	require.NoError(t, group1.PutAt(ctx, store.StreamMetaKey(key), legacyMeta, 1, 0))
	require.NoError(t, st.PutAt(ctx, redisTTLKey(key), encodeRedisTTL(expireAt), 2, 0))

	require.NoError(t, c.SyncOnce(ctx))

	raw, err := st.GetAt(ctx, store.StreamMetaKey(key), st.LastCommitTS())
	require.NoError(t, err)
	meta, err := store.UnmarshalStreamMeta(raw)
	require.NoError(t, err)
	require.Equal(t, store.StreamMeta{
		Length:   3,
		LastMs:   100,
		LastSeq:  7,
		ExpireAt: redisExpireAtMillis(expireAt),
	}, meta)
}

// capabilityHidingCompactionCoordinator forwards only the kv.Coordinator
// methods, the way a decorator that forgets an optional forward does.
type capabilityHidingCompactionCoordinator struct {
	kv.Coordinator
}

// The coordinator the compactor holds is wrapped in WithKeyVizLabel, which
// always exposes LocalLeaderGroupIDs. When a decorator underneath hides the
// capability the answer must stay unknown and the migrator must fall back to a
// whole-prefix scan; answering "no groups" there planned nothing and silently
// stopped the migration in production. A known empty list is different: this
// node leads no group, so there is nothing for it to migrate.
func TestDeltaCompactor_TTLInlineScanPlanWhenLocalLeaderGroupsUnknownOrEmpty(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name         string
		wrap         func(kv.Coordinator) kv.Coordinator
		groupIDs     []uint64
		wantFullScan bool
		wantMigrated bool
	}{
		{
			name: "unknown behind a decorator",
			wrap: func(c kv.Coordinator) kv.Coordinator {
				return capabilityHidingCompactionCoordinator{Coordinator: c}
			},
			groupIDs:     []uint64{2},
			wantFullScan: true,
			wantMigrated: true,
		},
		{
			name:     "known with no local leader group",
			wrap:     func(c kv.Coordinator) kv.Coordinator { return c },
			groupIDs: []uint64{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			st := &recordingTTLInlineGroupScanStore{MVCCStore: store.NewMVCCStore()}
			inner := &localLeaderGroupCompactionCoordinator{
				shardOnlyLeaderCompactionCoordinator: &shardOnlyLeaderCompactionCoordinator{
					localAdapterCoordinator: newLocalAdapterCoordinator(st),
				},
				groupIDs: tc.groupIDs,
			}
			c := NewDeltaCompactor(st, tc.wrap(inner), WithDeltaCompactorMaxDeltaCount(2))

			key := []byte("ttl:migrate:group-plan")
			expireAt := time.Now().Add(time.Hour)
			legacyMeta := make([]byte, redisSimpleMetaLegacySizeBytes)
			binary.BigEndian.PutUint64(legacyMeta, 1)
			require.NoError(t, st.PutAt(ctx, store.HashMetaKey(key), legacyMeta, 1, 0))
			require.NoError(t, st.PutAt(ctx, redisTTLKey(key), encodeRedisTTL(expireAt), 2, 0))

			require.NoError(t, c.SyncOnce(ctx))

			require.Empty(t, st.groupScans)
			if tc.wantFullScan {
				require.Contains(t, st.fullStarts, store.HashMetaPrefix)
			} else {
				require.NotContains(t, st.fullStarts, store.HashMetaPrefix)
			}
			raw, err := st.GetAt(ctx, store.HashMetaKey(key), st.LastCommitTS())
			require.NoError(t, err)
			meta, err := store.UnmarshalHashMeta(raw)
			require.NoError(t, err)
			wantExpireAt := uint64(0)
			if tc.wantMigrated {
				wantExpireAt = redisExpireAtMillis(expireAt)
			}
			require.Equal(t, wantExpireAt, meta.ExpireAt)
		})
	}
}

func TestDeltaCompactor_TTLInlineMigratesEmptyCollectionKeys(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	expireAt := time.Now().Add(time.Hour)
	cases := []struct {
		name string
		seed func(store.MVCCStore) error
		read func(store.MVCCStore, uint64) (int64, uint64, error)
	}{
		{
			name: "list",
			seed: func(st store.MVCCStore) error {
				meta := make([]byte, redisWideMetaLegacySizeBytes)
				binary.BigEndian.PutUint64(meta[0:8], 0)
				binary.BigEndian.PutUint64(meta[8:16], 1)
				binary.BigEndian.PutUint64(meta[16:24], 1)
				return st.PutAt(ctx, store.ListMetaKey(nil), meta, 1, 0)
			},
			read: func(st store.MVCCStore, readTS uint64) (int64, uint64, error) {
				raw, err := st.GetAt(ctx, store.ListMetaKey(nil), readTS)
				if err != nil {
					return 0, 0, err
				}
				meta, err := store.UnmarshalListMeta(raw)
				return meta.Len, meta.ExpireAt, err
			},
		},
		{
			name: "hash",
			seed: func(st store.MVCCStore) error {
				meta := make([]byte, redisSimpleMetaLegacySizeBytes)
				binary.BigEndian.PutUint64(meta, 1)
				return st.PutAt(ctx, store.HashMetaKey(nil), meta, 1, 0)
			},
			read: func(st store.MVCCStore, readTS uint64) (int64, uint64, error) {
				raw, err := st.GetAt(ctx, store.HashMetaKey(nil), readTS)
				if err != nil {
					return 0, 0, err
				}
				meta, err := store.UnmarshalHashMeta(raw)
				return meta.Len, meta.ExpireAt, err
			},
		},
		{
			name: "set",
			seed: func(st store.MVCCStore) error {
				meta := make([]byte, redisSimpleMetaLegacySizeBytes)
				binary.BigEndian.PutUint64(meta, 1)
				return st.PutAt(ctx, store.SetMetaKey(nil), meta, 1, 0)
			},
			read: func(st store.MVCCStore, readTS uint64) (int64, uint64, error) {
				raw, err := st.GetAt(ctx, store.SetMetaKey(nil), readTS)
				if err != nil {
					return 0, 0, err
				}
				meta, err := store.UnmarshalSetMeta(raw)
				return meta.Len, meta.ExpireAt, err
			},
		},
		{
			name: "zset",
			seed: func(st store.MVCCStore) error {
				meta := make([]byte, redisSimpleMetaLegacySizeBytes)
				binary.BigEndian.PutUint64(meta, 1)
				return st.PutAt(ctx, store.ZSetMetaKey(nil), meta, 1, 0)
			},
			read: func(st store.MVCCStore, readTS uint64) (int64, uint64, error) {
				raw, err := st.GetAt(ctx, store.ZSetMetaKey(nil), readTS)
				if err != nil {
					return 0, 0, err
				}
				meta, err := store.UnmarshalZSetMeta(raw)
				return meta.Len, meta.ExpireAt, err
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, c := newDeltaCompactorTestFixture(t)
			require.NoError(t, tc.seed(st))
			require.NoError(t, st.PutAt(ctx, redisTTLKey(nil), encodeRedisTTL(expireAt), 2, 0))

			require.NoError(t, c.SyncOnce(ctx))

			n, ttlMs, err := tc.read(st, st.LastCommitTS())
			require.NoError(t, err)
			require.Equal(t, int64(1), n)
			require.Equal(t, redisExpireAtMillis(expireAt), ttlMs)
		})
	}
}

//nolint:cyclop // The table pins four storage layouts whose fixtures must stay local to the assertion.
func TestDeltaCompactor_TTLInlineMigratesTTLIndexedDeltaOnlyCollections(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	expireAt := time.Now().Add(time.Hour)
	cases := []struct {
		name     string
		key      []byte
		seed     func(store.MVCCStore, []byte) ([]byte, error)
		readMeta func(store.MVCCStore, []byte, uint64) (int64, uint64, error)
	}{
		{
			name: "list",
			key:  []byte("ttl:migrate:delta-only:list"),
			seed: func(st store.MVCCStore, key []byte) ([]byte, error) {
				if err := st.PutAt(ctx, store.ListItemKey(key, 0), []byte("v"), 1, 0); err != nil {
					return nil, err
				}
				deltaKey := store.ListMetaDeltaKey(key, 2, 0)
				return deltaKey, st.PutAt(ctx, deltaKey, store.MarshalListMetaDelta(store.ListMetaDelta{LenDelta: 1}), 2, 0)
			},
			readMeta: func(st store.MVCCStore, key []byte, readTS uint64) (int64, uint64, error) {
				raw, err := st.GetAt(ctx, store.ListMetaKey(key), readTS)
				if err != nil {
					return 0, 0, err
				}
				meta, err := store.UnmarshalListMeta(raw)
				return meta.Len, meta.ExpireAt, err
			},
		},
		{
			name: "hash",
			key:  []byte("ttl:migrate:delta-only:hash"),
			seed: func(st store.MVCCStore, key []byte) ([]byte, error) {
				if err := st.PutAt(ctx, store.HashFieldKey(key, []byte("field")), []byte("value"), 1, 0); err != nil {
					return nil, err
				}
				deltaKey := store.HashMetaDeltaKey(key, 2, 0)
				return deltaKey, st.PutAt(ctx, deltaKey, store.MarshalHashMetaDelta(store.HashMetaDelta{LenDelta: 1}), 2, 0)
			},
			readMeta: func(st store.MVCCStore, key []byte, readTS uint64) (int64, uint64, error) {
				raw, err := st.GetAt(ctx, store.HashMetaKey(key), readTS)
				if err != nil {
					return 0, 0, err
				}
				meta, err := store.UnmarshalHashMeta(raw)
				return meta.Len, meta.ExpireAt, err
			},
		},
		{
			name: "set",
			key:  []byte("ttl:migrate:delta-only:set"),
			seed: func(st store.MVCCStore, key []byte) ([]byte, error) {
				if err := st.PutAt(ctx, store.SetMemberKey(key, []byte("member")), []byte{}, 1, 0); err != nil {
					return nil, err
				}
				deltaKey := store.SetMetaDeltaKey(key, 2, 0)
				return deltaKey, st.PutAt(ctx, deltaKey, store.MarshalSetMetaDelta(store.SetMetaDelta{LenDelta: 1}), 2, 0)
			},
			readMeta: func(st store.MVCCStore, key []byte, readTS uint64) (int64, uint64, error) {
				raw, err := st.GetAt(ctx, store.SetMetaKey(key), readTS)
				if err != nil {
					return 0, 0, err
				}
				meta, err := store.UnmarshalSetMeta(raw)
				return meta.Len, meta.ExpireAt, err
			},
		},
		{
			name: "zset",
			key:  []byte("ttl:migrate:delta-only:zset"),
			seed: func(st store.MVCCStore, key []byte) ([]byte, error) {
				score := 1.5
				member := []byte("member")
				if err := st.PutAt(ctx, store.ZSetMemberKey(key, member), store.MarshalZSetScore(score), 1, 0); err != nil {
					return nil, err
				}
				if err := st.PutAt(ctx, store.ZSetScoreKey(key, score, member), []byte{}, 1, 0); err != nil {
					return nil, err
				}
				deltaKey := store.ZSetMetaDeltaKey(key, 2, 0)
				return deltaKey, st.PutAt(ctx, deltaKey, store.MarshalZSetMetaDelta(store.ZSetMetaDelta{LenDelta: 1}), 2, 0)
			},
			readMeta: func(st store.MVCCStore, key []byte, readTS uint64) (int64, uint64, error) {
				raw, err := st.GetAt(ctx, store.ZSetMetaKey(key), readTS)
				if err != nil {
					return 0, 0, err
				}
				meta, err := store.UnmarshalZSetMeta(raw)
				return meta.Len, meta.ExpireAt, err
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, c := newDeltaCompactorTestFixture(t)
			deltaKey, err := tc.seed(st, tc.key)
			require.NoError(t, err)
			require.NoError(t, st.PutAt(ctx, redisTTLKey(tc.key), encodeRedisTTL(expireAt), 3, 0))

			require.NoError(t, c.SyncOnce(ctx))

			n, ttlMs, err := tc.readMeta(st, tc.key, st.LastCommitTS())
			require.NoError(t, err)
			require.Equal(t, int64(1), n)
			require.Equal(t, redisExpireAtMillis(expireAt), ttlMs)
			_, err = st.GetAt(ctx, deltaKey, st.LastCommitTS())
			require.ErrorIs(t, err, store.ErrKeyNotFound)
		})
	}
}

func TestDeltaCompactor_PreservesLegacyTTLWhenCompactingDeltaOnlyCollections(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	expireAt := time.Now().Add(time.Hour)
	cases := []struct {
		name     string
		key      []byte
		seed     func(*testing.T, store.MVCCStore, []byte) []byte
		readMeta func(*testing.T, store.MVCCStore, []byte, uint64) (int64, uint64)
	}{
		{
			name: "list",
			key:  []byte("ttl:compact:delta-only:list"),
			seed: func(t *testing.T, st store.MVCCStore, key []byte) []byte {
				t.Helper()
				require.NoError(t, st.PutAt(ctx, store.ListItemKey(key, 0), []byte("a"), 1, 0))
				require.NoError(t, st.PutAt(ctx, store.ListItemKey(key, 1), []byte("b"), 1, 0))
				d1 := store.ListMetaDeltaKey(key, 2, 0)
				d2 := store.ListMetaDeltaKey(key, 3, 0)
				require.NoError(t, st.PutAt(ctx, d1, store.MarshalListMetaDelta(store.ListMetaDelta{LenDelta: 1}), 2, 0))
				require.NoError(t, st.PutAt(ctx, d2, store.MarshalListMetaDelta(store.ListMetaDelta{LenDelta: 1}), 3, 0))
				return d2
			},
			readMeta: func(t *testing.T, st store.MVCCStore, key []byte, readTS uint64) (int64, uint64) {
				t.Helper()
				raw, err := st.GetAt(ctx, store.ListMetaKey(key), readTS)
				require.NoError(t, err)
				meta, err := store.UnmarshalListMeta(raw)
				require.NoError(t, err)
				return meta.Len, meta.ExpireAt
			},
		},
		{
			name: "hash",
			key:  []byte("ttl:compact:delta-only:hash"),
			seed: func(t *testing.T, st store.MVCCStore, key []byte) []byte {
				t.Helper()
				require.NoError(t, st.PutAt(ctx, store.HashFieldKey(key, []byte("a")), []byte("1"), 1, 0))
				require.NoError(t, st.PutAt(ctx, store.HashFieldKey(key, []byte("b")), []byte("2"), 1, 0))
				d1 := store.HashMetaDeltaKey(key, 2, 0)
				d2 := store.HashMetaDeltaKey(key, 3, 0)
				require.NoError(t, st.PutAt(ctx, d1, store.MarshalHashMetaDelta(store.HashMetaDelta{LenDelta: 1}), 2, 0))
				require.NoError(t, st.PutAt(ctx, d2, store.MarshalHashMetaDelta(store.HashMetaDelta{LenDelta: 1}), 3, 0))
				return d2
			},
			readMeta: func(t *testing.T, st store.MVCCStore, key []byte, readTS uint64) (int64, uint64) {
				t.Helper()
				raw, err := st.GetAt(ctx, store.HashMetaKey(key), readTS)
				require.NoError(t, err)
				meta, err := store.UnmarshalHashMeta(raw)
				require.NoError(t, err)
				return meta.Len, meta.ExpireAt
			},
		},
		{
			name: "set",
			key:  []byte("ttl:compact:delta-only:set"),
			seed: func(t *testing.T, st store.MVCCStore, key []byte) []byte {
				t.Helper()
				require.NoError(t, st.PutAt(ctx, store.SetMemberKey(key, []byte("a")), []byte{}, 1, 0))
				require.NoError(t, st.PutAt(ctx, store.SetMemberKey(key, []byte("b")), []byte{}, 1, 0))
				d1 := store.SetMetaDeltaKey(key, 2, 0)
				d2 := store.SetMetaDeltaKey(key, 3, 0)
				require.NoError(t, st.PutAt(ctx, d1, store.MarshalSetMetaDelta(store.SetMetaDelta{LenDelta: 1}), 2, 0))
				require.NoError(t, st.PutAt(ctx, d2, store.MarshalSetMetaDelta(store.SetMetaDelta{LenDelta: 1}), 3, 0))
				return d2
			},
			readMeta: func(t *testing.T, st store.MVCCStore, key []byte, readTS uint64) (int64, uint64) {
				t.Helper()
				raw, err := st.GetAt(ctx, store.SetMetaKey(key), readTS)
				require.NoError(t, err)
				meta, err := store.UnmarshalSetMeta(raw)
				require.NoError(t, err)
				return meta.Len, meta.ExpireAt
			},
		},
		{
			name: "zset",
			key:  []byte("ttl:compact:delta-only:zset"),
			seed: func(t *testing.T, st store.MVCCStore, key []byte) []byte {
				t.Helper()
				require.NoError(t, st.PutAt(ctx, store.ZSetMemberKey(key, []byte("a")), store.MarshalZSetScore(1), 1, 0))
				require.NoError(t, st.PutAt(ctx, store.ZSetMemberKey(key, []byte("b")), store.MarshalZSetScore(2), 1, 0))
				d1 := store.ZSetMetaDeltaKey(key, 2, 0)
				d2 := store.ZSetMetaDeltaKey(key, 3, 0)
				require.NoError(t, st.PutAt(ctx, d1, store.MarshalZSetMetaDelta(store.ZSetMetaDelta{LenDelta: 1}), 2, 0))
				require.NoError(t, st.PutAt(ctx, d2, store.MarshalZSetMetaDelta(store.ZSetMetaDelta{LenDelta: 1}), 3, 0))
				return d2
			},
			readMeta: func(t *testing.T, st store.MVCCStore, key []byte, readTS uint64) (int64, uint64) {
				t.Helper()
				raw, err := st.GetAt(ctx, store.ZSetMetaKey(key), readTS)
				require.NoError(t, err)
				meta, err := store.UnmarshalZSetMeta(raw)
				require.NoError(t, err)
				return meta.Len, meta.ExpireAt
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, c := newDeltaCompactorTestFixture(t)
			lastDelta := tc.seed(t, st, tc.key)
			require.NoError(t, st.PutAt(ctx, redisTTLKey(tc.key), encodeRedisTTL(expireAt), 4, 0))

			require.NoError(t, c.SyncOnce(ctx))

			readTS := st.LastCommitTS()
			n, ttlMs := tc.readMeta(t, st, tc.key, readTS)
			require.Equal(t, int64(2), n)
			require.Equal(t, redisExpireAtMillis(expireAt), ttlMs)
			var err error
			_, err = st.GetAt(ctx, lastDelta, readTS)
			require.ErrorIs(t, err, store.ErrKeyNotFound)
		})
	}
}

func TestDeltaCompactor_DoesNotInheritStaleTTLBeforeDeltaOnlyRecreate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	staleExpireAt := time.Now().Add(time.Hour)
	cases := []struct {
		name     string
		key      []byte
		metaKey  func([]byte) []byte
		seed     func(*testing.T, store.MVCCStore, []byte)
		build    func(*testing.T, *DeltaCompactor, store.MVCCStore, []byte, uint64) []*kv.Elem[kv.OP]
		readMeta func(*testing.T, []byte) (int64, uint64)
	}{
		{
			name:    "list",
			key:     []byte("ttl:compact:recreate:list"),
			metaKey: store.ListMetaKey,
			seed: func(t *testing.T, st store.MVCCStore, key []byte) {
				t.Helper()
				require.NoError(t, st.PutAt(ctx, store.ListItemKey(key, 0), []byte("a"), 3, 0))
				deltaKey := store.ListMetaDeltaKey(key, 3, 0)
				require.NoError(t, st.PutAt(ctx, deltaKey, store.MarshalListMetaDelta(store.ListMetaDelta{LenDelta: 1}), 3, 0))
			},
			build: func(t *testing.T, c *DeltaCompactor, st store.MVCCStore, key []byte, readTS uint64) []*kv.Elem[kv.OP] {
				t.Helper()
				prefix := store.ListMetaDeltaScanPrefix(key)
				deltas, err := st.ScanAt(ctx, prefix, store.PrefixScanEnd(prefix), 10, readTS)
				require.NoError(t, err)
				elems, err := c.buildListCompactElems(ctx, key, deltas, readTS)
				require.NoError(t, err)
				return elems
			},
			readMeta: func(t *testing.T, raw []byte) (int64, uint64) {
				t.Helper()
				meta, err := store.UnmarshalListMeta(raw)
				require.NoError(t, err)
				return meta.Len, meta.ExpireAt
			},
		},
		{
			name:    "list-legacy",
			key:     []byte("ttl:compact:recreate:list-legacy"),
			metaKey: store.ListMetaKey,
			seed: func(t *testing.T, st store.MVCCStore, key []byte) {
				t.Helper()
				require.NoError(t, st.PutAt(ctx, store.ListItemKey(key, 0), []byte("a"), 3, 0))
				deltaKey := legacyListMetaDeltaKey(key, 3)
				require.NoError(t, st.PutAt(ctx, deltaKey, store.MarshalListMetaDelta(store.ListMetaDelta{LenDelta: 1}), 3, 0))
			},
			build: func(t *testing.T, c *DeltaCompactor, st store.MVCCStore, key []byte, readTS uint64) []*kv.Elem[kv.OP] {
				t.Helper()
				prefix := store.LegacyListMetaDeltaScanPrefix(key)
				deltas, err := st.ScanAt(ctx, prefix, store.PrefixScanEnd(prefix), 10, readTS)
				require.NoError(t, err)
				elems, err := c.buildLegacyListCompactElems(ctx, key, deltas, readTS)
				require.NoError(t, err)
				return elems
			},
			readMeta: func(t *testing.T, raw []byte) (int64, uint64) {
				t.Helper()
				meta, err := store.UnmarshalListMeta(raw)
				require.NoError(t, err)
				return meta.Len, meta.ExpireAt
			},
		},
		{
			name:    "hash",
			key:     []byte("ttl:compact:recreate:hash"),
			metaKey: store.HashMetaKey,
			seed: func(t *testing.T, st store.MVCCStore, key []byte) {
				t.Helper()
				require.NoError(t, st.PutAt(ctx, store.HashFieldKey(key, []byte("a")), []byte("1"), 3, 0))
				deltaKey := store.HashMetaDeltaKey(key, 3, 0)
				require.NoError(t, st.PutAt(ctx, deltaKey, store.MarshalHashMetaDelta(store.HashMetaDelta{LenDelta: 1}), 3, 0))
			},
			build: func(t *testing.T, c *DeltaCompactor, st store.MVCCStore, key []byte, readTS uint64) []*kv.Elem[kv.OP] {
				t.Helper()
				prefix := store.HashMetaDeltaScanPrefix(key)
				deltas, err := st.ScanAt(ctx, prefix, store.PrefixScanEnd(prefix), 10, readTS)
				require.NoError(t, err)
				elems, err := c.buildSimpleCompactElems(
					ctx, key, deltas, readTS, store.HashMetaKey(key), prefix,
					func(raw []byte) (int64, uint64, error) {
						meta, err := store.UnmarshalHashMeta(raw)
						return meta.Len, meta.ExpireAt, err
					},
					func(raw []byte) (int64, error) {
						delta, err := store.UnmarshalHashMetaDelta(raw)
						return delta.LenDelta, err
					},
					func(n int64, expireAt uint64) []byte {
						return store.MarshalHashMeta(store.HashMeta{Len: n, ExpireAt: expireAt})
					},
				)
				require.NoError(t, err)
				return elems
			},
			readMeta: func(t *testing.T, raw []byte) (int64, uint64) {
				t.Helper()
				meta, err := store.UnmarshalHashMeta(raw)
				require.NoError(t, err)
				return meta.Len, meta.ExpireAt
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, c := newDeltaCompactorTestFixture(t)
			require.NoError(t, st.PutAt(ctx, redisTTLKey(tc.key), encodeRedisTTL(staleExpireAt), 1, 0))
			tc.seed(t, st, tc.key)

			elems := tc.build(t, c, st, tc.key, st.LastCommitTS())
			raw := elemValueForKey(t, elems, tc.metaKey(tc.key))
			n, ttlMs := tc.readMeta(t, raw)
			require.Equal(t, int64(1), n)
			require.Zero(t, ttlMs)
		})
	}
}

func TestRedisZSetInlineMetaCompactionPreservesLegacyTTL(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	server := &RedisServer{store: st}
	key := []byte("ttl:compact:inline:zset")
	expireAt := time.Now().Add(time.Hour)
	for ts := uint64(1); ts <= store.MaxDeltaScanLimit; ts++ {
		require.NoError(t, st.PutAt(ctx, store.ZSetMetaDeltaKey(key, ts, 0),
			store.MarshalZSetMetaDelta(store.ZSetMetaDelta{LenDelta: 1}), ts, 0))
	}
	require.NoError(t, st.PutAt(ctx, redisTTLKey(key), encodeRedisTTL(expireAt), uint64(store.MaxDeltaScanLimit+1), 0))

	elems, compacted, err := server.zsetInlineMetaCompactionElems(ctx, key, st.LastCommitTS(), 1)
	require.NoError(t, err)
	require.True(t, compacted)

	var meta store.ZSetMeta
	for _, elem := range elems {
		if elem.Op == kv.Put && bytes.Equal(elem.Key, store.ZSetMetaKey(key)) {
			meta, err = store.UnmarshalZSetMeta(elem.Value)
			require.NoError(t, err)
			break
		}
	}
	require.Equal(t, int64(store.MaxDeltaScanLimit+1), meta.Len)
	require.Equal(t, redisExpireAtMillis(expireAt), meta.ExpireAt)
}

func TestDeltaCompactor_TTLInlineMigratesLegacyCollectionBlobsFromTTLIndex(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	expireAt := time.Now().Add(time.Hour)
	cases := []struct {
		name        string
		key         []byte
		legacyKey   func([]byte) []byte
		seed        func(store.MVCCStore, []byte) error
		readMeta    func(store.MVCCStore, []byte, uint64) (int64, uint64, error)
		requireRows func(*testing.T, store.MVCCStore, []byte, uint64)
	}{
		{
			name:      "hash",
			key:       []byte("ttl:migrate:legacy:hash"),
			legacyKey: redisHashKey,
			seed: func(st store.MVCCStore, key []byte) error {
				raw, err := marshalHashValue(redisHashValue{"field": "value"})
				if err != nil {
					return err
				}
				return st.PutAt(ctx, redisHashKey(key), raw, 1, 0)
			},
			readMeta: func(st store.MVCCStore, key []byte, readTS uint64) (int64, uint64, error) {
				raw, err := st.GetAt(ctx, store.HashMetaKey(key), readTS)
				if err != nil {
					return 0, 0, err
				}
				meta, err := store.UnmarshalHashMeta(raw)
				return meta.Len, meta.ExpireAt, err
			},
			requireRows: func(t *testing.T, st store.MVCCStore, key []byte, readTS uint64) {
				t.Helper()
				raw, err := st.GetAt(ctx, store.HashFieldKey(key, []byte("field")), readTS)
				require.NoError(t, err)
				require.Equal(t, []byte("value"), raw)
			},
		},
		{
			name:      "set",
			key:       []byte("ttl:migrate:legacy:set"),
			legacyKey: redisSetKey,
			seed: func(st store.MVCCStore, key []byte) error {
				raw, err := marshalSetValue(redisSetValue{Members: []string{"member"}})
				if err != nil {
					return err
				}
				return st.PutAt(ctx, redisSetKey(key), raw, 1, 0)
			},
			readMeta: func(st store.MVCCStore, key []byte, readTS uint64) (int64, uint64, error) {
				raw, err := st.GetAt(ctx, store.SetMetaKey(key), readTS)
				if err != nil {
					return 0, 0, err
				}
				meta, err := store.UnmarshalSetMeta(raw)
				return meta.Len, meta.ExpireAt, err
			},
			requireRows: func(t *testing.T, st store.MVCCStore, key []byte, readTS uint64) {
				t.Helper()
				raw, err := st.GetAt(ctx, store.SetMemberKey(key, []byte("member")), readTS)
				require.NoError(t, err)
				require.Empty(t, raw)
			},
		},
		{
			name:      "zset",
			key:       []byte("ttl:migrate:legacy:zset"),
			legacyKey: redisZSetKey,
			seed: func(st store.MVCCStore, key []byte) error {
				raw, err := marshalZSetValue(redisZSetValue{Entries: []redisZSetEntry{{Member: "member", Score: 1.5}}})
				if err != nil {
					return err
				}
				return st.PutAt(ctx, redisZSetKey(key), raw, 1, 0)
			},
			readMeta: func(st store.MVCCStore, key []byte, readTS uint64) (int64, uint64, error) {
				raw, err := st.GetAt(ctx, store.ZSetMetaKey(key), readTS)
				if err != nil {
					return 0, 0, err
				}
				meta, err := store.UnmarshalZSetMeta(raw)
				return meta.Len, meta.ExpireAt, err
			},
			requireRows: func(t *testing.T, st store.MVCCStore, key []byte, readTS uint64) {
				t.Helper()
				raw, err := st.GetAt(ctx, store.ZSetMemberKey(key, []byte("member")), readTS)
				require.NoError(t, err)
				score, err := store.UnmarshalZSetScore(raw)
				require.NoError(t, err)
				require.Equal(t, 1.5, score)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, c := newDeltaCompactorTestFixture(t)
			require.NoError(t, tc.seed(st, tc.key))
			require.NoError(t, st.PutAt(ctx, redisTTLKey(tc.key), encodeRedisTTL(expireAt), 2, 0))

			require.NoError(t, c.SyncOnce(ctx))

			readTS := st.LastCommitTS()
			n, ttlMs, err := tc.readMeta(st, tc.key, readTS)
			require.NoError(t, err)
			require.Equal(t, int64(1), n)
			require.Equal(t, redisExpireAtMillis(expireAt), ttlMs)
			tc.requireRows(t, st, tc.key, readTS)
			_, err = st.GetAt(ctx, tc.legacyKey(tc.key), readTS)
			require.ErrorIs(t, err, store.ErrKeyNotFound)
		})
	}
}

type timeoutScanStore struct {
	store.MVCCStore
	scans  int
	starts [][]byte
}

func (s *timeoutScanStore) ScanAt(ctx context.Context, start []byte, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	s.scans++
	s.starts = append(s.starts, bytes.Clone(start))
	<-ctx.Done()
	return nil, ctx.Err()
}

type recordingCompactorCoordinator struct {
	stubAdapterCoordinator
	labels []keyviz.Label
}

func (c *recordingCompactorCoordinator) Dispatch(_ context.Context, reqs *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	if reqs != nil {
		c.labels = append(c.labels, reqs.KeyVizLabel)
	}
	return &kv.CoordinateResponse{}, nil
}

func TestDeltaCompactorStampsRedisKeyVizLabel(t *testing.T) {
	t.Parallel()
	rec := &recordingCompactorCoordinator{}
	c := NewDeltaCompactor(store.NewMVCCStore(), rec)
	_, err := c.coord.Dispatch(context.Background(), &kv.OperationGroup[kv.OP]{})
	require.NoError(t, err)
	require.Equal(t, []keyviz.Label{keyviz.LabelRedis}, rec.labels)
}

func TestDeltaCompactor_BackoffAfterTimeout(t *testing.T) {
	t.Parallel()

	base := store.NewMVCCStore()
	st := &timeoutScanStore{MVCCStore: base}
	coord := newLocalAdapterCoordinator(st)
	c := NewDeltaCompactor(
		st,
		coord,
		WithDeltaCompactorTimeout(time.Millisecond),
		WithDeltaCompactorTimeoutBackoff(time.Hour),
	)

	err := c.SyncOnce(context.Background())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, 1, st.scans, "timeout pass should run one scan before yielding")

	require.NoError(t, c.SyncOnce(context.Background()))
	require.Equal(t, 1, st.scans, "backoff pass should not scan again")
}

func TestDeltaCompactor_TTLInlineStopsAfterTimeout(t *testing.T) {
	t.Parallel()

	base := store.NewMVCCStore()
	st := &timeoutScanStore{MVCCStore: base}
	coord := &stubAdapterCoordinator{leaderSet: true, leader: false}
	c := NewDeltaCompactor(
		st,
		coord,
		WithDeltaCompactorTimeout(time.Millisecond),
		WithDeltaCompactorTimeoutBackoff(0),
	)

	err := c.SyncOnce(context.Background())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, 1, st.scans, "timeout pass should stop after the first ttl-inline scan")
	require.Len(t, st.starts, 1)
	require.True(t, bytes.HasPrefix(st.starts[0], []byte(redisStrPrefix)))
}

func TestDeltaCompactor_RotatesAllBackgroundWorkAfterTimeout(t *testing.T) {
	t.Parallel()

	base := store.NewMVCCStore()
	st := &timeoutScanStore{MVCCStore: base}
	coord := newLocalAdapterCoordinator(st)
	c := NewDeltaCompactor(
		st,
		coord,
		WithDeltaCompactorTimeout(10*time.Millisecond),
		WithDeltaCompactorTimeoutBackoff(0),
	)

	wantPrefixes := []string{
		store.ListMetaDeltaPrefix,
		store.LegacyListMetaDeltaPrefix,
		store.HashMetaDeltaPrefix,
		store.SetMetaDeltaPrefix,
		store.ZSetMetaDeltaPrefix,
	}
	for _, handler := range c.ttlInlineMigrationHandlers() {
		wantPrefixes = append(wantPrefixes, string(handler.prefix))
	}
	wantPrefixes = append(wantPrefixes, store.ListMetaDeltaPrefix)
	for _, prefix := range wantPrefixes {
		err := c.SyncOnce(context.Background())
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Len(t, st.starts, st.scans)
		require.True(t, bytes.HasPrefix(st.starts[len(st.starts)-1], []byte(prefix)),
			"scan should start with %q after timeout rotation; got %q", prefix, st.starts[len(st.starts)-1])
	}
}

func TestDeltaCompactor_ListDeltaFoldedIntoBaseMeta(t *testing.T) {
	t.Parallel()

	st, c := newDeltaCompactorTestFixture(t)
	ctx := context.Background()
	userKey := []byte("mylist")

	// Write a base meta: Head=0, Len=10.
	baseMeta := store.ListMeta{Head: 0, Len: 10}
	metaBytes, err := store.MarshalListMeta(baseMeta)
	require.NoError(t, err)
	require.NoError(t, st.PutAt(ctx, store.ListMetaKey(userKey), metaBytes, 1, 0))

	// Write 3 delta keys (HeadDelta=1, LenDelta=-1 each).
	delta := store.MarshalListMetaDelta(store.ListMetaDelta{HeadDelta: 1, LenDelta: -1})
	d1Key := store.ListMetaDeltaKey(userKey, 10, 0)
	d2Key := store.ListMetaDeltaKey(userKey, 11, 0)
	d3Key := store.ListMetaDeltaKey(userKey, 12, 0)
	require.NoError(t, st.PutAt(ctx, d1Key, delta, 10, 0))
	require.NoError(t, st.PutAt(ctx, d2Key, delta, 11, 0))
	require.NoError(t, st.PutAt(ctx, d3Key, delta, 12, 0))

	require.NoError(t, c.SyncOnce(ctx))

	readTS := st.LastCommitTS()

	// Base meta should reflect Head=3, Len=7.
	raw, err := st.GetAt(ctx, store.ListMetaKey(userKey), readTS)
	require.NoError(t, err)
	got, err := store.UnmarshalListMeta(raw)
	require.NoError(t, err)
	require.Equal(t, int64(3), got.Head)
	require.Equal(t, int64(7), got.Len)

	// All 3 delta keys should be deleted.
	for _, dk := range [][]byte{d1Key, d2Key, d3Key} {
		_, getErr := st.GetAt(ctx, dk, readTS)
		require.ErrorIs(t, getErr, store.ErrKeyNotFound, "delta key should be deleted after compaction: %s", dk)
	}
}

func TestDeltaCompactor_LegacyListDeltaFoldedIntoBaseMeta(t *testing.T) {
	t.Parallel()

	st, c := newDeltaCompactorTestFixture(t)
	ctx := context.Background()
	userKey := []byte("legacy-list")

	baseMeta := store.ListMeta{Head: 5, Len: 2}
	baseMeta.Tail = baseMeta.Head + baseMeta.Len
	metaBytes, err := store.MarshalListMeta(baseMeta)
	require.NoError(t, err)
	require.NoError(t, st.PutAt(ctx, store.ListMetaKey(userKey), metaBytes, 1, 0))

	delta := store.MarshalListMetaDelta(store.ListMetaDelta{HeadDelta: -1, LenDelta: 2})
	d1Key := legacyListMetaDeltaKey(userKey, 10)
	d2Key := legacyListMetaDeltaKey(userKey, 11)
	require.NoError(t, st.PutAt(ctx, d1Key, delta, 10, 0))
	require.NoError(t, st.PutAt(ctx, d2Key, delta, 11, 0))

	require.NoError(t, c.SyncOnce(ctx))

	readTS := st.LastCommitTS()
	raw, err := st.GetAt(ctx, store.ListMetaKey(userKey), readTS)
	require.NoError(t, err)
	got, err := store.UnmarshalListMeta(raw)
	require.NoError(t, err)
	require.Equal(t, int64(3), got.Head)
	require.Equal(t, int64(6), got.Len)
	require.Equal(t, int64(9), got.Tail)

	for _, dk := range [][]byte{d1Key, d2Key} {
		_, getErr := st.GetAt(ctx, dk, readTS)
		require.ErrorIs(t, getErr, store.ErrKeyNotFound, "legacy delta key should be deleted after compaction: %s", dk)
	}
}

func TestDeltaCompactor_ListBelowThresholdNotCompacted(t *testing.T) {
	t.Parallel()

	st, c := newDeltaCompactorTestFixture(t) // threshold = 2
	ctx := context.Background()
	userKey := []byte("shortlist")

	baseMeta := store.ListMeta{Head: 0, Len: 5}
	metaBytes, err := store.MarshalListMeta(baseMeta)
	require.NoError(t, err)
	require.NoError(t, st.PutAt(ctx, store.ListMetaKey(userKey), metaBytes, 1, 0))

	// Only 1 delta key — below the threshold of 2.
	delta := store.MarshalListMetaDelta(store.ListMetaDelta{HeadDelta: 1, LenDelta: -1})
	d1Key := store.ListMetaDeltaKey(userKey, 10, 0)
	require.NoError(t, st.PutAt(ctx, d1Key, delta, 10, 0))

	require.NoError(t, c.SyncOnce(ctx))

	readTS := st.LastCommitTS()

	// Delta key should still exist (not compacted).
	_, getErr := st.GetAt(ctx, d1Key, readTS)
	require.NoError(t, getErr, "delta key below threshold should not be deleted")

	// Base meta should be unchanged (Head=0, Len=5).
	raw, err := st.GetAt(ctx, store.ListMetaKey(userKey), readTS)
	require.NoError(t, err)
	got, err := store.UnmarshalListMeta(raw)
	require.NoError(t, err)
	require.Equal(t, int64(0), got.Head)
	require.Equal(t, int64(5), got.Len)
}

func TestDeltaCompactor_HashDeltaFoldedIntoBaseMeta(t *testing.T) {
	t.Parallel()

	st, c := newDeltaCompactorTestFixture(t)
	ctx := context.Background()
	userKey := []byte("myhash")

	// Write base meta: Len=10.
	require.NoError(t, st.PutAt(ctx, store.HashMetaKey(userKey), store.MarshalHashMeta(store.HashMeta{Len: 10}), 1, 0))

	// Write 2 delta keys with LenDelta=+1 each.
	delta := store.MarshalHashMetaDelta(store.HashMetaDelta{LenDelta: 1})
	d1Key := store.HashMetaDeltaKey(userKey, 10, 0)
	d2Key := store.HashMetaDeltaKey(userKey, 11, 0)
	require.NoError(t, st.PutAt(ctx, d1Key, delta, 10, 0))
	require.NoError(t, st.PutAt(ctx, d2Key, delta, 11, 0))

	require.NoError(t, c.SyncOnce(ctx))

	readTS := st.LastCommitTS()

	raw, err := st.GetAt(ctx, store.HashMetaKey(userKey), readTS)
	require.NoError(t, err)
	got, err := store.UnmarshalHashMeta(raw)
	require.NoError(t, err)
	require.Equal(t, int64(12), got.Len)

	for _, dk := range [][]byte{d1Key, d2Key} {
		_, getErr := st.GetAt(ctx, dk, readTS)
		require.ErrorIs(t, getErr, store.ErrKeyNotFound)
	}
}

func TestDeltaCompactor_SetDeltaFoldedIntoBaseMeta(t *testing.T) {
	t.Parallel()

	st, c := newDeltaCompactorTestFixture(t)
	ctx := context.Background()
	userKey := []byte("myset")

	require.NoError(t, st.PutAt(ctx, store.SetMetaKey(userKey), store.MarshalSetMeta(store.SetMeta{Len: 5}), 1, 0))

	delta := store.MarshalSetMetaDelta(store.SetMetaDelta{LenDelta: 2})
	d1Key := store.SetMetaDeltaKey(userKey, 10, 0)
	d2Key := store.SetMetaDeltaKey(userKey, 11, 0)
	require.NoError(t, st.PutAt(ctx, d1Key, delta, 10, 0))
	require.NoError(t, st.PutAt(ctx, d2Key, delta, 11, 0))

	require.NoError(t, c.SyncOnce(ctx))

	readTS := st.LastCommitTS()

	raw, err := st.GetAt(ctx, store.SetMetaKey(userKey), readTS)
	require.NoError(t, err)
	got, err := store.UnmarshalSetMeta(raw)
	require.NoError(t, err)
	require.Equal(t, int64(9), got.Len)

	for _, dk := range [][]byte{d1Key, d2Key} {
		_, getErr := st.GetAt(ctx, dk, readTS)
		require.ErrorIs(t, getErr, store.ErrKeyNotFound)
	}
}

func TestDeltaCompactor_ZSetDeltaFoldedIntoBaseMeta(t *testing.T) {
	t.Parallel()

	st, c := newDeltaCompactorTestFixture(t)
	ctx := context.Background()
	userKey := []byte("myzset")

	require.NoError(t, st.PutAt(ctx, store.ZSetMetaKey(userKey), store.MarshalZSetMeta(store.ZSetMeta{Len: 3}), 1, 0))

	delta := store.MarshalZSetMetaDelta(store.ZSetMetaDelta{LenDelta: 1})
	d1Key := store.ZSetMetaDeltaKey(userKey, 10, 0)
	d2Key := store.ZSetMetaDeltaKey(userKey, 11, 0)
	require.NoError(t, st.PutAt(ctx, d1Key, delta, 10, 0))
	require.NoError(t, st.PutAt(ctx, d2Key, delta, 11, 0))

	require.NoError(t, c.SyncOnce(ctx))

	readTS := st.LastCommitTS()

	raw, err := st.GetAt(ctx, store.ZSetMetaKey(userKey), readTS)
	require.NoError(t, err)
	got, err := store.UnmarshalZSetMeta(raw)
	require.NoError(t, err)
	require.Equal(t, int64(5), got.Len)

	for _, dk := range [][]byte{d1Key, d2Key} {
		_, getErr := st.GetAt(ctx, dk, readTS)
		require.ErrorIs(t, getErr, store.ErrKeyNotFound)
	}
}

func TestDeltaCompactor_NonLeaderSkips(t *testing.T) {
	t.Parallel()

	// Use a real (writing) coordinator so that if compaction were incorrectly
	// dispatched the delta keys would actually be deleted, making the assertion
	// meaningful. The stub's IsLeaderForKey returns false to simulate a follower.
	st := store.NewMVCCStore()
	coord := newLocalAdapterCoordinator(st)
	coord.leaderSet = true
	coord.leader = false
	c := NewDeltaCompactor(st, coord, WithDeltaCompactorMaxDeltaCount(1))
	ctx := context.Background()

	userKey := []byte("nonleaderlist")
	delta := store.MarshalListMetaDelta(store.ListMetaDelta{HeadDelta: 1, LenDelta: -1})
	d1Key := store.ListMetaDeltaKey(userKey, 10, 0)
	d2Key := store.ListMetaDeltaKey(userKey, 11, 0)
	require.NoError(t, st.PutAt(ctx, d1Key, delta, 10, 0))
	require.NoError(t, st.PutAt(ctx, d2Key, delta, 11, 0))

	require.NoError(t, c.SyncOnce(ctx))

	readTS := st.LastCommitTS()
	// Delta keys must NOT be touched since this node is not the per-key leader.
	_, getErr := st.GetAt(ctx, d1Key, readTS)
	require.NoError(t, getErr, "non-leader should not compact delta keys")
}

func TestDeltaCompactor_ListNoBaseMeta(t *testing.T) {
	t.Parallel()

	// Compactor should work even when the base meta key doesn't exist yet
	// (all state is in deltas). This can happen during migration.
	st, c := newDeltaCompactorTestFixture(t)
	ctx := context.Background()
	userKey := []byte("newlist")

	delta := store.MarshalListMetaDelta(store.ListMetaDelta{HeadDelta: 1, LenDelta: -1})
	d1Key := store.ListMetaDeltaKey(userKey, 10, 0)
	d2Key := store.ListMetaDeltaKey(userKey, 11, 0)
	require.NoError(t, st.PutAt(ctx, d1Key, delta, 10, 0))
	require.NoError(t, st.PutAt(ctx, d2Key, delta, 11, 0))

	require.NoError(t, c.SyncOnce(ctx))

	readTS := st.LastCommitTS()

	// When the accumulated deltas produce Len=0 (clamped from -2), the metadata key
	// must be deleted rather than written with Len=0. Redis semantics: an empty list
	// is non-existent, so EXISTS/TYPE must not return a stale entry.
	_, err := st.GetAt(ctx, store.ListMetaKey(userKey), readTS)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	// Delta keys must also be deleted by the compaction.
	_, err = st.GetAt(ctx, d1Key, readTS)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	_, err = st.GetAt(ctx, d2Key, readTS)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestDeltaCompactor_LegacyListCursorAdvancesPastFilteredRawRows(t *testing.T) {
	t.Parallel()

	st, c := newDeltaCompactorTestFixture(t)
	ctx := context.Background()
	meta, err := store.MarshalListMeta(store.ListMeta{Head: 4, Tail: 6, Len: 2})
	require.NoError(t, err)
	for i := uint64(1); i <= deltaCompactorTickScanLimit; i++ {
		userKey := deltaLookingListMetaUserKeyAt([]byte("compactor-collision"), i, 0)
		require.NoError(t, st.PutAt(ctx, store.ListMetaKey(userKey), meta, i, 0))
	}

	h := c.legacyListHandler()
	readTS := st.LastCommitTS()
	require.NoError(t, c.compactHandler(ctx, h, readTS))

	c.cursorMu.Lock()
	got := bytes.Clone(c.cursors[h.typeName])
	c.cursorMu.Unlock()
	require.Equal(t, store.ListMetaKey(deltaLookingListMetaUserKeyAt([]byte("compactor-collision"), deltaCompactorTickScanLimit, 0)), got)
}

func TestDeltaCompactor_LegacyListCursorAdvancesPastFilteredTailAfterBacktrack(t *testing.T) {
	t.Parallel()

	st, c := newDeltaCompactorTestFixture(t)
	ctx := context.Background()
	userKey := []byte("compactor-tail")
	delta := store.MarshalListMetaDelta(store.ListMetaDelta{HeadDelta: 0, LenDelta: 1})
	acceptedKey := legacyListMetaDeltaKey(userKey, 1)
	require.NoError(t, st.PutAt(ctx, acceptedKey, delta, 1, 0))

	meta, err := store.MarshalListMeta(store.ListMeta{Head: 4, Tail: 6, Len: 2})
	require.NoError(t, err)
	for i := uint64(2); i <= deltaCompactorTickScanLimit; i++ {
		collidingUserKey := deltaLookingListMetaUserKeyAt(userKey, i, 0)
		require.NoError(t, st.PutAt(ctx, store.ListMetaKey(collidingUserKey), meta, i, 0))
	}

	h := c.legacyListHandler()
	readTS := st.LastCommitTS()
	require.NoError(t, c.compactHandler(ctx, h, readTS))

	c.cursorMu.Lock()
	got := bytes.Clone(c.cursors[h.typeName])
	c.cursorMu.Unlock()
	require.Equal(t, store.ListMetaKey(deltaLookingListMetaUserKeyAt(userKey, deltaCompactorTickScanLimit, 0)), got)
	_, err = st.GetAt(ctx, acceptedKey, readTS)
	require.NoError(t, err)
}

func TestDeltaCompactor_LegacyListCursorAdvancesPastRejectedPrefixBeforeAcceptedTail(t *testing.T) {
	t.Parallel()

	st, c := newDeltaCompactorTestFixture(t)
	ctx := context.Background()
	userKey := []byte("compactor-tail-after-prefix")
	meta, err := store.MarshalListMeta(store.ListMeta{Head: 4, Tail: 6, Len: 2})
	require.NoError(t, err)
	for i := uint64(1); i < deltaCompactorTickScanLimit; i++ {
		collidingUserKey := deltaLookingListMetaUserKeyAt(userKey, i, 0)
		require.NoError(t, st.PutAt(ctx, store.ListMetaKey(collidingUserKey), meta, i, 0))
	}
	delta := store.MarshalListMetaDelta(store.ListMetaDelta{HeadDelta: 0, LenDelta: 1})
	acceptedKey := legacyListMetaDeltaKey(userKey, deltaCompactorTickScanLimit)
	require.NoError(t, st.PutAt(ctx, acceptedKey, delta, deltaCompactorTickScanLimit, 0))

	h := c.legacyListHandler()
	readTS := st.LastCommitTS()
	require.NoError(t, c.compactHandler(ctx, h, readTS))

	c.cursorMu.Lock()
	got := bytes.Clone(c.cursors[h.typeName])
	c.cursorMu.Unlock()
	require.Equal(t, store.ListMetaKey(deltaLookingListMetaUserKeyAt(userKey, deltaCompactorTickScanLimit-1, 0)), got)
	_, err = st.GetAt(ctx, acceptedKey, readTS)
	require.NoError(t, err)
}

func TestDeltaCompactor_CursorBacktracksBeforeWholeAcceptedTail(t *testing.T) {
	t.Parallel()

	userKey := []byte("compactor-accepted-tail")
	delta := store.MarshalListMetaDelta(store.ListMetaDelta{HeadDelta: 0, LenDelta: 1})
	d1 := legacyListMetaDeltaKey(userKey, 1)
	d2 := legacyListMetaDeltaKey(userKey, 2)
	rawKVs := []*store.KVPair{
		{Key: d1, Value: delta},
		{Key: d2, Value: delta},
	}

	got := deltaCompactorCursorAfterFiltering(rawKVs, rawKVs, nil, true, store.ExtractLegacyListUserKeyFromDelta)
	require.Nil(t, got)
}

func TestDeltaCompactor_ListDeltaDeletesPreserveScanRouteGroup(t *testing.T) {
	t.Parallel()

	_, c := newDeltaCompactorTestFixture(t)
	ctx := context.Background()
	userKey := []byte("compactor-route-group")
	delta := store.MarshalListMetaDelta(store.ListMetaDelta{LenDelta: 1})
	d1 := legacyListMetaDeltaKey(userKey, 1)
	d2 := legacyListMetaDeltaKey(userKey, 2)

	elems, err := c.buildListCompactElems(ctx, userKey, []*store.KVPair{
		{Key: d1, Value: delta, RouteGroupID: 7},
		{Key: d2, Value: delta, RouteGroupID: 7},
	}, 3)
	require.NoError(t, err)
	require.Zero(t, elems[0].GroupID)
	require.Equal(t, uint64(7), requireElemByKey(t, elems, d1).GroupID)
	require.Equal(t, uint64(7), requireElemByKey(t, elems, d2).GroupID)
}

// TestDeltaCompactor_UrgentCompactionTriggeredByChannel verifies that a request
// queued via TriggerUrgentCompaction is processed by the Run loop, compacting
// the targeted key without waiting for the next regular tick.
//
// maxCount is set high so the regular SyncOnce pass skips the key, ensuring it
// is the urgent path that performs the actual compaction.
func TestDeltaCompactor_UrgentCompactionTriggeredByChannel(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	coord := newLocalAdapterCoordinator(st)
	// High maxCount ensures SyncOnce does not compact our key; only the urgent path will.
	const hugeMaxCount = 1<<31 - 1
	c := NewDeltaCompactor(st, coord, WithDeltaCompactorMaxDeltaCount(hugeMaxCount))
	ctx := context.Background()
	userKey := []byte("urgent-hash-key")

	// Write base meta: Len=5.
	require.NoError(t, st.PutAt(ctx, store.HashMetaKey(userKey), store.MarshalHashMeta(store.HashMeta{Len: 5}), 1, 0))

	// Write 3 delta keys (LenDelta=+1 each). These are below hugeMaxCount so SyncOnce
	// skips them; TriggerUrgentCompaction must be what kicks off compaction.
	delta := store.MarshalHashMetaDelta(store.HashMetaDelta{LenDelta: 1})
	for i := uint64(0); i < 3; i++ {
		dKey := store.HashMetaDeltaKey(userKey, 10+i, 0)
		require.NoError(t, st.PutAt(ctx, dKey, delta, 10+i, 0))
	}

	// Queue the urgent compaction request.
	c.TriggerUrgentCompaction("hash", userKey)

	// Start the Run loop; it runs SyncOnce (skips the key) then drains the urgent channel.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = c.Run(runCtx) }()

	// Wait until the base meta reflects the compacted value (5 + 3 = 8).
	require.Eventually(t, func() bool {
		readTS := st.LastCommitTS()
		raw, err := st.GetAt(ctx, store.HashMetaKey(userKey), readTS)
		if err != nil {
			return false
		}
		got, err := store.UnmarshalHashMeta(raw)
		return err == nil && got.Len == 8
	}, 2*time.Second, 10*time.Millisecond, "urgent compaction should have updated the hash meta to Len=8")
}

// TestDeltaCompactor_UrgentCompactionPagination verifies that when a key has
// more than MaxDeltaScanLimit delta keys, compactUrgentKey loops until all
// batches are compacted, leaving the key readable.
//
// maxCount is set to MaxInt so the regular SyncOnce pass skips the key
// (threshold not met), ensuring only the urgent path exercises the pagination loop.
func TestDeltaCompactor_UrgentCompactionPagination(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	coord := newLocalAdapterCoordinator(st)
	// Use a very high maxCount so SyncOnce never compacts our test key, forcing
	// the urgent path to handle all batches via the pagination loop.
	const hugeMaxCount = 1<<31 - 1
	c := NewDeltaCompactor(st, coord, WithDeltaCompactorMaxDeltaCount(hugeMaxCount))
	ctx := context.Background()
	userKey := []byte("overflow-hash-key")

	// Write more than MaxDeltaScanLimit delta keys so that a single-pass scan
	// (capped at MaxDeltaScanLimit+1) leaves some unconsumed, requiring pagination.
	// Use a typed constant to avoid int->uint64 overflow warnings.
	const totalDeltasU64 = uint64(store.MaxDeltaScanLimit + 10) // 1034
	delta := store.MarshalHashMetaDelta(store.HashMetaDelta{LenDelta: 1})
	for i := uint64(0); i < totalDeltasU64; i++ {
		dKey := store.HashMetaDeltaKey(userKey, i+1, 0)
		require.NoError(t, st.PutAt(ctx, dKey, delta, i+1, 0))
	}

	// Exercise the urgent pagination loop directly. Running through c.Run would
	// also start an initial background SyncOnce; the local test coordinator applies
	// elems one-by-one instead of atomically, so a test read can observe the meta
	// update before all delete elems have been applied under the race detector.
	c.compactUrgentKey(ctx, urgentCompactionRequest{typeName: "hash", userKey: userKey})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = c.Run(runCtx) }()

	prefix := store.HashMetaDeltaScanPrefix(userKey)
	end := store.PrefixScanEnd(prefix)

	// Wait until the base meta holds the accumulated total and the consumed
	// delta keys are gone, so slow race-enabled CI has one explicit completion
	// condition for the whole urgent pagination loop.
	// The pagination loop should take two passes: first 1025, then 9.
	require.Eventually(t, func() bool {
		readTS := st.LastCommitTS()
		raw, err := st.GetAt(ctx, store.HashMetaKey(userKey), readTS)
		if err != nil {
			return false
		}
		got, err := store.UnmarshalHashMeta(raw)
		if err != nil || got.Len != int64(totalDeltasU64) {
			return false
		}
		remaining, err := st.ScanAt(ctx, prefix, end, int(totalDeltasU64)+1, readTS)
		return err == nil && len(remaining) == 0
	}, 5*time.Second, 20*time.Millisecond, "all %d delta keys should be compacted into base meta", totalDeltasU64)

	// No delta keys should remain after pagination compaction.
	readTS := st.LastCommitTS()
	remaining, err := st.ScanAt(ctx, prefix, end, int(totalDeltasU64)+1, readTS)
	require.NoError(t, err)
	require.Empty(t, remaining, "all delta keys must be deleted after urgent compaction")
}

// TestZSetInlineMetaCompaction verifies that zsetInlineMetaCompactionElems
// returns nil when below the threshold, and folds delta keys into the base
// ZSetMetaKey (deleting all scanned delta keys) when at or above the threshold.
func TestZSetInlineMetaCompaction(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := newLocalAdapterCoordinator(st)
	r := &RedisServer{store: st, coordinator: coord}
	userKey := []byte("inline:zset")

	// Write a base meta key with Len=10.
	require.NoError(t, st.PutAt(ctx, store.ZSetMetaKey(userKey),
		store.MarshalZSetMeta(store.ZSetMeta{Len: 10}), 1, 0))

	// Write (threshold - 1) delta keys: should NOT trigger compaction.
	const threshold = zsetInlineMetaCompactionThreshold
	delta := store.MarshalZSetMetaDelta(store.ZSetMetaDelta{LenDelta: 1})
	for i := range threshold - 1 {
		ts := uint64(10 + i) //nolint:gosec // i is bounded by threshold (1024)
		require.NoError(t, st.PutAt(ctx, store.ZSetMetaDeltaKey(userKey, ts, 0), delta, ts, 0))
	}

	readTS := st.LastCommitTS()
	elems, compacted, err := r.zsetInlineMetaCompactionElems(ctx, userKey, readTS, 0)
	require.NoError(t, err)
	require.False(t, compacted, "below threshold: should not compact")
	require.Nil(t, elems)

	// Write one more delta key to reach the threshold.
	const lastTS = uint64(10 + threshold - 1)
	require.NoError(t, st.PutAt(ctx, store.ZSetMetaDeltaKey(userKey, lastTS, 0), delta, lastTS, 0))

	readTS = st.LastCommitTS()
	elems, compacted, err = r.zsetInlineMetaCompactionElems(ctx, userKey, readTS, 2)
	require.NoError(t, err)
	require.True(t, compacted, "at threshold: should compact")
	// Expected new Len = base(10) + threshold deltas(each +1) + additionalDelta(2)
	expectedLen := int64(10+threshold) + 2

	// Dispatch the compaction elems.
	commitTS := coord.Clock().Next()
	_, dispatchErr := coord.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  0,
		CommitTS: commitTS,
		Elems:    elems,
	})
	require.NoError(t, dispatchErr)

	// Verify base meta holds the folded total.
	afterTS := st.LastCommitTS()
	raw, err := st.GetAt(ctx, store.ZSetMetaKey(userKey), afterTS)
	require.NoError(t, err)
	got, err := store.UnmarshalZSetMeta(raw)
	require.NoError(t, err)
	require.Equal(t, expectedLen, got.Len)

	// All delta keys must be deleted.
	prefix := store.ZSetMetaDeltaScanPrefix(userKey)
	scanEnd := store.PrefixScanEnd(prefix)
	remaining, err := st.ScanAt(ctx, prefix, scanEnd, threshold+1, afterTS)
	require.NoError(t, err)
	require.Empty(t, remaining, "all delta keys must be removed after inline compaction")
}

func TestListInlineMetaCompaction(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	r := &RedisServer{store: st}
	userKey := []byte("inline:list")

	baseMeta := store.ListMeta{Head: 100, Len: 10, ExpireAt: 1234}
	metaBytes, err := store.MarshalListMeta(baseMeta)
	require.NoError(t, err)
	require.NoError(t, st.PutAt(ctx, store.ListMetaKey(userKey), metaBytes, 1, 0))

	const threshold = listInlineMetaCompactionThreshold
	delta := store.MarshalListMetaDelta(store.ListMetaDelta{HeadDelta: 1, LenDelta: 1})
	for i := range threshold - 1 {
		ts := uint64(10 + i) //nolint:gosec // i is bounded by threshold.
		require.NoError(t, st.PutAt(ctx, store.ListMetaDeltaKey(userKey, ts, 0), delta, ts, 0))
	}

	readTS := st.LastCommitTS()
	elems, compacted, err := r.listInlineMetaCompactionElems(ctx, userKey, readTS, store.ListMetaDelta{HeadDelta: 1, LenDelta: 1}, 1)
	require.NoError(t, err)
	require.False(t, compacted, "below threshold: should not compact")
	require.Nil(t, elems)

	const lastTS = uint64(10 + threshold - 1)
	require.NoError(t, st.PutAt(ctx, store.ListMetaDeltaKey(userKey, lastTS, 0), delta, lastTS, 0))

	readTS = st.LastCommitTS()
	elems, compacted, err = r.listInlineMetaCompactionElems(ctx, userKey, readTS, store.ListMetaDelta{HeadDelta: -2, LenDelta: 3}, 1)
	require.NoError(t, err)
	require.True(t, compacted, "at threshold: should compact")

	coord := newLocalAdapterCoordinator(st)
	commitTS := coord.Clock().Next()
	_, dispatchErr := coord.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  0,
		CommitTS: commitTS,
		Elems:    elems,
	})
	require.NoError(t, dispatchErr)

	afterTS := st.LastCommitTS()
	raw, err := st.GetAt(ctx, store.ListMetaKey(userKey), afterTS)
	require.NoError(t, err)
	got, err := store.UnmarshalListMeta(raw)
	require.NoError(t, err)
	require.Equal(t, int64(100+threshold-2), got.Head)
	require.Equal(t, int64(10+threshold+3), got.Len)
	require.Equal(t, uint64(1234), got.ExpireAt)
	require.Equal(t, got.Head+got.Len, got.Tail)

	prefix := store.ListMetaDeltaScanPrefix(userKey)
	scanEnd := store.PrefixScanEnd(prefix)
	remaining, err := st.ScanAt(ctx, prefix, scanEnd, threshold+1, afterTS)
	require.NoError(t, err)
	require.Empty(t, remaining, "all list delta keys must be removed after inline compaction")
}

// ttlInlinePassTestCoordinator leads the data groups in groupIDs and, when
// leadsKeys is set, every key. Its failDispatch-th non-empty dispatch fails.
type ttlInlinePassTestCoordinator struct {
	*localAdapterCoordinator

	mu           sync.Mutex
	groupIDs     []uint64
	leadsKeys    bool
	dispatches   int
	failDispatch int
}

func (c *ttlInlinePassTestCoordinator) IsLeader() bool { return false }

func (c *ttlInlinePassTestCoordinator) IsLeaderForKey([]byte) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.leadsKeys
}

func (c *ttlInlinePassTestCoordinator) LocalLeaderGroupIDs() ([]uint64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]uint64(nil), c.groupIDs...), true
}

func (c *ttlInlinePassTestCoordinator) setGroupIDs(ids []uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.groupIDs = ids
}

func (c *ttlInlinePassTestCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	if req != nil && len(req.Elems) > 0 {
		c.mu.Lock()
		c.dispatches++
		fail := c.dispatches == c.failDispatch
		c.mu.Unlock()
		if fail {
			return nil, errors.New("injected dispatch failure")
		}
	}
	return c.localAdapterCoordinator.Dispatch(ctx, req)
}

type ttlInlinePassObservation struct {
	typeName string
	groupID  uint64
	outcome  string
}

type recordedTTLInlinePass struct {
	ttlInlinePassObservation
	startedAt time.Time
}

type recordingTTLInlinePassObserver struct {
	mu     sync.Mutex
	passes []recordedTTLInlinePass
}

func (o *recordingTTLInlinePassObserver) ObserveRedisTTLInlineMigrationPass(typeName string, groupID uint64, outcome string, startedAt time.Time) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.passes = append(o.passes, recordedTTLInlinePass{
		ttlInlinePassObservation: ttlInlinePassObservation{typeName: typeName, groupID: groupID, outcome: outcome},
		startedAt:                startedAt,
	})
}

func (o *recordingTTLInlinePassObserver) forType(typeName string) []ttlInlinePassObservation {
	var out []ttlInlinePassObservation
	for _, p := range o.recordedForType(typeName) {
		out = append(out, p.ttlInlinePassObservation)
	}
	return out
}

func (o *recordingTTLInlinePassObserver) recordedForType(typeName string) []recordedTTLInlinePass {
	o.mu.Lock()
	defer o.mu.Unlock()
	var out []recordedTTLInlinePass
	for _, p := range o.passes {
		if p.typeName == typeName {
			out = append(out, p)
		}
	}
	return out
}

type lockedLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n, err := b.buf.Write(p)
	return n, errors.WithStack(err)
}

// ttlInlinePassLine is the part of a "pass complete" log line the tests check.
type ttlInlinePassLine struct {
	Level        string `json:"level"`
	Type         string `json:"type"`
	GroupID      uint64 `json:"group_id"`
	Clean        bool   `json:"clean"`
	FullPass     bool   `json:"full_pass"`
	Pages        int    `json:"pages"`
	KeysScanned  int    `json:"keys_scanned"`
	KeysMigrated int    `json:"keys_migrated"`
	KeysSkipped  int    `json:"keys_skipped"`
	KeysFailed   int    `json:"keys_failed"`
}

func (b *lockedLogBuffer) ttlInlinePasses(t *testing.T, typeName string) []ttlInlinePassLine {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []ttlInlinePassLine
	for _, line := range bytes.Split(b.buf.Bytes(), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var rec struct {
			Msg string `json:"msg"`
			ttlInlinePassLine
		}
		require.NoError(t, json.Unmarshal(line, &rec))
		if rec.Msg == "ttl inline migrator: pass complete" && rec.Type == typeName {
			out = append(out, rec.ttlInlinePassLine)
		}
	}
	return out
}

// wantTTLInlinePassOutcome is the monitoring outcome a pass should report: a
// pass that is not clean but found nothing to rewrite, skip or fail did not
// read its whole prefix, so it is partial.
func wantTTLInlinePassOutcome(clean, foundNothing bool) string {
	switch {
	case clean:
		return monitoring.RedisTTLInlinePassClean
	case foundNothing:
		return monitoring.RedisTTLInlinePassPartial
	default:
		return monitoring.RedisTTLInlinePassNotClean
	}
}

func seedTTLInlinePassStrings(t *testing.T, st store.MVCCStore, n uint64, legacy bool) {
	t.Helper()
	ctx := context.Background()
	for i := uint64(0); i < n; i++ {
		key := []byte(fmt.Sprintf("ttl:pass:%04d", i))
		value := []byte("value")
		if !legacy {
			value = encodeRedisStr(value, nil)
		}
		require.NoError(t, st.PutAt(ctx, redisStrKey(key), value, i+1, 0))
	}
}

// The migrator reports each pass a scan plan completes over its prefix, so an
// operator can tell from a clean pass on every anchor type and group that the
// legacy TTL backlog has drained. A pass runs from the first page of the
// prefix to the first page that is not truncated, its counters restart with
// the next pass, and a pass that resumed a cursor from an earlier leadership
// term, migrated keys, or left anchors unchecked is not clean.
func TestDeltaCompactor_TTLInlineReportsCompletedPasses(t *testing.T) {
	t.Parallel()

	const typeName = "ttl-inline-string"
	group1 := []uint64{1}
	type pass struct {
		level        string
		clean        bool
		full         bool
		pages        int
		keysScanned  int
		keysMigrated int
		keysSkipped  int
		keysFailed   int
	}
	clean := func(level string, pages, scanned int) pass {
		return pass{level: level, clean: true, full: true, pages: pages, keysScanned: scanned}
	}
	tests := []struct {
		name         string
		seed         func(*testing.T, store.MVCCStore)
		notLeader    bool
		failDispatch int
		// ticks lists the groups this node leads at each tick.
		ticks [][]uint64
		want  []pass
	}{
		{
			name:  "clean pass",
			seed:  func(t *testing.T, st store.MVCCStore) { seedTTLInlinePassStrings(t, st, 3, false) },
			ticks: [][]uint64{group1},
			want:  []pass{clean("INFO", 1, 3)},
		},
		{
			name:  "pass that migrated keys, then clean passes",
			seed:  func(t *testing.T, st store.MVCCStore) { seedTTLInlinePassStrings(t, st, 3, true) },
			ticks: [][]uint64{group1, group1, group1},
			want: []pass{
				{level: "INFO", full: true, pages: 1, keysScanned: 3, keysMigrated: 3},
				clean("INFO", 1, 3),
				clean("DEBUG", 1, 3),
			},
		},
		{
			name: "pass spanning several ticks",
			seed: func(t *testing.T, st store.MVCCStore) {
				seedTTLInlinePassStrings(t, st, ttlInlineMigrationTickScanLimit+88, true)
			},
			ticks: [][]uint64{group1, group1, group1, group1},
			want: []pass{
				{level: "INFO", full: true, pages: 2, keysScanned: ttlInlineMigrationTickScanLimit + 88, keysMigrated: ttlInlineMigrationTickScanLimit + 88},
				clean("INFO", 2, ttlInlineMigrationTickScanLimit+88),
			},
		},
		{
			name: "leadership moves away mid-pass and back",
			seed: func(t *testing.T, st store.MVCCStore) {
				seedTTLInlinePassStrings(t, st, ttlInlineMigrationTickScanLimit+88, false)
			},
			ticks: [][]uint64{group1, {}, group1, group1, group1},
			want: []pass{
				{level: "INFO", full: false, pages: 1, keysScanned: 88},
				clean("INFO", 2, ttlInlineMigrationTickScanLimit+88),
			},
		},
		{
			name:      "anchors this node does not lead",
			seed:      func(t *testing.T, st store.MVCCStore) { seedTTLInlinePassStrings(t, st, 3, false) },
			notLeader: true,
			ticks:     [][]uint64{group1},
			want:      []pass{{level: "INFO", full: true, pages: 1, keysScanned: 3, keysSkipped: 3}},
		},
		{
			name: "anchor whose rewrite cannot be built",
			seed: func(t *testing.T, st store.MVCCStore) {
				ctx := context.Background()
				require.NoError(t, st.PutAt(ctx, redisStrKey([]byte("ttl:pass:bad")), []byte("value"), 1, 0))
				require.NoError(t, st.PutAt(ctx, redisTTLKey([]byte("ttl:pass:bad")), []byte{1}, 2, 0))
			},
			ticks: [][]uint64{group1},
			want:  []pass{{level: "INFO", full: true, pages: 1, keysScanned: 1, keysFailed: 1}},
		},
		{
			// The first batch of 32 commits and the second fails. The next
			// tick reads the page again and rewrites the 33rd key; the pass
			// counts the page once and both batches' keys.
			name: "page retried after a failed batch",
			seed: func(t *testing.T, st store.MVCCStore) {
				seedTTLInlinePassStrings(t, st, ttlInlineMigrationBatchKeyLimit+1, true)
			},
			failDispatch: 2,
			ticks:        [][]uint64{group1, group1},
			want: []pass{
				{level: "INFO", full: true, pages: 1, keysScanned: ttlInlineMigrationBatchKeyLimit + 1, keysMigrated: ttlInlineMigrationBatchKeyLimit + 1},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			st := &recordingTTLInlineGroupScanStore{MVCCStore: store.NewMVCCStore()}
			tc.seed(t, st)
			coord := &ttlInlinePassTestCoordinator{
				localAdapterCoordinator: newLocalAdapterCoordinator(st),
				leadsKeys:               !tc.notLeader,
				failDispatch:            tc.failDispatch,
			}
			logs := &lockedLogBuffer{}
			observer := &recordingTTLInlinePassObserver{}
			c := NewDeltaCompactor(st, coord,
				WithDeltaCompactorTimeout(time.Minute),
				WithDeltaCompactorLogger(slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))),
				WithDeltaCompactorTTLInlineMigrationObserver(observer),
			)

			for _, groups := range tc.ticks {
				coord.setGroupIDs(groups)
				err := c.SyncOnce(ctx)
				if tc.failDispatch == 0 {
					require.NoError(t, err)
				}
			}

			lines := logs.ttlInlinePasses(t, typeName)
			got := make([]pass, 0, len(lines))
			for _, l := range lines {
				require.Equal(t, uint64(1), l.GroupID)
				got = append(got, pass{
					level: l.Level, clean: l.Clean, full: l.FullPass, pages: l.Pages,
					keysScanned: l.KeysScanned, keysMigrated: l.KeysMigrated,
					keysSkipped: l.KeysSkipped, keysFailed: l.KeysFailed,
				})
			}
			require.Equal(t, tc.want, got)

			wantObserved := make([]ttlInlinePassObservation, 0, len(tc.want))
			for _, p := range tc.want {
				outcome := wantTTLInlinePassOutcome(p.clean, p.keysMigrated+p.keysSkipped+p.keysFailed == 0)
				wantObserved = append(wantObserved, ttlInlinePassObservation{typeName: typeName, groupID: 1, outcome: outcome})
			}
			require.Equal(t, wantObserved, observer.forType(typeName))
		})
	}
}

// hookedTTLInlineScanStore runs onScan before each group scan of prefix.
type hookedTTLInlineScanStore struct {
	store.MVCCStore

	prefix []byte
	onScan func()
}

func (s *hookedTTLInlineScanStore) ScanGroupOwnedAt(ctx context.Context, groupID uint64, start []byte, end []byte, limit int, ts uint64) ([]*store.KVPair, []byte, error) {
	if bytes.HasPrefix(start, s.prefix) && s.onScan != nil {
		s.onScan()
	}
	return scanPageWithResume(ctx, s.MVCCStore, start, end, limit, ts)
}

func newHookedTTLInlinePassCompactor(t *testing.T, st *hookedTTLInlineScanStore) (*DeltaCompactor, *lockedLogBuffer) {
	t.Helper()
	coord := &ttlInlinePassTestCoordinator{
		localAdapterCoordinator: newLocalAdapterCoordinator(st),
		groupIDs:                []uint64{1},
		leadsKeys:               true,
	}
	logs := &lockedLogBuffer{}
	c := NewDeltaCompactor(st, coord,
		WithDeltaCompactorTimeout(time.Minute),
		WithDeltaCompactorLogger(slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))),
	)
	return c, logs
}

// TTL-inline migration ticks never overlap. A tick that starts while another
// is migrating skips migration, so a page is scanned, tallied and reported by
// one tick only. Overlapping ticks double-counted a middle page; on a one-page
// plan, where the cursor resets to empty, the second tick even reported the
// same page as a second pass.
func TestDeltaCompactor_TTLInlineTicksDoNotOverlap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		keys  uint64
		ticks int
		pages int
	}{
		{name: "one-page plan", keys: 3, ticks: 1, pages: 1},
		{name: "middle page", keys: 2*ttlInlineMigrationTickScanLimit + 88, ticks: 3, pages: 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			st := &hookedTTLInlineScanStore{MVCCStore: store.NewMVCCStore(), prefix: []byte(redisStrPrefix)}
			seedTTLInlinePassStrings(t, st, tc.keys, false)
			c, logs := newHookedTTLInlinePassCompactor(t, st)

			// The scan of the last page runs a whole tick in between, as a
			// concurrent SyncOnce would.
			scans, nested := 0, false
			st.onScan = func() {
				scans++
				if scans == tc.pages && !nested {
					nested = true
					require.NoError(t, c.SyncOnce(ctx))
				}
			}
			for range tc.ticks {
				require.NoError(t, c.SyncOnce(ctx))
			}

			require.Equal(t, tc.pages, scans, "the overlapping tick must not scan")
			passes := logs.ttlInlinePasses(t, "ttl-inline-string")
			require.Len(t, passes, 1)
			require.Equal(t, tc.pages, passes[0].Pages)
			require.EqualValues(t, tc.keys, passes[0].KeysScanned)
			require.True(t, passes[0].Clean)
		})
	}
}

// A pass's duration includes its first page: the pass starts before that
// page's scan, not when the page has been migrated.
func TestDeltaCompactor_TTLInlinePassDurationIncludesFirstPage(t *testing.T) {
	t.Parallel()

	const scanDelay = 50 * time.Millisecond
	st := &hookedTTLInlineScanStore{
		MVCCStore: store.NewMVCCStore(),
		prefix:    []byte(redisStrPrefix),
		onScan:    func() { time.Sleep(scanDelay) },
	}
	seedTTLInlinePassStrings(t, st, 3, false)
	c, logs := newHookedTTLInlinePassCompactor(t, st)
	observer := &recordingTTLInlinePassObserver{}
	c.ttlInlineObserver = observer

	before := time.Now()
	require.NoError(t, c.SyncOnce(context.Background()))
	after := time.Now()

	durations := logs.ttlInlinePassDurations(t, "ttl-inline-string")
	require.Len(t, durations, 1)
	require.GreaterOrEqual(t, durations[0], scanDelay)

	// The observer gets the pass's start, which precedes its first page's
	// scan: a gate can then require a pass that started after an event.
	passes := observer.recordedForType("ttl-inline-string")
	require.Len(t, passes, 1)
	require.False(t, passes[0].startedAt.Before(before))
	require.LessOrEqual(t, passes[0].startedAt, after.Add(-scanDelay))
}

func (b *lockedLogBuffer) ttlInlinePassDurations(t *testing.T, typeName string) []time.Duration {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []time.Duration
	for _, line := range bytes.Split(b.buf.Bytes(), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var rec struct {
			Msg      string        `json:"msg"`
			Type     string        `json:"type"`
			Duration time.Duration `json:"duration"`
		}
		require.NoError(t, json.Unmarshal(line, &rec))
		if rec.Msg == "ttl inline migrator: pass complete" && rec.Type == typeName {
			out = append(out, rec.Duration)
		}
	}
	return out
}

// snapshotTimingCoordinator records when the compactor first asks for its
// clock, which is when SyncOnce acquires the tick's read timestamp, and then
// delays, as a slow read-timestamp acquisition would.
type snapshotTimingCoordinator struct {
	*ttlInlinePassTestCoordinator

	once       sync.Once
	snapshotAt time.Time
}

func (c *snapshotTimingCoordinator) Clock() *kv.HLC {
	c.once.Do(func() {
		c.snapshotAt = time.Now()
		time.Sleep(10 * time.Millisecond)
	})
	return c.ttlInlinePassTestCoordinator.Clock()
}

// A pass reads its pages at the tick's read timestamp, so it starts no later
// than that timestamp is acquired. Stamping it when the scan began let a route
// change between the two give a pass that read a snapshot from before the
// change a start after it.
func TestDeltaCompactor_TTLInlinePassStartsNoLaterThanItsReadTimestamp(t *testing.T) {
	t.Parallel()

	st := &recordingTTLInlineGroupScanStore{MVCCStore: store.NewMVCCStore()}
	seedTTLInlinePassStrings(t, st, 3, false)
	coord := &snapshotTimingCoordinator{ttlInlinePassTestCoordinator: &ttlInlinePassTestCoordinator{
		localAdapterCoordinator: newLocalAdapterCoordinator(st),
		groupIDs:                []uint64{1},
		leadsKeys:               true,
	}}
	observer := &recordingTTLInlinePassObserver{}
	c := NewDeltaCompactor(st, coord,
		WithDeltaCompactorTimeout(time.Minute),
		WithDeltaCompactorTTLInlineMigrationObserver(observer),
	)

	require.NoError(t, c.SyncOnce(context.Background()))

	passes := observer.recordedForType("ttl-inline-string")
	require.Len(t, passes, 1)
	require.False(t, coord.snapshotAt.IsZero(), "SyncOnce must read the coordinator's clock")
	require.LessOrEqual(t, passes[0].startedAt, coord.snapshotAt)
}
