package adapter

import (
	"bytes"
	"context"
	"encoding/binary"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/keyviz"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
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
	require.Len(t, raw, redisStreamMetaTrimSizeBytes)
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

func (c *localLeaderGroupCompactionCoordinator) LocalLeaderGroupIDs() []uint64 {
	return append([]uint64(nil), c.groupIDs...)
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

func (s *recordingTTLInlineGroupScanStore) ScanGroupAt(ctx context.Context, groupID uint64, start []byte, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	s.groupScans = append(s.groupScans, groupID)
	s.groupStarts = append(s.groupStarts, string(start))
	return s.MVCCStore.ScanAt(ctx, start, end, limit, ts)
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
		"sharded ttl migration should scan candidate metadata with ScanGroupAt")
	raw, err := st.GetAt(ctx, store.HashMetaKey(key), st.LastCommitTS())
	require.NoError(t, err)
	meta, err := store.UnmarshalHashMeta(raw)
	require.NoError(t, err)
	require.Equal(t, redisExpireAtMillis(expireAt), meta.ExpireAt)
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

func TestDeltaCompactor_RotatesHandlerAfterTimeout(t *testing.T) {
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
		store.HashMetaDeltaPrefix,
		store.SetMetaDeltaPrefix,
		store.ZSetMetaDeltaPrefix,
		store.ListMetaDeltaPrefix,
	}
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

	// Queue and process the urgent compaction.
	c.TriggerUrgentCompaction("hash", userKey)

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
