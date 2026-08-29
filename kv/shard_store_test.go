package kv

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/s3keys"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type exportCountingStore struct {
	store.MVCCStore
	exportCalls int
}

func (s *exportCountingStore) ExportVersions(ctx context.Context, opts store.ExportVersionsOptions) (store.ExportVersionsResult, error) {
	s.exportCalls++
	return s.MVCCStore.ExportVersions(ctx, opts)
}

func newStagedVisibilityShardStore(t *testing.T) (*ShardStore, *ShardGroup) {
	t.Helper()

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{
				RouteID:                1,
				Start:                  []byte("a"),
				End:                    []byte("z"),
				GroupID:                1,
				State:                  distribution.RouteStateActive,
				StagedVisibilityActive: true,
				MigrationJobID:         9,
				MinWriteTSExclusive:    100,
			},
		},
	}))
	group := &ShardGroup{Store: store.NewMVCCStore()}
	return NewShardStore(engine, map[uint64]*ShardGroup{1: group}), group
}

func newStagedVisibilityPebbleShardStore(t *testing.T) (*ShardStore, *ShardGroup) {
	t.Helper()

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{
				RouteID:                1,
				Start:                  []byte("a"),
				End:                    []byte("z"),
				GroupID:                1,
				State:                  distribution.RouteStateActive,
				StagedVisibilityActive: true,
				MigrationJobID:         9,
				MinWriteTSExclusive:    100,
			},
		},
	}))
	st, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	group := &ShardGroup{Store: st}
	return NewShardStore(engine, map[uint64]*ShardGroup{1: group}), group
}

func TestShardStoreGetAt_MergesStagedVisibility(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, group := newStagedVisibilityShardStore(t)
	rawKey := []byte("k")
	stagedKey := distribution.MigrationStagedDataKey(9, rawKey)

	require.NoError(t, group.Store.PutAt(ctx, rawKey, []byte("live-old"), 10, 0))
	require.NoError(t, group.Store.PutAt(ctx, stagedKey, []byte("staged-new"), 20, 0))
	got, err := st.GetAt(ctx, rawKey, 25)
	require.NoError(t, err)
	require.Equal(t, []byte("staged-new"), got)

	require.NoError(t, group.Store.PutAt(ctx, rawKey, []byte("live-new"), 30, 0))
	got, err = st.GetAt(ctx, rawKey, 35)
	require.NoError(t, err)
	require.Equal(t, []byte("live-new"), got)

	require.NoError(t, group.Store.DeleteAt(ctx, stagedKey, 40))
	_, err = st.GetAt(ctx, rawKey, 45)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestShardStoreGetAt_MergesStagedVisibilityPebbleExactKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, group := newStagedVisibilityPebbleShardStore(t)
	rawKey := []byte("k")
	stagedKey := distribution.MigrationStagedDataKey(9, rawKey)

	require.NoError(t, group.Store.PutAt(ctx, rawKey, []byte("live-old"), 10, 0))
	require.NoError(t, group.Store.PutAt(ctx, stagedKey, []byte("staged-new"), 20, 0))

	got, err := st.GetAt(ctx, rawKey, 25)
	require.NoError(t, err)
	require.Equal(t, []byte("staged-new"), got)
}

func TestShardStoreGetAt_MergesStagedVisibilityForS3BucketAuxiliary(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const bucket = "bucket-a"
	routeStart := s3keys.RoutePrefixForBucketAnyGeneration(bucket)
	routeEnd := prefixScanEnd(routeStart)
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{
				RouteID:                1,
				Start:                  routeStart,
				End:                    routeEnd,
				GroupID:                2,
				State:                  distribution.RouteStateActive,
				StagedVisibilityActive: true,
				MigrationJobID:         9,
			},
		},
	}))
	group := &ShardGroup{Store: store.NewMVCCStore()}
	st := NewShardStore(engine, map[uint64]*ShardGroup{2: group})

	for _, tc := range []struct {
		name  string
		key   []byte
		value []byte
	}{
		{name: "bucket meta", key: s3keys.BucketMetaKey(bucket), value: []byte("meta")},
		{name: "bucket generation", key: s3keys.BucketGenerationKey(bucket), value: []byte("generation")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, group.Store.PutAt(ctx, distribution.MigrationStagedDataKey(9, tc.key), tc.value, 20, 0))

			got, err := st.GetAt(ctx, tc.key, 25)
			require.NoError(t, err)
			require.Equal(t, tc.value, got)
		})
	}
}

func TestShardStoreGetAt_RoutesS3BucketAuxiliaryToPromotedOwner(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const bucket = "bucket-a"
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes:  s3BucketAuxiliaryPromotedRoutes(),
	}))
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	key := s3keys.BucketMetaKey(bucket)
	require.NoError(t, groups[1].Store.PutAt(ctx, key, []byte("stale-source"), 10, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, key, []byte("promoted-target"), 20, 0))

	got, err := st.GetAt(ctx, key, 30)
	require.NoError(t, err)
	require.Equal(t, []byte("promoted-target"), got)
}

func TestShardStoreS3BucketAuxiliaryScanFiltersStagedRoutesToBucketRange(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const (
		bucketA = "bucket-a"
		bucketB = "bucket-b"
		bucketC = "bucket-c"
	)
	routeStartA := s3keys.RoutePrefixForBucketAnyGeneration(bucketA)
	routeEndA := prefixScanEnd(routeStartA)
	routeStartB := s3keys.RoutePrefixForBucketAnyGeneration(bucketB)
	routeEndB := prefixScanEnd(routeStartB)
	require.Less(t, bytes.Compare(routeStartA, routeStartB), 0)

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte(""), End: routeStartA, GroupID: 1, State: distribution.RouteStateActive},
			{RouteID: 2, Start: routeStartA, End: routeEndA, GroupID: 1, State: distribution.RouteStateActive, StagedVisibilityActive: true, MigrationJobID: 9},
			{RouteID: 3, Start: routeEndA, End: routeStartB, GroupID: 1, State: distribution.RouteStateActive},
			{RouteID: 4, Start: routeStartB, End: routeEndB, GroupID: 1, State: distribution.RouteStateActive, StagedVisibilityActive: true, MigrationJobID: 10},
			{RouteID: 5, Start: routeEndB, End: nil, GroupID: 1, State: distribution.RouteStateActive},
		},
	}))
	group := &ShardGroup{Store: store.NewMVCCStore()}
	st := NewShardStore(engine, map[uint64]*ShardGroup{1: group})

	keyA := s3keys.BucketMetaKey(bucketA)
	keyB := s3keys.BucketMetaKey(bucketB)
	keyC := s3keys.BucketMetaKey(bucketC)
	require.NoError(t, group.Store.PutAt(ctx, keyA, []byte("live-a"), 10, 0))
	require.NoError(t, group.Store.PutAt(ctx, keyB, []byte("live-b"), 10, 0))
	require.NoError(t, group.Store.PutAt(ctx, keyC, []byte("live-c"), 10, 0))

	exactA, err := st.ScanAt(ctx, keyA, prefixScanEnd(keyA), 10, 20)
	require.NoError(t, err)
	require.Equal(t, []*store.KVPair{{Key: keyA, Value: []byte("live-a")}}, exactA)

	all, err := st.ScanAt(ctx, []byte(s3keys.BucketMetaPrefix), prefixScanEnd([]byte(s3keys.BucketMetaPrefix)), 10, 20)
	require.NoError(t, err)
	require.Equal(t, []*store.KVPair{
		{Key: keyA, Value: []byte("live-a")},
		{Key: keyB, Value: []byte("live-b")},
		{Key: keyC, Value: []byte("live-c")},
	}, all)

	reverseAll, err := st.ReverseScanAt(ctx, []byte(s3keys.BucketMetaPrefix), prefixScanEnd([]byte(s3keys.BucketMetaPrefix)), 10, 20)
	require.NoError(t, err)
	require.Equal(t, []*store.KVPair{
		{Key: keyC, Value: []byte("live-c")},
		{Key: keyB, Value: []byte("live-b")},
		{Key: keyA, Value: []byte("live-a")},
	}, reverseAll)
}

func TestShardStoreS3BucketAuxiliaryScanUsesPromotedOwner(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const (
		migratedBucket = "bucket-a"
		otherBucket    = "bucket-z"
	)
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes:  s3BucketAuxiliaryPromotedRoutes(),
	}))
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	migratedKey := s3keys.BucketMetaKey(migratedBucket)
	otherKey := s3keys.BucketMetaKey(otherBucket)
	require.NoError(t, groups[1].Store.PutAt(ctx, migratedKey, []byte("stale-source"), 10, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, migratedKey, []byte("promoted-target"), 20, 0))
	require.NoError(t, groups[1].Store.PutAt(ctx, otherKey, []byte("raw-owner"), 15, 0))

	start := []byte(s3keys.BucketMetaPrefix)
	end := prefixScanEnd(start)
	kvs, err := st.ScanAt(ctx, start, end, 10, 30)
	require.NoError(t, err)
	require.Equal(t, []*store.KVPair{
		{Key: migratedKey, Value: []byte("promoted-target")},
		{Key: otherKey, Value: []byte("raw-owner")},
	}, kvs)

	reverse, err := st.ReverseScanAt(ctx, start, end, 10, 30)
	require.NoError(t, err)
	require.Equal(t, []*store.KVPair{
		{Key: otherKey, Value: []byte("raw-owner")},
		{Key: migratedKey, Value: []byte("promoted-target")},
	}, reverse)
}

func TestShardStoreS3BucketAuxiliaryScanPreservesLegacyRawOnlyRows(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	for _, tc := range []struct {
		name     string
		prefix   string
		keyFor   func(string) []byte
		value    []byte
		rawValue []byte
	}{
		{name: "bucket meta", prefix: s3keys.BucketMetaPrefix, keyFor: s3keys.BucketMetaKey, value: []byte("legacy-meta"), rawValue: []byte("raw-meta")},
		{name: "bucket generation", prefix: s3keys.BucketGenerationPrefix, keyFor: s3keys.BucketGenerationKey, value: []byte("legacy-generation"), rawValue: []byte("raw-generation")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const (
				migratedBucket = "bucket-a"
				otherBucket    = "bucket-z"
			)
			engine := distribution.NewEngine()
			require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
				Version: 1,
				Routes:  s3BucketAuxiliaryPromotedRoutes(),
			}))
			groups := map[uint64]*ShardGroup{
				1: {Store: store.NewMVCCStore()},
				2: {Store: store.NewMVCCStore()},
			}
			st := NewShardStore(engine, groups)
			migratedKey := tc.keyFor(migratedBucket)
			otherKey := tc.keyFor(otherBucket)
			require.NoError(t, groups[1].Store.PutAt(ctx, migratedKey, tc.value, 10, 0))
			require.NoError(t, groups[1].Store.PutAt(ctx, otherKey, tc.rawValue, 15, 0))

			start := []byte(tc.prefix)
			end := prefixScanEnd(start)
			kvs, err := st.ScanAt(ctx, start, end, 10, 30)
			require.NoError(t, err)
			require.Equal(t, []*store.KVPair{
				{Key: migratedKey, Value: tc.value},
				{Key: otherKey, Value: tc.rawValue},
			}, kvs)

			reverse, err := st.ReverseScanAt(ctx, start, end, 10, 30)
			require.NoError(t, err)
			require.Equal(t, []*store.KVPair{
				{Key: otherKey, Value: tc.rawValue},
				{Key: migratedKey, Value: tc.value},
			}, reverse)

			exact, err := st.ScanAt(ctx, migratedKey, prefixScanEnd(migratedKey), 10, 30)
			require.NoError(t, err)
			require.Equal(t, []*store.KVPair{{Key: migratedKey, Value: tc.value}}, exact)
		})
	}
}

func TestShardStoreRouteBoundedS3BucketAuxiliaryScanKeepsStagedRows(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const bucket = "bucket-a"
	routeStart := s3keys.RoutePrefixForBucketAnyGeneration(bucket)
	routeEnd := prefixScanEnd(routeStart)
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte(""), End: routeStart, GroupID: 1, State: distribution.RouteStateActive},
			{RouteID: 2, Start: routeStart, End: routeEnd, GroupID: 1, State: distribution.RouteStateActive, StagedVisibilityActive: true, MigrationJobID: 9},
			{RouteID: 3, Start: routeEnd, End: nil, GroupID: 1, State: distribution.RouteStateActive},
		},
	}))
	group := &ShardGroup{Store: store.NewMVCCStore()}
	st := NewShardStore(engine, map[uint64]*ShardGroup{1: group})
	key := s3keys.BucketMetaKey(bucket)
	require.NoError(t, group.Store.PutAt(ctx, distribution.MigrationStagedDataKey(9, key), []byte("staged"), 20, 0))

	kvs, err := st.ScanAtWithReadFence(ctx, []byte(s3keys.BucketMetaPrefix), prefixScanEnd([]byte(s3keys.BucketMetaPrefix)), 10, 25, false, 0, 1, routeStart, routeEnd)
	require.NoError(t, err)
	require.Equal(t, []*store.KVPair{{Key: key, Value: []byte("staged")}}, kvs)
}

func TestShardStoreRejectsS3BucketAuxiliaryWriteAtMigrationTimestampFloor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const bucket = "bucket-a"
	routeStart := s3keys.RoutePrefixForBucketAnyGeneration(bucket)
	routeEnd := prefixScanEnd(routeStart)
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{
				RouteID:             1,
				Start:               []byte(""),
				End:                 routeStart,
				GroupID:             1,
				State:               distribution.RouteStateActive,
				MinWriteTSExclusive: 100,
			},
			{
				RouteID:             2,
				Start:               routeStart,
				End:                 routeEnd,
				GroupID:             2,
				State:               distribution.RouteStateActive,
				MinWriteTSExclusive: 100,
			},
			{
				RouteID:             3,
				Start:               routeEnd,
				End:                 nil,
				GroupID:             1,
				State:               distribution.RouteStateActive,
				MinWriteTSExclusive: 100,
			},
		},
	}))
	st := NewShardStore(engine, map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	})

	for _, key := range [][]byte{
		s3keys.BucketMetaKey(bucket),
		s3keys.BucketGenerationKey(bucket),
	} {
		require.ErrorIs(t, st.PutAt(ctx, key, []byte("v"), 100, 0), ErrRouteWriteTimestampTooLow)
		require.ErrorIs(t, st.ApplyMutations(ctx, []*store.KVPairMutation{{Op: store.OpTypePut, Key: key, Value: []byte("v")}}, nil, 90, 100), ErrRouteWriteTimestampTooLow)
	}
}

func TestShardStoreStagedVisibilityReadTSCompacted(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, group := newStagedVisibilityShardStore(t)
	retention, ok := group.Store.(store.RetentionController)
	require.True(t, ok)
	retention.SetMinRetainedTS(15)

	_, err := st.GetAt(ctx, []byte("k"), 10)
	require.ErrorIs(t, err, store.ErrReadTSCompacted)
	_, err = st.ScanAt(ctx, []byte("a"), []byte("z"), 10, 10)
	require.ErrorIs(t, err, store.ErrReadTSCompacted)
}

func TestShardStoreScanAndLatestCommitTS_MergeStagedVisibility(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, group := newStagedVisibilityShardStore(t)

	require.NoError(t, group.Store.PutAt(ctx, []byte("b"), []byte("live-b"), 10, 0))
	require.NoError(t, group.Store.PutAt(ctx, []byte("c"), []byte("live-c"), 30, 0))
	require.NoError(t, group.Store.PutAt(ctx, distribution.MigrationStagedDataKey(9, []byte("b")), []byte("staged-b"), 20, 0))
	require.NoError(t, group.Store.PutAt(ctx, distribution.MigrationStagedDataKey(9, []byte("d")), []byte("staged-d"), 15, 0))
	require.NoError(t, group.Store.PutAt(ctx, distribution.MigrationStagedDataKey(9, []byte("e")), []byte("staged-e"), 40, 0))
	require.NoError(t, group.Store.DeleteAt(ctx, []byte("d"), 25))

	kvs, err := st.ScanAt(ctx, []byte("a"), []byte("z"), 10, 50)
	require.NoError(t, err)
	require.Equal(t, []*store.KVPair{
		{Key: []byte("b"), Value: []byte("staged-b")},
		{Key: []byte("c"), Value: []byte("live-c")},
		{Key: []byte("e"), Value: []byte("staged-e")},
	}, kvs)

	kvs, err = st.ReverseScanAt(ctx, []byte("a"), []byte("z"), 10, 50)
	require.NoError(t, err)
	require.Equal(t, []*store.KVPair{
		{Key: []byte("e"), Value: []byte("staged-e")},
		{Key: []byte("c"), Value: []byte("live-c")},
		{Key: []byte("b"), Value: []byte("staged-b")},
	}, kvs)

	ts, exists, err := st.LatestCommitTS(ctx, []byte("b"))
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, uint64(20), ts)

	ts, exists, err = st.LatestCommitTS(ctx, []byte("d"))
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, uint64(25), ts)

	ts, exists, err = st.LatestCommitTS(ctx, []byte("e"))
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, uint64(40), ts)
}

func TestShardStoreStagedVisibilityScanUsesTwoRangeExports(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, group := newStagedVisibilityShardStore(t)
	counting := &exportCountingStore{MVCCStore: group.Store}
	group.Store = counting
	require.NoError(t, group.Store.PutAt(ctx, []byte("b"), []byte("live"), 10, 0))
	require.NoError(t, group.Store.PutAt(ctx, distribution.MigrationStagedDataKey(9, []byte("c")), []byte("staged"), 20, 0))

	kvs, err := st.ScanAt(ctx, []byte("a"), []byte("z"), 10, 30)
	require.NoError(t, err)
	require.Equal(t, []*store.KVPair{
		{Key: []byte("b"), Value: []byte("live")},
		{Key: []byte("c"), Value: []byte("staged")},
	}, kvs)
	require.Equal(t, 2, counting.exportCalls)
}

func TestStagedVisibilityScanBoundsTreatsEmptyEndAsUnbounded(t *testing.T) {
	t.Parallel()

	prefix := distribution.MigrationStagedDataKeyPrefix(9)
	start, end := stagedVisibilityScanBounds(9, []byte{}, []byte{})
	require.Equal(t, prefix, start)
	require.Equal(t, prefixScanEnd(prefix), end)
}

func TestShardStoreScanAt_FiltersStagedShadowRowsFromLiveCandidates(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, group := newStagedVisibilityShardStore(t)
	rawKey := []byte("b")
	require.NoError(t, group.Store.PutAt(ctx, distribution.MigrationStagedDataKey(9, rawKey), []byte("staged"), 20, 0))

	kvs, err := st.ScanAt(ctx, []byte(""), nil, 10, 30)
	require.NoError(t, err)
	require.Equal(t, []*store.KVPair{{Key: rawKey, Value: []byte("staged")}}, kvs)
}

func TestShardStoreScanAt_PreservesNonStagedRoutesDuringBroadStagedVisibilityScan(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte("a"), End: []byte("m"), GroupID: 1, State: distribution.RouteStateActive},
			{
				RouteID:                2,
				Start:                  []byte("m"),
				End:                    []byte("t"),
				GroupID:                1,
				State:                  distribution.RouteStateActive,
				StagedVisibilityActive: true,
				MigrationJobID:         9,
			},
			{RouteID: 3, Start: []byte("t"), End: []byte("z"), GroupID: 1, State: distribution.RouteStateActive},
		},
	}))
	group := &ShardGroup{Store: store.NewMVCCStore()}
	st := NewShardStore(engine, map[uint64]*ShardGroup{1: group})

	require.NoError(t, group.Store.PutAt(ctx, []byte("b"), []byte("live-b"), 10, 0))
	require.NoError(t, group.Store.PutAt(ctx, []byte("n"), []byte("live-n"), 10, 0))
	require.NoError(t, group.Store.PutAt(ctx, []byte("u"), []byte("live-u"), 10, 0))
	require.NoError(t, group.Store.PutAt(ctx, distribution.MigrationStagedDataKey(9, []byte("n")), []byte("staged-n"), 20, 0))
	require.NoError(t, group.Store.PutAt(ctx, distribution.MigrationStagedDataKey(9, []byte("o")), []byte("staged-o"), 20, 0))

	kvs, err := st.ScanAt(ctx, nil, nil, 10, 30)
	require.NoError(t, err)
	require.Equal(t, []*store.KVPair{
		{Key: []byte("b"), Value: []byte("live-b")},
		{Key: []byte("n"), Value: []byte("staged-n")},
		{Key: []byte("o"), Value: []byte("staged-o")},
		{Key: []byte("u"), Value: []byte("live-u")},
	}, kvs)

	kvs, err = st.ReverseScanAt(ctx, nil, nil, 10, 30)
	require.NoError(t, err)
	require.Equal(t, []*store.KVPair{
		{Key: []byte("u"), Value: []byte("live-u")},
		{Key: []byte("o"), Value: []byte("staged-o")},
		{Key: []byte("n"), Value: []byte("staged-n")},
		{Key: []byte("b"), Value: []byte("live-b")},
	}, kvs)
}

func TestShardStoreScanAt_RoutesS3BucketAuxiliaryStagedVisibility(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const bucket = "bucket-a"
	routeStart := s3keys.RoutePrefixForBucketAnyGeneration(bucket)
	routeEnd := prefixScanEnd(routeStart)
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte(""), End: routeStart, GroupID: 1, State: distribution.RouteStateActive},
			{
				RouteID:                2,
				Start:                  routeStart,
				End:                    routeEnd,
				GroupID:                2,
				State:                  distribution.RouteStateActive,
				StagedVisibilityActive: true,
				MigrationJobID:         9,
			},
			{RouteID: 3, Start: routeEnd, End: nil, GroupID: 1, State: distribution.RouteStateActive},
		},
	}))
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	for _, tc := range []struct {
		name   string
		prefix string
		key    []byte
		value  []byte
	}{
		{name: "bucket meta", prefix: s3keys.BucketMetaPrefix, key: s3keys.BucketMetaKey(bucket), value: []byte("meta")},
		{name: "bucket generation", prefix: s3keys.BucketGenerationPrefix, key: s3keys.BucketGenerationKey(bucket), value: []byte("generation")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, groups[2].Store.PutAt(ctx, distribution.MigrationStagedDataKey(9, tc.key), tc.value, 20, 0))

			kvs, err := st.ScanAt(ctx, []byte(tc.prefix), prefixScanEnd([]byte(tc.prefix)), 10, 30)
			require.NoError(t, err)
			require.Contains(t, kvs, &store.KVPair{Key: tc.key, Value: tc.value})
		})
	}
}

func TestShardStoreS3BucketAuxiliaryScanHonorsStagedTombstone(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const migratedBucket = "bucket-a"
	routeStart := s3keys.RoutePrefixForBucketAnyGeneration(migratedBucket)
	routeEnd := prefixScanEnd(routeStart)
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte(""), End: routeStart, GroupID: 1, State: distribution.RouteStateActive},
			{RouteID: 2, Start: routeStart, End: routeEnd, GroupID: 2, State: distribution.RouteStateActive, StagedVisibilityActive: true, MigrationJobID: 9},
			{RouteID: 3, Start: routeEnd, End: nil, GroupID: 1, State: distribution.RouteStateActive},
		},
	}))
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	deletedKey := s3keys.BucketMetaKey(migratedBucket)
	visibleKey := s3keys.BucketMetaKey("bucket-z")
	require.NoError(t, groups[1].Store.PutAt(ctx, deletedKey, []byte("stale"), 10, 0))
	require.NoError(t, groups[1].Store.PutAt(ctx, visibleKey, []byte("visible"), 10, 0))
	require.NoError(t, groups[2].Store.DeleteAt(ctx, distribution.MigrationStagedDataKey(9, deletedKey), 20))

	start := []byte(s3keys.BucketMetaPrefix)
	end := prefixScanEnd(start)
	kvs, err := st.ScanAt(ctx, start, end, 1, 30)
	require.NoError(t, err)
	require.Equal(t, []*store.KVPair{{Key: visibleKey, Value: []byte("visible")}}, kvs)

	kvs, err = st.ScanAt(ctx, deletedKey, prefixScanEnd(deletedKey), 1, 30)
	require.NoError(t, err)
	require.Empty(t, kvs)
}

type versionVisibleRawKVServer struct {
	pb.UnimplementedRawKVServer

	mu         sync.Mutex
	visible    map[string]bool
	latestReqs []*pb.RawLatestCommitTSRequest
}

func (s *versionVisibleRawKVServer) RawGet(context.Context, *pb.RawGetRequest) (*pb.RawGetResponse, error) {
	return &pb.RawGetResponse{}, nil
}

func (s *versionVisibleRawKVServer) RawScanAt(context.Context, *pb.RawScanAtRequest) (*pb.RawScanAtResponse, error) {
	return &pb.RawScanAtResponse{}, nil
}

func (s *versionVisibleRawKVServer) RawLatestCommitTS(_ context.Context, req *pb.RawLatestCommitTSRequest) (*pb.RawLatestCommitTSResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.latestReqs = append(s.latestReqs, &pb.RawLatestCommitTSRequest{
		Key:                bytes.Clone(req.GetKey()),
		GroupId:            req.GetGroupId(),
		ReadRouteVersion:   req.GetReadRouteVersion(),
		VersionVisibleAtTs: req.GetVersionVisibleAtTs(),
	})
	return &pb.RawLatestCommitTSResponse{
		VersionVisible:          s.visible[string(req.GetKey())],
		VersionVisibleSupported: true,
	}, nil
}

func TestShardStoreS3BucketAuxiliaryOwnerProbeUsesLeaderRoutedReadFence(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const bucket = "bucket-a"
	migratedKey := s3keys.BucketMetaKey(bucket)
	stagedKey := distribution.MigrationStagedDataKey(9, migratedKey)
	probe := &versionVisibleRawKVServer{
		visible: map[string]bool{string(stagedKey): true},
	}
	addr, stop := startRawKVServer(t, probe)
	t.Cleanup(stop)

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 77,
		Routes:  s3BucketAuxiliaryStagedRoutes(bucket, 1, 2),
	}))
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {
			Store:  store.NewMVCCStore(),
			Engine: &followerProxyEngine{leader: addr},
		},
	}
	st := NewShardStore(engine, groups)
	visibleKey := s3keys.BucketMetaKey("bucket-z")
	require.NoError(t, groups[1].Store.PutAt(ctx, migratedKey, []byte("stale-source"), 10, 0))
	require.NoError(t, groups[1].Store.PutAt(ctx, visibleKey, []byte("visible"), 10, 0))

	start := []byte(s3keys.BucketMetaPrefix)
	kvs, err := st.ScanAtWithReadFence(ctx, start, prefixScanEnd(start), 10, 30, false, 0, 77, nil, nil)
	require.NoError(t, err)
	require.Equal(t, []*store.KVPair{{Key: visibleKey, Value: []byte("visible")}}, kvs)

	probe.mu.Lock()
	defer probe.mu.Unlock()
	// Staged is probed first, and a staged hit answers the question, so the
	// live probe is never issued. Probing live first would have to fall
	// through to staged anyway, and that pair is what a concurrent promotion
	// slips between.
	require.Len(t, probe.latestReqs, 1)
	require.Equal(t, stagedKey, probe.latestReqs[0].GetKey())
	for _, req := range probe.latestReqs {
		require.Equal(t, uint64(2), req.GetGroupId())
		require.Equal(t, uint64(77), req.GetReadRouteVersion())
		require.Equal(t, uint64(30), req.GetVersionVisibleAtTs())
	}
}

func TestShardStoreS3BucketAuxiliaryOwnerProbeFailsWhenLeaderUnavailable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const bucket = "bucket-a"
	migratedKey := s3keys.BucketMetaKey(bucket)
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 77,
		Routes:  s3BucketAuxiliaryStagedRoutes(bucket, 1, 2),
	}))
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {
			Store:  store.NewMVCCStore(),
			Engine: &followerProxyEngine{},
		},
	}
	st := NewShardStore(engine, groups)
	require.NoError(t, groups[1].Store.PutAt(ctx, migratedKey, []byte("stale-source"), 10, 0))

	start := []byte(s3keys.BucketMetaPrefix)
	_, err := st.ScanAtWithReadFence(ctx, start, prefixScanEnd(start), 10, 30, false, 0, 77, nil, nil)
	require.ErrorIs(t, err, ErrLeaderNotFound)
}

func TestShardStoreExplicitGroupS3BucketAuxiliaryScanKeepsOwnerRoutes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const bucket = "bucket-a"
	migratedKey := s3keys.BucketMetaKey(bucket)
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 77,
		Routes:  s3BucketAuxiliaryStagedRoutes(bucket, 1, 2),
	}))
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	require.NoError(t, groups[1].Store.PutAt(ctx, migratedKey, []byte("stale-source"), 10, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, distribution.MigrationStagedDataKey(9, migratedKey), []byte("owner"), 20, 0))

	start := []byte(s3keys.BucketMetaPrefix)
	kvs, err := st.ScanAtWithReadFence(ctx, start, prefixScanEnd(start), 10, 30, false, 1, 77, nil, nil)
	require.NoError(t, err)
	require.Empty(t, kvs)
}

func TestShardStoreGetAt_ContinuesLatestVersionExportPages(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, group := newStagedVisibilityPebbleShardStore(t)
	rawKey := []byte("k")
	large := bytes.Repeat([]byte("x"), 1<<20)
	require.NoError(t, group.Store.PutAt(ctx, rawKey, []byte("old"), 20, 0))
	require.NoError(t, group.Store.PutAt(ctx, rawKey, large, 30, 0))

	got, err := st.GetAt(ctx, rawKey, 25)
	require.NoError(t, err)
	require.Equal(t, []byte("old"), got)
}

func TestShardStoreDeletePrefixAtDeletesStagedVisibilityRows(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, group := newStagedVisibilityShardStore(t)
	dropKey := []byte("b/drop")
	keepKey := []byte("b/keep")
	outsideKey := []byte("c/outside")

	require.NoError(t, group.Store.PutAt(ctx, distribution.MigrationStagedDataKey(9, dropKey), []byte("drop"), 20, 0))
	require.NoError(t, group.Store.PutAt(ctx, distribution.MigrationStagedDataKey(9, keepKey), []byte("keep"), 20, 0))
	require.NoError(t, group.Store.PutAt(ctx, distribution.MigrationStagedDataKey(9, outsideKey), []byte("outside"), 20, 0))

	require.NoError(t, st.DeletePrefixAt(ctx, []byte("b/"), []byte("b/keep"), 101))

	_, err := st.GetAt(ctx, dropKey, 150)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	got, err := st.GetAt(ctx, keepKey, 150)
	require.NoError(t, err)
	require.Equal(t, []byte("keep"), got)
	got, err = st.GetAt(ctx, outsideKey, 150)
	require.NoError(t, err)
	require.Equal(t, []byte("outside"), got)
}

func TestShardStoreRouteFilteredLeaderScanUsesStagedVisibility(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, group := newStagedVisibilityShardStore(t)
	require.NoError(t, group.Store.PutAt(ctx, distribution.MigrationStagedDataKey(9, []byte("b")), []byte("staged-b"), 20, 0))
	route, _, ok := st.routeAndGroupForKey([]byte("b"))
	require.True(t, ok)

	filtered, cursorKVs, err := st.scanRouteAtLeaderRouteFilter(
		ctx,
		group,
		route,
		[]byte("a"),
		[]byte("z"),
		10,
		10,
		25,
		false,
		[]byte("b"),
		[]byte("c"),
	)
	require.NoError(t, err)
	require.Equal(t, []*store.KVPair{{Key: []byte("b"), Value: []byte("staged-b")}}, filtered)
	require.Equal(t, []*store.KVPair{{Key: []byte("b"), Value: []byte("staged-b")}}, cursorKVs)
}

func TestShardStoreExplicitGroupReads_MergeStagedVisibility(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, group := newStagedVisibilityShardStore(t)

	require.NoError(t, group.Store.PutAt(ctx, []byte("b"), []byte("live-b"), 10, 0))
	require.NoError(t, group.Store.PutAt(ctx, distribution.MigrationStagedDataKey(9, []byte("b")), []byte("staged-b"), 20, 0))
	require.NoError(t, group.Store.PutAt(ctx, distribution.MigrationStagedDataKey(9, []byte("c")), []byte("staged-c"), 30, 0))

	got, err := st.GetGroupAt(ctx, 1, []byte("b"), 25)
	require.NoError(t, err)
	require.Equal(t, []byte("staged-b"), got)

	kvs, err := st.ScanGroupAt(ctx, 1, []byte("a"), []byte("z"), 10, 35)
	require.NoError(t, err)
	require.Equal(t, []*store.KVPair{
		{Key: []byte("b"), Value: []byte("staged-b")},
		{Key: []byte("c"), Value: []byte("staged-c")},
	}, kvs)

	kvs, err = st.ScanAtWithReadFence(ctx, []byte("a"), []byte("z"), 10, 35, false, 1, 0, []byte("a"), []byte("z"))
	require.NoError(t, err)
	require.Equal(t, []*store.KVPair{
		{Key: []byte("b"), Value: []byte("staged-b")},
		{Key: []byte("c"), Value: []byte("staged-c")},
	}, kvs)
}

func TestShardStoreExplicitGroupReads_FailClosedWhenRouteMovedToStagedGroup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{
				RouteID:                1,
				Start:                  []byte("a"),
				End:                    []byte("z"),
				GroupID:                2,
				State:                  distribution.RouteStateActive,
				StagedVisibilityActive: true,
				MigrationJobID:         9,
			},
		},
	}))
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	require.NoError(t, groups[1].Store.PutAt(ctx, []byte("b"), []byte("old-source"), 10, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, distribution.MigrationStagedDataKey(9, []byte("b")), []byte("staged-target"), 20, 0))

	_, err := st.GetGroupAt(ctx, 1, []byte("b"), 25)
	require.ErrorIs(t, err, ErrExplicitGroupStagedVisibilityUnresolved)

	_, err = st.ScanGroupAt(ctx, 1, []byte("a"), []byte("z"), 10, 25)
	require.ErrorIs(t, err, ErrExplicitGroupStagedVisibilityUnresolved)
}

func TestShardStoreExplicitGroupScan_NormalizesRouteMappedBoundsForStagedRoutes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{
				RouteID:                1,
				Start:                  sqsGlobalRouteKey,
				End:                    prefixScanEnd(sqsGlobalRouteKey),
				GroupID:                2,
				State:                  distribution.RouteStateActive,
				StagedVisibilityActive: true,
				MigrationJobID:         9,
			},
		},
	}))
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	start := []byte("!sqs|msg|vis|p|")
	end := prefixScanEnd(start)
	require.NoError(t, groups[1].Store.PutAt(ctx, []byte("!sqs|msg|vis|p|orders|1"), []byte("old-source"), 10, 0))

	_, err := st.ScanGroupAt(ctx, 1, start, end, 10, 25)
	require.ErrorIs(t, err, ErrExplicitGroupStagedVisibilityUnresolved)
}

func TestShardStoreExplicitGroupRead_IgnoresUnrelatedStagedRoutes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			// The read's own range is owned by the group it names, so the
			// staged route below is genuinely unrelated to it.
			{RouteID: 1, Start: []byte("a"), End: []byte("m"), GroupID: 1, State: distribution.RouteStateActive},
			{
				RouteID:                2,
				Start:                  []byte("m"),
				End:                    []byte("z"),
				GroupID:                2,
				State:                  distribution.RouteStateActive,
				StagedVisibilityActive: true,
				MigrationJobID:         9,
			},
		},
	}))
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	require.NoError(t, groups[1].Store.PutAt(ctx, []byte("b"), []byte("explicit-group"), 10, 0))

	got, err := st.GetGroupAt(ctx, 1, []byte("b"), 25)
	require.NoError(t, err)
	require.Equal(t, []byte("explicit-group"), got)

	kvs, err := st.ScanGroupAt(ctx, 1, []byte("b"), []byte("c"), 10, 25)
	require.NoError(t, err)
	require.Equal(t, []*store.KVPair{{Key: []byte("b"), Value: []byte("explicit-group")}}, kvs)
}

func TestShardStoreScanAt_ContinuesStagedVisibilityAfterCandidateWindow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, group := newStagedVisibilityShardStore(t)
	limit := stagedVisibilityMaxCandidateWindow + 3
	for i := range limit {
		key := []byte(fmt.Sprintf("k%05d", i))
		require.NoError(t, group.Store.PutAt(ctx, distribution.MigrationStagedDataKey(9, key), []byte(fmt.Sprintf("v%05d", i)), 10, 0))
	}

	kvs, err := st.ScanAt(ctx, []byte("a"), []byte("z"), limit, 20)
	require.NoError(t, err)
	require.Len(t, kvs, limit)
	require.Equal(t, []byte("k00000"), kvs[0].Key)
	require.Equal(t, []byte(fmt.Sprintf("k%05d", limit-1)), kvs[limit-1].Key)

	kvs, err = st.ReverseScanAt(ctx, []byte("a"), []byte("z"), limit, 20)
	require.NoError(t, err)
	require.Len(t, kvs, limit)
	require.Equal(t, []byte(fmt.Sprintf("k%05d", limit-1)), kvs[0].Key)
	require.Equal(t, []byte("k00000"), kvs[limit-1].Key)
}

func TestShardStoreScanAtRestrictsStagedVisibilityToSafeFrontier(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, group := newStagedVisibilityShardStore(t)
	limit := stagedVisibilityMaxCandidateWindow + 1
	for i := range limit {
		key := []byte(fmt.Sprintf("k%05d", i))
		require.NoError(t, group.Store.PutAt(ctx, key, []byte(fmt.Sprintf("live%05d", i)), 10, 0))
	}
	require.NoError(t, group.Store.PutAt(ctx, distribution.MigrationStagedDataKey(9, []byte("x-staged")), []byte("staged"), 10, 0))

	kvs, err := st.ScanAt(ctx, []byte("a"), []byte("z"), limit, 20)
	require.NoError(t, err)
	require.Len(t, kvs, limit)
	require.Equal(t, []byte("k00000"), kvs[0].Key)
	require.Equal(t, []byte(fmt.Sprintf("k%05d", limit-1)), kvs[limit-1].Key)
	for _, kvp := range kvs {
		require.NotEqual(t, []byte("x-staged"), kvp.Key)
	}
}

func TestStagedVisibilityCandidateBoundary_UsesSafeFrontier(t *testing.T) {
	t.Parallel()

	live := []*store.KVPair{{Key: []byte("a")}, {Key: []byte("c")}}
	staged := []*store.KVPair{
		{Key: distribution.MigrationStagedDataKey(9, []byte("b"))},
		{Key: distribution.MigrationStagedDataKey(9, []byte("z"))},
	}
	boundary, ok := stagedVisibilityCandidateBoundary(live, staged, false, false, false)
	require.True(t, ok)
	require.Equal(t, []byte("c"), boundary)

	boundary, ok = stagedVisibilityCandidateBoundary(live, staged, false, false, true)
	require.True(t, ok)
	require.Equal(t, []byte("b"), boundary)
}

func TestShardStoreApplyMutations_ValidatesStagedReadKeys(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, group := newStagedVisibilityShardStore(t)
	readKey := []byte("k")
	require.NoError(t, group.Store.PutAt(ctx, distribution.MigrationStagedDataKey(9, readKey), []byte("staged"), 20, 0))

	err := st.ApplyMutations(ctx, []*store.KVPairMutation{
		{Op: store.OpTypePut, Key: []byte("m"), Value: []byte("write")},
	}, [][]byte{readKey}, 10, 101)
	require.ErrorIs(t, err, store.ErrWriteConflict)
}

func TestShardStoreApplyMutations_ValidatesStagedWriteKeys(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, group := newStagedVisibilityShardStore(t)
	writeKey := []byte("k")
	require.NoError(t, group.Store.PutAt(ctx, distribution.MigrationStagedDataKey(9, writeKey), []byte("staged"), 20, 0))

	apply := []struct {
		name string
		fn   func(context.Context, []*store.KVPairMutation, [][]byte, uint64, uint64) error
	}{
		{
			name: "direct",
			fn:   st.ApplyMutations,
		},
		{
			name: "raft",
			fn:   st.ApplyMutationsRaft,
		},
		{
			name: "raft_at",
			fn: func(ctx context.Context, muts []*store.KVPairMutation, readKeys [][]byte, startTS, commitTS uint64) error {
				return st.ApplyMutationsRaftAt(ctx, muts, readKeys, startTS, commitTS, 1)
			},
		},
	}
	for _, tc := range apply {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.fn(ctx, []*store.KVPairMutation{
				{Op: store.OpTypePut, Key: writeKey, Value: []byte("write")},
			}, nil, 10, 101)
			require.ErrorIs(t, err, store.ErrWriteConflict)
		})
	}
}

func TestShardStorePhysicalLimitFallsBackToStagedVisibilityScan(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, group := newStagedVisibilityShardStore(t)
	require.NoError(t, group.Store.PutAt(ctx, []byte("b/live"), []byte("live"), 10, 0))
	require.NoError(t, group.Store.PutAt(ctx, distribution.MigrationStagedDataKey(9, []byte("b/staged")), []byte("staged"), 20, 0))

	kvs, limitReached, err := st.ScanAtPhysicalLimit(ctx, []byte("b"), []byte("c"), 10, 10, 50)
	require.NoError(t, err)
	require.False(t, limitReached)
	require.Equal(t, []*store.KVPair{
		{Key: []byte("b/live"), Value: []byte("live")},
		{Key: []byte("b/staged"), Value: []byte("staged")},
	}, kvs)

	kvs, limitReached, err = st.ReverseScanAtPhysicalLimit(ctx, []byte("b"), []byte("c"), 10, 10, 50)
	require.NoError(t, err)
	require.False(t, limitReached)
	require.Equal(t, []*store.KVPair{
		{Key: []byte("b/staged"), Value: []byte("staged")},
		{Key: []byte("b/live"), Value: []byte("live")},
	}, kvs)
}

func TestShardStoreRejectsWritesAtMigrationTimestampFloor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, _ := newStagedVisibilityShardStore(t)

	err := st.PutAt(ctx, []byte("k"), []byte("low"), 100, 0)
	require.ErrorIs(t, err, ErrRouteWriteTimestampTooLow)
	require.NoError(t, st.PutAt(ctx, []byte("k"), []byte("ok"), 101, 0))
}

func TestShardStoreRaftApplyRejectsMigrationTimestampFloor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, _ := newStagedVisibilityShardStore(t)

	require.ErrorIs(t, st.ApplyMutationsRaft(ctx, []*store.KVPairMutation{
		{Op: store.OpTypePut, Key: []byte("k-raft"), Value: []byte("v")},
	}, nil, 90, 100), ErrRouteWriteTimestampTooLow)
	require.ErrorIs(t, st.ApplyMutationsRaftAt(ctx, []*store.KVPairMutation{
		{Op: store.OpTypePut, Key: []byte("k-raft-at"), Value: []byte("v")},
	}, nil, 90, 100, 1), ErrRouteWriteTimestampTooLow)
	require.ErrorIs(t, st.DeletePrefixAtRaft(ctx, []byte("k-raft"), nil, 100), ErrRouteWriteTimestampTooLow)
	require.ErrorIs(t, st.DeletePrefixAtRaftAt(ctx, []byte("k-raft-at"), nil, 100, 2), ErrRouteWriteTimestampTooLow)
}

type followerProxyEngine struct {
	leader string
}

func (e *followerProxyEngine) Propose(context.Context, []byte) (*raftengine.ProposalResult, error) {
	return nil, ErrLeaderNotFound
}

func (e *followerProxyEngine) ProposeAdmin(context.Context, []byte) (*raftengine.ProposalResult, error) {
	return nil, ErrLeaderNotFound
}

func (e *followerProxyEngine) State() raftengine.State {
	return raftengine.StateFollower
}

func (e *followerProxyEngine) Leader() raftengine.LeaderInfo {
	return raftengine.LeaderInfo{Address: e.leader}
}

func (e *followerProxyEngine) VerifyLeader(context.Context) error {
	return ErrLeaderNotFound
}

func (e *followerProxyEngine) LinearizableRead(context.Context) (uint64, error) {
	return 0, ErrLeaderNotFound
}

func (e *followerProxyEngine) Status() raftengine.Status {
	return raftengine.Status{
		State:  raftengine.StateFollower,
		Leader: raftengine.LeaderInfo{Address: e.leader},
	}
}

func (e *followerProxyEngine) Configuration(context.Context) (raftengine.Configuration, error) {
	return raftengine.Configuration{}, nil
}

func (e *followerProxyEngine) Close() error {
	return nil
}

func TestShardStoreScanAt_IncludesListKeysAcrossShards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 1, 0))
	require.NoError(t, st.PutAt(ctx, []byte("b"), []byte("vb"), 2, 0))

	userKey := []byte("x")
	itemKey := store.ListItemKey(userKey, 0)
	require.NoError(t, st.PutAt(ctx, itemKey, []byte("v0"), 3, 0))

	// A full scan should surface internal list keys that may live on any shard.
	kvs, err := st.ScanAt(ctx, []byte(""), nil, 1, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, itemKey, kvs[0].Key)
}

func TestShardStoreScanAt_RoutesListItemScansByUserKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	userKey := []byte("x") // routes to group 2
	k0 := store.ListItemKey(userKey, 0)
	k1 := store.ListItemKey(userKey, 1)
	k2 := store.ListItemKey(userKey, 2)
	require.NoError(t, st.PutAt(ctx, k0, []byte("v0"), 1, 0))
	require.NoError(t, st.PutAt(ctx, k1, []byte("v1"), 2, 0))
	require.NoError(t, st.PutAt(ctx, k2, []byte("v2"), 3, 0))

	end := store.ListItemKey(userKey, 3) // exclusive upper bound
	kvs, err := st.ScanAt(ctx, k0, end, 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 3)
	require.Equal(t, k0, kvs[0].Key)
	require.Equal(t, k1, kvs[1].Key)
	require.Equal(t, k2, kvs[2].Key)
}

func TestShardStoreScanAt_RoutesListDeltaScansByUserKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	userKey := []byte("x") // routes to group 2; raw !lst|* prefixes route to group 1.
	for _, tc := range []struct {
		name          string
		key           []byte
		scanStart     []byte
		legacyRouting bool
	}{
		{name: "current", key: store.ListMetaDeltaKey(userKey, 10, 1), scanStart: store.ListMetaDeltaScanPrefix(userKey)},
		{name: "legacy", key: legacyListMetaDeltaKey(userKey, 10), scanStart: store.LegacyListMetaDeltaScanPrefix(userKey), legacyRouting: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := newTwoRouteShardStoreForScanTest()
			deltaValue := store.MarshalListMetaDelta(store.ListMetaDelta{LenDelta: 1})
			if tc.legacyRouting {
				require.NoError(t, st.groups[1].Store.PutAt(ctx, tc.key, deltaValue, 1, 0))
			} else {
				require.NoError(t, st.PutAt(ctx, tc.key, deltaValue, 1, 0))
			}

			kvs, err := st.ScanAt(ctx, tc.scanStart, store.PrefixScanEnd(tc.scanStart), 10, ^uint64(0))
			require.NoError(t, err)
			require.Len(t, kvs, 1)
			require.Equal(t, tc.key, kvs[0].Key)
		})
	}
}

func TestShardStoreScanAt_BroadLegacyListDeltaScansAllRoutes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := newTwoRouteShardStoreForScanTest()
	deltaValue := store.MarshalListMetaDelta(store.ListMetaDelta{LenDelta: 1})
	leftKey := legacyListMetaDeltaKey([]byte("left-list"), 10)
	rightKey := legacyListMetaDeltaKey([]byte("right-list"), 11)
	require.NoError(t, st.groups[1].Store.PutAt(ctx, leftKey, deltaValue, 1, 0))
	require.NoError(t, st.groups[2].Store.PutAt(ctx, rightKey, deltaValue, 1, 0))

	kvs, err := st.ScanAt(ctx, []byte(store.LegacyListMetaDeltaPrefix), store.PrefixScanEnd([]byte(store.LegacyListMetaDeltaPrefix)), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, leftKey, kvs[0].Key)
	require.Equal(t, uint64(1), kvs[0].RouteGroupID)
	require.Equal(t, rightKey, kvs[1].Key)
	require.Equal(t, uint64(2), kvs[1].RouteGroupID)
}

func TestShardStoreScanAt_RoutesWideColumnScansByUserKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	for _, tc := range []struct {
		name      string
		key       []byte
		scanStart []byte
	}{
		{name: "hash field", key: store.HashFieldKey([]byte("x"), []byte("f")), scanStart: store.HashFieldScanPrefix([]byte("x"))},
		{name: "hash delta", key: store.HashMetaDeltaKey([]byte("x"), 10, 0), scanStart: store.HashMetaDeltaScanPrefix([]byte("x"))},
		{name: "set member", key: store.SetMemberKey([]byte("x"), []byte("m")), scanStart: store.SetMemberScanPrefix([]byte("x"))},
		{name: "set delta", key: store.SetMetaDeltaKey([]byte("x"), 10, 0), scanStart: store.SetMetaDeltaScanPrefix([]byte("x"))},
		{name: "zset member", key: store.ZSetMemberKey([]byte("x"), []byte("m")), scanStart: store.ZSetMemberScanPrefix([]byte("x"))},
		{name: "zset score", key: store.ZSetScoreKey([]byte("x"), 1.5, []byte("m")), scanStart: store.ZSetScoreScanPrefix([]byte("x"))},
		{name: "zset delta", key: store.ZSetMetaDeltaKey([]byte("x"), 10, 0), scanStart: store.ZSetMetaDeltaScanPrefix([]byte("x"))},
		{name: "stream meta", key: store.StreamMetaKey([]byte("x")), scanStart: store.StreamMetaKey([]byte("x"))},
		{name: "stream entry", key: store.StreamEntryKey([]byte("x"), 10, 0), scanStart: store.StreamEntryScanPrefix([]byte("x"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := newTwoRouteShardStoreForScanTest()
			require.NoError(t, st.PutAt(ctx, tc.key, []byte("v"), 1, 0))

			kvs, err := st.ScanAt(ctx, tc.scanStart, store.PrefixScanEnd(tc.scanStart), 10, ^uint64(0))
			require.NoError(t, err)
			require.Len(t, kvs, 1)
			require.Equal(t, tc.key, kvs[0].Key)
		})
	}
}

func newTwoRouteShardStoreForScanTest() *ShardStore {
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	return NewShardStore(engine, groups)
}

func TestShardStoreScanGroupAt_UsesExplicitGroup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)
	groups := map[uint64]*ShardGroup{
		1:  {Store: store.NewMVCCStore()},
		42: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	key := []byte("!sqs|msg|vis|p|orders|partition-2")
	require.NoError(t, groups[42].Store.PutAt(ctx, key, []byte("msg-2"), 7, 0))

	kvs, err := st.ScanGroupAt(ctx, 42, []byte("!sqs|msg|vis|p|"), prefixScanEnd([]byte("!sqs|msg|vis|p|")), 10, 7)
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, key, kvs[0].Key)
	require.Equal(t, []byte("msg-2"), kvs[0].Value)
}

func TestShardStoreScanGroupAt_DoesNotClampRouteMappedRawBounds(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 42)
	groups := map[uint64]*ShardGroup{
		42: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	start := []byte("!sqs|msg|vis|p|")
	key := []byte("!sqs|msg|vis|p|orders|partition-2")
	require.NoError(t, groups[42].Store.PutAt(ctx, key, []byte("msg-2"), 7, 0))

	kvs, err := st.ScanGroupAt(ctx, 42, start, prefixScanEnd(start), 10, 7)
	require.NoError(t, err)
	require.Equal(t, []*store.KVPair{{Key: key, Value: []byte("msg-2")}}, kvs)
}

func TestShardStoreScanGroupAt_DeduplicatesRouteMappedSameGroupSplits(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	routeEnd := prefixScanEnd(sqsGlobalRouteKey)
	split := append(bytes.Clone(sqsGlobalRouteKey), 'm')
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: sqsGlobalRouteKey, End: split, GroupID: 42, State: distribution.RouteStateActive},
			{RouteID: 2, Start: split, End: routeEnd, GroupID: 42, State: distribution.RouteStateActive},
		},
	}))
	groups := map[uint64]*ShardGroup{
		42: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	start := []byte("!sqs|msg|vis|p|")
	key := []byte("!sqs|msg|vis|p|orders|partition-2")
	require.NoError(t, groups[42].Store.PutAt(ctx, key, []byte("msg-2"), 7, 0))

	kvs, err := st.ScanGroupAt(ctx, 42, start, prefixScanEnd(start), 10, 7)
	require.NoError(t, err)
	require.Equal(t, []*store.KVPair{{Key: key, Value: []byte("msg-2")}}, kvs)
}

func TestShardStoreGetGroupAt_UsesExplicitGroup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)
	groups := map[uint64]*ShardGroup{
		1:  {Store: store.NewMVCCStore()},
		42: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	key := []byte("!sqs|msg|data|p|orders|partition-2|msg-2")
	require.NoError(t, groups[42].Store.PutAt(ctx, key, []byte("payload"), 7, 0))

	val, err := st.GetGroupAt(ctx, 42, key, 7)
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), val)

	_, err = st.GetAt(ctx, key, 7)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestShardStore_ForwardsReadFenceStamps(t *testing.T) {
	t.Parallel()

	fake := &fakeRawKVServer{
		getResp: &pb.RawGetResponse{
			Exists: true,
			Value:  []byte("remote-v"),
		},
		scanResp: &pb.RawScanAtResponse{},
		latestResp: &pb.RawLatestCommitTSResponse{
			Ts:     42,
			Exists: true,
		},
	}
	addr, stop := startRawKVServer(t, fake)
	t.Cleanup(stop)

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 100,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte(""), GroupID: 1, State: distribution.RouteStateActive},
		},
	}))
	st := NewShardStore(engine, map[uint64]*ShardGroup{
		1: {
			Store:  store.NewMVCCStore(),
			Engine: &stubFollowerEngine{leaderAddr: addr},
		},
	})
	t.Cleanup(func() { _ = st.Close() })

	ctx := context.Background()
	_, err := st.GetAt(ctx, []byte("k"), 10)
	require.NoError(t, err)
	_, _, err = st.LatestCommitTS(ctx, []byte("k"))
	require.NoError(t, err)
	_, err = st.ScanAtWithReadFence(ctx, []byte("a"), []byte("z"), 10, 11, false, 0, 79, []byte("a"), []byte("m"))
	require.NoError(t, err)

	fake.mu.Lock()
	require.Equal(t, uint64(100), fake.lastGetReq.GetReadRouteVersion())
	require.Equal(t, uint64(100), fake.lastLatestReq.GetReadRouteVersion())
	require.Equal(t, uint64(100), fake.lastScanReq.GetReadRouteVersion())
	require.Equal(t, uint64(1), fake.lastScanReq.GetGroupId())
	require.Equal(t, []byte("a"), fake.lastScanReq.GetRouteStart())
	require.Equal(t, []byte("m"), fake.lastScanReq.GetRouteEnd())
	require.True(t, fake.lastScanReq.GetRouteBoundsPresent())
	fake.mu.Unlock()

	_, err = st.ScanAt(ctx, []byte("a"), []byte("z"), 10, 11)
	require.NoError(t, err)

	fake.mu.Lock()
	require.Equal(t, uint64(0), fake.lastScanReq.GetGroupId())
	require.Equal(t, uint64(100), fake.lastScanReq.GetReadRouteVersion())
	fake.mu.Unlock()

	_, err = st.ScanKeysAtWithReadFence(ctx, []byte("a"), []byte("z"), 10, 11, 0, 82)
	require.NoError(t, err)

	fake.mu.Lock()
	require.Equal(t, uint64(0), fake.lastScanReq.GetGroupId())
	require.Equal(t, uint64(100), fake.lastScanReq.GetReadRouteVersion())
	require.True(t, fake.lastScanReq.GetKeysOnly())
	fake.mu.Unlock()

	_, err = st.ScanAtWithReadFence(ctx, []byte("a"), []byte("z"), 10, 11, false, 0, 80, []byte{}, []byte{})
	require.NoError(t, err)

	fake.mu.Lock()
	require.Equal(t, uint64(1), fake.lastScanReq.GetGroupId())
	require.Equal(t, uint64(100), fake.lastScanReq.GetReadRouteVersion())
	require.True(t, fake.lastScanReq.GetRouteBoundsPresent())
	fake.mu.Unlock()

	_, err = st.ScanAtWithReadFence(ctx, []byte("a"), []byte("z"), 10, 11, false, 0, 81, nil, nil)
	require.NoError(t, err)

	fake.mu.Lock()
	require.Equal(t, uint64(0), fake.lastScanReq.GetGroupId())
	require.Equal(t, uint64(100), fake.lastScanReq.GetReadRouteVersion())
	require.False(t, fake.lastScanReq.GetRouteBoundsPresent())
	fake.mu.Unlock()

	_, err = st.ScanAt(ctx, []byte(""), nil, 10, 11)
	require.NoError(t, err)

	fake.mu.Lock()
	defer fake.mu.Unlock()
	require.Equal(t, uint64(1), fake.lastScanReq.GetGroupId())
	require.Equal(t, uint64(100), fake.lastScanReq.GetReadRouteVersion())
}

func TestShardStoreRoutesForScanUsesWideColumnUserKey(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)
	st := NewShardStore(engine, nil)
	userKey := []byte("z-user")

	for _, tc := range []struct {
		name   string
		prefix []byte
	}{
		{name: "hash fields", prefix: store.HashFieldScanPrefix(userKey)},
		{name: "hash deltas", prefix: store.HashMetaDeltaScanPrefix(userKey)},
		{name: "set members", prefix: store.SetMemberScanPrefix(userKey)},
		{name: "set deltas", prefix: store.SetMetaDeltaScanPrefix(userKey)},
		{name: "zset members", prefix: store.ZSetMemberScanPrefix(userKey)},
		{name: "zset scores", prefix: store.ZSetScoreScanPrefix(userKey)},
		{name: "zset deltas", prefix: store.ZSetMetaDeltaScanPrefix(userKey)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			routes, clamp := st.routesForScan(tc.prefix, prefixScanEnd(tc.prefix))
			require.False(t, clamp)
			require.Len(t, routes, 2)
			require.Equal(t, uint64(2), routes[0].GroupID)
			require.Equal(t, uint64(1), routes[1].GroupID)
		})
	}
}

func TestShardStoreScanAtRoutesWideColumnPrefixesByUserKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	userKey := []byte("z-user")

	for _, tc := range []struct {
		name   string
		key    []byte
		prefix []byte
	}{
		{name: "hash field", key: store.HashFieldKey(userKey, []byte("field")), prefix: store.HashFieldScanPrefix(userKey)},
		{name: "hash delta", key: store.HashMetaDeltaKey(userKey, 10, 1), prefix: store.HashMetaDeltaScanPrefix(userKey)},
		{name: "set member", key: store.SetMemberKey(userKey, []byte("member")), prefix: store.SetMemberScanPrefix(userKey)},
		{name: "set delta", key: store.SetMetaDeltaKey(userKey, 11, 1), prefix: store.SetMetaDeltaScanPrefix(userKey)},
		{name: "zset member", key: store.ZSetMemberKey(userKey, []byte("member")), prefix: store.ZSetMemberScanPrefix(userKey)},
		{name: "zset score", key: store.ZSetScoreKey(userKey, 1.5, []byte("member")), prefix: store.ZSetScoreScanPrefix(userKey)},
		{name: "zset delta", key: store.ZSetMetaDeltaKey(userKey, 12, 1), prefix: store.ZSetMetaDeltaScanPrefix(userKey)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, st.PutAt(ctx, tc.key, []byte("value"), 20, 0))
			kvs, err := st.ScanAt(ctx, tc.prefix, prefixScanEnd(tc.prefix), 10, 20)
			require.NoError(t, err)
			require.Equal(t, []*store.KVPair{{Key: tc.key, Value: []byte("value")}}, kvs)
			_, err = groups[1].Store.GetAt(ctx, tc.key, 20)
			require.ErrorIs(t, err, store.ErrKeyNotFound)
		})
	}
}

func TestShardStoreReadFenceFailsClosedWhileCatalogVersionIsBehind(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte(""), GroupID: 1, State: distribution.RouteStateActive},
		},
	}))
	groupStore := store.NewMVCCStore()
	require.NoError(t, groupStore.PutAt(context.Background(), []byte("k"), []byte("stale"), 1, 0))
	st := NewShardStore(engine, map[uint64]*ShardGroup{1: {Store: groupStore}})

	tests := []struct {
		name string
		read func(context.Context) error
	}{
		{
			name: "point read",
			read: func(ctx context.Context) error {
				_, err := st.GetAtWithReadFence(ctx, []byte("k"), 1, 0, 2)
				return err
			},
		},
		{
			name: "latest commit timestamp",
			read: func(ctx context.Context) error {
				_, _, err := st.LatestCommitTSWithReadFence(ctx, []byte("k"), 2)
				return err
			},
		},
		{
			name: "range scan",
			read: func(ctx context.Context) error {
				_, err := st.ScanAtWithReadFence(ctx, []byte("a"), []byte("z"), 1, 1, false, 0, 2, nil, nil)
				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			err := tc.read(ctx)
			require.ErrorIs(t, err, ErrReadRouteVersionUnavailable)
		})
	}
}

func TestShardStoreReadFenceWaitsForCatalogAndReroutesPointRead(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte(""), GroupID: 1, State: distribution.RouteStateActive},
		},
	}))
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	require.NoError(t, groups[1].Store.PutAt(context.Background(), []byte("k"), []byte("old-owner"), 1, 0))
	require.NoError(t, groups[2].Store.PutAt(context.Background(), []byte("k"), []byte("new-owner"), 1, 0))
	st := NewShardStore(engine, groups)

	applyErr := make(chan error, 1)
	go func() {
		time.Sleep(20 * time.Millisecond)
		applyErr <- engine.ApplySnapshot(distribution.CatalogSnapshot{
			Version: 2,
			Routes: []distribution.RouteDescriptor{
				{RouteID: 1, Start: []byte(""), GroupID: 2, State: distribution.RouteStateActive},
			},
		})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	value, err := st.GetAtWithReadFence(ctx, []byte("k"), 1, 0, 2)
	require.NoError(t, err)
	require.Equal(t, []byte("new-owner"), value)
	require.NoError(t, <-applyErr)
}

func TestShardStoreScanAtWithReadFence_RoutesUsingSuppliedBounds(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	rawPrefix := []byte("!redis|meta|")
	first := []byte("!redis|meta|x")
	second := []byte("!redis|meta|y")
	require.NoError(t, groups[2].Store.PutAt(ctx, first, []byte("v1"), 1, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, second, []byte("v2"), 2, 0))

	kvs, err := st.ScanAtWithReadFence(ctx, rawPrefix, prefixScanEnd(rawPrefix), 10, 2, false, 0, st.ReadRouteVersion(), []byte("m"), nil)
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, first, kvs[0].Key)
	require.Equal(t, second, kvs[1].Key)

	kvs, err = st.ScanAtWithReadFence(ctx, rawPrefix, prefixScanEnd(rawPrefix), 10, 2, true, 0, st.ReadRouteVersion(), []byte("m"), nil)
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, second, kvs[0].Key)
	require.Equal(t, first, kvs[1].Key)
}

func TestShardStoreScanAtWithReadFence_ScansSameGroupSuppliedBoundsAcrossRouteIntervals(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 1)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	rawPrefix := []byte("!redis|meta|")
	first := []byte("!redis|meta|a")
	second := []byte("!redis|meta|z")
	require.NoError(t, groups[1].Store.PutAt(ctx, first, []byte("v1"), 1, 0))
	require.NoError(t, groups[1].Store.PutAt(ctx, second, []byte("v2"), 2, 0))

	for _, tc := range []struct {
		name       string
		reverse    bool
		routeStart []byte
		routeEnd   []byte
		want       [][]byte
	}{
		{
			name:       "left interval only",
			routeStart: []byte("a"),
			routeEnd:   []byte("z"),
			want:       [][]byte{first},
		},
		{
			name:       "forward across intervals",
			routeStart: []byte("a"),
			want:       [][]byte{first, second},
		},
		{
			name:       "reverse across intervals",
			reverse:    true,
			routeStart: []byte("a"),
			want:       [][]byte{second, first},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kvs, err := st.ScanAtWithReadFence(ctx, rawPrefix, prefixScanEnd(rawPrefix), 10, 2, tc.reverse, 0, st.ReadRouteVersion(), tc.routeStart, tc.routeEnd)
			require.NoError(t, err)
			require.Len(t, kvs, len(tc.want))
			for i, want := range tc.want {
				require.Equal(t, want, kvs[i].Key)
			}
		})
	}
}

func TestShardStoreScanAtWithReadFence_FiltersWideRedisKeysByUserKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 1)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	rawPrefix := []byte("!hs|")
	left := store.HashFieldKey([]byte("alpha"), []byte("f"))
	right := store.HashFieldKey([]byte("zulu"), []byte("f"))
	require.NoError(t, groups[1].Store.PutAt(ctx, left, []byte("left"), 1, 0))
	require.NoError(t, groups[1].Store.PutAt(ctx, right, []byte("right"), 2, 0))

	kvs, err := st.ScanAtWithReadFence(ctx, rawPrefix, prefixScanEnd(rawPrefix), 1, 2, false, 0, st.ReadRouteVersion(), []byte("m"), nil)
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, right, kvs[0].Key)

	kvs, err = st.ScanAtWithReadFence(ctx, rawPrefix, prefixScanEnd(rawPrefix), 1, 2, true, 0, st.ReadRouteVersion(), []byte{}, []byte("m"))
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, left, kvs[0].Key)
}

func TestShardStoreScanAtWithReadFence_FiltersSuppliedBoundsByRouteKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 1)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	rawPrefix := []byte("!redis|meta|")
	left := []byte("!redis|meta|a")
	right := []byte("!redis|meta|z")
	require.NoError(t, groups[1].Store.PutAt(ctx, left, []byte("left"), 1, 0))
	require.NoError(t, groups[1].Store.PutAt(ctx, right, []byte("right"), 2, 0))

	kvs, err := st.ScanAtWithReadFence(ctx, rawPrefix, prefixScanEnd(rawPrefix), 1, 2, false, 0, st.ReadRouteVersion(), []byte("m"), nil)
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, right, kvs[0].Key)

	kvs, err = st.ScanAtWithReadFence(ctx, rawPrefix, prefixScanEnd(rawPrefix), 1, 2, true, 0, st.ReadRouteVersion(), []byte{}, []byte("m"))
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, left, kvs[0].Key)
}

func TestShardStoreScanAtWithReadFence_FiltersByEachRouteBounds(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), []byte("z"), 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	rawPrefix := []byte("!redis|meta|")
	left := []byte("!redis|meta|b")
	staleRightOnLeftGroup := []byte("!redis|meta|x")
	right := []byte("!redis|meta|y")
	require.NoError(t, groups[1].Store.PutAt(ctx, left, []byte("left"), 1, 0))
	require.NoError(t, groups[1].Store.PutAt(ctx, staleRightOnLeftGroup, []byte("stale"), 2, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, right, []byte("right"), 3, 0))

	kvs, err := st.ScanAtWithReadFence(ctx, rawPrefix, prefixScanEnd(rawPrefix), 10, 3, false, 0, st.ReadRouteVersion(), []byte("a"), []byte("z"))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, left, kvs[0].Key)
	require.Equal(t, right, kvs[1].Key)
}

func TestShardStoreScanAtWithReadFence_AllowsExplicitGroupRouteBoundReverse(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	rawPrefix := []byte("!redis|meta|")
	left := []byte("!redis|meta|a")
	right := []byte("!redis|meta|z")
	require.NoError(t, groups[1].Store.PutAt(ctx, left, []byte("left"), 1, 0))
	require.NoError(t, groups[1].Store.PutAt(ctx, right, []byte("right"), 2, 0))

	_, err := st.ScanAtWithReadFence(ctx, rawPrefix, prefixScanEnd(rawPrefix), 1, 2, true, 1, st.ReadRouteVersion(), nil, nil)
	require.ErrorIs(t, err, store.ErrNotSupported)

	kvs, err := st.ScanAtWithReadFence(ctx, rawPrefix, prefixScanEnd(rawPrefix), -1, 2, true, 1, st.ReadRouteVersion(), []byte("m"), nil)
	require.NoError(t, err)
	require.Empty(t, kvs)

	kvs, err = st.ScanAtWithReadFence(ctx, rawPrefix, prefixScanEnd(rawPrefix), 1, 2, true, 1, st.ReadRouteVersion(), []byte("m"), nil)
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, right, kvs[0].Key)
	require.Equal(t, []byte("right"), kvs[0].Value)
}

func TestShardStoreScanAt_IncludesS3ManifestKeysAcrossShards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	k1 := s3keys.ObjectManifestKey("bucket-a", 1, "alpha")
	k2 := s3keys.ObjectManifestKey("bucket-a", 1, "zeta")
	require.NoError(t, st.PutAt(ctx, k1, []byte("m1"), 1, 0))
	require.NoError(t, st.PutAt(ctx, k2, []byte("m2"), 2, 0))

	start := s3keys.ObjectManifestPrefixForBucket("bucket-a", 1)
	kvs, err := st.ScanAt(ctx, start, prefixScanEnd(start), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, k1, kvs[0].Key)
	require.Equal(t, k2, kvs[1].Key)
}

func TestShardStoreScanKeysAt_IncludesKeysAcrossShards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 1, 0))
	require.NoError(t, st.PutAt(ctx, []byte("b"), []byte("vb"), 2, 0))
	require.NoError(t, st.DeleteAt(ctx, []byte("b"), 3))
	require.NoError(t, st.PutAt(ctx, []byte("x"), []byte("vx"), 4, 0))
	require.NoError(t, st.PutAt(ctx, []byte("z"), []byte("vz"), 5, 0))

	keys, err := st.ScanKeysAt(ctx, []byte(""), nil, 2, ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("a"), []byte("x")}, keys)
}

func TestShardStoreScanKeysAt_DeduplicatesUnclampedGroups(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 1)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 1, 0))
	require.NoError(t, st.PutAt(ctx, []byte("z"), []byte("vz"), 2, 0))

	keys, err := st.ScanKeysAt(ctx, []byte(""), nil, 10, ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("a"), []byte("z")}, keys)
}

func TestShardStoreScanAt_DeduplicatesUnclampedGroups(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 1)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 1, 0))
	require.NoError(t, st.PutAt(ctx, []byte("z"), []byte("vz"), 2, 0))

	kvs, err := st.ScanAt(ctx, []byte(""), nil, 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, []byte("a"), kvs[0].Key)
	require.Equal(t, []byte("z"), kvs[1].Key)
}

func TestBackupScannerDeduplicatesUnclampedGroups(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 1)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 1, 0))
	require.NoError(t, st.PutAt(ctx, []byte("z"), []byte("vz"), 2, 0))

	sc := st.NewBackupScanner([]byte(""), nil, ^uint64(0), 10)
	defer sc.Close()

	var got [][]byte
	for {
		kvp, ok, err := sc.Next(ctx)
		require.NoError(t, err)
		if !ok {
			break
		}
		got = append(got, kvp.Key)
	}
	require.Equal(t, [][]byte{[]byte("a"), []byte("z")}, got)
}

func TestBackupScannerRetainsAllCapturedRangesForUnclampedGroups(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 1)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	require.NoError(t, groups[1].Store.PutAt(ctx, []byte("a"), []byte("va"), 1, 0))
	require.NoError(t, groups[1].Store.PutAt(ctx, []byte("z"), []byte("vz"), 2, 0))

	sc := st.NewBackupScanner([]byte(""), nil, ^uint64(0), 10)
	defer sc.Close()

	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 10,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte(""), End: []byte("m"), GroupID: 1, State: distribution.RouteStateActive},
			{RouteID: 2, Start: []byte("m"), GroupID: 2, State: distribution.RouteStateActive},
		},
	}))

	var got [][]byte
	for {
		kvp, ok, err := sc.Next(ctx)
		require.NoError(t, err)
		if !ok {
			break
		}
		got = append(got, kvp.Key)
	}
	require.Equal(t, [][]byte{[]byte("a"), []byte("z")}, got)
}

func TestBackupScannerRetainsAllCapturedChunkRanges(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	home := uint64(11)
	inodeA := uint64(22)
	inodeB := uint64(23)
	routeA := fskeys.ChunkRouteKey(home, inodeA)
	routeB := fskeys.ChunkRouteKey(home, inodeB)
	routeEnd := prefixScanEnd(routeB)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), routeA, 1)
	engine.UpdateRoute(routeA, routeB, 2)
	engine.UpdateRoute(routeB, routeEnd, 2)
	engine.UpdateRoute(routeEnd, nil, 3)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
		3: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	chunkA := fskeys.ChunkKey(home, inodeA, 0)
	chunkB := fskeys.ChunkKey(home, inodeB, 0)
	require.NoError(t, st.PutAt(ctx, chunkA, []byte("a"), 1, 0))
	require.NoError(t, st.PutAt(ctx, chunkB, []byte("b"), 2, 0))

	start := fskeys.ChunkPrefix(home, inodeA)
	end := prefixScanEnd(fskeys.ChunkPrefix(home, inodeB))
	sc := st.NewBackupScanner(start, end, ^uint64(0), 10)
	defer sc.Close()

	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 10,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte(""), End: routeA, GroupID: 1, State: distribution.RouteStateActive},
			{RouteID: 2, Start: routeA, End: routeB, GroupID: 2, State: distribution.RouteStateActive},
			{RouteID: 3, Start: routeB, End: routeEnd, GroupID: 1, State: distribution.RouteStateActive},
			{RouteID: 4, Start: routeEnd, GroupID: 3, State: distribution.RouteStateActive},
		},
	}))

	var got [][]byte
	for {
		kvp, ok, err := sc.Next(ctx)
		require.NoError(t, err)
		if !ok {
			break
		}
		got = append(got, kvp.Key)
	}
	require.Equal(t, [][]byte{chunkA, chunkB}, got)
}

func TestBackupScannerDoesNotUseLiveCatalogForMaterialization(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	require.NoError(t, groups[1].Store.PutAt(ctx, []byte("x"), []byte("stale-old-owner"), 1, 0))

	sc := st.NewBackupScanner([]byte(""), nil, ^uint64(0), 10)
	defer sc.Close()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 10,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte(""), GroupID: 1, State: distribution.RouteStateActive},
		},
	}))

	kvp, ok, err := sc.Next(ctx)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, kvp)
}

func TestShardStoreScanKeysRouteAtLeaderRefillsAfterTxnInternalKeys(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	g := &ShardGroup{Store: store.NewMVCCStore()}
	st := NewShardStore(distribution.NewEngine(), map[uint64]*ShardGroup{1: g})

	require.NoError(t, g.Store.PutAt(ctx, txnCommitKey([]byte("primary"), 10), []byte("commit"), 1, 0))
	require.NoError(t, g.Store.PutAt(ctx, []byte("a"), []byte("va"), 2, 0))

	keys, err := st.scanKeysRouteAtLeader(ctx, g, distribution.Route{GroupID: 1}, []byte(""), nil, 1, ^uint64(0), 0)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("a")}, keys)
}

func TestShardStoreScanKeysRouteAtLeaderRefillsAfterStagedControlKeys(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	g := &ShardGroup{Store: store.NewMVCCStore()}
	st := NewShardStore(distribution.NewEngine(), map[uint64]*ShardGroup{1: g})
	t.Cleanup(func() { _ = st.Close() })

	stagedKey := distribution.MigrationStagedDataKey(9, []byte("shadow"))
	require.NoError(t, g.Store.PutAt(ctx, stagedKey, []byte("internal"), 1, 0))
	require.NoError(t, g.Store.PutAt(ctx, []byte("a"), []byte("visible"), 2, 0))

	keys, err := st.scanKeysRouteAtLeader(ctx, g, distribution.Route{GroupID: 1}, []byte(""), nil, 1, ^uint64(0), 0)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("a")}, keys)
}

func TestShardStoreScanKeysRouteAtLeaderPreservesEmptyKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	g := &ShardGroup{Store: store.NewMVCCStore()}
	st := NewShardStore(distribution.NewEngine(), map[uint64]*ShardGroup{1: g})

	require.NoError(t, g.Store.PutAt(ctx, []byte(""), []byte("empty"), 1, 0))
	require.NoError(t, g.Store.PutAt(ctx, []byte("a"), []byte("va"), 2, 0))

	keys, err := st.scanKeysRouteAtLeader(ctx, g, distribution.Route{GroupID: 1}, nil, nil, 2, ^uint64(0), 0)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte(""), []byte("a")}, keys)
}

func TestShardStoreScanKeysRouteAtLeaderIncludesStagedOnlyKeys(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	g := &ShardGroup{Store: store.NewMVCCStore()}
	st := NewShardStore(distribution.NewEngine(), map[uint64]*ShardGroup{1: g})
	route := distribution.Route{
		GroupID:                1,
		StagedVisibilityActive: true,
		MigrationJobID:         9,
	}
	key := []byte("staged-key")
	require.NoError(t, g.Store.PutAt(ctx, distribution.MigrationStagedDataKey(route.MigrationJobID, key), []byte("value"), 1, 0))

	keys, err := st.scanKeysRouteAtLeader(ctx, g, route, []byte(""), nil, 10, ^uint64(0), 0)
	require.NoError(t, err)
	require.Equal(t, [][]byte{key}, keys)
}

func TestShardStoreScanKeysAtIncludesStagedOnlyKeys(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{{
			RouteID:                1,
			Start:                  []byte(""),
			End:                    nil,
			GroupID:                1,
			State:                  distribution.RouteStateActive,
			StagedVisibilityActive: true,
			MigrationJobID:         9,
		}},
	}))
	g := &ShardGroup{Store: store.NewMVCCStore()}
	st := NewShardStore(engine, map[uint64]*ShardGroup{1: g})
	key := []byte("staged-key")
	require.NoError(t, g.Store.PutAt(ctx, distribution.MigrationStagedDataKey(9, key), []byte("value"), 1, 0))

	keys, err := st.ScanKeysAt(ctx, []byte(""), nil, 10, ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, [][]byte{key}, keys)
}

func TestShardStoreProxyScanKeysAtUsesSelectedGroup(t *testing.T) {
	t.Parallel()

	fake := &fakeRawKVServer{
		scanResp: &pb.RawScanAtResponse{
			Kv: []*pb.RawKVPair{{Key: []byte("k"), Value: []byte("v")}},
		},
	}
	addr, stop := startRawKVServer(t, fake)
	t.Cleanup(stop)

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 42)
	g := &ShardGroup{
		Engine: &followerProxyEngine{leader: addr},
		Store:  store.NewMVCCStore(),
	}
	st := NewShardStore(engine, map[uint64]*ShardGroup{42: g})

	keys, err := st.scanKeyRouteAt(ctx, distribution.Route{GroupID: 42}, []byte(""), nil, 10, ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("k")}, keys)

	fake.mu.Lock()
	defer fake.mu.Unlock()
	require.Equal(t, 1, fake.scanCalls)
	require.Equal(t, uint64(42), fake.lastScanGroupID)
	require.True(t, fake.lastScanKeysOnly)
}

func TestShardStoreProxyScanKeysAtCarriesStagedRouteBounds(t *testing.T) {
	t.Parallel()

	fake := &fakeRawKVServer{
		scanResp: &pb.RawScanAtResponse{
			Kv: []*pb.RawKVPair{{Key: []byte("k"), Value: []byte("v")}},
		},
	}
	addr, stop := startRawKVServer(t, fake)
	t.Cleanup(stop)

	ctx := context.Background()
	g := &ShardGroup{
		Engine: &followerProxyEngine{leader: addr},
		Store:  store.NewMVCCStore(),
	}
	st := NewShardStore(distribution.NewEngine(), map[uint64]*ShardGroup{42: g})
	t.Cleanup(func() { _ = st.Close() })
	route := distribution.Route{
		Start:                  []byte("a"),
		End:                    []byte("m"),
		GroupID:                42,
		StagedVisibilityActive: true,
		MigrationJobID:         9,
	}

	keys, err := st.scanKeyRouteAtWithReadFence(ctx, route, []byte("a"), []byte("m"), 10, ^uint64(0), false, 7)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("k")}, keys)

	fake.mu.Lock()
	defer fake.mu.Unlock()
	require.Equal(t, uint64(42), fake.lastScanReq.GetGroupId())
	require.Equal(t, uint64(7), fake.lastScanReq.GetReadRouteVersion())
	require.Equal(t, []byte("a"), fake.lastScanReq.GetRouteStart())
	require.Equal(t, []byte("m"), fake.lastScanReq.GetRouteEnd())
	require.True(t, fake.lastScanReq.GetRouteBoundsPresent())
	require.True(t, fake.lastScanReq.GetKeysOnly())
}

func TestShardStoreProxyScanAtUsesSelectedGroup(t *testing.T) {
	t.Parallel()

	fake := &fakeRawKVServer{
		scanResp: &pb.RawScanAtResponse{
			Kv: []*pb.RawKVPair{{Key: []byte("k"), Value: []byte("v")}},
		},
	}
	addr, stop := startRawKVServer(t, fake)
	t.Cleanup(stop)

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 42)
	g := &ShardGroup{
		Engine: &followerProxyEngine{leader: addr},
		Store:  store.NewMVCCStore(),
	}
	st := NewShardStore(engine, map[uint64]*ShardGroup{42: g})

	kvs, err := st.ScanAt(ctx, []byte(""), nil, 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, []byte("k"), kvs[0].Key)

	fake.mu.Lock()
	defer fake.mu.Unlock()
	require.Equal(t, 1, fake.scanCalls)
	require.Equal(t, uint64(42), fake.lastScanGroupID)
}

func TestShardStoreProxyForwardPageAdvancesFromRawPage(t *testing.T) {
	t.Parallel()

	internalKey := txnCommitKey([]byte("primary"), 10)
	fake := &fakeRawKVServer{
		scanResp: &pb.RawScanAtResponse{
			Kv: []*pb.RawKVPair{
				{Key: []byte("a"), Value: []byte("va")},
				{Key: internalKey, Value: []byte("commit")},
			},
		},
	}
	addr, stop := startRawKVServer(t, fake)
	t.Cleanup(stop)

	ctx := context.Background()
	g := &ShardGroup{
		Engine: &followerProxyEngine{leader: addr},
		Store:  store.NewMVCCStore(),
	}
	st := NewShardStore(distribution.NewEngine(), map[uint64]*ShardGroup{42: g})

	page, err := st.scanRouteAtForwardPage(ctx, distribution.Route{GroupID: 42}, g, []byte(""), nil, 2, ^uint64(0), true, 0, nil, nil)
	require.NoError(t, err)
	require.True(t, page.full)
	require.Equal(t, internalKey, page.advanceKey)
	require.Len(t, page.kvs, 1)
	require.Equal(t, []byte("a"), page.kvs[0].Key)
}

func TestKeyOnlyScanHelpersPreserveEmptyKey(t *testing.T) {
	t.Parallel()

	keys := keysFromKVs(kvPairsFromKeys([][]byte{nil, []byte(""), []byte("a")}))
	require.Equal(t, [][]byte{[]byte(""), []byte("a")}, keys)
}

func TestMergeAndTrimScanKeysSortsBeforeTruncating(t *testing.T) {
	t.Parallel()

	keys := mergeAndTrimScanKeys(nil, [][]byte{[]byte("c"), []byte("d"), []byte("b")}, 2)
	require.Equal(t, [][]byte{[]byte("b"), []byte("c")}, keys)
}

func TestBackupScannerPaging(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	var commitTS uint64 = 1
	for _, key := range [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("x"), []byte("z")} {
		require.NoError(t, st.PutAt(ctx, key, []byte("v"), commitTS, 0))
		commitTS++
	}

	sc := st.NewBackupScanner([]byte(""), nil, ^uint64(0), 2)
	defer sc.Close()

	var got [][]byte
	for {
		kvp, ok, err := sc.Next(ctx)
		require.NoError(t, err)
		if !ok {
			break
		}
		got = append(got, kvp.Key)
	}
	require.Equal(t, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("x"), []byte("z")}, got)
}

func TestBackupScannerMaterializesFromCapturedRoute(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	require.NoError(t, groups[1].Store.PutAt(ctx, []byte("a"), []byte("old-owner"), 1, 0))

	sc := st.NewBackupScanner([]byte(""), nil, ^uint64(0), 1)
	defer sc.Close()

	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte(""), GroupID: 2, State: distribution.RouteStateActive},
		},
	}))

	kvp, ok, err := sc.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("a"), kvp.Key)
	require.Equal(t, []byte("old-owner"), kvp.Value)

	kvp, ok, err = sc.Next(ctx)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, kvp)
}

func TestBackupScannerMaterializesFromEnumeratedRoute(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	require.NoError(t, groups[1].Store.PutAt(ctx, []byte("a"), []byte("stale-first-route"), 1, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, []byte("a"), []byte("enumerated-route"), 2, 0))

	sc := &backupScanner{
		store:         st,
		routes:        []distribution.Route{{RouteID: 1, Start: []byte(""), GroupID: 1}, {RouteID: 2, Start: []byte(""), GroupID: 2}},
		clampToRoutes: false,
		cursor:        []byte(""),
		ts:            ^uint64(0),
		pageSize:      1,
	}
	defer sc.Close()

	kvp, ok, err := sc.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("a"), kvp.Key)
	require.Equal(t, []byte("enumerated-route"), kvp.Value)
}

func TestBackupScannerPreservesFullRoutingAfterListKeyCursor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	listKey := store.ListItemKey([]byte("x"), 0)
	require.NoError(t, st.PutAt(ctx, listKey, []byte("list"), 1, 0))
	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("plain"), 2, 0))

	sc := st.NewBackupScanner([]byte(""), nil, ^uint64(0), 1)
	defer sc.Close()

	var got [][]byte
	for {
		kvp, ok, err := sc.Next(ctx)
		require.NoError(t, err)
		if !ok {
			break
		}
		got = append(got, kvp.Key)
	}
	require.Equal(t, [][]byte{listKey, []byte("a")}, got)
}

func TestBackupScannerContinuesPastTxnInternalOnlyPage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	require.NoError(t, groups[1].Store.PutAt(ctx, txnCommitKey([]byte("primary"), 10), []byte("commit"), 1, 0))
	require.NoError(t, groups[1].Store.PutAt(ctx, []byte("a"), []byte("visible"), 2, 0))

	sc := st.NewBackupScanner([]byte(""), nil, ^uint64(0), 1)
	defer sc.Close()

	kvp, ok, err := sc.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("a"), kvp.Key)

	kvp, ok, err = sc.Next(ctx)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, kvp)
}

func TestBackupScannerContinuesPastStaleOffRoutePage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	require.NoError(t, groups[1].Store.PutAt(ctx, []byte("x"), []byte("stale-old-owner"), 1, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, []byte("z"), []byte("visible-new-owner"), 2, 0))

	sc := st.NewBackupScanner([]byte(""), nil, ^uint64(0), 1)
	defer sc.Close()

	kvp, ok, err := sc.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("z"), kvp.Key)
	require.Equal(t, []byte("visible-new-owner"), kvp.Value)

	kvp, ok, err = sc.Next(ctx)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, kvp)
}

func TestBackupScannerEmptyKeyContinuesAfterPage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	require.NoError(t, st.PutAt(ctx, []byte(""), []byte("empty"), 1, 0))
	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("later"), 2, 0))

	sc := st.NewBackupScanner(nil, nil, ^uint64(0), 1)
	defer sc.Close()

	kvp, ok, err := sc.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte(""), kvp.Key)
	require.Equal(t, []byte("empty"), kvp.Value)

	kvp, ok, err = sc.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("a"), kvp.Key)
	require.Equal(t, []byte("later"), kvp.Value)

	kvp, ok, err = sc.Next(ctx)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, kvp)
}

func TestBackupScannerPebblePagingDoesNotRepeatLastKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)
	pebbleStore, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pebbleStore.Close()) })

	groups := map[uint64]*ShardGroup{
		1: {Store: pebbleStore},
	}
	st := NewShardStore(engine, groups)
	require.NoError(t, pebbleStore.PutAt(ctx, []byte("a"), []byte("va"), 1, 0))
	require.NoError(t, pebbleStore.PutAt(ctx, []byte("b"), []byte("vb"), 2, 0))

	sc := st.NewBackupScanner(nil, nil, ^uint64(0), 1)
	defer sc.Close()

	kvp, ok, err := sc.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("a"), kvp.Key)

	kvp, ok, err = sc.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("b"), kvp.Key)

	kvp, ok, err = sc.Next(ctx)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, kvp)
}

func TestBackupScannerPagesPebbleKeysInLogicalOrder(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)
	pebbleStore, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	defer pebbleStore.Close()

	groups := map[uint64]*ShardGroup{
		1: {Store: pebbleStore},
	}
	st := NewShardStore(engine, groups)
	require.NoError(t, pebbleStore.PutAt(ctx, []byte("a"), []byte("va"), 10, 0))
	require.NoError(t, pebbleStore.PutAt(ctx, []byte("a\x80"), []byte("vax"), 20, 0))

	sc := st.NewBackupScanner(nil, nil, 20, 1)
	defer sc.Close()

	kvp, ok, err := sc.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("a"), kvp.Key)
	require.Equal(t, []byte("va"), kvp.Value)

	kvp, ok, err = sc.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("a\x80"), kvp.Key)
	require.Equal(t, []byte("vax"), kvp.Value)

	kvp, ok, err = sc.Next(ctx)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, kvp)
}

func TestShardStoreScanAt_RoutesS3ManifestScansByLogicalObjectKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	k0 := s3keys.ObjectManifestKey("bucket-a", 1, "z/object-0")
	k1 := s3keys.ObjectManifestKey("bucket-a", 1, "z/object-1")
	require.NoError(t, st.PutAt(ctx, k0, []byte("m0"), 1, 0))
	require.NoError(t, st.PutAt(ctx, k1, []byte("m1"), 2, 0))

	start := s3keys.ObjectManifestScanStart("bucket-a", 1, "z/")
	end := prefixScanEnd(start)
	kvs, err := st.ScanAt(ctx, start, end, 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, k0, kvs[0].Key)
	require.Equal(t, k1, kvs[1].Key)
}

func TestShardStoreScanAt_RoutesRedisWideColumnPrefixAcrossShards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("am"), 1)
	engine.UpdateRoute([]byte("am"), nil, 2)
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	t.Cleanup(func() {
		_ = groups[1].Store.Close()
		_ = groups[2].Store.Close()
	})
	st := NewShardStore(engine, groups)

	left := store.HashFieldKey([]byte("alice"), []byte("field"))
	right := store.HashFieldKey([]byte("amy"), []byte("field"))
	require.NoError(t, st.PutAt(ctx, left, []byte("left"), 1, 0))
	require.NoError(t, st.PutAt(ctx, right, []byte("right"), 2, 0))

	start := store.HashFieldScanPrefix([]byte("a"))
	end := prefixScanEnd([]byte(store.HashFieldPrefix))
	kvs, err := st.ScanAt(ctx, start, end, 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.ElementsMatch(t, [][]byte{left, right}, [][]byte{kvs[0].Key, kvs[1].Key})
}

func TestShardStoreScanAt_RoutesBareRedisWideColumnFamilyAcrossShards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	t.Cleanup(func() {
		_ = groups[1].Store.Close()
		_ = groups[2].Store.Close()
	})
	st := NewShardStore(engine, groups)

	left := store.HashFieldKey([]byte("anna"), []byte("field"))
	right := store.HashFieldKey([]byte("zoey"), []byte("field"))
	require.NoError(t, st.PutAt(ctx, left, []byte("left"), 1, 0))
	require.NoError(t, st.PutAt(ctx, right, []byte("right"), 2, 0))

	prefix := []byte(store.HashFieldPrefix)
	kvs, err := st.ScanAt(ctx, prefix, prefixScanEnd(prefix), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, [][]byte{left, right}, [][]byte{kvs[0].Key, kvs[1].Key})
}

func TestShardStoreScanAt_RoutesRedisWideColumnCursorAcrossRemainingShards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	t.Cleanup(func() {
		_ = groups[1].Store.Close()
		_ = groups[2].Store.Close()
	})
	st := NewShardStore(engine, groups)

	left := store.HashFieldKey([]byte("anna"), []byte("field"))
	right := store.HashFieldKey([]byte("zoey"), []byte("field"))
	require.NoError(t, st.PutAt(ctx, left, []byte("left"), 1, 0))
	require.NoError(t, st.PutAt(ctx, right, []byte("right"), 2, 0))

	prefix := []byte(store.HashFieldPrefix)
	kvs, err := st.ScanAt(ctx, nextScanCursor(left), prefixScanEnd(prefix), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, [][]byte{right}, [][]byte{kvs[0].Key})
}

func TestShardStoreScanAt_RoutesExactRedisWideColumnScanToOneShard(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("am"), 1)
	engine.UpdateRoute([]byte("am"), nil, 2)
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	t.Cleanup(func() {
		_ = groups[1].Store.Close()
		_ = groups[2].Store.Close()
	})
	st := NewShardStore(engine, groups)

	start := store.HashFieldScanPrefix([]byte("alice"))
	routes, clamp, _ := st.routesForScanWithVersion(start, prefixScanEnd(start))
	require.False(t, clamp)
	require.Len(t, routes, 1)
	require.Equal(t, uint64(1), routes[0].GroupID)
}

func TestShardStoreScanAt_RoutesFilesystemChunkScansByChunkRouteKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	home := uint64(11)
	inode := uint64(22)
	routeKey := fskeys.ChunkRouteKey(home, inode)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), routeKey, 1)
	engine.UpdateRoute(routeKey, nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	k0 := fskeys.ChunkKey(home, inode, 0)
	k1 := fskeys.ChunkKey(home, inode, 1)
	require.NoError(t, st.PutAt(ctx, k0, []byte("c0"), 1, 0))
	require.NoError(t, st.PutAt(ctx, k1, []byte("c1"), 2, 0))

	start := fskeys.ChunkPrefix(home, inode)
	kvs, err := st.ScanAt(ctx, start, prefixScanEnd(start), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, k0, kvs[0].Key)
	require.Equal(t, k1, kvs[1].Key)
}

func TestShardStoreResolveFilesystemHomeSlot(t *testing.T) {
	t.Parallel()

	boundary := fskeys.ChunkRouteKey(100, 0)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), boundary, 1)
	engine.UpdateRoute(boundary, nil, 2)
	st := NewShardStore(engine, map[uint64]*ShardGroup{})

	home, err := st.ResolveFilesystemHomeSlot(1, 77)
	require.NoError(t, err)
	groupID, ok := st.FilesystemGroupForHome(home, 77)
	require.True(t, ok)
	require.EqualValues(t, 1, groupID)
	require.Less(t, home, uint64(100))

	home, err = st.ResolveFilesystemHomeSlot(2, 77)
	require.NoError(t, err)
	groupID, ok = st.FilesystemGroupForHome(home, 77)
	require.True(t, ok)
	require.EqualValues(t, 2, groupID)
	require.GreaterOrEqual(t, home, uint64(100))

	_, err = st.ResolveFilesystemHomeSlot(3, 77)
	require.ErrorIs(t, err, ErrFilesystemPlacementTargetNotFound)
}

func TestShardStoreFilesystemGroupIDsReturnsPhysicalGroupsSorted(t *testing.T) {
	t.Parallel()

	st := NewShardStore(distribution.NewEngine(), map[uint64]*ShardGroup{
		9: {},
		2: {},
		5: {},
	})
	require.Equal(t, []uint64{2, 5, 9}, st.FilesystemGroupIDs())
}

func TestShardStoreScanAt_RoutesFilesystemUsageCountersAcrossRouteGroups(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	usagePrefix := fskeys.UsageRouteAllPrefix()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), usagePrefix, 1)
	engine.UpdateRoute(usagePrefix, nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	key := fskeys.UsageRouteKey(fskeys.InodeKey(22))
	staleOnlyKey := fskeys.UsageRouteKey(fskeys.InodeKey(23))
	require.NoError(t, st.PutAt(ctx, key, []byte("usage"), 1, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, key, []byte("stale"), 2, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, staleOnlyKey, []byte("stale-only"), 2, 0))

	kvs, err := st.ScanAt(ctx, usagePrefix, prefixScanEnd(usagePrefix), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, key, kvs[0].Key)
	require.Equal(t, []byte("usage"), kvs[0].Value)

	keys, err := st.ScanKeysAt(ctx, usagePrefix, prefixScanEnd(usagePrefix), 10, ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, [][]byte{key}, keys)

	kvs, err = st.ReverseScanAt(ctx, usagePrefix, prefixScanEnd(usagePrefix), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, key, kvs[0].Key)
	require.Equal(t, []byte("usage"), kvs[0].Value)
}

func TestShardStoreScanAt_RefillsAfterStaleFilesystemUsageCounters(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	usagePrefix := fskeys.UsageRouteAllPrefix()
	ownerBoundary := fskeys.InodeKey(50)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), ownerBoundary, 1)
	engine.UpdateRoute(ownerBoundary, nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	staleOne := fskeys.UsageRouteKey(fskeys.InodeKey(1))
	staleTwo := fskeys.UsageRouteKey(fskeys.InodeKey(2))
	owned := fskeys.UsageRouteKey(fskeys.InodeKey(99))
	require.NoError(t, groups[2].Store.PutAt(ctx, staleOne, []byte("stale-1"), 1, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, staleTwo, []byte("stale-2"), 2, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, owned, []byte("owned"), 3, 0))

	kvs, err := st.ScanAt(ctx, usagePrefix, prefixScanEnd(usagePrefix), 1, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, owned, kvs[0].Key)
	require.Equal(t, []byte("owned"), kvs[0].Value)

	keys, err := st.ScanKeysAt(ctx, usagePrefix, prefixScanEnd(usagePrefix), 1, ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, [][]byte{owned}, keys)
}

func TestBackupScannerPrefersFilesystemUsageOwnerCopy(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	usagePrefix := fskeys.UsageRouteAllPrefix()
	ownerBoundary := fskeys.InodeKey(50)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), ownerBoundary, 1)
	engine.UpdateRoute(ownerBoundary, nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	key := fskeys.UsageRouteKey(fskeys.InodeKey(22))
	require.NoError(t, groups[1].Store.PutAt(ctx, key, []byte("owner"), 1, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, key, []byte("stale"), 2, 0))

	scanner := st.NewBackupScanner(usagePrefix, prefixScanEnd(usagePrefix), ^uint64(0), 1)
	defer scanner.Close()

	pair, ok, err := scanner.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, key, pair.Key)
	require.Equal(t, []byte("owner"), pair.Value)
	pair, ok, err = scanner.Next(ctx)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, pair)
}

func TestShardStoreScanAt_RoutesFilesystemChunkSubrangeByChunkRouteKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	home := uint64(11)
	inode := uint64(22)
	routeKey := fskeys.ChunkRouteKey(home, inode)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), routeKey, 1)
	engine.UpdateRoute(routeKey, nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	k0 := fskeys.ChunkKey(home, inode, 0)
	k1 := fskeys.ChunkKey(home, inode, 1)
	require.NoError(t, st.PutAt(ctx, k0, []byte("c0"), 1, 0))
	require.NoError(t, st.PutAt(ctx, k1, []byte("c1"), 2, 0))

	kvs, err := st.ScanAt(ctx, k0, nextScanCursor(k0), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, k0, kvs[0].Key)
}

func TestShardStoreScanAt_RoutesFilesystemChunkCrossFileSubrangeEndRoute(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	home := uint64(11)
	inodeA := uint64(22)
	inodeB := uint64(23)
	routeA := fskeys.ChunkRouteKey(home, inodeA)
	routeB := fskeys.ChunkRouteKey(home, inodeB)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), routeA, 1)
	engine.UpdateRoute(routeA, routeB, 2)
	engine.UpdateRoute(routeB, nil, 3)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
		3: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	kA := fskeys.ChunkKey(home, inodeA, 7)
	kB0 := fskeys.ChunkKey(home, inodeB, 0)
	kB5 := fskeys.ChunkKey(home, inodeB, 5)
	require.NoError(t, st.PutAt(ctx, kA, []byte("a7"), 1, 0))
	require.NoError(t, st.PutAt(ctx, kB0, []byte("b0"), 2, 0))
	require.NoError(t, st.PutAt(ctx, kB5, []byte("b5"), 3, 0))

	kvs, err := st.ScanAt(ctx, kA, kB5, 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, kA, kvs[0].Key)
	require.Equal(t, kB0, kvs[1].Key)
}

func TestShardStoreScanAt_RoutesFilesystemChunkCrossFileCarriedPrefixEnd(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	home := uint64(11)
	inodeA := uint64(0xfe)
	inodeB := uint64(0xff)
	routeA := fskeys.ChunkRouteKey(home, inodeA)
	routeB := fskeys.ChunkRouteKey(home, inodeB)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), routeA, 1)
	engine.UpdateRoute(routeA, routeB, 2)
	engine.UpdateRoute(routeB, nil, 3)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
		3: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	kA := fskeys.ChunkKey(home, inodeA, 7)
	kB := fskeys.ChunkKey(home, inodeB, 0)
	require.NoError(t, st.PutAt(ctx, kA, []byte("a7"), 1, 0))
	require.NoError(t, st.PutAt(ctx, kB, []byte("b0"), 2, 0))

	kvs, err := st.ScanAt(ctx, kA, prefixScanEnd(fskeys.ChunkPrefix(home, inodeB)), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, kA, kvs[0].Key)
	require.Equal(t, kB, kvs[1].Key)
}

func TestShardStoreScanAt_UnboundedFilesystemChunkScanIncludesRawKeys(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	home := uint64(11)
	inode := uint64(22)
	routeKey := fskeys.ChunkRouteKey(home, inode)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), routeKey, 1)
	engine.UpdateRoute(routeKey, nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	k0 := fskeys.ChunkKey(home, inode, 0)
	k1 := fskeys.ChunkKey(home, inode, 1)
	inodeKey := fskeys.InodeKey(99)
	require.NoError(t, st.PutAt(ctx, k0, []byte("c0"), 1, 0))
	require.NoError(t, st.PutAt(ctx, k1, []byte("c1"), 2, 0))
	require.NoError(t, st.PutAt(ctx, inodeKey, []byte("inode"), 3, 0))

	kvs, err := st.ScanAt(ctx, nextScanCursor(k0), nil, 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, k1, kvs[0].Key)
	require.Equal(t, inodeKey, kvs[1].Key)
}

func TestShardStoreScanAt_BoundedFilesystemChunkScanIncludesRawKeys(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	home := uint64(11)
	inode := uint64(22)
	routeKey := fskeys.ChunkRouteKey(home, inode)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), routeKey, 1)
	engine.UpdateRoute(routeKey, nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	k0 := fskeys.ChunkKey(home, inode, 0)
	k1 := fskeys.ChunkKey(home, inode, 1)
	inodeKey := fskeys.InodeKey(99)
	require.NoError(t, st.PutAt(ctx, k0, []byte("c0"), 1, 0))
	require.NoError(t, st.PutAt(ctx, k1, []byte("c1"), 2, 0))
	require.NoError(t, st.PutAt(ctx, inodeKey, []byte("inode"), 3, 0))

	kvs, err := st.ScanAt(ctx, nextScanCursor(k0), prefixScanEnd(fskeys.InodeAllPrefix()), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, k1, kvs[0].Key)
	require.Equal(t, inodeKey, kvs[1].Key)
}

func TestShardStoreScanAt_UpperBoundedFilesystemChunkScanIncludesChunkRoutes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	home := uint64(11)
	inode := uint64(22)
	routeKey := fskeys.ChunkRouteKey(home, inode)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), routeKey, 1)
	engine.UpdateRoute(routeKey, nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	k0 := fskeys.ChunkKey(home, inode, 0)
	k1 := fskeys.ChunkKey(home, inode, 1)
	require.NoError(t, st.PutAt(ctx, k0, []byte("c0"), 1, 0))
	require.NoError(t, st.PutAt(ctx, k1, []byte("c1"), 2, 0))

	kvs, err := st.ScanAt(ctx, nil, prefixScanEnd(fskeys.ChunkAllPrefix()), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, k0, kvs[0].Key)
	require.Equal(t, k1, kvs[1].Key)
}

func TestShardStoreScanAt_NilStartFanoutUsesExplicitGroupForProxy(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	recorder := &recordingRawScanKVServer{}
	addr, stop := startRawKVServer(t, recorder)
	t.Cleanup(stop)

	rawRouteEnd := fskeys.ChunkRouteKey(11, 22)
	chunkRouteEnd := prefixScanEnd(rawRouteEnd)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), rawRouteEnd, 1)
	engine.UpdateRoute(rawRouteEnd, chunkRouteEnd, 2)
	engine.UpdateRoute(chunkRouteEnd, nil, 3)

	groups := map[uint64]*ShardGroup{
		1: {Engine: followerEngineForTest(addr), Store: store.NewMVCCStore()},
		2: {Engine: followerEngineForTest(addr), Store: store.NewMVCCStore()},
		3: {Engine: followerEngineForTest(addr), Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	t.Cleanup(func() { require.NoError(t, st.Close()) })

	_, err := st.ScanAt(ctx, nil, prefixScanEnd(fskeys.ChunkAllPrefix()), 10, ^uint64(0))
	require.NoError(t, err)

	groupIDs := recorder.rawScanGroupIDs()
	require.NotContains(t, groupIDs, uint64(0))
	require.Contains(t, groupIDs, uint64(1))
	require.Contains(t, groupIDs, uint64(2))
}

func TestShardStoreReverseScanAt_UpperBoundedFilesystemChunkScanIncludesChunkRoutes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	home := uint64(11)
	inode := uint64(22)
	routeKey := fskeys.ChunkRouteKey(home, inode)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), routeKey, 1)
	engine.UpdateRoute(routeKey, nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	k0 := fskeys.ChunkKey(home, inode, 0)
	k1 := fskeys.ChunkKey(home, inode, 1)
	require.NoError(t, st.PutAt(ctx, k0, []byte("c0"), 1, 0))
	require.NoError(t, st.PutAt(ctx, k1, []byte("c1"), 2, 0))

	kvs, err := st.ReverseScanAt(ctx, nil, prefixScanEnd(fskeys.ChunkAllPrefix()), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, k1, kvs[0].Key)
	require.Equal(t, k0, kvs[1].Key)
}

type recordingRawScanKVServer struct {
	pb.UnimplementedRawKVServer

	mu       sync.Mutex
	groupIDs []uint64
}

func (s *recordingRawScanKVServer) RawScanAt(_ context.Context, req *pb.RawScanAtRequest) (*pb.RawScanAtResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.groupIDs = append(s.groupIDs, req.GetGroupId())
	return &pb.RawScanAtResponse{}, nil
}

func (s *recordingRawScanKVServer) rawScanGroupIDs() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]uint64(nil), s.groupIDs...)
}

type followerLeaderAddrEngine struct {
	*fakeLeaseEngine
	addr string
}

func followerEngineForTest(addr string) *followerLeaderAddrEngine {
	inner := &fakeLeaseEngine{}
	inner.state.Store(raftengine.StateFollower)
	return &followerLeaderAddrEngine{fakeLeaseEngine: inner, addr: addr}
}

func (e *followerLeaderAddrEngine) Leader() raftengine.LeaderInfo {
	return raftengine.LeaderInfo{ID: "n1", Address: e.addr}
}

func TestShardStoreScanAt_DeduplicatesFilesystemChunkRoutesByGroup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	home := uint64(11)
	inode := uint64(22)
	routeStart := fskeys.ChunkRouteKey(home, inode)
	routeEnd := prefixScanEnd(routeStart)
	routeSplit := append(append([]byte(nil), routeStart...), 0x80)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), routeStart, 1)
	engine.UpdateRoute(routeStart, routeSplit, 2)
	engine.UpdateRoute(routeSplit, routeEnd, 2)
	engine.UpdateRoute(routeEnd, nil, 3)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
		3: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	k0 := fskeys.ChunkKey(home, inode, 0)
	k1 := fskeys.ChunkKey(home, inode, 1)
	require.NoError(t, st.PutAt(ctx, k0, []byte("c0"), 1, 0))
	require.NoError(t, st.PutAt(ctx, k1, []byte("c1"), 2, 0))

	start := fskeys.ChunkPrefix(home, inode)
	kvs, err := st.ScanAt(ctx, start, prefixScanEnd(start), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, k0, kvs[0].Key)
	require.Equal(t, k1, kvs[1].Key)
}

func TestShardStoreReverseScanAt_RoutesFilesystemChunkScansByChunkRouteKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	home := uint64(11)
	inode := uint64(22)
	routeKey := fskeys.ChunkRouteKey(home, inode)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), routeKey, 1)
	engine.UpdateRoute(routeKey, nil, 2)
	st := NewShardStore(engine, map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	})

	k0 := fskeys.ChunkKey(home, inode, 0)
	k1 := fskeys.ChunkKey(home, inode, 1)
	require.NoError(t, st.PutAt(ctx, k0, []byte("c0"), 1, 0))
	require.NoError(t, st.PutAt(ctx, k1, []byte("c1"), 2, 0))

	routes, clampToRoutes := st.routesForReverseScan(fskeys.ChunkPrefix(home, inode), prefixScanEnd(fskeys.ChunkPrefix(home, inode)))
	require.False(t, clampToRoutes)
	require.Len(t, routes, 1)
	require.Equal(t, uint64(2), routes[0].GroupID)

	kvs, err := st.ReverseScanAt(ctx, fskeys.ChunkPrefix(home, inode), prefixScanEnd(fskeys.ChunkPrefix(home, inode)), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, k1, kvs[0].Key)
	require.Equal(t, k0, kvs[1].Key)
}

// TestShardStoreReverseScanAt_DescendingOrderAcrossShards verifies that
// ReverseScanAt with a nil start (clampToRoutes=false) merges results from all
// shards and returns them in descending key order.
func TestShardStoreReverseScanAt_DescendingOrderAcrossShards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	// Shard 1 (keys < "m")
	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 1, 0))
	require.NoError(t, st.PutAt(ctx, []byte("c"), []byte("vc"), 2, 0))
	// Shard 2 (keys >= "m")
	require.NoError(t, st.PutAt(ctx, []byte("x"), []byte("vx"), 3, 0))
	require.NoError(t, st.PutAt(ctx, []byte("z"), []byte("vz"), 4, 0))

	// nil start → clampToRoutes=false; both shards must be merged in descending order.
	kvs, err := st.ReverseScanAt(ctx, nil, nil, 4, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 4)
	require.Equal(t, []byte("z"), kvs[0].Key)
	require.Equal(t, []byte("x"), kvs[1].Key)
	require.Equal(t, []byte("c"), kvs[2].Key)
	require.Equal(t, []byte("a"), kvs[3].Key)
}

// TestShardStoreReverseScanAt_LimitAcrossShards verifies that the limit is
// correctly applied when results from multiple shards are merged in descending
// order. The top-N keys across all shards must be returned.
func TestShardStoreReverseScanAt_LimitAcrossShards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	// Shard 1 (keys < "m")
	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 1, 0))
	require.NoError(t, st.PutAt(ctx, []byte("b"), []byte("vb"), 2, 0))
	require.NoError(t, st.PutAt(ctx, []byte("c"), []byte("vc"), 3, 0))
	// Shard 2 (keys >= "m")
	require.NoError(t, st.PutAt(ctx, []byte("x"), []byte("vx"), 4, 0))
	require.NoError(t, st.PutAt(ctx, []byte("y"), []byte("vy"), 5, 0))
	require.NoError(t, st.PutAt(ctx, []byte("z"), []byte("vz"), 6, 0))

	// limit=4: top-4 in descending order are z, y, x, c.
	kvs, err := st.ReverseScanAt(ctx, nil, nil, 4, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 4)
	require.Equal(t, []byte("z"), kvs[0].Key)
	require.Equal(t, []byte("y"), kvs[1].Key)
	require.Equal(t, []byte("x"), kvs[2].Key)
	require.Equal(t, []byte("c"), kvs[3].Key)
}

// TestShardStoreReverseScanAt_SingleShard verifies that ReverseScanAt on a
// single shard returns results in descending key order.
func TestShardStoreReverseScanAt_SingleShard(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 1, 0))
	require.NoError(t, st.PutAt(ctx, []byte("b"), []byte("vb"), 2, 0))
	require.NoError(t, st.PutAt(ctx, []byte("c"), []byte("vc"), 3, 0))

	kvs, err := st.ReverseScanAt(ctx, nil, nil, 2, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, []byte("c"), kvs[0].Key)
	require.Equal(t, []byte("b"), kvs[1].Key)
}

// TestShardStoreReverseScanAt_IncludesS3ManifestKeysDescending mirrors
// TestShardStoreScanAt_IncludesS3ManifestKeysAcrossShards but for
// ReverseScanAt — results must be returned in descending key order.
func TestShardStoreReverseScanAt_IncludesS3ManifestKeysDescending(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	k1 := s3keys.ObjectManifestKey("bucket-a", 1, "alpha")
	k2 := s3keys.ObjectManifestKey("bucket-a", 1, "zeta")
	require.NoError(t, st.PutAt(ctx, k1, []byte("m1"), 1, 0))
	require.NoError(t, st.PutAt(ctx, k2, []byte("m2"), 2, 0))

	start := s3keys.ObjectManifestPrefixForBucket("bucket-a", 1)
	kvs, err := st.ReverseScanAt(ctx, start, prefixScanEnd(start), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	// "zeta" > "alpha" → descending order puts k2 first.
	require.Equal(t, k2, kvs[0].Key)
	require.Equal(t, k1, kvs[1].Key)
}

// TestMergeAndTrimReverseScanResults verifies that the helper merges two
// slices, sorts them in descending key order, and trims to the given limit.
func TestMergeAndTrimReverseScanResults(t *testing.T) {
	t.Parallel()

	out := []*store.KVPair{
		{Key: []byte("z"), Value: []byte("vz")},
		{Key: []byte("m"), Value: []byte("vm")},
	}
	kvs := []*store.KVPair{
		{Key: []byte("y"), Value: []byte("vy")},
		{Key: []byte("a"), Value: []byte("va")},
	}

	result := mergeAndTrimReverseScanResults(out, kvs, 3)
	require.Len(t, result, 3)
	require.Equal(t, []byte("z"), result[0].Key)
	require.Equal(t, []byte("y"), result[1].Key)
	require.Equal(t, []byte("m"), result[2].Key)
}

func TestMergeAndTrimReverseScanResults_EmptyInput(t *testing.T) {
	t.Parallel()

	out := []*store.KVPair{{Key: []byte("z"), Value: []byte("vz")}}
	result := mergeAndTrimReverseScanResults(out, nil, 10)
	require.Len(t, result, 1)
	require.Equal(t, []byte("z"), result[0].Key)
}

func TestMergeAndTrimReverseScanResults_WithinLimit(t *testing.T) {
	t.Parallel()

	out := []*store.KVPair{{Key: []byte("z"), Value: []byte("vz")}}
	kvs := []*store.KVPair{{Key: []byte("a"), Value: []byte("va")}}

	result := mergeAndTrimReverseScanResults(out, kvs, 10)
	require.Len(t, result, 2)
	require.Equal(t, []byte("z"), result[0].Key)
	require.Equal(t, []byte("a"), result[1].Key)
}

func TestMergeAndTrimReverseScanResults_ExactLimit(t *testing.T) {
	t.Parallel()

	out := []*store.KVPair{
		{Key: []byte("z"), Value: []byte("vz")},
		{Key: []byte("c"), Value: []byte("vc")},
	}
	kvs := []*store.KVPair{
		{Key: []byte("y"), Value: []byte("vy")},
		{Key: []byte("a"), Value: []byte("va")},
	}

	// limit=2: top-2 in descending order are "z", "y".
	result := mergeAndTrimReverseScanResults(out, kvs, 2)
	require.Len(t, result, 2)
	require.Equal(t, []byte("z"), result[0].Key)
	require.Equal(t, []byte("y"), result[1].Key)
}

func TestScanLockBoundsForKVs_ReverseOrder(t *testing.T) {
	t.Parallel()

	kvs := []*store.KVPair{
		{Key: []byte("c"), Value: []byte("vc")},
		{Key: []byte("b"), Value: []byte("vb")},
	}

	lockStart, lockEnd := scanLockBoundsForKVs(kvs, []byte("a"), []byte("d"), 2)
	require.Equal(t, []byte("a"), lockStart)
	require.Equal(t, nextScanCursor([]byte("c")), lockEnd)
}

func TestScanLockBoundsForKVsDirection_ReverseUsesReturnedWindow(t *testing.T) {
	t.Parallel()

	kvs := []*store.KVPair{
		{Key: []byte("z"), Value: []byte("vz")},
		{Key: []byte("y"), Value: []byte("vy")},
	}

	lockStart, lockEnd := scanLockBoundsForKVsDirection(kvs, []byte("a"), []byte("zz"), 2, true)
	require.Equal(t, []byte("y"), lockStart)
	require.Equal(t, []byte("zz"), lockEnd)
}

func TestScanLockBoundsForKVs_PreservesOriginalStart(t *testing.T) {
	t.Parallel()

	kvs := []*store.KVPair{
		{Key: []byte("c"), Value: []byte("vc")},
		{Key: []byte("e"), Value: []byte("ve")},
	}

	lockStart, lockEnd := scanLockBoundsForKVs(kvs, []byte("a"), []byte("z"), 2)
	require.Equal(t, []byte("a"), lockStart)
	require.Equal(t, nextScanCursor([]byte("e")), lockEnd)
}

func TestScanLockBoundsForKVs_IncompleteScanUsesOriginalRange(t *testing.T) {
	t.Parallel()

	kvs := []*store.KVPair{
		{Key: []byte("c"), Value: []byte("vc")},
	}

	lockStart, lockEnd := scanLockBoundsForKVs(kvs, []byte("a"), []byte("z"), 2)
	require.Equal(t, []byte("a"), lockStart)
	require.Equal(t, []byte("z"), lockEnd)
}

func TestScanLockBoundsForKVs_EmptyUsesOriginalRange(t *testing.T) {
	t.Parallel()

	lockStart, lockEnd := scanLockBoundsForKVs(nil, []byte("a"), []byte("z"), 10)
	require.Equal(t, []byte("a"), lockStart)
	require.Equal(t, []byte("z"), lockEnd)
}

func TestScanLockBoundsForKVs_FullInternalPageUsesRawPageBound(t *testing.T) {
	t.Parallel()

	internalKey := txnCommitKey([]byte("primary"), 10)
	kvs := []*store.KVPair{
		{Key: internalKey, Value: []byte("commit")},
	}

	lockStart, lockEnd := scanLockBoundsForKVs(kvs, []byte(""), nil, 1)
	require.Equal(t, []byte(""), lockStart)
	require.Equal(t, nextScanCursor(internalKey), lockEnd)
}

func TestScanLockBoundsForKVs_ReverseInternalOnlyPageUsesOriginalRange(t *testing.T) {
	t.Parallel()

	internalKey := txnCommitKey([]byte("primary"), 10)
	kvs := []*store.KVPair{
		{Key: internalKey, Value: []byte("commit")},
	}

	lockStart, lockEnd := scanLockBoundsForKVsDirection(kvs, []byte(""), []byte("z"), 1, true)
	require.Equal(t, []byte(""), lockStart)
	require.Equal(t, []byte("z"), lockEnd)
}

// Reverse-scan counterpart of TestShardStoreS3BucketAuxiliaryScanHonorsStagedTombstone.
// A staged tombstone must hide the stale live row from the old raw route in both
// scan directions, not just forward.
func TestShardStoreS3BucketAuxiliaryReverseScanHonorsStagedTombstone(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const migratedBucket = "bucket-a"
	routeStart := s3keys.RoutePrefixForBucketAnyGeneration(migratedBucket)
	routeEnd := prefixScanEnd(routeStart)
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte(""), End: routeStart, GroupID: 1, State: distribution.RouteStateActive},
			{RouteID: 2, Start: routeStart, End: routeEnd, GroupID: 2, State: distribution.RouteStateActive, StagedVisibilityActive: true, MigrationJobID: 9},
			{RouteID: 3, Start: routeEnd, End: nil, GroupID: 1, State: distribution.RouteStateActive},
		},
	}))
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	deletedKey := s3keys.BucketMetaKey(migratedBucket)
	visibleKey := s3keys.BucketMetaKey("bucket-z")
	require.NoError(t, groups[1].Store.PutAt(ctx, deletedKey, []byte("stale"), 10, 0))
	require.NoError(t, groups[1].Store.PutAt(ctx, visibleKey, []byte("visible"), 10, 0))
	require.NoError(t, groups[2].Store.DeleteAt(ctx, distribution.MigrationStagedDataKey(9, deletedKey), 20))

	start := []byte(s3keys.BucketMetaPrefix)
	end := prefixScanEnd(start)

	// Reverse over the whole family: the tombstoned bucket must not appear.
	kvs, err := st.ReverseScanAt(ctx, start, end, 10, 30)
	require.NoError(t, err)
	require.Equal(t, []*store.KVPair{{Key: visibleKey, Value: []byte("visible")}}, kvs)

	// Reverse scoped to the tombstoned bucket alone.
	kvs, err = st.ReverseScanAt(ctx, deletedKey, prefixScanEnd(deletedKey), 10, 30)
	require.NoError(t, err)
	require.Empty(t, kvs)
}

// An exact per-user-key legacy delta scan must carry RouteGroupID too. Redis
// cleanup and compaction build their deletes as {Del, pair.Key, GroupID:
// pair.RouteGroupID}; a zero GroupID routes the delete by the raw
// "!lst|meta|d|..." key instead of the logical list key, so after a split it
// lands on the wrong shard and the stale delta survives.
func TestShardStoreScanAt_ExactLegacyListDeltaScanMarksRouteGroup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := newTwoRouteShardStoreForScanTest()
	deltaValue := store.MarshalListMetaDelta(store.ListMetaDelta{LenDelta: 1})

	// "right-list" sorts into the second route's group.
	userKey := []byte("right-list")
	key := legacyListMetaDeltaKey(userKey, 11)
	require.NoError(t, st.groups[2].Store.PutAt(ctx, key, deltaValue, 1, 0))

	scanStart := store.LegacyListMetaDeltaScanPrefix(userKey)
	require.False(t, isBroadLegacyListDeltaScan(scanStart),
		"this test is only meaningful for the exact-scan shape")

	kvs, err := st.ScanAt(ctx, scanStart, store.PrefixScanEnd(scanStart), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, key, kvs[0].Key)
	require.Equal(t, uint64(2), kvs[0].RouteGroupID,
		"an exact legacy delta scan must still report the owning route group")
}

// A coordinator that has not yet applied a promotion keeps forwarding the
// pre-cutover source group. Once StagedVisibilityActive is cleared the source's
// former range belongs to the target, and the staged-visibility rejection stops
// covering the request -- exactly while the source's pre-cutover MVCC is still
// sitting there waiting for cleanup. Serving that is a stale read, so the
// mismatch must fail closed instead.
func TestShardStoreExplicitGroupRead_FailsClosedAfterPromotionClearsStaging(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 2,
		Routes: []distribution.RouteDescriptor{
			// Promotion completed: the range now belongs to group 2 and the
			// staged-visibility flag is gone.
			{RouteID: 1, Start: []byte("a"), End: []byte("z"), GroupID: 2, State: distribution.RouteStateActive},
		},
	}))
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	require.NoError(t, groups[1].Store.PutAt(ctx, []byte("b"), []byte("pre-cutover"), 10, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, []byte("b"), []byte("post-cutover"), 20, 0))

	_, err := st.GetGroupAt(ctx, 1, []byte("b"), 25)
	require.ErrorIs(t, err, ErrExplicitGroupRouteOwnerMismatch)

	_, err = st.ScanGroupAt(ctx, 1, []byte("a"), []byte("z"), 10, 25)
	require.ErrorIs(t, err, ErrExplicitGroupRouteOwnerMismatch)

	_, err = st.ScanAtWithReadFence(ctx, []byte("a"), []byte("z"), 10, 25, false, 1, 0, []byte("a"), []byte("z"))
	require.ErrorIs(t, err, ErrExplicitGroupRouteOwnerMismatch)

	// The group the catalog does name still serves the post-cutover value.
	got, err := st.GetGroupAt(ctx, 2, []byte("b"), 25)
	require.NoError(t, err)
	require.Equal(t, []byte("post-cutover"), got)
}

// SQS resolves its owning group through the (queue, partition) resolver rather
// than the byte-range catalog, so a catalog route naming another group must not
// reject those reads.
func TestShardStoreExplicitGroupRead_AllowsResolverOwnedKeysOnMismatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)
	groups := map[uint64]*ShardGroup{
		1:  {Store: store.NewMVCCStore()},
		42: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	key := []byte("!sqs|msg|data|p|orders|partition-2|msg-2")
	require.NoError(t, groups[42].Store.PutAt(ctx, key, []byte("payload"), 7, 0))

	got, err := st.GetGroupAt(ctx, 42, key, 7)
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), got)

	start := []byte("!sqs|msg|data|p|orders|partition-2|")
	kvs, err := st.ScanGroupAt(ctx, 42, start, prefixScanEnd(start), 10, 7)
	require.NoError(t, err)
	require.Equal(t, []*store.KVPair{{Key: key, Value: []byte("payload")}}, kvs)
}

// Filesystem placement stats scan the whole chunk keyspace once per filesystem
// group, so most of those groups are not the catalog owner of the range. The
// explicit-group gate must let them through the way it lets SQS through.
func TestShardStoreExplicitGroupScan_AllowsFilesystemChunkKeys(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	chunkKey := fskeys.ChunkKey(3, 7, 0)
	require.NoError(t, groups[2].Store.PutAt(ctx, chunkKey, []byte("chunk"), 7, 0))

	start := fskeys.ChunkAllPrefix()
	kvs, err := st.ScanGroupAt(ctx, 2, start, prefixScanEnd(start), 10, 7)
	require.NoError(t, err)
	require.Equal(t, []*store.KVPair{{Key: chunkKey, Value: []byte("chunk")}}, kvs)
}

// Between cutover and promotion a key can be visible through its staged alias
// while the live key holds nothing. Both store implementations read the live key
// first and return ErrKeyNotFound when it is absent, so an expiration issued in
// that window failed for a value the same route serves happily through GetAt.
func TestExpireAtAppliesToStagedOnlyValues(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, group := newStagedVisibilityShardStore(t)

	key := []byte("b")
	require.NoError(t, group.Store.PutAt(ctx,
		distribution.MigrationStagedDataKey(9, key), []byte("staged-b"), 20, 0))

	// The value is visible even though the live key has nothing.
	got, err := st.GetAt(ctx, key, 25)
	require.NoError(t, err)
	require.Equal(t, []byte("staged-b"), got)

	// The expiry is in the future relative to the commit timestamp, so the value
	// survives; the point is that ExpireAt no longer fails outright.
	require.NoError(t, st.ExpireAt(ctx, key, 5_000, 300))

	got, err = st.GetAt(ctx, key, 300)
	require.NoError(t, err)
	require.Equal(t, []byte("staged-b"), got)

	// The expiration was recorded as a live MVCC version, which is where every
	// other post-cutover write goes.
	live, err := group.Store.GetAt(ctx, key, 300)
	require.NoError(t, err)
	require.Equal(t, []byte("staged-b"), live)

	// And it takes effect once the read passes the expiry.
	_, err = st.GetAt(ctx, key, 6_000)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestExpireAtUsesNewerStagedValueOverLiveValue(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, group := newStagedVisibilityShardStore(t)

	key := []byte("b")
	require.NoError(t, group.Store.PutAt(ctx, key, []byte("live-b"), 10, 0))
	require.NoError(t, group.Store.PutAt(ctx,
		distribution.MigrationStagedDataKey(9, key), []byte("staged-b"), 20, 0))

	require.NoError(t, st.ExpireAt(ctx, key, 5_000, 300))

	got, err := st.GetAt(ctx, key, 300)
	require.NoError(t, err)
	require.Equal(t, []byte("staged-b"), got)

	live, err := group.Store.GetAt(ctx, key, 300)
	require.NoError(t, err)
	require.Equal(t, []byte("staged-b"), live)
}

func TestExpireAtHonorsNewerStagedTombstone(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, group := newStagedVisibilityShardStore(t)

	key := []byte("b")
	require.NoError(t, group.Store.PutAt(ctx, key, []byte("live-b"), 10, 0))
	require.NoError(t, group.Store.DeleteAt(ctx, distribution.MigrationStagedDataKey(9, key), 20))

	require.ErrorIs(t, st.ExpireAt(ctx, key, 5_000, 300), store.ErrKeyNotFound)

	_, err := st.GetAt(ctx, key, 300)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	live, err := group.Store.GetAt(ctx, key, 300)
	require.NoError(t, err)
	require.Equal(t, []byte("live-b"), live)
}

// A key with nothing on either side still reports ErrKeyNotFound.
func TestExpireAtStillFailsWhenNothingIsVisible(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, _ := newStagedVisibilityShardStore(t)

	require.ErrorIs(t, st.ExpireAt(ctx, []byte("absent"), 40, 300), store.ErrKeyNotFound)
}

// promotingExportStore runs a hook after the first ExportVersions call, which
// is how a promotion batch is landed exactly between the two probes
// getAtWithStagedVisibility makes.
type promotingExportStore struct {
	store.MVCCStore
	afterFirst func()
	calls      int
}

func (s *promotingExportStore) ExportVersions(
	ctx context.Context,
	opts store.ExportVersionsOptions,
) (store.ExportVersionsResult, error) {
	res, err := s.MVCCStore.ExportVersions(ctx, opts)
	s.fireAfterFirst()
	return res, err
}

// ScanAt fires the promotion when the *live* range is scanned. That is the
// only interleaving that distinguishes the two orderings: live-first means the
// live scan misses the key and the staged scan that follows misses it too,
// while staged-first has already captured it before promotion runs.
func (s *promotingExportStore) ScanAt(
	ctx context.Context,
	start, end []byte,
	limit int,
	ts uint64,
) ([]*store.KVPair, error) {
	kvs, err := s.MVCCStore.ScanAt(ctx, start, end, limit, ts)
	if !isMigrationStagedDataKey(start) && s.afterFirst != nil {
		s.fireAfterFirst()
	}
	return kvs, err
}

// LatestCommitTS fires the promotion on the live-key probe, which is the
// interleaving that separates the two orderings for the watermark reads.
func (s *promotingExportStore) LatestCommitTS(ctx context.Context, key []byte) (uint64, bool, error) {
	ts, exists, err := s.MVCCStore.LatestCommitTS(ctx, key)
	if !isMigrationStagedDataKey(key) && s.afterFirst != nil {
		s.fireAfterFirst()
	}
	return ts, exists, err
}

func (s *promotingExportStore) fireAfterFirst() {
	s.calls++
	if s.calls == 1 && s.afterFirst != nil {
		s.afterFirst()
	}
}

// A promotion batch landing between the staged and live probes must not make a
// key disappear. Promotion writes the live version and drops the staged alias,
// so a live-then-staged order misses both sides of a staged-only key: live has
// not been written yet at the first probe, and the alias is gone by the second.
func TestShardStoreGetAt_StagedVisibilitySurvivesPromotionBetweenProbes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{{
			RouteID: 1, Start: []byte("a"), End: []byte("z"), GroupID: 1,
			State: distribution.RouteStateActive, StagedVisibilityActive: true,
			MigrationJobID: 9, MinWriteTSExclusive: 100,
		}},
	}))

	inner := store.NewMVCCStore()
	t.Cleanup(func() { _ = inner.Close() })
	rawKey := []byte("k")
	stagedKey := distribution.MigrationStagedDataKey(9, rawKey)
	require.NoError(t, inner.PutAt(ctx, stagedKey, []byte("staged-only"), 20, 0))

	promoting := &promotingExportStore{MVCCStore: inner}
	promoting.afterFirst = func() {
		// The promotion batch: the row becomes live at its original commit ts
		// and the staged alias goes away.
		require.NoError(t, inner.PutAt(ctx, rawKey, []byte("staged-only"), 20, 0))
		require.NoError(t, inner.DeleteAt(ctx, stagedKey, 21))
	}
	st := NewShardStore(engine, map[uint64]*ShardGroup{1: {Store: promoting}})

	got, err := st.GetAt(ctx, rawKey, 25)
	require.NoError(t, err, "a promotion between the probes must not hide the key")
	require.Equal(t, []byte("staged-only"), got)
}

// Every place that reads the live and staged namespaces as two separate store
// calls has to read staged first, for the reason getAtWithStagedVisibility
// documents. Fixing only the point read left the scan, the TTL winner, and the
// watermark on the old order, each with its own way of losing the key.
func TestStagedVisibilityProbesReadStagedFirst(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{{
			RouteID: 1, Start: []byte("a"), End: []byte("z"), GroupID: 1,
			State: distribution.RouteStateActive, StagedVisibilityActive: true,
			MigrationJobID: 9, MinWriteTSExclusive: 100,
		}},
	}))

	rawKey := []byte("k")
	stagedKey := distribution.MigrationStagedDataKey(9, rawKey)

	newStore := func(t *testing.T) (*ShardStore, *promotingExportStore, store.MVCCStore) {
		t.Helper()
		inner := store.NewMVCCStore()
		t.Cleanup(func() { _ = inner.Close() })
		require.NoError(t, inner.PutAt(ctx, stagedKey, []byte("staged-only"), 20, 0))
		promoting := &promotingExportStore{MVCCStore: inner}
		promoting.afterFirst = func() {
			// The real promotion batch: PromoteVersions moves the staged row to
			// its live key and removes the staged version physically. Modelling
			// it with a Delete would write a tombstone that legitimately hides
			// the key, which is a different scenario.
			promoter, ok := inner.(store.MigrationPromoter)
			require.True(t, ok)
			_, err := promoter.PromoteVersions(ctx, store.PromoteVersionsOptions{
				JobID:       9,
				StartKey:    distribution.MigrationStagedDataKeyPrefix(9),
				EndKey:      prefixScanEnd(distribution.MigrationStagedDataKeyPrefix(9)),
				MaxVersions: 16,
				TargetKey: func(staged []byte) ([]byte, bool) {
					_, raw, ok := distribution.MigrationStagedDataKeyParts(staged)
					return raw, ok
				},
			})
			require.NoError(t, err)
		}
		return NewShardStore(engine, map[uint64]*ShardGroup{1: {Store: promoting}}), promoting, inner
	}

	t.Run("scan", func(t *testing.T) {
		t.Parallel()
		st, _, _ := newStore(t)
		kvs, err := st.ScanAt(ctx, []byte("a"), []byte("z"), 10, 25)
		require.NoError(t, err)
		require.Len(t, kvs, 1, "a promotion between the scans must not drop the key")
		require.Equal(t, rawKey, kvs[0].Key)
	})

	t.Run("point read", func(t *testing.T) {
		t.Parallel()
		st, _, _ := newStore(t)
		got, err := st.GetAt(ctx, rawKey, 25)
		require.NoError(t, err)
		require.Equal(t, []byte("staged-only"), got)
	})
}
