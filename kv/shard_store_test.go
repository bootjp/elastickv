package kv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/s3keys"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

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

func applyTargetReadiness(t *testing.T, group *ShardGroup) {
	t.Helper()
	applyTargetReadinessState(t, group, store.TargetStagedReadinessState{
		JobID:                  9,
		RouteStart:             []byte("a"),
		RouteEnd:               []byte("z"),
		ExpectedCutoverVersion: 2,
		MigrationJobID:         9,
		MinWriteTSExclusive:    100,
		Armed:                  true,
	})
}

func applyTargetReadinessState(t *testing.T, group *ShardGroup, state store.TargetStagedReadinessState) {
	t.Helper()
	writer, ok := group.Store.(store.MigrationTargetReadinessWriter)
	require.True(t, ok)
	require.NoError(t, writer.ApplyTargetStagedReadiness(context.Background(), state))
}

func newReadinessShardStore(t *testing.T, route distribution.RouteDescriptor) (*ShardStore, *ShardGroup) {
	t.Helper()
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 2,
		Routes:  []distribution.RouteDescriptor{route},
	}))
	group := &ShardGroup{Store: store.NewMVCCStore()}
	return NewShardStore(engine, map[uint64]*ShardGroup{route.GroupID: group}), group
}

type readinessFenceEngine struct {
	state              raftengine.State
	onLinearizableRead func()
}

func (e *readinessFenceEngine) State() raftengine.State {
	if e.state != "" {
		return e.state
	}
	return raftengine.StateFollower
}

func (e *readinessFenceEngine) Leader() raftengine.LeaderInfo {
	return raftengine.LeaderInfo{ID: "leader"}
}

func (e *readinessFenceEngine) VerifyLeader(context.Context) error {
	return nil
}

func (e *readinessFenceEngine) LinearizableRead(context.Context) (uint64, error) {
	if e.onLinearizableRead != nil {
		e.onLinearizableRead()
	}
	return 1, nil
}

func (e *readinessFenceEngine) Propose(context.Context, []byte) (*raftengine.ProposalResult, error) {
	return nil, errors.New("unexpected propose")
}

func (e *readinessFenceEngine) ProposeAdmin(context.Context, []byte) (*raftengine.ProposalResult, error) {
	return nil, errors.New("unexpected propose admin")
}

func (e *readinessFenceEngine) Status() raftengine.Status {
	return raftengine.Status{State: e.State()}
}

func (e *readinessFenceEngine) Configuration(context.Context) (raftengine.Configuration, error) {
	return raftengine.Configuration{}, nil
}

func (e *readinessFenceEngine) Close() error {
	return nil
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

func TestShardStoreCommittedVersionAtChecksStagedVisibilityKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, group := newStagedVisibilityShardStore(t)
	rawKey := []byte("k")
	require.NoError(t, group.Store.PutAt(ctx, distribution.MigrationStagedDataKey(9, rawKey), []byte("staged"), 77, 0))

	landed, err := st.CommittedVersionAt(ctx, rawKey, 77)
	require.NoError(t, err)
	require.True(t, landed)
}

func TestShardStoreCommittedVersionAtChecksTargetReadiness(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, group := newReadinessShardStore(t, distribution.RouteDescriptor{
		RouteID: 1,
		Start:   []byte("a"),
		End:     []byte("z"),
		GroupID: 1,
		State:   distribution.RouteStateActive,
	})
	applyTargetReadiness(t, group)
	require.NoError(t, group.Store.PutAt(ctx, []byte("k"), []byte("live"), 77, 0))

	landed, err := st.CommittedVersionAt(ctx, []byte("k"), 77)
	require.False(t, landed)
	require.ErrorIs(t, err, ErrRouteCutoverPending)
}

func TestShardStoreCommittedVersionAtRechecksTargetReadinessAfterFence(t *testing.T) {
	ctx := context.Background()
	st, group := newReadinessShardStore(t, distribution.RouteDescriptor{
		RouteID: 1,
		Start:   []byte("a"),
		End:     []byte("z"),
		GroupID: 1,
		State:   distribution.RouteStateActive,
	})
	group.Engine = &readinessFenceEngine{
		onLinearizableRead: func() {
			applyTargetReadiness(t, group)
		},
	}
	require.NoError(t, group.Store.PutAt(ctx, []byte("k"), []byte("live"), 77, 0))

	landed, err := st.CommittedVersionAt(ctx, []byte("k"), 77)
	require.False(t, landed)
	require.ErrorIs(t, err, ErrRouteCutoverPending)
}

func TestShardStoreTargetReadinessFailsClosedWithoutDescriptorProof(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, group := newReadinessShardStore(t, distribution.RouteDescriptor{
		RouteID: 1,
		Start:   []byte("a"),
		End:     []byte("z"),
		GroupID: 1,
		State:   distribution.RouteStateActive,
	})
	applyTargetReadiness(t, group)

	_, err := st.GetAt(ctx, []byte("k"), 120)
	require.ErrorIs(t, err, ErrRouteCutoverPending)

	err = st.PutAt(ctx, []byte("k"), []byte("v"), 120, 0)
	require.ErrorIs(t, err, ErrRouteCutoverPending)
}

func TestShardStoreTargetReadinessNormalizesInternalKeyRange(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, group := newReadinessShardStore(t, distribution.RouteDescriptor{
		RouteID: 1,
		Start:   []byte("a"),
		End:     []byte("z"),
		GroupID: 1,
		State:   distribution.RouteStateActive,
	})
	applyTargetReadiness(t, group)

	itemKey := store.ListItemKey([]byte("b"), 0)
	require.NoError(t, group.Store.PutAt(ctx, itemKey, []byte("item"), 120, 0))

	_, err := st.GetAt(ctx, itemKey, 120)
	require.ErrorIs(t, err, ErrRouteCutoverPending)
}

func TestShardStoreTargetReadinessAcceptsStagedDescriptor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, group := newStagedVisibilityShardStore(t)
	applyTargetReadiness(t, group)
	rawKey := []byte("k")
	require.NoError(t, group.Store.PutAt(ctx, distribution.MigrationStagedDataKey(9, rawKey), []byte("staged"), 120, 0))

	got, err := st.GetAt(ctx, rawKey, 130)
	require.NoError(t, err)
	require.Equal(t, []byte("staged"), got)
}

func TestShardStoreTargetReadinessAcceptsClearedDescriptorAndRetainsFloor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, group := newReadinessShardStore(t, distribution.RouteDescriptor{
		RouteID:             1,
		Start:               []byte("a"),
		End:                 []byte("z"),
		GroupID:             1,
		State:               distribution.RouteStateActive,
		MinWriteTSExclusive: 100,
	})
	applyTargetReadiness(t, group)
	require.NoError(t, group.Store.PutAt(ctx, []byte("k"), []byte("live"), 120, 0))

	got, err := st.GetAt(ctx, []byte("k"), 130)
	require.NoError(t, err)
	require.Equal(t, []byte("live"), got)

	err = st.PutAt(ctx, []byte("k"), []byte("low"), 100, 0)
	require.ErrorIs(t, err, ErrRouteWriteBelowFloor)

	err = st.ExpireAt(ctx, []byte("k"), 200, 100)
	require.ErrorIs(t, err, ErrRouteWriteBelowFloor)

	err = st.ApplyMutations(ctx, []*store.KVPairMutation{{
		Op:    store.OpTypePut,
		Key:   []byte("n"),
		Value: []byte("low"),
	}}, nil, 0, 100)
	require.ErrorIs(t, err, ErrRouteWriteBelowFloor)

	err = st.PutAt(ctx, []byte("k"), []byte("ok"), 101, 0)
	require.NoError(t, err)

	err = st.ApplyMutations(ctx, []*store.KVPairMutation{{
		Op:    store.OpTypePut,
		Key:   []byte("n"),
		Value: []byte("ok"),
	}}, nil, 0, 101)
	require.NoError(t, err)
}

func TestShardStoreTargetReadinessRejectsClearedDescriptorBeforeCutoverVersion(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{{
			RouteID:             1,
			Start:               []byte("a"),
			End:                 []byte("z"),
			GroupID:             1,
			State:               distribution.RouteStateActive,
			MinWriteTSExclusive: 100,
		}},
	}))
	group := &ShardGroup{Store: store.NewMVCCStore()}
	st := NewShardStore(engine, map[uint64]*ShardGroup{1: group})
	applyTargetReadiness(t, group)
	require.NoError(t, group.Store.PutAt(ctx, []byte("k"), []byte("live"), 120, 0))

	_, err := st.GetAt(ctx, []byte("k"), 130)
	require.ErrorIs(t, err, ErrRouteCutoverPending)
}

func TestShardStoreTargetReadinessUsesSingleCatalogSnapshotProof(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{{
			RouteID:             1,
			Start:               []byte("a"),
			End:                 []byte("z"),
			GroupID:             1,
			State:               distribution.RouteStateActive,
			MinWriteTSExclusive: 100,
		}},
	}))
	staleRoute, ok := engine.GetRoute([]byte("b"))
	require.True(t, ok)
	group := &ShardGroup{Store: store.NewMVCCStore()}
	shards := NewShardStore(engine, map[uint64]*ShardGroup{1: group})
	applyTargetReadiness(t, group)
	require.NoError(t, group.Store.PutAt(ctx, []byte("b"), []byte("live-old"), 80, 0))
	require.NoError(t, group.Store.PutAt(ctx, distribution.MigrationStagedDataKey(9, []byte("b")), []byte("staged-new"), 120, 0))

	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 2,
		Routes: []distribution.RouteDescriptor{{
			RouteID:                1,
			Start:                  []byte("a"),
			End:                    []byte("z"),
			GroupID:                1,
			State:                  distribution.RouteStateActive,
			StagedVisibilityActive: true,
			MigrationJobID:         9,
			MinWriteTSExclusive:    100,
		}},
	}))

	got, err := shards.localGetAt(ctx, group, staleRoute, []byte("b"), 130)
	require.NoError(t, err)
	require.Equal(t, []byte("staged-new"), got)
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

func TestShardStoreDeletePrefixAtUsesReadinessProofRouteFloor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{{
			RouteID: 1,
			Start:   []byte("a"),
			End:     []byte("z"),
			GroupID: 1,
			State:   distribution.RouteStateActive,
		}},
	}))
	group := &ShardGroup{Store: store.NewMVCCStore()}
	shards := NewShardStore(engine, map[uint64]*ShardGroup{1: group})
	applyTargetReadiness(t, group)
	require.NoError(t, group.Store.PutAt(ctx, []byte("b"), []byte("live"), 80, 0))

	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 2,
		Routes: []distribution.RouteDescriptor{{
			RouteID:                1,
			Start:                  []byte("a"),
			End:                    []byte("z"),
			GroupID:                1,
			State:                  distribution.RouteStateActive,
			StagedVisibilityActive: true,
			MigrationJobID:         9,
			MinWriteTSExclusive:    100,
		}},
	}))

	err := shards.DeletePrefixAt(ctx, []byte("b"), nil, 100)
	require.ErrorIs(t, err, ErrRouteWriteBelowFloor)

	got, getErr := group.Store.GetAt(ctx, []byte("b"), ^uint64(0))
	require.NoError(t, getErr)
	require.Equal(t, []byte("live"), got)
}

func TestShardStoreDeletePrefixAtUsesReadinessProofRouteForStagedCleanup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{{
			RouteID:             1,
			Start:               []byte("a"),
			End:                 []byte("z"),
			GroupID:             1,
			State:               distribution.RouteStateActive,
			MinWriteTSExclusive: 100,
		}},
	}))
	group := &ShardGroup{Store: store.NewMVCCStore()}
	shards := NewShardStore(engine, map[uint64]*ShardGroup{1: group})
	applyTargetReadiness(t, group)
	stagedKey := distribution.MigrationStagedDataKey(9, []byte("b"))
	require.NoError(t, group.Store.PutAt(ctx, stagedKey, []byte("staged"), 120, 0))

	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 2,
		Routes: []distribution.RouteDescriptor{{
			RouteID:                1,
			Start:                  []byte("a"),
			End:                    []byte("z"),
			GroupID:                1,
			State:                  distribution.RouteStateActive,
			StagedVisibilityActive: true,
			MigrationJobID:         9,
			MinWriteTSExclusive:    100,
		}},
	}))

	require.NoError(t, shards.DeletePrefixAt(ctx, []byte("b"), nil, 130))

	_, err := group.Store.GetAt(ctx, stagedKey, 140)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	_, err = shards.GetAt(ctx, []byte("b"), 140)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestShardStoreExplicitGroupReadUsesRouteProofForReadiness(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, group := newReadinessShardStore(t, distribution.RouteDescriptor{
		RouteID:             1,
		Start:               []byte("a"),
		End:                 []byte("z"),
		GroupID:             42,
		State:               distribution.RouteStateActive,
		MinWriteTSExclusive: 100,
	})
	applyTargetReadiness(t, group)
	require.NoError(t, group.Store.PutAt(ctx, []byte("b"), []byte("live"), 120, 0))
	require.NoError(t, group.Store.PutAt(ctx, []byte("c"), []byte("scan"), 121, 0))

	got, err := st.GetGroupAt(ctx, 42, []byte("b"), 130)
	require.NoError(t, err)
	require.Equal(t, []byte("live"), got)

	kvs, err := st.ScanGroupAt(ctx, 42, []byte("a"), []byte("z"), 10, 130)
	require.NoError(t, err)
	require.Len(t, kvs, 2)
}

func TestShardStoreScanGroupAtChecksEveryCoveredRoute(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 2,
		Routes: []distribution.RouteDescriptor{
			{
				RouteID:             1,
				Start:               []byte("a"),
				End:                 []byte("m"),
				GroupID:             42,
				State:               distribution.RouteStateActive,
				MinWriteTSExclusive: 100,
			},
			{
				RouteID: 2,
				Start:   []byte("m"),
				End:     []byte("z"),
				GroupID: 42,
				State:   distribution.RouteStateActive,
			},
		},
	}))
	group := &ShardGroup{Store: store.NewMVCCStore()}
	st := NewShardStore(engine, map[uint64]*ShardGroup{42: group})
	applyTargetReadinessState(t, group, store.TargetStagedReadinessState{
		JobID:                  9,
		RouteStart:             []byte("m"),
		RouteEnd:               []byte("z"),
		ExpectedCutoverVersion: 2,
		MigrationJobID:         9,
		MinWriteTSExclusive:    100,
		Armed:                  true,
	})
	require.NoError(t, group.Store.PutAt(ctx, []byte("b"), []byte("left"), 120, 0))
	require.NoError(t, group.Store.PutAt(ctx, []byte("x"), []byte("right"), 120, 0))

	_, err := st.ScanGroupAt(ctx, 42, []byte("a"), []byte("z"), 10, 130)
	require.ErrorIs(t, err, ErrRouteCutoverPending)
}

func TestShardStoreScanGroupAtClampsSingleMatchedRoute(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{{
			RouteID: 1,
			Start:   []byte("m"),
			End:     []byte("z"),
			GroupID: 42,
			State:   distribution.RouteStateActive,
		}},
	}))
	group := &ShardGroup{Store: store.NewMVCCStore()}
	st := NewShardStore(engine, map[uint64]*ShardGroup{42: group})
	require.NoError(t, group.Store.PutAt(ctx, []byte("b"), []byte("outside"), 7, 0))
	require.NoError(t, group.Store.PutAt(ctx, []byte("n"), []byte("inside"), 7, 0))

	kvs, err := st.ScanGroupAt(ctx, 42, []byte("a"), []byte("z"), 10, 7)
	require.NoError(t, err)
	require.Equal(t, []*store.KVPair{{Key: []byte("n"), Value: []byte("inside")}}, kvs)
}

func TestShardStoreScanGroupAtSplitsCoveredRoutesForStagedVisibility(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 2,
		Routes: []distribution.RouteDescriptor{
			{
				RouteID: 1,
				Start:   []byte("a"),
				End:     []byte("m"),
				GroupID: 42,
				State:   distribution.RouteStateActive,
			},
			{
				RouteID:                2,
				Start:                  []byte("m"),
				End:                    []byte("z"),
				GroupID:                42,
				State:                  distribution.RouteStateActive,
				StagedVisibilityActive: true,
				MigrationJobID:         10,
			},
		},
	}))
	group := &ShardGroup{Store: store.NewMVCCStore()}
	st := NewShardStore(engine, map[uint64]*ShardGroup{42: group})
	require.NoError(t, group.Store.PutAt(ctx, []byte("b"), []byte("left-live"), 110, 0))
	require.NoError(t, group.Store.PutAt(ctx, distribution.MigrationStagedDataKey(10, []byte("x")), []byte("right-staged"), 120, 0))

	kvs, err := st.ScanGroupAt(ctx, 42, []byte("a"), []byte("z"), 10, 130)
	require.NoError(t, err)
	require.Equal(t, []*store.KVPair{
		{Key: []byte("b"), Value: []byte("left-live")},
		{Key: []byte("x"), Value: []byte("right-staged")},
	}, kvs)
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

func TestShardStorePhysicalLimitScanChecksTargetReadiness(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, group := newReadinessShardStore(t, distribution.RouteDescriptor{
		RouteID: 1,
		Start:   []byte("a"),
		End:     []byte("z"),
		GroupID: 1,
		State:   distribution.RouteStateActive,
	})
	applyTargetReadiness(t, group)

	userKey := []byte("b")
	start := store.ListItemKey(userKey, 0)
	end := store.ListItemKey(userKey, 2)
	require.NoError(t, group.Store.PutAt(ctx, start, []byte("v"), 120, 0))

	_, _, err := st.ScanAtPhysicalLimit(ctx, start, end, 10, 10, 130)
	require.ErrorIs(t, err, ErrRouteCutoverPending)
}

func TestShardStorePhysicalLimitScanRechecksTargetReadinessAfterFence(t *testing.T) {
	ctx := context.Background()
	st, group := newReadinessShardStore(t, distribution.RouteDescriptor{
		RouteID: 1,
		Start:   []byte("a"),
		End:     []byte("z"),
		GroupID: 1,
		State:   distribution.RouteStateActive,
	})
	group.Engine = &readinessFenceEngine{
		state: raftengine.StateLeader,
		onLinearizableRead: func() {
			applyTargetReadiness(t, group)
		},
	}

	userKey := []byte("b")
	start := store.ListItemKey(userKey, 0)
	end := store.ListItemKey(userKey, 2)
	require.NoError(t, group.Store.PutAt(ctx, start, []byte("v"), 120, 0))

	_, _, err := st.ScanAtPhysicalLimit(ctx, start, end, 10, 10, 130)
	require.ErrorIs(t, err, ErrRouteCutoverPending)
}

func TestShardStoreS3ManifestScanChecksTargetReadinessInRouteSpace(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	start := s3keys.ObjectManifestScanStart("bucket-a", 1, "z/")
	end := prefixScanEnd(start)
	routeStart, routeEnd, ok := s3keys.ManifestScanRouteBounds(start, end)
	require.True(t, ok)

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 2,
		Routes: []distribution.RouteDescriptor{{
			RouteID: 1,
			Start:   routeStart,
			End:     routeEnd,
			GroupID: 1,
			State:   distribution.RouteStateActive,
		}},
	}))
	group := &ShardGroup{Store: store.NewMVCCStore()}
	st := NewShardStore(engine, map[uint64]*ShardGroup{1: group})
	applyTargetReadinessState(t, group, store.TargetStagedReadinessState{
		JobID:                  9,
		RouteStart:             routeStart,
		RouteEnd:               routeEnd,
		ExpectedCutoverVersion: 2,
		MigrationJobID:         9,
		MinWriteTSExclusive:    100,
		Armed:                  true,
	})

	key := s3keys.ObjectManifestKey("bucket-a", 1, "z/object-0")
	require.NoError(t, group.Store.PutAt(ctx, key, []byte("manifest"), 120, 0))

	_, err := st.ScanAt(ctx, start, end, 10, 130)
	require.ErrorIs(t, err, ErrRouteCutoverPending)
	_, _, err = st.ScanAtPhysicalLimit(ctx, start, end, 10, 10, 130)
	require.ErrorIs(t, err, ErrRouteCutoverPending)
}

func TestShardStoreTargetReadinessSkipsNonOverlappingGuardForScannedRoute(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	start := s3keys.ObjectManifestScanStart("bucket-a", 1, "a/")
	end := s3keys.ObjectManifestScanStart("bucket-a", 1, "z/")
	routeStart, routeEnd, ok := s3keys.ManifestScanRouteBounds(start, end)
	require.True(t, ok)
	routeBoundary, _, ok := s3keys.ManifestScanRouteBounds(
		s3keys.ObjectManifestScanStart("bucket-a", 1, "m/"),
		end,
	)
	require.True(t, ok)

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 2,
		Routes: []distribution.RouteDescriptor{
			{
				RouteID:             1,
				Start:               routeStart,
				End:                 routeBoundary,
				GroupID:             1,
				State:               distribution.RouteStateActive,
				MinWriteTSExclusive: 100,
			},
			{
				RouteID: 2,
				Start:   routeBoundary,
				End:     routeEnd,
				GroupID: 1,
				State:   distribution.RouteStateActive,
			},
		},
	}))
	group := &ShardGroup{Store: store.NewMVCCStore()}
	st := NewShardStore(engine, map[uint64]*ShardGroup{1: group})
	applyTargetReadinessState(t, group, store.TargetStagedReadinessState{
		JobID:                  9,
		RouteStart:             routeStart,
		RouteEnd:               routeBoundary,
		ExpectedCutoverVersion: 2,
		MigrationJobID:         9,
		MinWriteTSExclusive:    100,
		Armed:                  true,
	})

	kvs, err := st.ScanAt(ctx, start, end, 10, 130)
	require.NoError(t, err)
	require.Empty(t, kvs)
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

func TestShardStoreExplicitGroupS3ManifestScanUsesRouteProofForReadiness(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	start := s3keys.ObjectManifestScanStart("bucket-a", 1, "z/")
	end := prefixScanEnd(start)
	routeStart, routeEnd, ok := s3keys.ManifestScanRouteBounds(start, end)
	require.True(t, ok)

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 2,
		Routes: []distribution.RouteDescriptor{{
			RouteID:             1,
			Start:               routeStart,
			End:                 routeEnd,
			GroupID:             42,
			State:               distribution.RouteStateActive,
			MinWriteTSExclusive: 100,
		}},
	}))
	group := &ShardGroup{Store: store.NewMVCCStore()}
	st := NewShardStore(engine, map[uint64]*ShardGroup{42: group})
	applyTargetReadinessState(t, group, store.TargetStagedReadinessState{
		JobID:                  9,
		RouteStart:             routeStart,
		RouteEnd:               routeEnd,
		ExpectedCutoverVersion: 2,
		MigrationJobID:         9,
		MinWriteTSExclusive:    100,
		Armed:                  true,
	})

	key := s3keys.ObjectManifestKey("bucket-a", 1, "z/object-0")
	require.NoError(t, group.Store.PutAt(ctx, key, []byte("manifest"), 120, 0))

	kvs, err := st.ScanGroupAt(ctx, 42, start, end, 10, 130)
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, key, kvs[0].Key)
}

func TestShardStoreDeletePrefixAtChecksTargetReadinessBeforeDelete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, group := newReadinessShardStore(t, distribution.RouteDescriptor{
		RouteID: 1,
		Start:   []byte("a"),
		End:     []byte("z"),
		GroupID: 1,
		State:   distribution.RouteStateActive,
	})
	applyTargetReadiness(t, group)
	require.NoError(t, group.Store.PutAt(ctx, []byte("b"), []byte("v"), 1, 0))

	err := st.DeletePrefixAt(ctx, []byte("b"), nil, 120)
	require.ErrorIs(t, err, ErrRouteCutoverPending)

	got, getErr := group.Store.GetAt(ctx, []byte("b"), ^uint64(0))
	require.NoError(t, getErr)
	require.Equal(t, []byte("v"), got)
}

func TestShardStoreDeletePrefixAtRaftChecksTargetReadinessBeforeDelete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, group := newReadinessShardStore(t, distribution.RouteDescriptor{
		RouteID: 1,
		Start:   []byte("a"),
		End:     []byte("z"),
		GroupID: 1,
		State:   distribution.RouteStateActive,
	})
	applyTargetReadiness(t, group)
	require.NoError(t, group.Store.PutAt(ctx, []byte("b"), []byte("v"), 1, 0))

	err := st.DeletePrefixAtRaft(ctx, []byte("b"), nil, 120)
	require.ErrorIs(t, err, ErrRouteCutoverPending)

	got, getErr := group.Store.GetAt(ctx, []byte("b"), ^uint64(0))
	require.NoError(t, getErr)
	require.Equal(t, []byte("v"), got)
}

func TestShardStoreDeletePrefixAtRaftAtChecksTargetReadinessBeforeDelete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, group := newReadinessShardStore(t, distribution.RouteDescriptor{
		RouteID: 1,
		Start:   []byte("a"),
		End:     []byte("z"),
		GroupID: 1,
		State:   distribution.RouteStateActive,
	})
	applyTargetReadiness(t, group)
	require.NoError(t, group.Store.PutAt(ctx, []byte("b"), []byte("v"), 1, 0))

	err := st.DeletePrefixAtRaftAt(ctx, []byte("b"), nil, 120, 10)
	require.ErrorIs(t, err, ErrRouteCutoverPending)

	got, getErr := group.Store.GetAt(ctx, []byte("b"), ^uint64(0))
	require.NoError(t, getErr)
	require.Equal(t, []byte("v"), got)
}

func TestShardStoreDeletePrefixAtFailsClosedWithoutRouteProof(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 2,
		Routes: []distribution.RouteDescriptor{{
			RouteID: 1,
			Start:   []byte("m"),
			End:     []byte("z"),
			GroupID: 1,
			State:   distribution.RouteStateActive,
		}},
	}))
	group := &ShardGroup{Store: store.NewMVCCStore()}
	st := NewShardStore(engine, map[uint64]*ShardGroup{1: group})
	require.NoError(t, group.Store.PutAt(ctx, []byte("b"), []byte("v"), 1, 0))

	err := st.DeletePrefixAt(ctx, []byte("b"), nil, 120)
	require.ErrorIs(t, err, ErrRouteCutoverPending)

	got, getErr := group.Store.GetAt(ctx, []byte("b"), ^uint64(0))
	require.NoError(t, getErr)
	require.Equal(t, []byte("v"), got)
}

func TestShardStoreDeletePrefixAtTombstonesStagedVisibilityRows(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, group := newStagedVisibilityShardStore(t)
	rawKey := []byte("b")
	stagedKey := distribution.MigrationStagedDataKey(9, rawKey)
	require.NoError(t, group.Store.PutAt(ctx, stagedKey, []byte("staged"), 120, 0))

	require.NoError(t, st.DeletePrefixAt(ctx, rawKey, nil, 130))

	_, err := st.GetAt(ctx, rawKey, 140)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	_, err = group.Store.GetAt(ctx, stagedKey, 140)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestShardStoreDeletePrefixAtRaftAtAdvancesApplyIndexOnlyOnLiveDelete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, group := newStagedVisibilityShardStore(t)
	recording := &deletePrefixIndexRecordingStore{MVCCStore: group.Store}
	group.Store = recording
	rawKey := []byte("b")
	stagedKey := distribution.MigrationStagedDataKey(9, rawKey)
	require.NoError(t, group.Store.PutAt(ctx, stagedKey, []byte("staged"), 120, 0))

	require.NoError(t, st.DeletePrefixAtRaftAt(ctx, rawKey, nil, 130, 88))

	require.Equal(t, []uint64{0, 88}, recording.deletePrefixIndexes)
	require.Equal(t, stagedKey, recording.deletePrefixPrefixes[0])
	require.Equal(t, rawKey, recording.deletePrefixPrefixes[1])
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

func TestShardStoreStagedVisibilityPreservesCompactionErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, group := newStagedVisibilityShardStore(t)
	retention, ok := group.Store.(store.RetentionController)
	require.True(t, ok)
	require.NoError(t, group.Store.PutAt(ctx, distribution.MigrationStagedDataKey(9, []byte("b")), []byte("staged"), 10, 0))
	retention.SetMinRetainedTS(20)

	_, err := st.GetAt(ctx, []byte("b"), 15)
	require.ErrorIs(t, err, store.ErrReadTSCompacted)

	_, err = st.ScanAt(ctx, []byte("a"), []byte("z"), 10, 15)
	require.ErrorIs(t, err, store.ErrReadTSCompacted)
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
			{RouteID: 1, Start: []byte("a"), End: []byte("m"), GroupID: 2, State: distribution.RouteStateActive},
			{
				RouteID:                2,
				Start:                  []byte("m"),
				End:                    []byte("z"),
				GroupID:                1,
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

func TestShardStoreRaftApplySkipsPointMigrationTimestampFloorButGuardsPrefixDeletes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, _ := newStagedVisibilityShardStore(t)

	require.NoError(t, st.ApplyMutationsRaft(ctx, []*store.KVPairMutation{
		{Op: store.OpTypePut, Key: []byte("k-raft"), Value: []byte("v")},
	}, nil, 90, 100))
	require.NoError(t, st.ApplyMutationsRaftAt(ctx, []*store.KVPairMutation{
		{Op: store.OpTypePut, Key: []byte("k-raft-at"), Value: []byte("v")},
	}, nil, 90, 100, 1))
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
		{name: "legacy", key: legacyListMetaDeltaKey(userKey, 10, 1), scanStart: store.LegacyListMetaDeltaScanPrefix(userKey), legacyRouting: true},
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
	_, err := st.GetAtWithReadFence(ctx, []byte("k"), 10, 0, 77)
	require.NoError(t, err)
	_, _, err = st.LatestCommitTSWithReadFence(ctx, []byte("k"), 78)
	require.NoError(t, err)
	_, err = st.ScanAtWithReadFence(ctx, []byte("a"), []byte("z"), 10, 11, false, 0, 79, []byte("a"), []byte("m"))
	require.NoError(t, err)

	fake.mu.Lock()
	require.Equal(t, uint64(77), fake.lastGetReq.GetReadRouteVersion())
	require.Equal(t, uint64(78), fake.lastLatestReq.GetReadRouteVersion())
	require.Equal(t, uint64(79), fake.lastScanReq.GetReadRouteVersion())
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

	_, err = st.ScanAtWithReadFence(ctx, []byte("a"), []byte("z"), 10, 11, false, 0, 80, []byte{}, []byte{})
	require.NoError(t, err)

	fake.mu.Lock()
	require.Equal(t, uint64(1), fake.lastScanReq.GetGroupId())
	require.Equal(t, uint64(80), fake.lastScanReq.GetReadRouteVersion())
	require.True(t, fake.lastScanReq.GetRouteBoundsPresent())
	fake.mu.Unlock()

	_, err = st.ScanAtWithReadFence(ctx, []byte("a"), []byte("z"), 10, 11, false, 0, 81, nil, nil)
	require.NoError(t, err)

	fake.mu.Lock()
	defer fake.mu.Unlock()
	require.Equal(t, uint64(0), fake.lastScanReq.GetGroupId())
	require.Equal(t, uint64(81), fake.lastScanReq.GetReadRouteVersion())
	require.False(t, fake.lastScanReq.GetRouteBoundsPresent())
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
			require.Len(t, routes, 1)
			require.Equal(t, uint64(2), routes[0].GroupID)
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

func TestShardStoreScanKeysRouteAtLeaderRefillsAfterTxnInternalKeys(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	g := &ShardGroup{Store: store.NewMVCCStore()}
	st := NewShardStore(distribution.NewEngine(), map[uint64]*ShardGroup{1: g})

	require.NoError(t, g.Store.PutAt(ctx, txnCommitKey([]byte("primary"), 10), []byte("commit"), 1, 0))
	require.NoError(t, g.Store.PutAt(ctx, []byte("a"), []byte("va"), 2, 0))

	keys, err := st.scanKeysRouteAtLeader(ctx, g, distribution.Route{GroupID: 1}, []byte(""), nil, 1, ^uint64(0))
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

	keys, err := st.scanKeysRouteAtLeader(ctx, g, distribution.Route{GroupID: 1}, nil, nil, 2, ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte(""), []byte("a")}, keys)
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
