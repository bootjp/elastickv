package kv

import (
	"context"
	"errors"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/s3keys"
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

func TestShardStoreApplyMutationsRaftAtEnforcesRouteFloor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, _ := newReadinessShardStore(t, distribution.RouteDescriptor{
		RouteID:             1,
		Start:               []byte("a"),
		End:                 []byte("z"),
		GroupID:             1,
		State:               distribution.RouteStateActive,
		MinWriteTSExclusive: 100,
	})

	err := st.ApplyMutationsRaftAt(ctx, []*store.KVPairMutation{{
		Op:    store.OpTypePut,
		Key:   []byte("b"),
		Value: []byte("low"),
	}}, nil, 0, 100, 7)
	require.ErrorIs(t, err, ErrRouteWriteBelowFloor)
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

func TestShardStoreDeletePrefixAtRaftEnforcesRouteFloorBeforeDelete(t *testing.T) {
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
	require.NoError(t, group.Store.PutAt(ctx, []byte("b"), []byte("v"), 1, 0))

	err := st.DeletePrefixAtRaft(ctx, []byte("b"), nil, 100)
	require.ErrorIs(t, err, ErrRouteWriteBelowFloor)

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
