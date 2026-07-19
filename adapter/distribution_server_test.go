package adapter

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDistributionServerGetRoute_HitAndMiss(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	s := NewDistributionServer(engine, nil)
	ctx := context.Background()

	hit, err := s.GetRoute(ctx, &pb.GetRouteRequest{Key: []byte("b")})
	require.NoError(t, err)
	require.Equal(t, []byte("a"), hit.Start)
	require.Equal(t, []byte("m"), hit.End)
	require.Equal(t, uint64(1), hit.RaftGroupId)

	miss, err := s.GetRoute(ctx, &pb.GetRouteRequest{Key: []byte("0")})
	require.NoError(t, err)
	require.Equal(t, uint64(0), miss.RaftGroupId)
	require.Nil(t, miss.Start)
	require.Nil(t, miss.End)
}

func TestDistributionServerGetRoute_NormalizesFilesystemChunkKeys(t *testing.T) {
	t.Parallel()

	home := uint64(11)
	inode := uint64(22)
	routeKey := fskeys.ChunkRouteKey(home, inode)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), routeKey, 1)
	engine.UpdateRoute(routeKey, nil, 2)

	s := NewDistributionServer(engine, nil)
	resp, err := s.GetRoute(context.Background(), &pb.GetRouteRequest{
		Key: fskeys.ChunkKey(home, inode, 99),
	})
	require.NoError(t, err)
	require.Equal(t, routeKey, resp.Start)
	require.Equal(t, uint64(2), resp.RaftGroupId)
}

func TestDistributionServerGetTimestamp_IsMonotonic(t *testing.T) {
	t.Parallel()

	s := NewDistributionServer(distribution.NewEngine(), nil)
	ctx := context.Background()

	first, err := s.GetTimestamp(ctx, &pb.GetTimestampRequest{})
	require.NoError(t, err)

	second, err := s.GetTimestamp(ctx, &pb.GetTimestampRequest{})
	require.NoError(t, err)

	require.Greater(t, second.Timestamp, first.Timestamp)
}

func TestNewDistributionServer_DefaultCatalogReloadRetryPolicy(t *testing.T) {
	t.Parallel()

	s := NewDistributionServer(distribution.NewEngine(), distribution.NewCatalogStore(store.NewMVCCStore()))
	require.Equal(t, defaultCatalogReloadRetryAttempts, s.reloadRetry.attempts)
	require.Equal(t, defaultCatalogReloadRetryInterval, s.reloadRetry.interval)
}

func TestWithCatalogReloadRetryPolicy_OverridesDefaults(t *testing.T) {
	t.Parallel()

	s := NewDistributionServer(
		distribution.NewEngine(),
		distribution.NewCatalogStore(store.NewMVCCStore()),
		WithCatalogReloadRetryPolicy(3, 5*time.Millisecond),
	)
	require.Equal(t, 3, s.reloadRetry.attempts)
	require.Equal(t, 5*time.Millisecond, s.reloadRetry.interval)
}

func TestDistributionServerListRoutes_ReadsDurableCatalog(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	catalog := distribution.NewCatalogStore(store.NewMVCCStore(), distribution.WithCatalogRouteDescriptorV2Writes(true))
	saved, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{
		{
			RouteID:                2,
			Start:                  []byte("m"),
			End:                    nil,
			GroupID:                2,
			State:                  distribution.RouteStateWriteFenced,
			ParentRouteID:          1,
			StagedVisibilityActive: true,
			MigrationJobID:         42,
			MinWriteTSExclusive:    99,
			SplitAtHLC:             100,
		},
		{
			RouteID:       1,
			Start:         []byte(""),
			End:           []byte("m"),
			GroupID:       1,
			State:         distribution.RouteStateActive,
			ParentRouteID: 0,
		},
	})
	require.NoError(t, err)

	s := NewDistributionServer(distribution.NewEngine(), catalog)
	resp, err := s.ListRoutes(ctx, &pb.ListRoutesRequest{})
	require.NoError(t, err)

	require.Equal(t, saved.Version, resp.CatalogVersion)
	require.Len(t, resp.Routes, 2)
	require.Equal(t, uint64(1), resp.Routes[0].RouteId)
	require.Equal(t, []byte(""), resp.Routes[0].Start)
	require.Equal(t, []byte("m"), resp.Routes[0].End)
	require.Equal(t, uint64(1), resp.Routes[0].RaftGroupId)
	require.Equal(t, pb.RouteState_ROUTE_STATE_ACTIVE, resp.Routes[0].State)
	require.Equal(t, uint64(2), resp.Routes[1].RouteId)
	require.Nil(t, resp.Routes[1].End)
	require.Equal(t, pb.RouteState_ROUTE_STATE_WRITE_FENCED, resp.Routes[1].State)
	require.True(t, resp.Routes[1].StagedVisibilityActive)
	require.Equal(t, uint64(42), resp.Routes[1].MigrationJobId)
	require.Equal(t, uint64(99), resp.Routes[1].MinWriteTsExclusive)
	require.Equal(t, uint64(100), resp.Routes[1].SplitAtHlc)
}

func TestDistributionServerListRoutes_RequiresCatalog(t *testing.T) {
	t.Parallel()

	s := NewDistributionServer(distribution.NewEngine(), nil)
	_, err := s.ListRoutes(context.Background(), &pb.ListRoutesRequest{})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.ErrorContains(t, err, errDistributionCatalogNotConfigured.Error())
}

func TestDistributionServerSplitRange_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore, distribution.WithCatalogRouteDescriptorV2Writes(true))
	saved, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{
		{
			RouteID:             1,
			Start:               []byte(""),
			End:                 []byte("m"),
			GroupID:             1,
			State:               distribution.RouteStateActive,
			ParentRouteID:       0,
			MinWriteTSExclusive: 99,
		},
		{
			RouteID:       2,
			Start:         []byte("m"),
			End:           nil,
			GroupID:       2,
			State:         distribution.RouteStateActive,
			ParentRouteID: 0,
		},
	})
	require.NoError(t, err)

	engine := distribution.NewEngine()
	s := NewDistributionServer(
		engine,
		catalog,
		WithDistributionCoordinator(newDistributionCoordinatorStub(baseStore, true)),
	)

	resp, err := s.SplitRange(ctx, &pb.SplitRangeRequest{
		ExpectedCatalogVersion: saved.Version,
		RouteId:                1,
		SplitKey:               []byte("g"),
	})
	require.NoError(t, err)

	require.Equal(t, uint64(2), resp.CatalogVersion)
	require.Equal(t, uint64(3), resp.Left.RouteId)
	require.Equal(t, []byte(""), resp.Left.Start)
	require.Equal(t, []byte("g"), resp.Left.End)
	require.Equal(t, uint64(1), resp.Left.RaftGroupId)
	require.Equal(t, uint64(1), resp.Left.ParentRouteId)
	require.Equal(t, uint64(99), resp.Left.MinWriteTsExclusive)
	require.Equal(t, uint64(4), resp.Right.RouteId)
	require.Equal(t, []byte("g"), resp.Right.Start)
	require.Equal(t, []byte("m"), resp.Right.End)
	require.Equal(t, uint64(1), resp.Right.RaftGroupId)
	require.Equal(t, uint64(1), resp.Right.ParentRouteId)
	require.Equal(t, uint64(99), resp.Right.MinWriteTsExclusive)
	require.NotZero(t, resp.Left.SplitAtHlc)
	require.Equal(t, resp.Left.SplitAtHlc, resp.Right.SplitAtHlc)

	snapshot, err := catalog.Snapshot(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), snapshot.Version)
	require.Len(t, snapshot.Routes, 3)
	// Catalog snapshots are sorted by range start key.
	require.Equal(t, uint64(3), snapshot.Routes[0].RouteID)
	require.Equal(t, uint64(99), snapshot.Routes[0].MinWriteTSExclusive)
	require.Equal(t, uint64(4), snapshot.Routes[1].RouteID)
	require.Equal(t, uint64(99), snapshot.Routes[1].MinWriteTSExclusive)
	require.Equal(t, uint64(2), snapshot.Routes[2].RouteID)
	require.NotZero(t, snapshot.Routes[0].SplitAtHLC)
	require.Equal(t, snapshot.Routes[0].SplitAtHLC, snapshot.Routes[1].SplitAtHLC)
	require.Equal(t, snapshot.Routes[0].SplitAtHLC, resp.Left.SplitAtHlc)

	require.Equal(t, uint64(2), engine.Version())
	leftRoute, ok := engine.GetRoute([]byte("b"))
	require.True(t, ok)
	require.Equal(t, uint64(3), leftRoute.RouteID)
	require.Equal(t, uint64(99), leftRoute.MinWriteTSExclusive)
	rightRoute, ok := engine.GetRoute([]byte("h"))
	require.True(t, ok)
	require.Equal(t, uint64(4), rightRoute.RouteID)
	require.Equal(t, uint64(99), rightRoute.MinWriteTSExclusive)
}

func TestDistributionServerSplitRange_SnapsFilesystemChunkKeyToFileBoundary(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore)
	saved, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{
		{
			RouteID: 1,
			Start:   []byte("!fs|route|chk|"),
			End:     nil,
			GroupID: 1,
			State:   distribution.RouteStateActive,
		},
	})
	require.NoError(t, err)

	s := NewDistributionServer(
		distribution.NewEngine(),
		catalog,
		WithDistributionCoordinator(newDistributionCoordinatorStub(baseStore, true)),
	)
	wantBoundary := fskeys.ChunkRouteKey(11, 22)

	resp, err := s.SplitRange(ctx, &pb.SplitRangeRequest{
		ExpectedCatalogVersion: saved.Version,
		RouteId:                1,
		SplitKey:               fskeys.ChunkKey(11, 22, 99),
	})
	require.NoError(t, err)
	require.Equal(t, wantBoundary, resp.Left.End)
	require.Equal(t, wantBoundary, resp.Right.Start)
}

func TestDistributionServerSplitRange_SnapsTxnWrappedFilesystemChunkKeyToFileBoundary(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore)
	saved, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{
		{
			RouteID: 1,
			Start:   []byte("!fs|route|chk|"),
			End:     nil,
			GroupID: 1,
			State:   distribution.RouteStateActive,
		},
	})
	require.NoError(t, err)

	s := NewDistributionServer(
		distribution.NewEngine(),
		catalog,
		WithDistributionCoordinator(newDistributionCoordinatorStub(baseStore, true)),
	)
	wantBoundary := fskeys.ChunkRouteKey(11, 22)
	txnChunkKey := append([]byte(kv.TxnKeyPrefix+"lock|"), fskeys.ChunkKey(11, 22, 99)...)

	resp, err := s.SplitRange(ctx, &pb.SplitRangeRequest{
		ExpectedCatalogVersion: saved.Version,
		RouteId:                1,
		SplitKey:               txnChunkKey,
	})
	require.NoError(t, err)
	require.Equal(t, wantBoundary, resp.Left.End)
	require.Equal(t, wantBoundary, resp.Right.Start)
}

func TestDistributionServerSplitRange_RejectsFilesystemPinnedHotspotBoundary(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore)
	routeStart := fskeys.ChunkRouteKey(11, 22)
	saved, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{
		{
			RouteID: 1,
			Start:   routeStart,
			End:     fskeys.ChunkRouteKey(11, 23),
			GroupID: 1,
			State:   distribution.RouteStateActive,
		},
	})
	require.NoError(t, err)

	observer := &recordingDistributionFilesystemObserver{}
	s := NewDistributionServer(
		distribution.NewEngine(),
		catalog,
		WithDistributionCoordinator(newDistributionCoordinatorStub(baseStore, true)),
		WithDistributionFilesystemObserver(observer),
	)
	insideSameFile := append(append([]byte(nil), routeStart...), 0x01)

	_, err = s.SplitRange(ctx, &pb.SplitRangeRequest{
		ExpectedCatalogVersion: saved.Version,
		RouteId:                1,
		SplitKey:               insideSameFile,
	})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.ErrorContains(t, err, errDistributionSplitKeyAtBoundary.Error())
	require.Equal(t, []string{DistributionFilePinnedHotspotSplitBoundary}, observer.reasons)
}

func TestDistributionServerSplitRange_DoesNotRecordNonFilesystemNormalizedBoundary(t *testing.T) {
	t.Parallel()

	routeStart := []byte("queue")
	tests := []struct {
		name     string
		splitKey []byte
	}{
		{
			name:     "list item",
			splitKey: store.ListItemKey(routeStart, 1),
		},
		{
			name:     "txn wrapped list item",
			splitKey: append([]byte(kv.TxnKeyPrefix+"lock|"), store.ListItemKey(routeStart, 1)...),
		},
		{
			name:     "txn wrapped redis internal key",
			splitKey: []byte(kv.TxnKeyPrefix + "lock|!redis|string|queue"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			baseStore := store.NewMVCCStore()
			catalog := distribution.NewCatalogStore(baseStore)
			saved, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{
				{
					RouteID: 1,
					Start:   routeStart,
					End:     []byte("queuez"),
					GroupID: 1,
					State:   distribution.RouteStateActive,
				},
			})
			require.NoError(t, err)

			observer := &recordingDistributionFilesystemObserver{}
			s := NewDistributionServer(
				distribution.NewEngine(),
				catalog,
				WithDistributionCoordinator(newDistributionCoordinatorStub(baseStore, true)),
				WithDistributionFilesystemObserver(observer),
			)

			_, err = s.SplitRange(ctx, &pb.SplitRangeRequest{
				ExpectedCatalogVersion: saved.Version,
				RouteId:                1,
				SplitKey:               tt.splitKey,
			})
			require.Error(t, err)
			require.Equal(t, codes.InvalidArgument, status.Code(err))
			require.ErrorContains(t, err, errDistributionSplitKeyAtBoundary.Error())
			require.Empty(t, observer.reasons)
		})
	}
}

func TestDistributionServerSplitRange_RequiresCoordinator(t *testing.T) {
	t.Parallel()

	s, version := seededDistributionServerWithoutCoordinator(t)
	_, err := s.SplitRange(context.Background(), &pb.SplitRangeRequest{
		ExpectedCatalogVersion: version,
		RouteId:                1,
		SplitKey:               []byte("g"),
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.ErrorContains(t, err, errDistributionCoordinatorRequired.Error())
}

func TestDistributionServerSplitRange_UnknownRoute(t *testing.T) {
	t.Parallel()

	s, version := seededDistributionServer(t)
	_, err := s.SplitRange(context.Background(), &pb.SplitRangeRequest{
		ExpectedCatalogVersion: version,
		RouteId:                999,
		SplitKey:               []byte("g"),
	})
	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err))
	require.ErrorContains(t, err, errDistributionUnknownRoute.Error())
}

func TestDistributionServerSplitRange_InvalidSplitKey(t *testing.T) {
	t.Parallel()

	s, version := seededDistributionServer(t)
	_, err := s.SplitRange(context.Background(), &pb.SplitRangeRequest{
		ExpectedCatalogVersion: version,
		RouteId:                1,
		SplitKey:               []byte("z"),
	})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.ErrorContains(t, err, errDistributionInvalidSplitKey.Error())
}

func TestDistributionServerSplitRange_SplitKeyAtBoundary(t *testing.T) {
	t.Parallel()

	s, version := seededDistributionServer(t)
	_, err := s.SplitRange(context.Background(), &pb.SplitRangeRequest{
		ExpectedCatalogVersion: version,
		RouteId:                1,
		SplitKey:               []byte("a"),
	})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.ErrorContains(t, err, errDistributionSplitKeyAtBoundary.Error())
}

func TestDistributionServerSplitRange_VersionConflict(t *testing.T) {
	t.Parallel()

	s, version := seededDistributionServer(t)
	_, err := s.SplitRange(context.Background(), &pb.SplitRangeRequest{
		ExpectedCatalogVersion: version - 1,
		RouteId:                1,
		SplitKey:               []byte("g"),
	})
	require.Error(t, err)
	require.Equal(t, codes.Aborted, status.Code(err))
	require.ErrorContains(t, err, errDistributionCatalogConflict.Error())
}

func TestDistributionServerSplitRange_UsesCoordinatorForCatalogWrites(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore)
	saved, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{
		{
			RouteID:       1,
			Start:         []byte(""),
			End:           []byte("m"),
			GroupID:       1,
			State:         distribution.RouteStateActive,
			ParentRouteID: 0,
		},
		{
			RouteID:       2,
			Start:         []byte("m"),
			End:           nil,
			GroupID:       2,
			State:         distribution.RouteStateActive,
			ParentRouteID: 0,
		},
	})
	require.NoError(t, err)

	engine := distribution.NewEngine()
	coordinator := newDistributionCoordinatorStub(baseStore, true)
	s := NewDistributionServer(engine, catalog, WithDistributionCoordinator(coordinator))
	readSnapshot, err := catalog.Snapshot(ctx)
	require.NoError(t, err)

	resp, err := s.SplitRange(ctx, &pb.SplitRangeRequest{
		ExpectedCatalogVersion: saved.Version,
		RouteId:                1,
		SplitKey:               []byte("g"),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), resp.CatalogVersion)
	require.Equal(t, 1, coordinator.dispatchCalls)
	require.Equal(t, readSnapshot.ReadTS, coordinator.lastStartTS)
	require.Zero(t, coordinator.lastRequestedCommitTS)
	require.NotZero(t, coordinator.lastCommitTS)
	require.Greater(t, coordinator.lastCommitTS, coordinator.lastStartTS)

	snapshot, err := catalog.Snapshot(ctx)
	require.NoError(t, err)
	left, found := findRouteByID(snapshot.Routes, resp.Left.RouteId)
	require.True(t, found)
	right, found := findRouteByID(snapshot.Routes, resp.Right.RouteId)
	require.True(t, found)
	require.Equal(t, coordinator.lastCommitTS, left.SplitAtHLC)
	require.Equal(t, coordinator.lastCommitTS, right.SplitAtHLC)
	require.Equal(t, coordinator.lastCommitTS, resp.Left.SplitAtHlc)
	require.Equal(t, coordinator.lastCommitTS, resp.Right.SplitAtHlc)
}

func TestDistributionServerSplitRange_UsesPersistentNextRouteID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore)
	saved, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{
		{
			RouteID:       1,
			Start:         []byte(""),
			End:           []byte("m"),
			GroupID:       1,
			State:         distribution.RouteStateActive,
			ParentRouteID: 0,
		},
		{
			RouteID:       2,
			Start:         []byte("m"),
			End:           nil,
			GroupID:       2,
			State:         distribution.RouteStateActive,
			ParentRouteID: 0,
		},
	})
	require.NoError(t, err)

	engine := distribution.NewEngine()
	coordinator := newDistributionCoordinatorStub(baseStore, true)
	s := NewDistributionServer(engine, catalog, WithDistributionCoordinator(coordinator))

	first, err := s.SplitRange(ctx, &pb.SplitRangeRequest{
		ExpectedCatalogVersion: saved.Version,
		RouteId:                1,
		SplitKey:               []byte("g"),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(3), first.Left.RouteId)
	require.Equal(t, uint64(4), first.Right.RouteId)

	nextRouteID, err := catalog.NextRouteID(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(5), nextRouteID)

	afterDelete, err := catalog.Save(ctx, first.CatalogVersion, []distribution.RouteDescriptor{
		{
			RouteID:       3,
			Start:         []byte(""),
			End:           []byte("g"),
			GroupID:       1,
			State:         distribution.RouteStateActive,
			ParentRouteID: 1,
		},
		{
			RouteID:       2,
			Start:         []byte("m"),
			End:           nil,
			GroupID:       2,
			State:         distribution.RouteStateActive,
			ParentRouteID: 0,
		},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(3), afterDelete.Version)

	second, err := s.SplitRange(ctx, &pb.SplitRangeRequest{
		ExpectedCatalogVersion: afterDelete.Version,
		RouteId:                3,
		SplitKey:               []byte("c"),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(5), second.Left.RouteId)
	require.Equal(t, uint64(6), second.Right.RouteId)
}

func TestDistributionServerSplitRange_ReturnsExactCommittedSplitVersion(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore)
	saved, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{
		{
			RouteID:       1,
			Start:         []byte(""),
			End:           []byte("m"),
			GroupID:       1,
			State:         distribution.RouteStateActive,
			ParentRouteID: 0,
		},
		{
			RouteID:       2,
			Start:         []byte("m"),
			End:           nil,
			GroupID:       2,
			State:         distribution.RouteStateActive,
			ParentRouteID: 0,
		},
	})
	require.NoError(t, err)

	engine := distribution.NewEngine()
	coordinator := newDistributionCoordinatorStub(baseStore, true)
	coordinator.afterDispatch = func(ctx context.Context, st store.MVCCStore, commitTS uint64) error {
		return st.PutAt(ctx, distribution.CatalogVersionKey(), distribution.EncodeCatalogVersion(3), commitTS+1, 0)
	}
	s := NewDistributionServer(engine, catalog, WithDistributionCoordinator(coordinator))

	resp, err := s.SplitRange(ctx, &pb.SplitRangeRequest{
		ExpectedCatalogVersion: saved.Version,
		RouteId:                1,
		SplitKey:               []byte("g"),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), resp.CatalogVersion)
	require.Equal(t, uint64(2), engine.Version())
	require.Equal(t, uint64(3), resp.Left.RouteId)
	require.Equal(t, uint64(4), resp.Right.RouteId)
	require.Equal(t, coordinator.lastCommitTS, resp.Left.SplitAtHlc)
	require.Equal(t, coordinator.lastCommitTS, resp.Right.SplitAtHlc)

	committed, err := catalog.SnapshotAt(ctx, coordinator.lastCommitTS)
	require.NoError(t, err)
	require.Equal(t, uint64(2), committed.Version)

	latest, err := catalog.Snapshot(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(3), latest.Version)
}

func TestDistributionServerSplitRange_RetriesCatalogReloadUntilVisible(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore)
	saved, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{
		{
			RouteID:       1,
			Start:         []byte(""),
			End:           []byte("m"),
			GroupID:       1,
			State:         distribution.RouteStateActive,
			ParentRouteID: 0,
		},
		{
			RouteID:       2,
			Start:         []byte("m"),
			End:           nil,
			GroupID:       2,
			State:         distribution.RouteStateActive,
			ParentRouteID: 0,
		},
	})
	require.NoError(t, err)

	engine := distribution.NewEngine()
	coordinator := newDistributionCoordinatorStub(baseStore, true)
	coordinator.asyncApplyDelay = 15 * time.Millisecond
	coordinator.asyncApplyDone = make(chan error, 1)
	s := NewDistributionServer(engine, catalog, WithDistributionCoordinator(coordinator))

	resp, err := s.SplitRange(ctx, &pb.SplitRangeRequest{
		ExpectedCatalogVersion: saved.Version,
		RouteId:                1,
		SplitKey:               []byte("g"),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), resp.CatalogVersion)

	select {
	case applyErr := <-coordinator.asyncApplyDone:
		require.NoError(t, applyErr)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for async split apply")
	}
}

func TestDistributionServerSplitRange_RequiresCatalogLeaderWhenCoordinatorConfigured(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore)
	saved, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{
		{
			RouteID:       1,
			Start:         []byte(""),
			End:           nil,
			GroupID:       1,
			State:         distribution.RouteStateActive,
			ParentRouteID: 0,
		},
	})
	require.NoError(t, err)

	s := NewDistributionServer(
		distribution.NewEngine(),
		catalog,
		WithDistributionCoordinator(newDistributionCoordinatorStub(baseStore, false)),
	)
	_, err = s.SplitRange(ctx, &pb.SplitRangeRequest{
		ExpectedCatalogVersion: saved.Version,
		RouteId:                1,
		SplitKey:               []byte("g"),
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.ErrorContains(t, err, errDistributionNotLeader.Error())
}

func TestBuildCatalogSplitOps_UsesSurgicalSplitMutations(t *testing.T) {
	t.Parallel()

	left := distribution.RouteDescriptor{
		RouteID:       3,
		Start:         []byte(""),
		End:           []byte("g"),
		GroupID:       1,
		State:         distribution.RouteStateActive,
		ParentRouteID: 1,
	}
	right := distribution.RouteDescriptor{
		RouteID:       4,
		Start:         []byte("g"),
		End:           []byte("m"),
		GroupID:       1,
		State:         distribution.RouteStateActive,
		ParentRouteID: 1,
	}

	ops, err := buildCatalogSplitOps(1, left, right, 2, 5, false)
	require.NoError(t, err)
	require.Len(t, ops, 5)
	require.Equal(t, kv.Del, ops[0].Op)
	require.Equal(t, distribution.CatalogRouteKey(1), ops[0].Key)
	require.Equal(t, kv.Put, ops[1].Op)
	require.Equal(t, distribution.CatalogRouteKey(3), ops[1].Key)
	require.Equal(t, kv.Put, ops[2].Op)
	require.Equal(t, distribution.CatalogRouteKey(4), ops[2].Key)
	require.Equal(t, kv.Put, ops[3].Op)
	require.Equal(t, distribution.CatalogVersionKey(), ops[3].Key)
	require.Equal(t, kv.Put, ops[4].Op)
	require.Equal(t, distribution.CatalogNextRouteIDKey(), ops[4].Key)
	nextRouteID, err := distribution.DecodeCatalogNextRouteID(ops[4].Value)
	require.NoError(t, err)
	require.Equal(t, uint64(5), nextRouteID)
}

func seededDistributionServer(t *testing.T) (*DistributionServer, uint64) {
	t.Helper()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore)
	saved, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{
		{
			RouteID:       1,
			Start:         []byte("a"),
			End:           []byte("m"),
			GroupID:       7,
			State:         distribution.RouteStateActive,
			ParentRouteID: 0,
		},
	})
	require.NoError(t, err)

	return NewDistributionServer(
		distribution.NewEngine(),
		catalog,
		WithDistributionCoordinator(newDistributionCoordinatorStub(baseStore, true)),
	), saved.Version
}

func seededDistributionServerWithoutCoordinator(t *testing.T) (*DistributionServer, uint64) {
	t.Helper()

	ctx := context.Background()
	catalog := distribution.NewCatalogStore(store.NewMVCCStore())
	saved, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{
		{
			RouteID:       1,
			Start:         []byte("a"),
			End:           []byte("m"),
			GroupID:       7,
			State:         distribution.RouteStateActive,
			ParentRouteID: 0,
		},
	})
	require.NoError(t, err)

	return NewDistributionServer(distribution.NewEngine(), catalog), saved.Version
}

type distributionCoordinatorStub struct {
	store                 store.MVCCStore
	leader                bool
	clock                 *kv.HLC
	nextTS                uint64
	lastStartTS           uint64
	lastCommitTS          uint64
	lastRequestedCommitTS uint64
	afterDispatch         func(context.Context, store.MVCCStore, uint64) error
	asyncApplyDone        chan error
	asyncApplyDelay       time.Duration
	dispatchCalls         int
}

func newDistributionCoordinatorStub(st store.MVCCStore, leader bool) *distributionCoordinatorStub {
	return &distributionCoordinatorStub{
		store:  st,
		leader: leader,
		clock:  kv.NewHLC(),
	}
}

func (s *distributionCoordinatorStub) Dispatch(ctx context.Context, reqs *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	if err := s.validateDispatch(reqs); err != nil {
		return nil, err
	}
	s.dispatchCalls++
	s.lastRequestedCommitTS = reqs.CommitTS
	startTS, commitTS := s.nextTimestamps(reqs.StartTS, reqs.CommitTS)
	s.lastStartTS = startTS
	s.lastCommitTS = commitTS

	if err := kv.ValidateElemCommitTSPatches(reqs.Elems, commitTS); err != nil {
		return nil, err
	}
	mutations, err := coordinatorStubMutations(reqs.Elems, commitTS)
	if err != nil {
		return nil, err
	}
	if s.asyncApplyDelay > 0 {
		done := s.asyncApplyDone
		delay := s.asyncApplyDelay
		go func() {
			time.Sleep(delay)
			err := s.applyDispatch(ctx, mutations, startTS, commitTS)
			if done != nil {
				done <- err
			}
		}()
		return &kv.CoordinateResponse{CommitIndex: commitTS, CommitTS: commitTS}, nil
	}
	if err := s.applyDispatch(ctx, mutations, startTS, commitTS); err != nil {
		return nil, err
	}
	return &kv.CoordinateResponse{CommitIndex: commitTS, CommitTS: commitTS}, nil
}

func (s *distributionCoordinatorStub) validateDispatch(reqs *kv.OperationGroup[kv.OP]) error {
	if !s.leader {
		return kv.ErrLeaderNotFound
	}
	if reqs == nil || len(reqs.Elems) == 0 || !reqs.IsTxn {
		return kv.ErrInvalidRequest
	}
	return nil
}

func (s *distributionCoordinatorStub) nextTimestamps(startTS uint64, requestedCommitTS uint64) (uint64, uint64) {
	if requestedCommitTS != 0 {
		if s.clock != nil {
			s.clock.Observe(requestedCommitTS)
		}
		return startTS, requestedCommitTS
	}
	if s.nextTS == 0 {
		s.nextTS = s.store.LastCommitTS() + 1
	}
	commitTS := s.nextTS
	if startTS > 0 && commitTS <= startTS {
		commitTS = startTS + 1
	}
	s.nextTS = commitTS + 1
	return startTS, commitTS
}

func (s *distributionCoordinatorStub) applyDispatch(
	ctx context.Context,
	mutations []*store.KVPairMutation,
	startTS uint64,
	commitTS uint64,
) error {
	if err := s.store.ApplyMutations(ctx, mutations, nil, startTS, commitTS); err != nil {
		return err
	}
	if s.afterDispatch != nil {
		if err := s.afterDispatch(ctx, s.store, commitTS); err != nil {
			return err
		}
	}
	return nil
}

func coordinatorStubMutations(elems []*kv.Elem[kv.OP], commitTS uint64) ([]*store.KVPairMutation, error) {
	mutations := make([]*store.KVPairMutation, 0, len(elems))
	for _, elem := range elems {
		mutation, err := coordinatorStubMutation(elem, commitTS)
		if err != nil {
			return nil, err
		}
		mutations = append(mutations, mutation)
	}
	return mutations, nil
}

func coordinatorStubMutation(elem *kv.Elem[kv.OP], commitTS uint64) (*store.KVPairMutation, error) {
	if elem == nil {
		return nil, kv.ErrInvalidRequest
	}
	switch elem.Op {
	case kv.Put:
		value := distribution.CloneBytes(elem.Value)
		if elem.CommitTSValueOffset != 0 {
			binary.BigEndian.PutUint64(value[elem.CommitTSValueOffset:elem.CommitTSValueOffset+8], commitTS)
		}
		return &store.KVPairMutation{
			Op:    store.OpTypePut,
			Key:   distribution.CloneBytes(elem.Key),
			Value: value,
		}, nil
	case kv.Del:
		return &store.KVPairMutation{
			Op:  store.OpTypeDelete,
			Key: distribution.CloneBytes(elem.Key),
		}, nil
	case kv.DelPrefix:
		return nil, kv.ErrInvalidRequest
	default:
		return nil, kv.ErrInvalidRequest
	}
}

func (s *distributionCoordinatorStub) IsLeader() bool {
	return s.leader
}

func (s *distributionCoordinatorStub) VerifyLeader(context.Context) error {
	if !s.leader {
		return kv.ErrLeaderNotFound
	}
	return nil
}

func (s *distributionCoordinatorStub) RaftLeader() string {
	return ""
}

func (s *distributionCoordinatorStub) IsLeaderForKey(_ []byte) bool {
	return s.leader
}

func (s *distributionCoordinatorStub) VerifyLeaderForKey(_ context.Context, _ []byte) error {
	if !s.leader {
		return kv.ErrLeaderNotFound
	}
	return nil
}

func (s *distributionCoordinatorStub) RaftLeaderForKey(_ []byte) string {
	return ""
}

func (s *distributionCoordinatorStub) Clock() *kv.HLC {
	return s.clock
}

func (s *distributionCoordinatorStub) LinearizableRead(_ context.Context) (uint64, error) {
	return 0, nil
}

func (s *distributionCoordinatorStub) LeaseRead(ctx context.Context) (uint64, error) {
	return s.LinearizableRead(ctx)
}

func (s *distributionCoordinatorStub) LeaseReadForKey(ctx context.Context, _ []byte) (uint64, error) {
	return s.LinearizableRead(ctx)
}

type recordingDistributionFilesystemObserver struct {
	reasons []string
}

func (o *recordingDistributionFilesystemObserver) ObserveFilePinnedHotspot(reason string) {
	o.reasons = append(o.reasons, reason)
}
