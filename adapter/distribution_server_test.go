package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
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

func TestDistributionServerListRoutes_ReadsDurableCatalog(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	catalog := distribution.NewCatalogStore(store.NewMVCCStore())
	saved, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{
		{
			RouteID:       2,
			Start:         []byte("m"),
			End:           nil,
			GroupID:       2,
			State:         distribution.RouteStateWriteFenced,
			ParentRouteID: 1,
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
	catalog := distribution.NewCatalogStore(store.NewMVCCStore())
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
	s := NewDistributionServer(engine, catalog)

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
	require.Equal(t, uint64(4), resp.Right.RouteId)
	require.Equal(t, []byte("g"), resp.Right.Start)
	require.Equal(t, []byte("m"), resp.Right.End)
	require.Equal(t, uint64(1), resp.Right.RaftGroupId)
	require.Equal(t, uint64(1), resp.Right.ParentRouteId)

	snapshot, err := catalog.Snapshot(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), snapshot.Version)
	require.Len(t, snapshot.Routes, 3)
	require.Equal(t, uint64(2), snapshot.Routes[0].RouteID)
	require.Equal(t, uint64(3), snapshot.Routes[1].RouteID)
	require.Equal(t, uint64(4), snapshot.Routes[2].RouteID)

	require.Equal(t, uint64(2), engine.Version())
	leftRoute, ok := engine.GetRoute([]byte("b"))
	require.True(t, ok)
	require.Equal(t, uint64(3), leftRoute.RouteID)
	rightRoute, ok := engine.GetRoute([]byte("h"))
	require.True(t, ok)
	require.Equal(t, uint64(4), rightRoute.RouteID)
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

func seededDistributionServer(t *testing.T) (*DistributionServer, uint64) {
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
