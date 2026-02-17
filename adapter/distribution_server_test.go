package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
)

func TestDistributionServerGetRoute_HitAndMiss(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	s := NewDistributionServer(engine)
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

	s := NewDistributionServer(distribution.NewEngine())
	ctx := context.Background()

	first, err := s.GetTimestamp(ctx, &pb.GetTimestampRequest{})
	require.NoError(t, err)

	second, err := s.GetTimestamp(ctx, &pb.GetTimestampRequest{})
	require.NoError(t, err)

	require.Greater(t, second.Timestamp, first.Timestamp)
}
