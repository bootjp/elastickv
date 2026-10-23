package adapter

import (
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/stretchr/testify/require"
)

func TestMigrationResplitMoveBackReacquiresHistoricalNonOwner(t *testing.T) {
	r := newResplitAcceptanceRuntime(t)
	first := r.migrate(t, 1, 2, "m")
	second := r.migrate(t, r.ownership(t, 2)[0].RouteID, 3, "t")
	require.NoError(t, r.server.RunMigrationOwnershipRefreshOnce(t.Context()))
	third := r.migrate(t, r.ownership(t, 3)[0].RouteID, 1, "x")
	require.NoError(t, r.server.RunMigrationOwnershipRefreshOnce(t.Context()))
	require.GreaterOrEqual(t, third.FenceTS, second.FenceTS)
	r.read(t, "n", "original-n")
	r.read(t, "y", "original-y")
	require.ErrorIs(t, r.put(t, 1, "y", "too-old", third.FenceTS), kv.ErrRouteWriteTimestampTooLow)
	require.NoError(t, r.put(t, 1, "y", "returned-owner", third.FenceTS+100))
	r.read(t, "y", "returned-owner")
	require.NoError(t, r.put(t, 2, "n", "retained-left", third.FenceTS+101))
	r.read(t, "n", "retained-left")
	require.ErrorIs(t, r.put(t, 2, "n", "too-old", first.FenceTS), kv.ErrRouteWriteTimestampTooLow)
	require.Error(t, r.put(t, 3, "y", "retired-owner", third.FenceTS+102))
}

func TestMigrationResplitMoveBackDoesNotRequireBackgroundGeometryRefresh(t *testing.T) {
	r := newResplitAcceptanceRuntime(t)
	r.migrate(t, 1, 2, "m")
	r.migrate(t, r.ownership(t, 2)[0].RouteID, 3, "t")
	returned := r.migrate(t, r.ownership(t, 3)[0].RouteID, 1, "x")
	require.NoError(t, r.put(t, 1, "y", "returned-without-background", returned.FenceTS+100))
	r.read(t, "y", "returned-without-background")
}
