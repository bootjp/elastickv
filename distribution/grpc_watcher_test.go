package distribution

import (
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
)

func TestRouteDescriptorFromProtoPreservesMigrationFields(t *testing.T) {
	t.Parallel()

	route, err := routeDescriptorFromProto(&pb.RouteDescriptor{
		RouteId:                7,
		Start:                  []byte("a"),
		End:                    []byte("z"),
		RaftGroupId:            3,
		State:                  pb.RouteState_ROUTE_STATE_MIGRATING_TARGET,
		ParentRouteId:          6,
		SplitAtHlc:             101,
		StagedVisibilityActive: true,
		MigrationJobId:         42,
		MinWriteTsExclusive:    99,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(7), route.RouteID)
	require.Equal(t, []byte("a"), route.Start)
	require.Equal(t, []byte("z"), route.End)
	require.Equal(t, uint64(3), route.GroupID)
	require.Equal(t, RouteStateMigratingTarget, route.State)
	require.Equal(t, uint64(6), route.ParentRouteID)
	require.Equal(t, uint64(101), route.SplitAtHLC)
	require.True(t, route.StagedVisibilityActive)
	require.Equal(t, uint64(42), route.MigrationJobID)
	require.Equal(t, uint64(99), route.MinWriteTSExclusive)
}
