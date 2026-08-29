package adapter

import (
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/stretchr/testify/require"
)

func splitJobForSourceGroupTest(t *testing.T) distribution.SplitJob {
	t.Helper()

	parent := distribution.RouteDescriptor{
		RouteID: 1,
		Start:   []byte("a"),
		End:     []byte("z"),
		GroupID: 7,
		State:   distribution.RouteStateActive,
	}
	job, err := distribution.InitializeSplitJobPlan(distribution.SplitJob{
		JobID:         1,
		SourceRouteID: parent.RouteID,
		SplitKey:      []byte("m"),
		TargetGroupID: 9,
	}, parent, 1000)
	require.NoError(t, err)
	return job
}

// FENCE replaces the source route with a left child that keeps the source group
// and a right child that moves. SplitRange then permits another split wholly
// inside the left child, because that range is disjoint from the moving one --
// and it replaces the left child with grandchildren that name the left child as
// their parent. From then on no route ends at the split key under the original
// parent, and the parent itself is gone, so the group that still holds the
// source data cannot be read back out of the route shape at all. Cleanup would
// fail with ErrMigrationSourceRouteChanged on every attempt after cutover and
// keep the job live with its guards and retention pin held.
func TestSplitJobSourceRouteStateSurvivesDisjointSiblingSplit(t *testing.T) {
	t.Parallel()

	job := splitJobForSourceGroupTest(t)
	require.Equal(t, uint64(7), job.SourceGroupID, "the plan records the source group")

	routes := []distribution.RouteDescriptor{
		// Grandchildren of the left child: their parent is route 2, not route 1.
		{RouteID: 4, ParentRouteID: 2, Start: []byte("a"), End: []byte("f"), GroupID: 7},
		{RouteID: 5, ParentRouteID: 2, Start: []byte("f"), End: []byte("m"), GroupID: 7},
		// The moved child still names the original parent.
		{RouteID: 3, ParentRouteID: 1, Start: []byte("m"), End: []byte("z"), GroupID: 9},
	}

	groupID, routeEnd, ok := splitJobSourceRouteState(routes, job)
	require.True(t, ok)
	require.Equal(t, uint64(7), groupID)
	require.Equal(t, []byte("z"), routeEnd, "the moved range still bounds the cleanup")
}

// The live route shape stays authoritative while it is intact, so a job written
// before the source group was recorded resolves exactly as it did before.
func TestSplitJobSourceRouteStateStillReadsTheRouteShape(t *testing.T) {
	t.Parallel()

	job := splitJobForSourceGroupTest(t)
	job.SourceGroupID = 0

	routes := []distribution.RouteDescriptor{
		{RouteID: 2, ParentRouteID: 1, Start: []byte("a"), End: []byte("m"), GroupID: 7},
		{RouteID: 3, ParentRouteID: 1, Start: []byte("m"), End: []byte("z"), GroupID: 9},
	}

	groupID, routeEnd, ok := splitJobSourceRouteState(routes, job)
	require.True(t, ok)
	require.Equal(t, uint64(7), groupID)
	require.Equal(t, []byte("z"), routeEnd)
}

// Pre-fence the parent is still present and answers for both.
func TestSplitJobSourceRouteStateFallsBackToTheParent(t *testing.T) {
	t.Parallel()

	job := splitJobForSourceGroupTest(t)
	job.SourceGroupID = 0

	routes := []distribution.RouteDescriptor{
		{RouteID: 1, Start: []byte("a"), End: []byte("z"), GroupID: 7},
	}

	groupID, routeEnd, ok := splitJobSourceRouteState(routes, job)
	require.True(t, ok)
	require.Equal(t, uint64(7), groupID)
	require.Equal(t, []byte("z"), routeEnd)
}

// With neither a recorded group nor a route to read it from, the caller must
// still be told the source route changed rather than handed group 0.
func TestSplitJobSourceRouteStateReportsAnUnresolvableSource(t *testing.T) {
	t.Parallel()

	job := splitJobForSourceGroupTest(t)
	job.SourceGroupID = 0

	routes := []distribution.RouteDescriptor{
		{RouteID: 4, ParentRouteID: 2, Start: []byte("a"), End: []byte("f"), GroupID: 7},
		{RouteID: 5, ParentRouteID: 2, Start: []byte("f"), End: []byte("m"), GroupID: 7},
		{RouteID: 3, ParentRouteID: 1, Start: []byte("m"), End: []byte("z"), GroupID: 9},
	}

	_, _, ok := splitJobSourceRouteState(routes, job)
	require.False(t, ok)
}
