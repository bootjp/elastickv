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

// TestSplitJobSourceGroupBackfillRecordsTheGroupWhileTheShapeStillAnswers pins
// the repair for a job persisted before source_group_id existed.
//
// InitializeSplitJobPlan records the field for every job it creates, but it runs
// only at plan time: a legacy job decodes as zero and is never re-planned, so
// nothing populated it. It is needed in exactly the state where the route shape
// stops answering, so it has to be written while the shape still does.
func TestSplitJobSourceGroupBackfillRecordsTheGroupWhileTheShapeStillAnswers(t *testing.T) {
	t.Parallel()

	legacy := splitJobForSourceGroupTest(t)
	legacy.SourceGroupID = 0
	legacy.Phase = distribution.SplitJobPhaseBackfill

	intact := []distribution.RouteDescriptor{
		{RouteID: 2, ParentRouteID: 1, Start: []byte("a"), End: []byte("m"), GroupID: 7},
		{RouteID: 3, ParentRouteID: 1, Start: []byte("m"), End: []byte("z"), GroupID: 9},
	}

	groupID, needed := splitJobSourceGroupBackfill(intact, legacy)
	require.True(t, needed, "a legacy job must be repaired while the shape answers")
	require.Equal(t, uint64(7), groupID)

	// The degraded shape from TestSplitJobSourceRouteStateReportsAnUnresolvableSource,
	// which that test pins as unresolvable for a zero-field job.
	degraded := []distribution.RouteDescriptor{
		{RouteID: 4, ParentRouteID: 2, Start: []byte("a"), End: []byte("f"), GroupID: 7},
		{RouteID: 5, ParentRouteID: 2, Start: []byte("f"), End: []byte("m"), GroupID: 7},
		{RouteID: 3, ParentRouteID: 1, Start: []byte("m"), End: []byte("z"), GroupID: 9},
	}
	_, _, ok := splitJobSourceRouteState(degraded, legacy)
	require.False(t, ok, "precondition: the zero-field job cannot resolve the degraded shape")

	// Once the backfill has been recorded, the same degraded shape resolves, so
	// CLEANUP no longer fails with ErrMigrationSourceRouteChanged forever while
	// holding the job's guards and retention pin.
	repaired := legacy
	repaired.SourceGroupID = groupID
	resolvedGroup, routeEnd, ok := splitJobSourceRouteState(degraded, repaired)
	require.True(t, ok)
	require.Equal(t, uint64(7), resolvedGroup)
	require.Equal(t, []byte("z"), routeEnd, "the moved range still bounds the cleanup")
}

// TestSplitJobSourceGroupBackfillSkipsWhatItMustNotWrite covers every case that
// must not produce a catalog write.
func TestSplitJobSourceGroupBackfillSkipsWhatItMustNotWrite(t *testing.T) {
	t.Parallel()

	intact := []distribution.RouteDescriptor{
		{RouteID: 2, ParentRouteID: 1, Start: []byte("a"), End: []byte("m"), GroupID: 7},
		{RouteID: 3, ParentRouteID: 1, Start: []byte("m"), End: []byte("z"), GroupID: 9},
	}
	degraded := []distribution.RouteDescriptor{
		{RouteID: 4, ParentRouteID: 2, Start: []byte("a"), End: []byte("f"), GroupID: 7},
		{RouteID: 5, ParentRouteID: 2, Start: []byte("f"), End: []byte("m"), GroupID: 7},
		{RouteID: 3, ParentRouteID: 1, Start: []byte("m"), End: []byte("z"), GroupID: 9},
	}

	t.Run("the field is already recorded", func(t *testing.T) {
		t.Parallel()
		job := splitJobForSourceGroupTest(t)
		job.Phase = distribution.SplitJobPhaseCleanup
		require.Equal(t, uint64(7), job.SourceGroupID)

		_, needed := splitJobSourceGroupBackfill(intact, job)
		require.False(t, needed)
	})

	for _, phase := range []distribution.SplitJobPhase{
		distribution.SplitJobPhaseNone,
		distribution.SplitJobPhaseDone,
		distribution.SplitJobPhaseFailed,
		distribution.SplitJobPhaseAbandoned,
	} {
		t.Run("terminal phase never reads the field", func(t *testing.T) {
			t.Parallel()
			job := splitJobForSourceGroupTest(t)
			job.SourceGroupID = 0
			job.Phase = phase

			_, needed := splitJobSourceGroupBackfill(intact, job)
			require.False(t, needed, "phase %v", phase)
		})
	}

	t.Run("the shape cannot answer either", func(t *testing.T) {
		t.Parallel()
		job := splitJobForSourceGroupTest(t)
		job.SourceGroupID = 0
		job.Phase = distribution.SplitJobPhaseCleanup

		// Nothing to record, and the phase must surface the real error rather
		// than a backfill failure.
		_, needed := splitJobSourceGroupBackfill(degraded, job)
		require.False(t, needed)
	})
}
