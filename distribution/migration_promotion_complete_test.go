package distribution

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestCompleteTargetPromotionStateClearsStagedFieldsAndRetainsFloor(t *testing.T) {
	t.Parallel()

	job := promotionCompleteTestJob()
	routes := promotionCompleteTestRoutes()

	result, err := CompleteTargetPromotionState(job, routes, 1000)
	require.NoError(t, err)
	require.True(t, result.Changed)
	require.Equal(t, []uint64{3}, result.ClearedRouteIDs)
	require.True(t, result.Job.TargetPromotionDone)
	require.Zero(t, result.Job.PromotionCompletedTS)
	require.Equal(t, int64(1000), result.Job.UpdatedAtMs)
	require.Zero(t, result.Job.TerminalAtMs)
	require.Equal(t, SplitJobPhaseCleanup, result.Job.Phase)

	target := routeByID(t, result.Routes, 3)
	require.False(t, target.StagedVisibilityActive)
	require.Equal(t, uint64(0), target.MigrationJobID)
	require.Equal(t, uint64(777), target.MinWriteTSExclusive)
	require.Equal(t, uint64(42), target.ParentRouteID)

	badRoutes := promotionCompleteTestRoutes()
	badRoutes[1].ParentRouteID = 777
	_, err = CompleteTargetPromotionState(job, badRoutes, 1000)
	require.ErrorIs(t, err, ErrMigrationInvalidRoute)
}

func TestCompleteTargetPromotionStateAcceptsAlreadyClearedDescriptor(t *testing.T) {
	t.Parallel()

	job := promotionCompleteTestJob()
	job.TargetPromotionDone = true
	job.PromotionCompletedTS = 900
	job.UpdatedAtMs = 1000
	routes := promotionCompleteTestRoutes()
	routes[1].StagedVisibilityActive = false
	routes[1].MigrationJobID = 0

	result, err := CompleteTargetPromotionState(job, routes, 1100)
	require.NoError(t, err)
	require.False(t, result.Changed)
	require.Equal(t, SplitJobPhaseCleanup, result.Job.Phase)
	require.Equal(t, uint64(900), result.Job.PromotionCompletedTS)
	require.Equal(t, int64(1000), result.Job.UpdatedAtMs)
	require.Zero(t, result.Job.TerminalAtMs)

	target := routeByID(t, result.Routes, 3)
	require.False(t, target.StagedVisibilityActive)
	require.Equal(t, uint64(0), target.MigrationJobID)
	require.Equal(t, uint64(777), target.MinWriteTSExclusive)
}

func TestCatalogStoreCompleteSplitJobTargetPromotionCommitsRouteAndJobTogether(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cs := NewCatalogStore(store.NewMVCCStore(), WithCatalogRouteDescriptorV2Writes(true))
	saved, err := cs.Save(ctx, 0, promotionCompleteTestRoutes())
	require.NoError(t, err)
	job := promotionCompleteTestJob()
	require.NoError(t, cs.CreateSplitJob(ctx, job))
	before, err := cs.Snapshot(ctx)
	require.NoError(t, err)

	snapshot, completed, err := cs.CompleteSplitJobTargetPromotion(ctx, saved.Version, job, 1000)
	require.NoError(t, err)
	require.Equal(t, saved.Version+1, snapshot.Version)
	require.True(t, completed.TargetPromotionDone)
	require.Equal(t, SplitJobPhaseCleanup, completed.Phase)
	require.Greater(t, completed.PromotionCompletedTS, before.ReadTS)
	require.Equal(t, int64(1000), completed.UpdatedAtMs)

	loadedJob, found, err := cs.SplitJob(ctx, job.JobID)
	require.NoError(t, err)
	require.True(t, found)
	assertSplitJobEqual(t, completed, loadedJob)

	loaded, err := cs.Snapshot(ctx)
	require.NoError(t, err)
	require.Equal(t, saved.Version+1, loaded.Version)
	target := routeByID(t, loaded.Routes, 3)
	require.False(t, target.StagedVisibilityActive)
	require.Equal(t, uint64(0), target.MigrationJobID)
	require.Equal(t, uint64(777), target.MinWriteTSExclusive)
}

func TestCatalogStoreCompleteSplitJobTargetPromotionRetryAfterCommit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cs := NewCatalogStore(store.NewMVCCStore(), WithCatalogRouteDescriptorV2Writes(true))
	saved, err := cs.Save(ctx, 0, promotionCompleteTestRoutes())
	require.NoError(t, err)
	job := promotionCompleteTestJob()
	require.NoError(t, cs.CreateSplitJob(ctx, job))

	firstSnapshot, firstCompleted, err := cs.CompleteSplitJobTargetPromotion(ctx, saved.Version, job, 1000)
	require.NoError(t, err)

	retrySnapshot, retryCompleted, err := cs.CompleteSplitJobTargetPromotion(ctx, saved.Version, job, 2000)
	require.NoError(t, err)
	require.Equal(t, firstSnapshot.Version, retrySnapshot.Version)
	assertSplitJobEqual(t, firstCompleted, retryCompleted)

	loaded, err := cs.Snapshot(ctx)
	require.NoError(t, err)
	require.Equal(t, firstSnapshot.Version, loaded.Version)
}

func TestCatalogStoreCompleteSplitJobTargetPromotionIsIdempotentAfterClearedDescriptor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cs := NewCatalogStore(store.NewMVCCStore(), WithCatalogRouteDescriptorV2Writes(true))
	routes := promotionCompleteTestRoutes()
	routes[1].StagedVisibilityActive = false
	routes[1].MigrationJobID = 0
	saved, err := cs.Save(ctx, 0, routes)
	require.NoError(t, err)
	job := promotionCompleteTestJob()
	job.TargetPromotionDone = true
	job.Phase = SplitJobPhaseCleanup
	job.PromotionCompletedTS = 900
	job.UpdatedAtMs = 1000
	require.NoError(t, cs.CreateSplitJob(ctx, job))

	snapshot, completed, err := cs.CompleteSplitJobTargetPromotion(ctx, saved.Version, job, 1100)
	require.NoError(t, err)
	require.Equal(t, saved.Version, snapshot.Version)
	require.Equal(t, SplitJobPhaseCleanup, completed.Phase)
	require.Equal(t, uint64(900), completed.PromotionCompletedTS)
	require.Equal(t, int64(1000), completed.UpdatedAtMs)
	require.Zero(t, completed.TerminalAtMs)

	loaded, err := cs.Snapshot(ctx)
	require.NoError(t, err)
	require.Equal(t, saved.Version, loaded.Version)
}

func TestCompleteTargetPromotionStateBackfillsMissingTerminalTime(t *testing.T) {
	t.Parallel()

	job := promotionCompleteTestJob()
	job.TargetPromotionDone = true
	job.Phase = SplitJobPhaseDone
	routes := promotionCompleteTestRoutes()
	routes[1].StagedVisibilityActive = false
	routes[1].MigrationJobID = 0

	result, err := CompleteTargetPromotionState(job, routes, 1200)
	require.NoError(t, err)
	require.True(t, result.Changed)
	require.Equal(t, int64(1200), result.Job.TerminalAtMs)
	require.Equal(t, int64(1200), result.Job.UpdatedAtMs)
}

func TestCatalogStoreCompleteSplitJobTargetPromotionRejectsStaleInputs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cs := NewCatalogStore(store.NewMVCCStore(), WithCatalogRouteDescriptorV2Writes(true))
	saved, err := cs.Save(ctx, 0, promotionCompleteTestRoutes())
	require.NoError(t, err)
	job := promotionCompleteTestJob()
	require.NoError(t, cs.CreateSplitJob(ctx, job))

	_, _, err = cs.CompleteSplitJobTargetPromotion(ctx, saved.Version+1, job, 1000)
	require.True(t, errors.Is(err, ErrCatalogVersionMismatch), "got %v", err)

	advanced := job
	advanced.Cursor = []byte("advanced")
	advanced.UpdatedAtMs++
	require.NoError(t, cs.SaveSplitJob(ctx, job, advanced))

	_, _, err = cs.CompleteSplitJobTargetPromotion(ctx, saved.Version, job, 1000)
	require.True(t, errors.Is(err, ErrCatalogSplitJobConflict), "got %v", err)
}

func TestCatalogStoreCompleteSplitJobTargetPromotionPreservesVersionConflict(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cs := NewCatalogStore(store.NewMVCCStore(), WithCatalogRouteDescriptorV2Writes(true))
	saved, err := cs.Save(ctx, 0, promotionCompleteTestRoutes())
	require.NoError(t, err)
	job := promotionCompleteTestJob()
	require.NoError(t, cs.CreateSplitJob(ctx, job))

	readTS, _, routes, _, _, err := cs.loadPromotionCompleteInputs(ctx, saved.Version, job)
	require.NoError(t, err)
	completion, err := CompleteTargetPromotionState(job, routes, 1000)
	require.NoError(t, err)
	plan, mutations, commitTS, err := cs.buildPromotionCompleteMutations(ctx, readTS, saved.Version, job.JobID, &completion)
	require.NoError(t, err)

	_, err = cs.Save(ctx, saved.Version, promotionCompleteTestRoutes())
	require.NoError(t, err)

	err = cs.applyPromotionCompleteMutations(ctx, plan, mutations, job.JobID, commitTS)
	require.True(t, errors.Is(err, ErrCatalogVersionMismatch), "got %v", err)
}

func promotionCompleteTestJob() SplitJob {
	return SplitJob{
		JobID:         99,
		SourceRouteID: 42,
		SplitKey:      []byte("m"),
		TargetGroupID: 2,
		Phase:         SplitJobPhaseCleanup,
	}
}

func promotionCompleteTestRoutes() []RouteDescriptor {
	return []RouteDescriptor{
		{
			RouteID:       2,
			Start:         []byte(""),
			End:           []byte("m"),
			GroupID:       1,
			State:         RouteStateActive,
			ParentRouteID: 42,
		},
		{
			RouteID:                3,
			Start:                  []byte("m"),
			End:                    nil,
			GroupID:                2,
			State:                  RouteStateActive,
			ParentRouteID:          42,
			StagedVisibilityActive: true,
			MigrationJobID:         99,
			MinWriteTSExclusive:    777,
		},
	}
}

func routeByID(t *testing.T, routes []RouteDescriptor, routeID uint64) RouteDescriptor {
	t.Helper()
	for _, route := range routes {
		if route.RouteID == routeID {
			return route
		}
	}
	t.Fatalf("route %d not found", routeID)
	return RouteDescriptor{}
}
