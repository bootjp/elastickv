package adapter

import (
	"context"
	"math"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestSplitJobForgedCleanupCannotDispatchPrivilegedRPC(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(st, distribution.WithCatalogRouteDescriptorV2Writes(true))
	_, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{{RouteID: 1, Start: []byte("a"), End: []byte("z"), GroupID: 7}})
	require.NoError(t, err)
	job := splitJobForSourceGroupTest(t)
	job.Phase = distribution.SplitJobPhaseCleanup
	job.FenceTS = math.MaxUint64
	job.CutoverVersion = 1
	job.TargetPromotionDone = true
	job.TargetClearedDescriptorAckCursor = distribution.CloneBytes(splitCleanupDoneCursor)
	job.SourceCutoverAckCursor = distribution.CloneBytes(splitCleanupDoneCursor)
	job.SourceReadDrainCursor = distribution.CloneBytes(splitCleanupDoneCursor)
	require.NoError(t, catalog.CreateSplitJob(ctx, job))
	server := NewDistributionServer(distribution.NewEngine(), catalog,
		WithDistributionCoordinator(newDistributionCoordinatorStub(st, true)),
		WithSplitPromotionClientFactory(func(context.Context, distribution.SplitJob) (SplitPromotionClient, error) {
			t.Fatal("forged job must not dispatch promotion")
			return nil, nil
		}),
		WithSplitMigrationClientFactory(func(context.Context, distribution.SplitJob, uint64) (SplitMigrationClient, SplitMigrationClient, error) {
			t.Fatal("forged catalog cursors must not authorize privileged RPC dispatch")
			return nil, nil, nil
		}))
	err = server.RunSplitJobRunnerOnce(ctx)
	require.ErrorContains(t, err, "cutover witness")
}

func TestSplitJobCleanupRequiresAuthoritativeCutoverDescriptor(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"staged target", "promoted target", "source still live", "wrong owner", "wrong floor", "future version", "unpromoted descriptor", "different job"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			st := store.NewMVCCStore()
			catalog := distribution.NewCatalogStore(st, distribution.WithCatalogRouteDescriptorV2Writes(true))
			job, routes := splitCleanupWitnessFixture(name)
			_, err := catalog.Save(ctx, 0, routes)
			require.NoError(t, err)
			server := NewDistributionServer(distribution.NewEngine(), catalog)
			err = server.verifySplitJobCleanupCutover(ctx, job)
			if name == "staged target" || name == "promoted target" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func splitCleanupWitnessFixture(name string) (distribution.SplitJob, []distribution.RouteDescriptor) {
	job := promotionCompleteDistributionJob()
	routes := promotionCompleteDistributionRoutes()
	switch name {
	case "staged target":
	case "promoted target":
		job.TargetPromotionDone = true
		routes[1].StagedVisibilityActive = false
		routes[1].MigrationJobID = 0
	case "source still live":
		routes[0].RouteID = job.SourceRouteID
	case "wrong owner":
		routes[1].GroupID = job.SourceGroupID
	case "wrong floor":
		routes[1].MinWriteTSExclusive++
	case "future version":
		job.CutoverVersion++
	case "unpromoted descriptor":
		job.TargetPromotionDone = true
	case "different job":
		routes[1].MigrationJobID++
	}
	return job, routes
}
