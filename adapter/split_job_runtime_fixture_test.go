package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// Keep the transport recording used by runner assertions, but execute every
// migration mutation against a real store/FSM with an applied configuration.
type splitRuntimeClient struct {
	*ownershipRuntimeClient
}

func (c *splitRuntimeClient) ApplyTargetStagedReadiness(ctx context.Context, req *pb.TargetStagedReadinessRequest, opts ...grpc.CallOption) (*pb.TargetStagedReadinessResponse, error) {
	c.events = append(c.events, "control")
	return c.ownershipRuntimeClient.ApplyTargetStagedReadiness(ctx, req, opts...)
}

func (c *splitRuntimeClient) ExportRangeVersions(ctx context.Context, req *pb.ExportRangeVersionsRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[pb.ExportRangeVersionsResponse], error) {
	c.exports = append(c.exports, req)
	capture := &captureExportRangeVersionsStream{ctx: ctx}
	if err := c.internal.ExportRangeVersions(req, capture); err != nil {
		return nil, err
	}
	return &splitMigrationStreamStub{responses: capture.responses}, nil
}

func (c *splitRuntimeClient) ImportRangeVersions(ctx context.Context, req *pb.ImportRangeVersionsRequest, _ ...grpc.CallOption) (*pb.ImportRangeVersionsResponse, error) {
	c.imports = append(c.imports, req)
	return c.internal.ImportRangeVersions(ctx, req)
}

func (c *splitRuntimeClient) PromoteStagedVersions(ctx context.Context, req *pb.PromoteStagedVersionsRequest, _ ...grpc.CallOption) (*pb.PromoteStagedVersionsResponse, error) {
	c.calls++
	c.requests = append(c.requests, req)
	return c.internal.PromoteStagedVersions(ctx, req)
}

func newSplitRuntimeClient(t *testing.T, engine *distribution.Engine, st store.MVCCStore, groupID uint64) *splitRuntimeClient {
	t.Helper()
	client, _ := newOwnershipRuntimeClient(t, st, groupID)
	client.internal.routeEngine = engine
	client.internal.migrationImportGate = func(context.Context) error { return nil }
	client.internal.migrationPromoteGate = func(context.Context) error { return nil }
	return &splitRuntimeClient{ownershipRuntimeClient: client}
}

func splitRuntimeOptions(source, target *splitRuntimeClient, sourceGroupID uint64) []DistributionServerOption {
	return []DistributionServerOption{
		WithSplitMigrationClientFactory(func(context.Context, distribution.SplitJob, uint64) (SplitMigrationClient, SplitMigrationClient, error) {
			return source, target, nil
		}),
		WithSplitMigrationGroupClientFactory(func(_ context.Context, groupID uint64) (SplitMigrationClient, error) {
			if groupID == sourceGroupID {
				return source, nil
			}
			return target, nil
		}),
		WithSplitMigrationVoterFactory(func(_ context.Context, groupID uint64) ([]SplitMigrationVoter, error) {
			var client SplitMigrationClient = target
			if groupID == sourceGroupID {
				client = source
			}
			return []SplitMigrationVoter{{ID: "voter", Address: "voter:1", Client: client}}, nil
		}),
		WithSplitPromotionClientFactory(func(context.Context, distribution.SplitJob) (SplitPromotionClient, error) { return target, nil }),
	}
}

func newSplitRuntimeRunner(t *testing.T, root store.MVCCStore, catalog *distribution.CatalogStore, job distribution.SplitJob) (*DistributionServer, *splitRuntimeClient, *splitRuntimeClient) {
	t.Helper()
	engine := distribution.NewEngine()
	snapshot, err := catalog.Snapshot(t.Context())
	require.NoError(t, err)
	require.NoError(t, engine.ApplySnapshot(snapshot))
	sourceStore, targetStore := store.NewMVCCStore(), store.NewMVCCStore()
	require.NoError(t, sourceStore.PutAt(t.Context(), []byte("n"), []byte("v"), 10, 0))
	source := newSplitRuntimeClient(t, engine, sourceStore, job.SourceGroupID)
	target := newSplitRuntimeClient(t, engine, targetStore, job.TargetGroupID)
	if job.Phase == distribution.SplitJobPhaseCleanup {
		seedSplitRuntimeCleanup(t, source, target, job, snapshot)
	}
	options := splitRuntimeOptions(source, target, job.SourceGroupID)
	options = append(options, WithDistributionCoordinator(&ownershipCatalogCoordinator{
		distributionCoordinatorStub: newDistributionCoordinatorStub(root, true),
	}))
	return NewDistributionServer(engine, catalog, options...), source, target
}

func seedSplitRuntimeCleanup(t *testing.T, source, target *splitRuntimeClient, job distribution.SplitJob, snapshot distribution.CatalogSnapshot) {
	t.Helper()
	_, err := source.ApplyTargetStagedReadiness(t.Context(), &pb.TargetStagedReadinessRequest{
		JobId: job.JobID, MigrationJobId: job.JobID, RouteStart: job.SplitKey, RouteEnd: snapshot.Routes[1].End,
		Armed: true, SourceWriteFence: true, SourceReadFence: true, RetentionPinTs: 1,
		ExpectedCutoverVersion: snapshot.Version, MinWriteTsExclusive: job.FenceTS,
	})
	require.NoError(t, err)
	if !job.TargetPromotionDone {
		_, err = target.ApplyTargetStagedReadiness(t.Context(), &pb.TargetStagedReadinessRequest{
			JobId: job.JobID, MigrationJobId: job.JobID, RouteStart: job.SplitKey, RouteEnd: snapshot.Routes[1].End,
			Armed: true, RetentionPinTs: 1, ExpectedCutoverVersion: snapshot.Version, MinWriteTsExclusive: job.FenceTS,
		})
		require.NoError(t, err)
	}
}
