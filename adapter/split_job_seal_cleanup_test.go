package adapter

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type routedSealCleanupClient struct {
	splitMigrationClientStub
	server   *Internal
	families []uint32
}

func (c *routedSealCleanupClient) CleanupMigration(ctx context.Context, req *pb.CleanupMigrationRequest, _ ...grpc.CallOption) (*pb.CleanupMigrationResponse, error) {
	c.families = append(c.families, req.GetKeyFamily())
	req.MaxVersions = 1
	return c.server.CleanupMigration(ctx, req)
}

func TestSplitJobSourceCleanupRetiresRoutedDynamoSeals(t *testing.T) {
	t.Parallel()
	for _, legacyDone := range []bool{false, true} {
		t.Run(map[bool]string{false: "appended bracket", true: "legacy done cursor"}[legacyDone], func(t *testing.T) {
			t.Parallel()
			testSplitJobSealCleanup(t, legacyDone)
		})
	}
}

func testSplitJobSealCleanup(t *testing.T, legacyDone bool) {
	t.Helper()
	ctx := context.Background()
	sourceStore := store.NewMVCCStore()
	table := base64.RawURLEncoding.EncodeToString([]byte("foo"))
	neighbor := base64.RawURLEncoding.EncodeToString([]byte("fooX"))
	seal := []byte(kv.DynamoSealPrefix + table + "|7|0001")
	other := []byte(kv.DynamoSealPrefix + neighbor + "|7|0001")
	for _, ts := range []uint64{10, 20, 80} {
		require.NoError(t, sourceStore.PutAt(ctx, seal, []byte("seal"), ts, 0))
	}
	require.NoError(t, sourceStore.PutAt(ctx, other, []byte("neighbor"), 20, 0))
	start := kv.RouteKey(seal)
	end := append(append([]byte(nil), start...), 0)
	catalogStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(catalogStore, distribution.WithCatalogRouteDescriptorV2Writes(true))
	_, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{{RouteID: 3, ParentRouteID: 42, Start: start, End: end, GroupID: 9}})
	require.NoError(t, err)
	job := promotionCompleteDistributionJob()
	job.SourceGroupID, job.TargetGroupID, job.FenceTS = 7, 9, 50
	job.SplitKey = start
	brackets, err := distribution.PlanCleanupBrackets(start, end)
	require.NoError(t, err)
	job.Cursor = encodeSplitCleanupCursor(len(brackets)-1, nil, false)
	if legacyDone {
		job.Cursor = distribution.CloneBytes(splitCleanupDoneCursor)
	}
	require.NoError(t, catalog.CreateSplitJob(ctx, job))
	client := &routedSealCleanupClient{server: NewInternalWithEngine(nil, mockInternalLeader{}, nil, nil,
		WithInternalStore(sourceStore),
		WithInternalMigrationProposer(&applyingMigrationProposer{fsm: kv.NewKvFSMWithHLC(sourceStore, nil)}),
		WithInternalMigrationCleanupGate(func(context.Context) error { return nil }),
	)}
	server := NewDistributionServer(distribution.NewEngine(), catalog,
		WithDistributionCoordinator(newDistributionCoordinatorStub(catalogStore, true)),
		WithSplitMigrationClientFactory(func(_ context.Context, _ distribution.SplitJob, groupID uint64) (SplitMigrationClient, SplitMigrationClient, error) {
			require.Equal(t, uint64(7), groupID)
			return client, &splitMigrationClientStub{}, nil
		}))
	for range 10 {
		require.NoError(t, server.cleanupSplitJobSourceData(ctx, job))
		var found bool
		job, found, err = catalog.SplitJob(ctx, job.JobID)
		require.NoError(t, err)
		require.True(t, found)
		if string(job.Cursor) == string(splitSourceCleanupDoneCursor) {
			break
		}
	}
	require.Equal(t, splitSourceCleanupDoneCursor, job.Cursor)
	require.Greater(t, len(client.families), 1, "cleanup must persist and resume bounded pages")
	for _, family := range client.families {
		require.Equal(t, distribution.MigrationFamilyDynamoSnapshotSeal, family)
	}
	_, err = sourceStore.GetAt(ctx, seal, 20)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	_, err = sourceStore.GetAt(ctx, seal, 80)
	require.NoError(t, err, "versions above the fence must survive")
	_, err = sourceStore.GetAt(ctx, other, 20)
	require.NoError(t, err, "the neighboring table must survive")
}
