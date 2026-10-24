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

// realImportTargetStub answers ImportRangeVersions out of a real MVCC store, so
// the duplicate detection exercised here is the production
// store.ImportVersions path (validateNextImportBatch and the durable ack it
// reads) rather than a hand-written answer.
type realImportTargetStub struct {
	splitMigrationClientStub
	st store.MVCCStore
}

func (s *realImportTargetStub) ImportRangeVersions(
	ctx context.Context,
	req *pb.ImportRangeVersionsRequest,
	_ ...grpc.CallOption,
) (*pb.ImportRangeVersionsResponse, error) {
	versions := make([]store.MVCCVersion, 0, len(req.GetVersions()))
	for _, version := range req.GetVersions() {
		versions = append(versions, store.MVCCVersion{
			Key:      distribution.CloneBytes(version.GetKey()),
			Value:    distribution.CloneBytes(version.GetValue()),
			CommitTS: version.GetCommitTs(),
		})
	}
	result, err := s.st.ImportVersions(ctx, store.ImportVersionsOptions{
		JobID:     req.GetJobId(),
		BracketID: req.GetBracketId(),
		BatchSeq:  req.GetBatchSeq(),
		Cursor:    distribution.CloneBytes(req.GetCursor()),
		Versions:  versions,
	})
	if err != nil {
		return nil, err
	}
	return &pb.ImportRangeVersionsResponse{
		AckedCursor: result.AckedCursor,
		Duplicate:   result.Duplicate,
	}, nil
}

// importReplayFixture builds a copy-phase job with one bracket plus a target
// backed by its own real store.
func importReplayFixture(t *testing.T) (
	context.Context,
	*DistributionServer,
	*distribution.CatalogStore,
	distribution.SplitJob,
	distribution.MigrationBracket,
	*realImportTargetStub,
) {
	t.Helper()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	t.Cleanup(func() { _ = baseStore.Close() })
	catalog := distribution.NewCatalogStore(baseStore, distribution.WithCatalogRouteDescriptorV2Writes(true))
	saved, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{{
		RouteID: 1,
		Start:   []byte("a"),
		End:     []byte("z"),
		GroupID: 1,
		State:   distribution.RouteStateActive,
	}})
	require.NoError(t, err)

	job, err := distribution.InitializeSplitJobPlan(distribution.SplitJob{
		JobID:         1,
		SourceRouteID: 1,
		SplitKey:      []byte("m"),
		TargetGroupID: 2,
	}, saved.Routes[0], 1000)
	require.NoError(t, err)
	job.Phase = distribution.SplitJobPhaseBackfill
	require.NoError(t, catalog.CreateSplitJob(ctx, job))
	require.NotEmpty(t, job.BracketProgress)

	targetStore := store.NewMVCCStore()
	t.Cleanup(func() { _ = targetStore.Close() })
	target := &realImportTargetStub{st: targetStore}
	s := NewDistributionServer(
		distribution.NewEngine(),
		catalog,
		WithDistributionCoordinator(newDistributionCoordinatorStub(baseStore, true)),
	)

	bracket := distribution.MigrationBracket{
		BracketID: job.BracketProgress[0].BracketID,
		Family:    job.BracketProgress[0].Family,
		Start:     []byte("m"),
		End:       []byte("z"),
	}
	return ctx, s, catalog, job, bracket, target
}

// A runner that dies after the target durably acknowledged a batch, but before
// it persisted its own progress, replays that batch on restart. The source
// keeps taking writes while the copy runs, so the replayed chunk can end on a
// different boundary than the one the target accepted. Requiring the boundary
// to match would fail the job on that replay and on every retry after it,
// wedging the migration with its guards and retention pin held. The target's
// durable acknowledgement is the authority instead.
func TestCopySplitJobStreamAdoptsDuplicateImportAck(t *testing.T) {
	t.Parallel()

	ctx, s, catalog, job, bracket, target := importReplayFixture(t)

	// The attempt whose progress was lost: batch 1 accepted at cursor-a.
	acceptedCursor := []byte("cursor-a")
	_, err := target.ImportRangeVersions(ctx, &pb.ImportRangeVersionsRequest{
		JobId:     job.JobID,
		BracketId: bracket.BracketID,
		BatchSeq:  1,
		Cursor:    acceptedCursor,
		Versions:  []*pb.MVCCVersion{{Key: []byte("m1"), Value: []byte("v"), CommitTs: 10}},
	})
	require.NoError(t, err)

	// Restart: progress still reads batch 0, so the runner re-sends batch 1 --
	// this time from a re-export that ran longer and ended at cursor-b.
	progress := job.BracketProgress[0]
	progress.Cursor = nil
	progress.LastAckedBatchSeq = 0
	stream := &splitMigrationStreamStub{responses: []*pb.ExportRangeVersionsResponse{{
		Versions:   []*pb.MVCCVersion{{Key: []byte("m1"), Value: []byte("v"), CommitTs: 10}, {Key: []byte("m2"), Value: []byte("v"), CommitTs: 11}},
		NextCursor: []byte("cursor-b"),
		Done:       true,
	}}}

	require.NoError(t, s.copySplitJobStream(ctx, job, 0, bracket, progress, stream, target))

	loaded, found, err := catalog.SplitJob(ctx, job.JobID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, acceptedCursor, loaded.BracketProgress[0].Cursor,
		"progress must adopt the cursor the target durably acknowledged")
	require.Equal(t, uint64(1), loaded.BracketProgress[0].LastAckedBatchSeq)
	require.False(t, loaded.BracketProgress[0].Done,
		"the replayed export's end-of-range answer describes a chunk the target never took")
}

// The strict boundary check still fires when the target acknowledges a cursor
// it was never sent and does not claim the batch as a duplicate.
func TestCopySplitJobStreamRejectsUnexplainedCursorMismatch(t *testing.T) {
	t.Parallel()

	ctx, s, _, job, bracket, target := importReplayFixture(t)

	// Batch 1 has never been seen by the target, so nothing is duplicated: the
	// store acknowledges exactly the cursor it was sent. Rewriting it here
	// stands in for a target that answers with a cursor of its own.
	lying := &lyingImportTargetStub{realImportTargetStub: target, cursor: []byte("cursor-x")}
	progress := job.BracketProgress[0]
	stream := &splitMigrationStreamStub{responses: []*pb.ExportRangeVersionsResponse{{
		Versions:   []*pb.MVCCVersion{{Key: []byte("m1"), Value: []byte("v"), CommitTs: 10}},
		NextCursor: []byte("cursor-b"),
		Done:       true,
	}}}

	err := s.copySplitJobStream(ctx, job, 0, bracket, progress, stream, lying)
	require.ErrorContains(t, err, "acknowledged a different cursor")
}

type lyingImportTargetStub struct {
	*realImportTargetStub
	cursor []byte
}

func (s *lyingImportTargetStub) ImportRangeVersions(
	ctx context.Context,
	req *pb.ImportRangeVersionsRequest,
	opts ...grpc.CallOption,
) (*pb.ImportRangeVersionsResponse, error) {
	resp, err := s.realImportTargetStub.ImportRangeVersions(ctx, req, opts...)
	if err != nil {
		return nil, err
	}
	resp.AckedCursor = distribution.CloneBytes(s.cursor)
	return resp, nil
}
