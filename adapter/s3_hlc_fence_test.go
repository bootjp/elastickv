package adapter

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

// TestS3TxnStartTSFailsClosedOnExpiredCeiling verifies that the s3
// adapter's txnStartTS surfaces ErrCeilingExpired when the HLC's
// physical ceiling has expired, rather than silently issuing a
// stale-leader timestamp.  Regression for PR #867 Phase 2a: the
// adapter's direct clock.Next() callers were migrated to
// NextFenced() so the HLC-4 (iii) ceiling fence applies on the
// adapter side too (kv-layer was migrated in PR #867 Phase 1).
//
// Without this fence, an S3 PUT / DELETE issued after the lease
// renewal stopped would mint a startTS inside a stale window and
// risk colliding with a subsequent leader's commit window.
func TestS3TxnStartTSFailsClosedOnExpiredCeiling(t *testing.T) {
	t.Parallel()

	clock := kv.NewHLC()
	// Ceiling sits in the deep past relative to wall_now so the
	// fence trips on the next NextFenced() call.
	clock.SetPhysicalCeiling(1)

	srv := &S3Server{coordinator: &stubAdapterCoordinator{clock: clock}}

	// Sentinel ^uint64(0) means "allocate via HLC" — this is the
	// path the fence guards.
	_, err := srv.txnStartTS(context.Background(), ^uint64(0))
	require.ErrorIs(t, err, kv.ErrCeilingExpired,
		"s3 txnStartTS must propagate ErrCeilingExpired when the HLC's physical ceiling is stale")
}

// TestS3TxnStartTSPassesThroughExplicitReadTS verifies that the
// fence is not consulted when the caller supplies a concrete readTS
// (the non-sentinel path).  Pre-bootstrap and read-after-readTS
// paths must remain functional even with an unrenewed ceiling.
func TestS3TxnStartTSPassesThroughExplicitReadTS(t *testing.T) {
	t.Parallel()

	clock := kv.NewHLC()
	clock.SetPhysicalCeiling(1) // expired

	srv := &S3Server{coordinator: &stubAdapterCoordinator{clock: clock}}

	ts, err := srv.txnStartTS(context.Background(), 42)
	require.NoError(t, err)
	require.Equal(t, uint64(42), ts)
}

func TestS3BeginTxnReadTimestampPhaseDPreservesAppliedWatermark(t *testing.T) {
	t.Parallel()

	allocator := &distributionTSOAllocator{base: 100, phaseD: true, phaseDFloor: 10}
	coord := newDistributionCoordinatorStub(nil, true)
	coord.allocator = allocator
	srv := &S3Server{coordinator: coord}

	readTimestamp, err := srv.beginTxnReadTimestamp(context.Background(), 42, "test")
	require.NoError(t, err)
	require.Equal(t, uint64(42), readTimestamp.Timestamp())
	require.Zero(t, allocator.count, "an applied Phase-D read watermark must not allocate ahead of Raft apply")
}

func TestS3BeginTxnReadTimestampPhaseDRejectsLatestSentinel(t *testing.T) {
	t.Parallel()

	allocator := &distributionTSOAllocator{base: 100, phaseD: true, phaseDFloor: 10}
	coord := newDistributionCoordinatorStub(nil, true)
	coord.allocator = allocator
	srv := &S3Server{coordinator: coord}

	_, err := srv.beginTxnReadTimestamp(context.Background(), ^uint64(0), "test")
	require.ErrorIs(t, err, kv.ErrTSOTimestampInvalid)
	require.Zero(t, allocator.count, "the latest sentinel must fail closed instead of allocating an unapplied read timestamp")
}

// TestS3NextTxnCommitTSFailsClosedOnExpiredCeiling verifies that
// nextTxnCommitTS surfaces ErrCeilingExpired through the
// NextFenced() it calls after Observe(startTS).  This is the
// commit-ts allocation site for S3 multipart and PUT.
func TestS3NextTxnCommitTSFailsClosedOnExpiredCeiling(t *testing.T) {
	t.Parallel()

	clock := kv.NewHLC()
	// Future ceiling, then advance wall past it — emulate the
	// "leader's lease renewal stopped" hazard.
	futureMs := time.Now().UnixMilli() + 5_000
	clock.SetPhysicalCeiling(futureMs)
	// First call succeeds (wall < ceiling).
	startTS, err := clock.NextFenced()
	require.NoError(t, err)
	// Now retroactively set the ceiling to the deep past so the
	// next NextFenced trips the fence.
	pastClock := kv.NewHLC()
	pastClock.SetPhysicalCeiling(1)
	pastClock.Observe(startTS)

	srv := &S3Server{coordinator: &stubAdapterCoordinator{clock: pastClock}}

	_, err = srv.nextTxnCommitTS(context.Background(), startTS)
	require.ErrorIs(t, err, kv.ErrCeilingExpired,
		"s3 nextTxnCommitTS must propagate ErrCeilingExpired from NextFenced")
}

func TestS3CommitUploadPartRechecksUploadAtLatestAppliedWatermark(t *testing.T) {
	st := store.NewMVCCStore()
	const generation = uint64(1)
	uploadMetaKey := s3keys.UploadMetaKey("bucket", generation, "object", "upload")
	require.NoError(t, st.PutAt(context.Background(), uploadMetaKey, []byte("meta"), 10, 0))
	require.NoError(t, st.DeleteAt(context.Background(), uploadMetaKey, 20))
	coord := &recordingS3DispatchCoordinator{}
	server := NewS3Server(nil, "", st, coord, nil)

	_, err := server.commitS3UploadPart(context.Background(), &s3UploadPartState{
		partNo:        1,
		readTS:        10,
		meta:          &s3BucketMeta{Generation: generation},
		uploadMetaKey: uploadMetaKey,
	}, s3ChunkUploadResult{}, "bucket", "object", "upload", 10, 30)
	require.ErrorContains(t, err, "upload not found")
	require.Nil(t, coord.request, "an upload removed after startTS must not dispatch an orphan part")
}

func TestS3CommitUploadPartIncludesUploadMetaInReadSet(t *testing.T) {
	st := store.NewMVCCStore()
	const generation = uint64(1)
	uploadMetaKey := s3keys.UploadMetaKey("bucket", generation, "object", "upload")
	require.NoError(t, st.PutAt(context.Background(), uploadMetaKey, []byte("meta"), 10, 0))
	coord := &recordingS3DispatchCoordinator{}
	server := NewS3Server(nil, "", st, coord, nil)

	_, err := server.commitS3UploadPart(context.Background(), &s3UploadPartState{
		partNo:        1,
		readTS:        10,
		meta:          &s3BucketMeta{Generation: generation},
		uploadMetaKey: uploadMetaKey,
	}, s3ChunkUploadResult{}, "bucket", "object", "upload", 10, 30)
	require.NoError(t, err)
	require.NotNil(t, coord.request)
	require.Equal(t, [][]byte{uploadMetaKey}, coord.request.ReadKeys)
}

type recordingS3DispatchCoordinator struct {
	stubAdapterCoordinator
	request *kv.OperationGroup[kv.OP]
}

func (c *recordingS3DispatchCoordinator) Dispatch(_ context.Context, request *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	c.request = request
	return &kv.CoordinateResponse{}, nil
}
