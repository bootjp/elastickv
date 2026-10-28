package adapter

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/testutil"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

// cleanupBlockingCoordinator holds every Dispatch until release is closed
// or the request's context ends, the way a cleanup batch waits on Raft.
// unwind delays the return after a cancellation, the way a proposal takes a
// moment to give up; ignoreCancel keeps waiting for release regardless.
type cleanupBlockingCoordinator struct {
	*localAdapterCoordinator
	unwind       time.Duration
	ignoreCancel bool
	returned     atomic.Bool
	dispatches   atomic.Int64
	entered      chan struct{}
	enterOnce    sync.Once
	release      chan struct{}
	cancelled    chan struct{}
	cancelOnce   sync.Once
}

func newCleanupBlockingCoordinator(st store.MVCCStore) *cleanupBlockingCoordinator {
	return &cleanupBlockingCoordinator{
		localAdapterCoordinator: newLocalAdapterCoordinator(st),
		entered:                 make(chan struct{}),
		release:                 make(chan struct{}),
		cancelled:               make(chan struct{}),
	}
}

func (c *cleanupBlockingCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	c.dispatches.Add(1)
	defer c.returned.Store(true)
	c.enterOnce.Do(func() { close(c.entered) })
	done := ctx.Done()
	if c.ignoreCancel {
		done = nil
	}
	select {
	case <-c.release:
	case <-done:
		c.cancelOnce.Do(func() { close(c.cancelled) })
		time.Sleep(c.unwind)
		return nil, ctx.Err() //nolint:wrapcheck // test double reports the cancellation as is.
	}
	return c.localAdapterCoordinator.Dispatch(ctx, req)
}

// startBlockedManifestCleanup starts the background cleanup an overwrite
// or delete triggers and waits until its delete batch is in Dispatch.
func startBlockedManifestCleanup(t *testing.T, s *S3Server, coord *cleanupBlockingCoordinator) {
	t.Helper()
	s.cleanupManifestBlobsAsync("bucket", 1, "object", &s3ObjectManifest{
		UploadID: "upload",
		Parts:    []s3ObjectPart{{PartNo: 1, ChunkSizes: []uint64{1}}},
	})
	testutil.Closed(t, coord.entered, 5*time.Second, "the cleanup never dispatched its delete batch")
}

// TestS3ServerStopWaitsForCleanupBeforeClosingBlobCluster pins that a
// background cleanup started by an overwrite, delete or failed upload
// keeps Stop, and with it the blob cluster and the stores closed after
// Stop, until it finished, inside the graceful window.
func TestS3ServerStopWaitsForCleanupBeforeClosingBlobCluster(t *testing.T) {
	shortenHTTPShutdownTimeout(t, 5*time.Second)
	st := store.NewMVCCStore()
	coord := newCleanupBlockingCoordinator(st)
	s := NewS3Server(nil, "", st, coord, nil)
	cluster := &closeRecordingBlobCluster{fakeS3BlobCluster: newFakeS3BlobCluster()}
	s.blobCluster = cluster
	startBlockedManifestCleanup(t, s, coord)

	stopDone := stopInBackground(s)
	requireStopStillDraining(t, stopDone, cluster)
	close(coord.release)
	waitStopped(t, stopDone)
	require.True(t, cluster.closed.Load(), "the cluster is closed once the cleanup finished")
}

// A cleanup still running when the bound expires holds Stop for the bound
// only; its context is then cancelled so it stops dispatching against
// stores that are about to close.
func TestS3ServerStopCancelsCleanupAfterBound(t *testing.T) {
	shortenHTTPShutdownTimeout(t, 200*time.Millisecond)
	st := store.NewMVCCStore()
	coord := newCleanupBlockingCoordinator(st)
	t.Cleanup(func() { close(coord.release) })
	s := NewS3Server(nil, "", st, coord, nil)
	startBlockedManifestCleanup(t, s, coord)

	start := time.Now()
	s.Stop()
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 150*time.Millisecond, "the cleanup must be given its bound")
	require.Less(t, elapsed, 3*time.Second, "Stop must not wait on a cleanup past the bound")
	testutil.Closed(t, coord.cancelled, 5*time.Second, "the cleanup's context was not cancelled after the bound")
}

// Once Stop has begun, a new background cleanup is not started.
func TestS3ServerStopRefusesLateCleanup(t *testing.T) {
	st := store.NewMVCCStore()
	coord := newCleanupBlockingCoordinator(st)
	close(coord.release)
	s := NewS3Server(nil, "", st, coord, nil)
	s.Stop()

	s.cleanupManifestBlobsAsync("bucket", 1, "object", &s3ObjectManifest{
		UploadID: "upload",
		Parts:    []s3ObjectPart{{PartNo: 1, ChunkSizes: []uint64{1}}},
	})
	time.Sleep(100 * time.Millisecond)
	require.Zero(t, coord.dispatches.Load(), "a cleanup must not start after Stop")
}

// A cleanup cancelled at the bound still has to unwind out of its dispatch.
// Stop must wait for that, briefly, before the blob cluster and the stores
// close underneath it.
func TestS3ServerStopJoinsCleanupAfterCancellingIt(t *testing.T) {
	shortenHTTPShutdownTimeout(t, 100*time.Millisecond)
	st := store.NewMVCCStore()
	coord := newCleanupBlockingCoordinator(st)
	coord.unwind = 300 * time.Millisecond
	t.Cleanup(func() { close(coord.release) })
	s := NewS3Server(nil, "", st, coord, nil)
	cluster := &closeRecordingBlobCluster{fakeS3BlobCluster: newFakeS3BlobCluster()}
	s.blobCluster = cluster
	startBlockedManifestCleanup(t, s, coord)

	s.Stop()
	require.True(t, coord.returned.Load(), "Stop returned while the cancelled cleanup was still in Dispatch")
	require.Zero(t, s.cleanupWorkers.remaining(), "Stop returned before the cancelled cleanup exited")
	require.True(t, cluster.closed.Load())
}

// A cleanup that does not observe its cancellation holds Stop for the
// bound and the join only.
func TestS3ServerStopBoundsJoinOfCleanupIgnoringCancellation(t *testing.T) {
	shortenHTTPShutdownTimeout(t, 100*time.Millisecond)
	shortenS3CleanupJoinTimeout(t, 200*time.Millisecond)
	st := store.NewMVCCStore()
	coord := newCleanupBlockingCoordinator(st)
	coord.ignoreCancel = true
	t.Cleanup(func() { close(coord.release) })
	s := NewS3Server(nil, "", st, coord, nil)
	startBlockedManifestCleanup(t, s, coord)

	start := time.Now()
	s.Stop()
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 250*time.Millisecond, "the cancelled cleanup must be given its join")
	require.Less(t, elapsed, 3*time.Second, "Stop must not wait on a stuck cleanup past the join")

	// A second Stop does not spend the bound or the join again.
	start = time.Now()
	s.Stop()
	require.Less(t, time.Since(start), 100*time.Millisecond)
}

func shortenS3CleanupJoinTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	old := s3CleanupJoinTimeout
	s3CleanupJoinTimeout = d
	t.Cleanup(func() { s3CleanupJoinTimeout = old })
}
