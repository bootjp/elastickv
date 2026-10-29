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

// firstDispatchHoldingCoordinator holds the first Dispatch after arm is
// called until release is closed, and counts the dispatches that follow
// it, the way an admin delete transaction waits on Raft before its
// background cleanup dispatches the blob deletes.
type firstDispatchHoldingCoordinator struct {
	*localAdapterCoordinator
	armed   atomic.Bool
	held    atomic.Bool
	after   atomic.Int64
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newFirstDispatchHoldingCoordinator(st store.MVCCStore) *firstDispatchHoldingCoordinator {
	return &firstDispatchHoldingCoordinator{
		localAdapterCoordinator: newLocalAdapterCoordinator(st),
		entered:                 make(chan struct{}),
		release:                 make(chan struct{}),
	}
}

func (c *firstDispatchHoldingCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	if c.armed.Load() {
		if c.held.CompareAndSwap(false, true) {
			c.once.Do(func() { close(c.entered) })
			<-c.release
		} else {
			c.after.Add(1)
		}
	}
	return c.localAdapterCoordinator.Dispatch(ctx, req)
}

// An admin delete already running when Stop begins must keep Stop, and
// with it the cleanup gate, open until it finished, so the blob cleanup it
// schedules after committing still runs instead of being refused and
// leaving the deleted object's blob keys behind. A delete that arrives
// once Stop has begun is refused.
func TestS3ServerStopWaitsForAdminDeleteAndItsCleanup(t *testing.T) {
	shortenHTTPShutdownTimeout(t, 5*time.Second)
	st := store.NewMVCCStore()
	coord := newFirstDispatchHoldingCoordinator(st)
	s := NewS3Server(nil, "", st, coord, nil)
	putObjectForAdminTest(t, s, "deletable", "k1", "hello")
	cluster := &closeRecordingBlobCluster{fakeS3BlobCluster: newFakeS3BlobCluster()}
	s.blobCluster = cluster

	coord.armed.Store(true)
	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- s.AdminDeleteObject(context.Background(), fullAdminBucketsPrincipal(), "deletable", "k1")
	}()
	testutil.Closed(t, coord.entered, 5*time.Second, "the admin delete never dispatched its transaction")

	stopDone := stopInBackground(s)
	requireStopStillDraining(t, stopDone, cluster)
	close(coord.release)
	require.NoError(t, <-deleteDone)
	waitStopped(t, stopDone)
	require.Positive(t, coord.after.Load(), "the delete's blob cleanup must run, not be refused by the stopping server")
	require.True(t, cluster.closed.Load())

	err := s.AdminDeleteObject(context.Background(), fullAdminBucketsPrincipal(), "deletable", "k1")
	require.ErrorIs(t, err, ErrAdminS3Stopping)
}

func TestS3ServerTimedOutAdminDeleteRetainsCleanupReservation(t *testing.T) {
	shortenHTTPShutdownTimeout(t, 10*time.Millisecond)
	st := store.NewMVCCStore()
	coord := newFirstDispatchHoldingCoordinator(st)
	s := NewS3Server(nil, "", st, coord, nil)
	putObjectForAdminTest(t, s, "deletable", "k1", "hello")
	coord.armed.Store(true)
	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- s.AdminDeleteObject(context.Background(), fullAdminBucketsPrincipal(), "deletable", "k1")
	}()
	testutil.Closed(t, coord.entered, time.Second, "delete did not dispatch")
	stopDone := stopInBackground(s)
	testutil.Closed(t, s.reservedCleanups.ctx.Done(), time.Second, "Stop did not reach reserved cleanup cancellation")
	close(coord.release)
	require.NoError(t, <-deleteDone)
	waitStopped(t, stopDone)
	require.Positive(t, coord.after.Load(), "late committed delete must clean its manifest blobs")
}
