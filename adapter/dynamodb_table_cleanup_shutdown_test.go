package adapter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/testutil"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestDynamoDBServerStopWaitsForTableCleanup pins that the cleanup a
// DeleteTable (or a legacy-table migration) starts after its request
// returned keeps Stop, and with it the stores closed after Stop, until it
// finished, inside the graceful window.
func TestDynamoDBServerStopWaitsForTableCleanup(t *testing.T) {
	shortenHTTPShutdownTimeout(t, 5*time.Second)
	st := store.NewMVCCStore()
	coord := newCleanupBlockingCoordinator(st)
	d := NewDynamoDBServer(nil, st, coord)
	launchReservedTableCleanup(t, d, "orders", 1)
	waitDispatchEntered(t, coord)

	stopDone := make(chan struct{})
	go func() {
		d.Stop()
		close(stopDone)
	}()
	testutil.NoRecv(t, stopDone, 100*time.Millisecond, "Stop returned while the table cleanup was still dispatching")
	close(coord.release)
	testutil.Closed(t, stopDone, 5*time.Second, "Stop did not return once the table cleanup finished")
}

// A table cleanup still running when the bound expires is cancelled and
// joined: Stop returns only once it is out of its dispatch.
func TestDynamoDBServerStopCancelsAndJoinsTableCleanup(t *testing.T) {
	shortenHTTPShutdownTimeout(t, 100*time.Millisecond)
	st := store.NewMVCCStore()
	coord := newCleanupBlockingCoordinator(st)
	coord.unwind = 300 * time.Millisecond
	t.Cleanup(func() { close(coord.release) })
	d := NewDynamoDBServer(nil, st, coord)
	launchReservedTableCleanup(t, d, "orders", 1)
	waitDispatchEntered(t, coord)

	d.Stop()
	require.True(t, coord.returned.Load(), "Stop returned while the cancelled cleanup was still in Dispatch")
	require.Zero(t, d.tableCleanups.gate.remaining())

	// No cleanup slot can be reserved once Stop has begun.
	_, ok := d.tableCleanups.reserve()
	require.False(t, ok, "a cleanup slot must not be reserved after Stop")
}

func launchReservedTableCleanup(t *testing.T, d *DynamoDBServer, tableName string, generation uint64) {
	t.Helper()
	cleanup, ok := d.tableCleanups.reserve()
	require.True(t, ok)
	d.launchDeletedTableCleanup(cleanup, tableName, generation)
}

func waitDispatchEntered(t *testing.T, coord *cleanupBlockingCoordinator) {
	t.Helper()
	select {
	case <-coord.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the cleanup never dispatched")
	}
}

// TestDynamoDBAdminDeleteTableRefusedOnceStopBegan pins that a table delete
// arriving through the admin surface (which Stop's listener drain does not
// cover) after Stop began is refused before it commits: committing it
// would leave the deleted generation's items and index entries behind,
// since its cleanup can no longer start.
func TestDynamoDBAdminDeleteTableRefusedOnceStopBegan(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	srv := nodes[0].dynamoServer
	ctx := context.Background()
	_, err := srv.AdminCreateTable(ctx, fullAdminPrincipal, AdminCreateTableInput{
		TableName:    "orders",
		PartitionKey: AdminAttribute{Name: "id", Type: "S"},
	})
	require.NoError(t, err)

	srv.Stop()
	err = srv.AdminDeleteTable(ctx, fullAdminPrincipal, "orders")
	require.ErrorIs(t, err, ErrDynamoServerStopping)
	_, exists, err := srv.AdminDescribeTable(ctx, "orders")
	require.NoError(t, err)
	require.True(t, exists, "a delete refused for the shutdown must not commit")
}

// A slot reserved before stop holds it until the job started through it
// returns, or until it is released unused; a reservation is refused once
// stop has begun.
func TestBackgroundWorkersReservationHoldsStop(t *testing.T) {
	w := newBackgroundWorkers()
	started, ok := w.reserve()
	require.True(t, ok)
	unused, ok := w.reserve()
	require.True(t, ok)

	stopDone := make(chan struct{})
	go func() {
		w.stop("test", 5*time.Second, time.Second)
		close(stopDone)
	}()
	require.Eventually(t, func() bool {
		r, ok := w.reserve()
		if ok {
			r.release()
		}
		return !ok
	}, 5*time.Second, time.Millisecond, "a reservation must be refused once stop began")

	finish := make(chan struct{})
	started.start(time.Minute, func(context.Context) { <-finish })
	unused.release()
	unused.release()
	testutil.NoRecv(t, stopDone, 100*time.Millisecond, "stop returned while a reserved job was still running")
	close(finish)
	testutil.Closed(t, stopDone, 5*time.Second, "stop did not return once the reserved job finished")
}

// The public listener answers the stopping refusal with a retryable 503
// ServiceUnavailable, not with the 500 an unknown error gets.
func TestDynamoDBStoppingErrorIsServiceUnavailable(t *testing.T) {
	rec := httptest.NewRecorder()
	writeDynamoErrorFromErr(rec, errors.WithStack(ErrDynamoServerStopping))
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Equal(t, dynamoErrServiceUnavailable, rec.Header().Get("x-amzn-ErrorType"))
}
