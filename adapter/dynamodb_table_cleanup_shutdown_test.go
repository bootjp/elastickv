package adapter

import (
	"testing"
	"time"

	"github.com/bootjp/elastickv/store"
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
	d.launchDeletedTableCleanup("orders", 1)
	waitDispatchEntered(t, coord)

	stopDone := make(chan struct{})
	go func() {
		d.Stop()
		close(stopDone)
	}()
	select {
	case <-stopDone:
		t.Fatal("Stop returned while the table cleanup was still dispatching")
	case <-time.After(100 * time.Millisecond):
	}
	close(coord.release)
	select {
	case <-stopDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return once the table cleanup finished")
	}
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
	d.launchDeletedTableCleanup("orders", 1)
	waitDispatchEntered(t, coord)

	d.Stop()
	require.True(t, coord.returned.Load(), "Stop returned while the cancelled cleanup was still in Dispatch")
	require.Zero(t, d.tableCleanups.gate.remaining())

	// A cleanup requested once Stop has begun is not started.
	before := coord.dispatches.Load()
	d.launchDeletedTableCleanup("orders", 2)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, before, coord.dispatches.Load(), "a cleanup must not start after Stop")
}

func waitDispatchEntered(t *testing.T, coord *cleanupBlockingCoordinator) {
	t.Helper()
	select {
	case <-coord.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the cleanup never dispatched")
	}
}
