package adapter

import (
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/testutil"
	"github.com/stretchr/testify/require"
)

func shortenSQSWorkerStopTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	old := sqsWorkerStopTimeout
	sqsWorkerStopTimeout = d
	t.Cleanup(func() { sqsWorkerStopTimeout = old })
}

// TestSQSServerStopJoinsBackgroundWorkers pins that Stop does not return
// while a background worker Run started (the retention reaper inside a
// store scan or a Raft dispatch) is still running: in a listenless
// deployment the HTTP shutdown returns at once, Run unblocks on the same
// cancellation, and the errgroup would otherwise let the deferred cleanup
// close the stores underneath the worker. The join is bounded, and only
// the first Stop waits.
func TestSQSServerStopJoinsBackgroundWorkers(t *testing.T) {
	shortenSQSWorkerStopTimeout(t, 5*time.Second)
	s := NewSQSServer(nil, nil, nil)
	release := make(chan struct{})
	finished := make(chan struct{})
	require.True(t, s.workers.start(func() {
		<-s.reaperCtx.Done()
		<-release
		close(finished)
	}))
	go func() {
		time.Sleep(100 * time.Millisecond)
		close(release)
	}()

	start := time.Now()
	s.Stop()
	require.GreaterOrEqual(t, time.Since(start), 80*time.Millisecond, "Stop must wait for the running worker")
	select {
	case <-finished:
	default:
		t.Fatal("Stop returned before the worker finished")
	}
	require.False(t, s.workers.start(func() {}), "no worker may start once Stop began")
}

func TestSQSServerStopGivesUpOnStuckWorkerAfterBound(t *testing.T) {
	shortenSQSWorkerStopTimeout(t, 150*time.Millisecond)
	s := NewSQSServer(nil, nil, nil)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	require.True(t, s.workers.start(func() { <-release }))

	start := time.Now()
	s.Stop()
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 100*time.Millisecond, "the worker must be given its bound")
	require.Less(t, elapsed, 3*time.Second, "Stop must not wait on a worker that never finishes")

	start = time.Now()
	s.Stop()
	require.Less(t, time.Since(start), 50*time.Millisecond, "second Stop must not wait the bound again")
}

// A listenless Run (admin-only deployment) returns once Stop cancelled it,
// with its sweep worker joined.
func TestSQSServerListenlessRunReturnsAfterStop(t *testing.T) {
	shortenSQSWorkerStopTimeout(t, 5*time.Second)
	s := NewSQSServer(nil, nil, nil)
	runDone := make(chan error, 1)
	go func() { runDone <- s.Run() }()
	time.Sleep(20 * time.Millisecond)
	s.Stop()
	err := testutil.Recv(t, runDone, 5*time.Second, "Run did not return after Stop")
	require.NoError(t, err)
}

// Stop runs from two places at once (the context waiter and the deferred
// call after Run returns). A Stop that starts while the first is still
// joining the workers must wait for them as well: returning at once lets
// its caller close the stores under a worker that is still running.
func TestSQSServerConcurrentStopsShareTheWorkerJoin(t *testing.T) {
	shortenSQSWorkerStopTimeout(t, 5*time.Second)
	s := NewSQSServer(nil, nil, nil)
	release := make(chan struct{})
	finished := make(chan struct{})
	require.True(t, s.workers.start(func() {
		<-release
		close(finished)
	}))
	firstStop := make(chan struct{})
	go func() {
		s.Stop()
		close(firstStop)
	}()
	time.Sleep(100 * time.Millisecond)
	go func() {
		time.Sleep(200 * time.Millisecond)
		close(release)
	}()

	s.Stop()
	select {
	case <-finished:
	default:
		t.Fatal("a concurrent Stop returned before the worker finished")
	}
	<-firstStop
}
