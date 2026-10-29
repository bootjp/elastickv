package adapter

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/testutil"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

// blockingRedisCommand is a test-only command whose handler reports that it
// started and then waits until the test releases it, the shape of a command
// waiting on Raft (a SETEX with the leader unavailable).
const blockingRedisCommand = "BLOCKTEST"

// startBlockingRedisServer serves a RedisServer on a loopback listener with
// blockingRedisCommand installed and returns what the tests need to drive
// it: the server, the channel the handler reports on, the channel that
// releases the handler, and Run's result.
func startBlockingRedisServer(t *testing.T) (server *RedisServer, started <-chan struct{}, release chan struct{}, runDone <-chan error) {
	t.Helper()
	lis := listenLoopback(t)
	server = NewRedisServer(lis, lis.Addr().String(), nil, nil, nil, nil)
	startedCh := make(chan struct{}, 1)
	release = make(chan struct{})
	server.route[blockingRedisCommand] = func(conn redcon.Conn, _ redcon.Command) {
		select {
		case startedCh <- struct{}{}:
		default:
		}
		<-release
		conn.WriteString("OK")
	}
	done := make(chan error, 1)
	go func() { done <- server.Run() }()
	return server, startedCh, release, done
}

// sendBlockingRedisCommand opens a client connection and issues the
// blocking command on it; the connection stays open until the test ends.
func sendBlockingRedisCommand(t *testing.T, server *RedisServer, started <-chan struct{}) {
	t.Helper()
	conn, err := (&net.Dialer{}).DialContext(context.Background(), "tcp", server.listen.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	_, err = conn.Write([]byte("*1\r\n$9\r\n" + blockingRedisCommand + "\r\n"))
	require.NoError(t, err)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the blocking command did not start")
	}
}

func shortenRedisShutdownTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	old := redisShutdownTimeout
	redisShutdownTimeout = d
	t.Cleanup(func() { redisShutdownTimeout = old })
}

// TestRedisServerStopWaitsForInflightCommandWithinBound pins that Stop gives
// a command still being executed its bounded window instead of returning as
// soon as the listener is closed: redcon does not join its handlers, so
// without the wait the errgroup task in main would finish and the stores be
// closed underneath the handler. A command that never finishes is given up
// on after the bound, and the second Stop (the deferred one after Run
// returns) does not spend the bound again.
func TestRedisServerStopWaitsForInflightCommandWithinBound(t *testing.T) {
	shortenRedisShutdownTimeout(t, 200*time.Millisecond)
	server, started, release, runDone := startBlockingRedisServer(t)
	t.Cleanup(func() { close(release) })
	sendBlockingRedisCommand(t, server, started)

	start := time.Now()
	server.Stop()
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 150*time.Millisecond, "Stop must give the running command its bound")
	require.Less(t, elapsed, 3*time.Second, "Stop must not wait on a command that never finishes")
	err := testutil.Recv(t, runDone, 5*time.Second, "Run did not return after Stop")
	require.NoError(t, err)

	start = time.Now()
	server.Stop()
	require.Less(t, time.Since(start), 50*time.Millisecond, "second Stop must not wait the bound again")
}

// TestRedisServerStopReturnsOnceInflightCommandFinishes: a command that does
// finish inside the window ends the wait at once; Stop neither returns before
// it nor waits out the whole bound.
func TestRedisServerStopReturnsOnceInflightCommandFinishes(t *testing.T) {
	shortenRedisShutdownTimeout(t, 5*time.Second)
	server, started, release, runDone := startBlockingRedisServer(t)
	sendBlockingRedisCommand(t, server, started)
	go func() {
		time.Sleep(100 * time.Millisecond)
		close(release)
	}()

	start := time.Now()
	server.Stop()
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 80*time.Millisecond, "Stop must wait for the running command")
	require.Less(t, elapsed, 2*time.Second, "Stop must return as soon as the command finished, not after the bound")
	err := testutil.Recv(t, runDone, 5*time.Second, "Run did not return after Stop")
	require.NoError(t, err)
}

// With nothing in flight Stop returns at once.
func TestRedisServerStopReturnsPromptlyWhenIdle(t *testing.T) {
	shortenRedisShutdownTimeout(t, 5*time.Second)
	server, _, release, runDone := startBlockingRedisServer(t)
	close(release)
	time.Sleep(20 * time.Millisecond)

	start := time.Now()
	server.Stop()
	require.Less(t, time.Since(start), time.Second)
	err := testutil.Recv(t, runDone, 5*time.Second, "Run did not return after Stop")
	require.NoError(t, err)
}

// countingRedisCommand is a test-only command that only records that it
// ran.
const countingRedisCommand = "COUNTTEST"

// TestRedisServerStopRejectsPipelinedCommandAfterDrain pins that a command
// redcon hands over after the drain began is not executed. redcon parses a
// whole pipeline before it looks at the connection again, so the command
// behind a long-running one is dispatched even though Stop already closed
// the connection; once the running command finished, Stop has declared the
// drain complete, and the late command would run against stores the
// caller is about to close.
func TestRedisServerStopRejectsPipelinedCommandAfterDrain(t *testing.T) {
	shortenRedisShutdownTimeout(t, 5*time.Second)
	server, started, release, runDone := startBlockingRedisServer(t)
	var lateRuns atomic.Int32
	server.route[countingRedisCommand] = func(conn redcon.Conn, _ redcon.Command) {
		lateRuns.Add(1)
		conn.WriteString("OK")
	}
	conn, err := (&net.Dialer{}).DialContext(context.Background(), "tcp", server.listen.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	_, err = conn.Write([]byte("*1\r\n$9\r\n" + blockingRedisCommand + "\r\n*1\r\n$9\r\n" + countingRedisCommand + "\r\n"))
	require.NoError(t, err)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the blocking command did not start")
	}

	go func() {
		time.Sleep(100 * time.Millisecond)
		close(release)
	}()
	server.Stop()
	select {
	case err := <-runDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after Stop")
	}
	time.Sleep(200 * time.Millisecond)
	require.Zero(t, lateRuns.Load(), "a command handed over after the drain began must not run")
}
