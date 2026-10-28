package adapter

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/testutil"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

// stalledHandler never finishes a request on its own: it only returns once
// the server tears the connection down. That is the shape of a client that
// stalls mid-upload, which http.Server.Shutdown would wait on forever.
func stalledHandler(started chan<- struct{}) http.Handler {
	return http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-r.Context().Done()
	})
}

// httpAdapterUnderTest builds one HTTP adapter around a listener and
// exposes the pieces the bounded-stop tests need: the handler slot to
// stall, Run, and Stop.
type httpAdapterUnderTest struct {
	name  string
	build func(t *testing.T, lis net.Listener) (srv *http.Server, run func() error, stop func())
}

func httpAdaptersUnderTest() []httpAdapterUnderTest {
	return []httpAdapterUnderTest{
		{
			name: "dynamodb",
			build: func(_ *testing.T, lis net.Listener) (*http.Server, func() error, func()) {
				s := NewDynamoDBServer(lis, nil, nil)
				return s.httpServer, s.Run, s.Stop
			},
		},
		{
			name: "s3",
			build: func(_ *testing.T, lis net.Listener) (*http.Server, func() error, func()) {
				s := NewS3Server(lis, lis.Addr().String(), nil, nil, nil)
				return s.httpServer, s.Run, s.Stop
			},
		},
		{
			name: "sqs",
			build: func(_ *testing.T, lis net.Listener) (*http.Server, func() error, func()) {
				s := NewSQSServer(lis, nil, nil)
				return s.httpServer, s.Run, s.Stop
			},
		},
	}
}

func shortenHTTPShutdownTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	old := httpShutdownTimeout
	httpShutdownTimeout = d
	t.Cleanup(func() { httpShutdownTimeout = old })
}

func listenLoopback(t *testing.T) net.Listener {
	t.Helper()
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	return lis
}

// TestHTTPAdapterStopBoundsStalledRequest pins that Stop on each HTTP
// adapter returns within the shutdown bound even while a request is still
// in flight, and that the stalled connection is closed rather than left to
// finish on its own. Without the bound, a SIGTERM would wait on the client
// for as long as it stays connected while repeat signals are swallowed, so
// the WAL/store cleanup behind the errgroup would never run.
func TestHTTPAdapterStopBoundsStalledRequest(t *testing.T) {
	shortenHTTPShutdownTimeout(t, 200*time.Millisecond)
	for _, tc := range httpAdaptersUnderTest() {
		t.Run(tc.name, func(t *testing.T) {
			lis := listenLoopback(t)
			srv, run, stop := tc.build(t, lis)
			started := make(chan struct{}, 1)
			srv.Handler = stalledHandler(started)
			runDone := make(chan error, 1)
			go func() { runDone <- run() }()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+lis.Addr().String()+"/", http.NoBody)
			require.NoError(t, err)
			client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
			reqDone := make(chan error, 1)
			go func() {
				resp, err := client.Do(req)
				if resp != nil {
					_ = resp.Body.Close()
				}
				reqDone <- err
			}()
			testutil.Recv(t, started, 5*time.Second, "handler did not start for the stalled request")

			start := time.Now()
			stop()
			elapsed := time.Since(start)
			require.GreaterOrEqual(t, elapsed, 150*time.Millisecond, "graceful shutdown must be given its bound first")
			require.Less(t, elapsed, 3*time.Second, "Stop must not wait on a client that never finishes its request")

			select {
			case err := <-runDone:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("Run did not return after Stop")
			}
			select {
			case err := <-reqDone:
				require.Error(t, err, "the stalled request must be cut off, not completed")
			case <-time.After(5 * time.Second):
				t.Fatal("the stalled request was not closed by Stop")
			}
		})
	}
}

// With nothing in flight each adapter's Stop drains within the graceful
// phase and returns long before the bound.
func TestHTTPAdapterStopReturnsPromptlyWhenIdle(t *testing.T) {
	shortenHTTPShutdownTimeout(t, 5*time.Second)
	for _, tc := range httpAdaptersUnderTest() {
		t.Run(tc.name, func(t *testing.T) {
			lis := listenLoopback(t)
			_, run, stop := tc.build(t, lis)
			runDone := make(chan error, 1)
			go func() { runDone <- run() }()
			time.Sleep(20 * time.Millisecond)

			start := time.Now()
			stop()
			require.Less(t, time.Since(start), 2*time.Second)
			err := testutil.Recv(t, runDone, 5*time.Second, "Run did not return after Stop")
			require.NoError(t, err)
		})
	}
}

// Stop is called twice on the normal path (once from the context waiter,
// once from the deferred call after Run returns); the second call must be a
// prompt no-op even after the first one had to force connections closed.
func TestHTTPAdapterStopIsIdempotentAfterForcedClose(t *testing.T) {
	shortenHTTPShutdownTimeout(t, 100*time.Millisecond)
	lis := listenLoopback(t)
	srv, run, stop := httpAdaptersUnderTest()[0].build(t, lis)
	started := make(chan struct{}, 1)
	srv.Handler = stalledHandler(started)
	runDone := make(chan error, 1)
	go func() { runDone <- run() }()

	conn, err := (&net.Dialer{}).DialContext(context.Background(), "tcp", lis.Addr().String())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	_, err = conn.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"))
	require.NoError(t, err)
	testutil.Recv(t, started, 5*time.Second, "handler did not start")

	stop()
	start := time.Now()
	stop()
	require.Less(t, time.Since(start), 50*time.Millisecond, "second Stop must not wait the bound again")
	select {
	case err := <-runDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after Stop")
	}
}

// closeRecordingBlobCluster records when Stop closes the blob peer cluster.
type closeRecordingBlobCluster struct {
	*fakeS3BlobCluster
	closed atomic.Bool
}

func (c *closeRecordingBlobCluster) Close() error {
	c.closed.Store(true)
	return nil
}

// stopInBackground runs Stop on its own goroutine and returns a channel
// closed once it returned.
func stopInBackground(s *S3Server) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		s.Stop()
		close(done)
	}()
	return done
}

// requireStopStillDraining checks, partway into Stop's graceful window,
// that Stop has not returned and the blob cluster is still open.
func requireStopStillDraining(t *testing.T, stopDone <-chan struct{}, cluster *closeRecordingBlobCluster) {
	t.Helper()
	time.Sleep(100 * time.Millisecond)
	select {
	case <-stopDone:
		t.Fatal("Stop returned before its graceful window was over")
	default:
	}
	require.False(t, cluster.closed.Load(), "the blob cluster must stay open during the graceful window")
}

func waitStopped(t *testing.T, stopDone <-chan struct{}) {
	t.Helper()
	select {
	case <-stopDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return")
	}
}

// TestS3ServerStopDrainsHTTPBeforeClosingBlobCluster pins the order in
// Stop: while an S3 request is still running inside the graceful window
// the blob cluster stays open (its peer replication or fetch would
// otherwise fail at once), and the cluster is closed only once the HTTP
// drain has finished or forced the remaining connections closed. The
// check runs while Stop is still inside the window, not from the handler,
// because a forced Close does not wait for the handler to return.
func TestS3ServerStopDrainsHTTPBeforeClosingBlobCluster(t *testing.T) {
	shortenHTTPShutdownTimeout(t, 300*time.Millisecond)
	lis := listenLoopback(t)
	s := NewS3Server(lis, lis.Addr().String(), nil, nil, nil)
	cluster := &closeRecordingBlobCluster{fakeS3BlobCluster: newFakeS3BlobCluster()}
	s.blobCluster = cluster
	started := make(chan struct{}, 1)
	s.httpServer.Handler = stalledHandler(started)
	runDone := make(chan error, 1)
	go func() { runDone <- s.Run() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+lis.Addr().String()+"/", http.NoBody)
	require.NoError(t, err)
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	go func() {
		resp, _ := client.Do(req)
		if resp != nil {
			_ = resp.Body.Close()
		}
	}()
	testutil.Recv(t, started, 5*time.Second, "handler did not start")

	stopDone := stopInBackground(s)
	requireStopStillDraining(t, stopDone, cluster)
	waitStopped(t, stopDone)
	require.True(t, cluster.closed.Load(), "Stop must close the blob cluster once the HTTP drain is over")
	select {
	case err := <-runDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after Stop")
	}
}

// TestS3ServerStopWaitsForAdminObjectReadBeforeClosingBlobCluster pins
// that the admin listener's S3 object bridge, which calls into the same
// server outside its HTTP listener, keeps the blob cluster while an object
// operation is still running: with the S3 listener idle its drain is over
// at once, and closing the cluster then would fail the admin download (or
// upload) that is still fetching (or replicating) chunk blobs. Once Stop
// began, a new admin object operation is refused instead of running
// against a cluster that is about to close.
func TestS3ServerStopWaitsForAdminObjectReadBeforeClosingBlobCluster(t *testing.T) {
	shortenHTTPShutdownTimeout(t, 5*time.Second)
	st := store.NewMVCCStore()
	s := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)
	createBucketForAdminTest(t, s, "downloads")
	require.NoError(t, s.AdminPutObject(context.Background(),
		fullAdminBucketsPrincipal(), "downloads", "report.txt",
		strings.NewReader("hello, world"), "text/plain"))
	cluster := &closeRecordingBlobCluster{fakeS3BlobCluster: newFakeS3BlobCluster()}
	s.blobCluster = cluster

	body, _, err := s.AdminGetObject(context.Background(),
		fullAdminBucketsPrincipal(), "downloads", "report.txt")
	require.NoError(t, err)

	stopDone := stopInBackground(s)
	requireStopStillDraining(t, stopDone, cluster)
	got, err := io.ReadAll(body)
	require.NoError(t, err, "the admin download keeps reading during the graceful window")
	require.Equal(t, "hello, world", string(got))
	require.NoError(t, body.Close())
	waitStopped(t, stopDone)
	require.True(t, cluster.closed.Load(), "the cluster is closed once the admin read finished")

	err = s.AdminPutObject(context.Background(),
		fullAdminBucketsPrincipal(), "downloads", "late.txt",
		strings.NewReader("late"), "text/plain")
	require.ErrorIs(t, err, ErrAdminS3Stopping)
	_, _, err = s.AdminGetObject(context.Background(),
		fullAdminBucketsPrincipal(), "downloads", "report.txt")
	require.ErrorIs(t, err, ErrAdminS3Stopping)
}

// An admin object read whose body is never closed holds Stop for the
// bound only, then the cluster is closed anyway.
func TestS3ServerStopGivesUpOnUnclosedAdminReadAfterBound(t *testing.T) {
	shortenHTTPShutdownTimeout(t, 200*time.Millisecond)
	st := store.NewMVCCStore()
	s := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)
	createBucketForAdminTest(t, s, "downloads")
	require.NoError(t, s.AdminPutObject(context.Background(),
		fullAdminBucketsPrincipal(), "downloads", "report.txt",
		strings.NewReader("x"), "text/plain"))
	cluster := &closeRecordingBlobCluster{fakeS3BlobCluster: newFakeS3BlobCluster()}
	s.blobCluster = cluster
	body, _, err := s.AdminGetObject(context.Background(),
		fullAdminBucketsPrincipal(), "downloads", "report.txt")
	require.NoError(t, err)
	t.Cleanup(func() { _ = body.Close() })

	start := time.Now()
	s.Stop()
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 150*time.Millisecond, "the admin read must be given its bound")
	require.Less(t, elapsed, 3*time.Second, "Stop must not wait on a body nobody closes")
	require.True(t, cluster.closed.Load())
}

func shortenHTTPHandlerJoinTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	old := httpHandlerJoinTimeout
	httpHandlerJoinTimeout = d
	t.Cleanup(func() { httpHandlerJoinTimeout = old })
}

// slowUnwindingHandler stands for a handler still inside a store call when
// the server forces its connection closed: it notices the cancellation but
// takes unwind longer to return, or never returns while release
// stays open.
func slowUnwindingHandler(started chan<- struct{}, unwind time.Duration, release <-chan struct{}, finished *atomic.Bool) http.Handler {
	return http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-r.Context().Done()
		time.Sleep(unwind)
		<-release
		finished.Store(true)
	})
}

// startStalledRequest sends one request the handler will not finish and
// waits until the handler runs.
func startStalledRequest(t *testing.T, lis net.Listener, started <-chan struct{}) {
	t.Helper()
	conn, err := (&net.Dialer{}).DialContext(context.Background(), "tcp", lis.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	_, err = conn.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"))
	require.NoError(t, err)
	testutil.Recv(t, started, 5*time.Second, "handler did not start")
}

// TestHTTPAdapterStopJoinsHandlerAfterForcedClose pins that Stop does not
// return while a handler is still running on a connection the bound forced
// closed: http.Server.Close does not wait for handlers, so the caller would
// otherwise close the stores under a handler still unwinding out of a store
// call.
func TestHTTPAdapterStopJoinsHandlerAfterForcedClose(t *testing.T) {
	shortenHTTPShutdownTimeout(t, 100*time.Millisecond)
	shortenHTTPHandlerJoinTimeout(t, 5*time.Second)
	for _, tc := range httpAdaptersUnderTest() {
		t.Run(tc.name, func(t *testing.T) {
			lis := listenLoopback(t)
			srv, run, stop := tc.build(t, lis)
			started := make(chan struct{}, 1)
			release := make(chan struct{})
			close(release)
			var finished atomic.Bool
			srv.Handler = slowUnwindingHandler(started, 300*time.Millisecond, release, &finished)
			runDone := make(chan error, 1)
			go func() { runDone <- run() }()
			startStalledRequest(t, lis, started)

			stop()
			require.True(t, finished.Load(), "Stop returned while the handler on the closed connection was still running")
			err := testutil.Recv(t, runDone, 5*time.Second, "Run did not return after Stop")
			require.NoError(t, err)
		})
	}
}

// A handler that never returns holds Stop for the join bound only.
func TestHTTPAdapterStopBoundsJoinOfHandlerIgnoringClose(t *testing.T) {
	shortenHTTPShutdownTimeout(t, 100*time.Millisecond)
	shortenHTTPHandlerJoinTimeout(t, 400*time.Millisecond)
	lis := listenLoopback(t)
	srv, run, stop := httpAdaptersUnderTest()[0].build(t, lis)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	var finished atomic.Bool
	srv.Handler = slowUnwindingHandler(started, 0, release, &finished)
	runDone := make(chan error, 1)
	go func() { runDone <- run() }()
	startStalledRequest(t, lis, started)

	start := time.Now()
	stop()
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 400*time.Millisecond, "the handler must be given the join bound")
	require.Less(t, elapsed, 3*time.Second, "Stop must not wait on a handler that never returns")
	require.False(t, finished.Load())

	start = time.Now()
	stop()
	require.Less(t, time.Since(start), 50*time.Millisecond, "second Stop must not wait the join bound again")
	err := testutil.Recv(t, runDone, 5*time.Second, "Run did not return after Stop")
	require.NoError(t, err)
}
