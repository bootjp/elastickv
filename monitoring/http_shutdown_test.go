package monitoring

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// stalledHandler never finishes on its own: it returns only when the server
// tears the connection down, like a profile request for 30 seconds or a
// scrape whose client stops reading.
func stalledHandler(started chan<- struct{}) http.Handler {
	return http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-r.Context().Done()
	})
}

func listenLoopback(t *testing.T) net.Listener {
	t.Helper()
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	return lis
}

// startStalledRequest opens a request against addr that the stalled handler
// will hold, waits until the handler is running, and returns a channel that
// yields the client's error once the request ends.
func startStalledRequest(t *testing.T, addr string, started <-chan struct{}) <-chan error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/", http.NoBody)
	require.NoError(t, err)
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	done := make(chan error, 1)
	go func() {
		resp, err := client.Do(req)
		if resp != nil {
			_ = resp.Body.Close()
		}
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not start for the stalled request")
	}
	return done
}

func TestShutdownHTTPServerForcesCloseAfterTimeout(t *testing.T) {
	lis := listenLoopback(t)
	started := make(chan struct{}, 1)
	srv := &http.Server{Handler: stalledHandler(started), ReadHeaderTimeout: time.Second}
	serveDone := make(chan error, 1)
	go func() { serveDone <- srv.Serve(lis) }()
	reqDone := startStalledRequest(t, lis.Addr().String(), started)

	start := time.Now()
	err := ShutdownHTTPServer(context.Background(), srv, "test", 200*time.Millisecond)
	elapsed := time.Since(start)
	require.NoError(t, err, "a forced close after the bound is the expected bounded shutdown, not a failure")
	require.GreaterOrEqual(t, elapsed, 150*time.Millisecond, "the graceful drain must be given its bound first")
	require.Less(t, elapsed, 3*time.Second, "the shutdown must not wait on a request that never finishes")

	select {
	case err := <-serveDone:
		require.ErrorIs(t, err, http.ErrServerClosed)
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after the forced close")
	}
	select {
	case err := <-reqDone:
		require.Error(t, err, "the stalled request must be cut off")
	case <-time.After(5 * time.Second):
		t.Fatal("the stalled request was not closed")
	}
}

func TestShutdownHTTPServerReturnsPromptlyWhenIdle(t *testing.T) {
	lis := listenLoopback(t)
	srv := &http.Server{Handler: http.NotFoundHandler(), ReadHeaderTimeout: time.Second}
	serveDone := make(chan error, 1)
	go func() { serveDone <- srv.Serve(lis) }()
	time.Sleep(20 * time.Millisecond)

	start := time.Now()
	require.NoError(t, ShutdownHTTPServer(context.Background(), srv, "idle", 5*time.Second))
	require.Less(t, time.Since(start), 2*time.Second)
	select {
	case <-serveDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after the shutdown")
	}
}

func TestShutdownHTTPServerNilIsNoop(t *testing.T) {
	require.NoError(t, ShutdownHTTPServer(context.Background(), nil, "nil", time.Second))
}

// The errgroup tasks main wires for the metrics and pprof listeners must
// return nil after a bounded, forced shutdown: a context.DeadlineExceeded
// from them would reach main after a SIGTERM, fail the "only cancellation"
// check, and turn the documented status-0 shutdown into a Fatalf.
func TestAuxiliaryShutdownTasksReturnNilAfterForcedClose(t *testing.T) {
	old := metricsShutdownTimeout
	metricsShutdownTimeout = 200 * time.Millisecond
	t.Cleanup(func() { metricsShutdownTimeout = old })

	cases := []struct {
		name     string
		serve    func(*http.Server, net.Listener, string) func() error
		shutdown func(context.Context, *http.Server, string) func() error
	}{
		{name: "metrics", serve: MetricsServeTask, shutdown: MetricsShutdownTask},
		{name: "pprof", serve: PprofServeTask, shutdown: PprofShutdownTask},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lis := listenLoopback(t)
			started := make(chan struct{}, 1)
			srv := &http.Server{Handler: stalledHandler(started), ReadHeaderTimeout: time.Second}
			ctx, cancel := context.WithCancel(context.Background())
			eg, egCtx := errgroup.WithContext(ctx)
			eg.Go(tc.serve(srv, lis, lis.Addr().String()))
			eg.Go(tc.shutdown(egCtx, srv, lis.Addr().String()))
			reqDone := startStalledRequest(t, lis.Addr().String(), started)

			start := time.Now()
			cancel()
			waitDone := make(chan error, 1)
			go func() { waitDone <- eg.Wait() }()
			select {
			case err := <-waitDone:
				require.NoError(t, err, "the bounded shutdown must not surface as an errgroup failure")
			case <-time.After(5 * time.Second):
				t.Fatal("the errgroup did not drain after cancellation")
			}
			elapsed := time.Since(start)
			require.GreaterOrEqual(t, elapsed, 150*time.Millisecond)
			require.Less(t, elapsed, 3*time.Second)
			select {
			case err := <-reqDone:
				require.Error(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("the stalled request was not closed")
			}
		})
	}
}
