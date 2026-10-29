package main

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/admin"
	"github.com/bootjp/elastickv/internal/testutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// TestRegisterAdminLifecycleReturnsCleanAfterForcedClose pins that an admin
// request outliving the drain window ends the admin tasks with a forced
// close and a nil error. Previously the shutdown task returned
// context.DeadlineExceeded, which is not a pure cancellation, so a SIGTERM
// during a slow admin request made run() report a failure and the process
// exit 1 instead of the documented status 0.
func TestRegisterAdminLifecycleReturnsCleanAfterForcedClose(t *testing.T) {
	old := adminShutdownTimeout
	adminShutdownTimeout = 200 * time.Millisecond
	t.Cleanup(func() { adminShutdownTimeout = old })

	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	started := make(chan struct{}, 1)
	httpSrv := &http.Server{
		Handler: http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			select {
			case started <- struct{}{}:
			default:
			}
			<-r.Context().Done()
		}),
		ReadHeaderTimeout: time.Second,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eg, egCtx := errgroup.WithContext(ctx)
	registerAdminLifecycle(egCtx, eg, httpSrv, lis, &admin.Config{Listen: lis.Addr().String()}, false, "test")

	reqCtx, reqCancel := context.WithCancel(context.Background())
	defer reqCancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, "http://"+lis.Addr().String()+"/", http.NoBody)
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
	testutil.Recv(t, started, 5*time.Second, "admin handler did not start for the stalled request")

	start := time.Now()
	cancel()
	waitDone := make(chan error, 1)
	go func() { waitDone <- eg.Wait() }()
	select {
	case err := <-waitDone:
		require.NoError(t, err, "a forced close after the admin drain window must not fail the shutdown")
	case <-time.After(5 * time.Second):
		t.Fatal("the admin tasks did not drain after cancellation")
	}
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 150*time.Millisecond, "the drain window must be given first")
	require.Less(t, elapsed, 3*time.Second, "the shutdown must not wait on the stalled request")
	select {
	case err := <-reqDone:
		require.Error(t, err, "the stalled admin request must be cut off")
	case <-time.After(5 * time.Second):
		t.Fatal("the stalled admin request was not closed")
	}
}

// An admin object request that reaches an S3 server which already began
// stopping is answered like a follower's (503 with Retry-After), so the
// client retries against another node instead of seeing a 500.
func TestTranslateAdminObjectsErrorMapsStoppingToNotLeader(t *testing.T) {
	err := translateAdminObjectsError(errors.Wrap(adapter.ErrAdminS3Stopping, "admin put"))
	require.ErrorIs(t, err, admin.ErrObjectsNotLeader)
}
