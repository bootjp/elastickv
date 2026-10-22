package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/admin"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestAdminHandlerDrainJoinsHandlerAfterForcedClose(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	srv := &http.Server{ReadHeaderTimeout: time.Second, Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(entered)
		<-release
	})}
	drain := trackAdminHandlers(srv)
	go srv.Handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil))
	<-entered
	done := make(chan struct{})
	go func() { drain.join(time.Second); close(done) }()
	select {
	case <-done:
		t.Fatal("join returned while the admitted handler was running")
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handler did not join")
	}
	response := httptest.NewRecorder()
	srv.Handler.ServeHTTP(response, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil))
	require.Equal(t, http.StatusServiceUnavailable, response.Code)
}

func TestAdminLifecycleWaitsForForceClosedHandler(t *testing.T) {
	old := adminShutdownTimeout
	adminShutdownTimeout = 10 * time.Millisecond
	t.Cleanup(func() { adminShutdownTimeout = old })
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	srv := &http.Server{ReadHeaderTimeout: time.Second, Handler: http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
		close(canceled)
		<-release
	})}
	listenConfig := &net.ListenConfig{}
	listener, err := listenConfig.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	group := &errgroup.Group{}
	registerAdminLifecycle(ctx, group, srv, listener, &admin.Config{Listen: listener.Addr().String()}, false, "test")
	clientDone := make(chan struct{})
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+listener.Addr().String(), nil)
	require.NoError(t, err)
	go func() {
		defer close(clientDone)
		client := &http.Client{Timeout: time.Second}
		response, requestErr := client.Do(request)
		if requestErr == nil {
			_ = response.Body.Close()
		}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("admin handler did not start")
	}
	cancel()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("admin connection was not forced closed")
	}
	done := make(chan error, 1)
	go func() { done <- group.Wait() }()
	select {
	case <-done:
		t.Fatal("admin lifecycle returned before its handler unwound")
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-done)
	<-clientDone
}
