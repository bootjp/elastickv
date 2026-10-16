package main

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestStopGRPCServerBoundedForcesStopOnLongLivedStream opens a stream whose
// handler only returns when its context ends, the shape of a catalog watch
// or a Raft transport stream. GracefulStop alone would wait on it for as
// long as the client stays connected; the bounded stop must force it closed
// after the timeout and return.
func TestStopGRPCServerBoundedForcesStopOnLongLivedStream(t *testing.T) {
	old := grpcGracefulStopTimeout
	grpcGracefulStopTimeout = 200 * time.Millisecond
	t.Cleanup(func() { grpcGracefulStopTimeout = old })

	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	started := make(chan struct{}, 1)
	srv := grpc.NewServer(grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		select {
		case started <- struct{}{}:
		default:
		}
		<-stream.Context().Done()
		return stream.Context().Err()
	}))
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		_ = srv.Serve(lis)
	}()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{StreamName: "Watch", ServerStreams: true, ClientStreams: true}, "/elastickv.test.LongLived/Watch")
	require.NoError(t, err)
	require.NotNil(t, stream)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("server handler did not start for the long-lived stream")
	}

	start := time.Now()
	stopGRPCServerBounded(context.Background(), srv, "test")
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 150*time.Millisecond, "GracefulStop must have been given its bound first")
	require.Less(t, elapsed, 3*time.Second, "the stop must not wait on the client that never disconnects")
	select {
	case <-serveDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after the forced stop")
	}
}

// A server with nothing in flight stops within GracefulStop, well before
// the bound, and nothing is forced.
func TestStopGRPCServerBoundedReturnsPromptlyWhenIdle(t *testing.T) {
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		_ = srv.Serve(lis)
	}()

	start := time.Now()
	stopGRPCServerBounded(context.Background(), srv, "idle")
	require.Less(t, time.Since(start), 2*time.Second)
	select {
	case <-serveDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after the stop")
	}
}

// newLongLivedStreamServer returns a gRPC server whose only handler holds
// the stream open until its context ends, plus a channel that receives once
// the handler has started and one that is closed when it returns.
func newLongLivedStreamServer() (srv *grpc.Server, started <-chan struct{}, ended <-chan struct{}) {
	startedCh := make(chan struct{}, 1)
	endedCh := make(chan struct{})
	srv = grpc.NewServer(grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		select {
		case startedCh <- struct{}{}:
		default:
		}
		<-stream.Context().Done()
		close(endedCh)
		return stream.Context().Err()
	}))
	return srv, startedCh, endedCh
}

// openLongLivedStream dials addr, opens a stream the server holds open for
// the rest of the test, and waits until the handler reports it started.
func openLongLivedStream(t *testing.T, addr string, started <-chan struct{}) {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	streamCtx, streamCancel := context.WithCancel(context.Background())
	t.Cleanup(streamCancel)
	_, err = conn.NewStream(streamCtx, &grpc.StreamDesc{StreamName: "Watch", ServerStreams: true, ClientStreams: true}, "/elastickv.test.LongLived/Watch")
	require.NoError(t, err)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("server handler did not start for the long-lived stream")
	}
}

// TestGRPCServeReturnsOnlyAfterBoundedStop pins the property the per-group
// serve task in startRaftServers relies on: grpc.Server.Serve does not
// return when GracefulStop closes the listener, only once the stop has
// finished, after the in-flight RPCs drained or the bound forced them
// closed. Were Serve to return earlier, the errgroup task would report the
// server as stopped while handlers were still running, and the cleanup
// would close the stores and Raft runtimes underneath them.
func TestGRPCServeReturnsOnlyAfterBoundedStop(t *testing.T) {
	old := grpcGracefulStopTimeout
	grpcGracefulStopTimeout = 200 * time.Millisecond
	t.Cleanup(func() { grpcGracefulStopTimeout = old })

	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv, started, ended := newLongLivedStreamServer()
	serveDone := make(chan error, 1)
	go func() { serveDone <- srv.Serve(lis) }()
	openLongLivedStream(t, lis.Addr().String(), started)

	stopDone := make(chan struct{})
	go func() {
		defer close(stopDone)
		stopGRPCServerBounded(context.Background(), srv, "test")
	}()
	// The listener is closed as soon as GracefulStop starts; Serve must still
	// be blocked while the drain waits on the stream, well inside the bound.
	select {
	case err := <-serveDone:
		t.Fatalf("Serve returned (%v) while the bounded stop was still draining", err)
	case <-time.After(100 * time.Millisecond):
	}
	select {
	case <-stopDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the bounded stop did not finish")
	}
	select {
	case err := <-serveDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after the bounded stop")
	}
	select {
	case <-ended:
	default:
		t.Fatal("Serve returned while the handler was still running")
	}
}
