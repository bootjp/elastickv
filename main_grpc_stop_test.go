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
