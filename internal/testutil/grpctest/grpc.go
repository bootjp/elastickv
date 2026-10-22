// Package grpctest runs loopback gRPC servers with production message limits.
package grpctest

import (
	"testing"

	"github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/testutil"
	"google.golang.org/grpc"
)

// Serve registers services before starting a server and stops it on cleanup.
// Explicit opts override the production defaults.
func Serve(tb testing.TB, register func(*grpc.Server), opts ...grpc.ServerOption) string {
	tb.Helper()
	lis := testutil.Listen(tb)
	server := grpc.NewServer(append(internal.GRPCServerOptions(), opts...)...)
	tb.Cleanup(server.Stop)
	register(server)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = server.Serve(lis)
	}()
	tb.Cleanup(func() {
		server.Stop()
		<-done
	})
	return lis.Addr().String()
}

// Dial creates a lazy client using the production loopback dial options.
// Explicit opts can override credentials or message limits.
func Dial(tb testing.TB, addr string, opts ...grpc.DialOption) *grpc.ClientConn {
	tb.Helper()
	conn, err := grpc.NewClient(addr, append(internal.GRPCDialOptions(), opts...)...)
	if err != nil {
		tb.Fatalf("create gRPC client: %v", err)
	}
	tb.Cleanup(func() { _ = conn.Close() })
	return conn
}
