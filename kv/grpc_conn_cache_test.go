package kv

import (
	"context"
	"net"
	"sync"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestGRPCConnCache_ConnForEmptyAddr(t *testing.T) {
	t.Parallel()

	var c GRPCConnCache
	conn, err := c.ConnFor("")
	require.Nil(t, conn)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrLeaderNotFound))
}

func TestGRPCConnCache_ReusesConnection(t *testing.T) {
	t.Parallel()

	var lc net.ListenConfig
	lis, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer()
	go func() {
		_ = srv.Serve(lis)
	}()
	t.Cleanup(func() {
		srv.Stop()
		_ = lis.Close()
	})

	addr := raft.ServerAddress(lis.Addr().String())

	var c GRPCConnCache
	conn1, err := c.ConnFor(addr)
	require.NoError(t, err)
	conn2, err := c.ConnFor(addr)
	require.NoError(t, err)
	require.Same(t, conn1, conn2)

	require.NoError(t, c.Close())

	conn3, err := c.ConnFor(addr)
	require.NoError(t, err)
	require.NotSame(t, conn1, conn3)

	require.NoError(t, c.Close())
}

func TestGRPCConnCache_ConcurrentConnFor(t *testing.T) {
	t.Parallel()

	var lc net.ListenConfig
	lis, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer()
	go func() {
		_ = srv.Serve(lis)
	}()
	t.Cleanup(func() {
		srv.Stop()
		_ = lis.Close()
	})

	addr := raft.ServerAddress(lis.Addr().String())

	var c GRPCConnCache

	const n = 32
	conns := make([]*grpc.ClientConn, n)
	errs := make([]error, n)

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			conn, err := c.ConnFor(addr)
			conns[i] = conn
			errs[i] = err
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		require.NoError(t, errs[i])
	}
	for i := 1; i < n; i++ {
		require.Same(t, conns[0], conns[i])
	}

	require.NoError(t, c.Close())
}
