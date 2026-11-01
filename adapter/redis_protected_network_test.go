package adapter

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/bootjp/elastickv/monitoring"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestRedisProtectedNetworkIngress(t *testing.T) {
	base := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	check := func(ctx context.Context) { require.Equal(t, "network-owner", ctx.Value(redisOwnershipTestKey{})) }
	st := &redisOwnershipReadStore{MVCCStore: base, onRead: check}
	coordinate := &redisContextCheckingCoordinator{Coordinator: newLocalAdapterCoordinator(base), check: check}
	s := NewRedisServer(listener, listener.Addr().String(), st, coordinate, nil, nil,
		WithRedisRequestObserver(monitoring.NewRegistry("protected-network", "local").RedisObserver()),
		WithRedisRequestAdmission(func(ctx context.Context) (RequestOwnership, error) {
			return &redisTestOwnership{ctx: context.WithValue(ctx, redisOwnershipTestKey{}, "network-owner")}, nil
		}))
	done := make(chan error, 1)
	go func() { done <- s.Run() }()
	t.Cleanup(func() {
		s.Stop()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("Redis Serve did not join")
		}
	})
	c, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	require.NoError(t, c.SetDeadline(time.Now().Add(5*time.Second)))
	_, err = io.WriteString(c, "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nw\r\n*1\r\n$4\r\nEXEC\r\n")
	require.NoError(t, err)
	refusal := "-" + errRedisTransactionOwnership.Error() + "\r\n"
	want := "+OK\r\n$1\r\nv\r\n" + refusal + refusal + refusal
	reply := make([]byte, len(want))
	_, err = io.ReadFull(c, reply)
	require.NoError(t, err)
	require.Equal(t, want, string(reply))
	_, err = io.WriteString(c, "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")
	require.NoError(t, err)
	reply = make([]byte, len("$1\r\nv\r\n"))
	_, err = io.ReadFull(c, reply)
	require.NoError(t, err)
	require.Equal(t, "$1\r\nv\r\n", string(reply), "refused pipelined SET must not execute outside its transaction")
	_, err = io.WriteString(c, "*2\r\n$9\r\nSUBSCRIBE\r\n$1\r\nc\r\n")
	require.NoError(t, err)
	want = "*3\r\n$9\r\nsubscribe\r\n$1\r\nc\r\n:1\r\n"
	reply = make([]byte, len(want))
	_, err = io.ReadFull(c, reply)
	require.NoError(t, err)
	require.Equal(t, want, string(reply))
	// Close is a join, not just a cancellation signal for the detached reader.
	require.NoError(t, s.Close())
	_, err = c.Read(make([]byte, 1))
	require.Error(t, err)
}
