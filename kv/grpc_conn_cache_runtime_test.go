package kv

import (
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestGRPCConnCacheRuntimeCapabilityIsImmutableAndRechecked(t *testing.T) {
	var allowed atomic.Bool
	allowed.Store(true)
	denied := errors.New("runtime pinned roster rejected address")
	provider := func(address string) ([]grpc.DialOption, error) {
		if address != "runtime.test:1" || !allowed.Load() {
			return nil, denied
		}
		return []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, nil
	}
	var cache GRPCConnCache
	require.NoError(t, cache.SetDialOptions(provider))
	require.Error(t, cache.SetDialOptions(provider))
	conn, err := cache.ConnFor("runtime.test:1")
	require.NoError(t, err)
	again, err := cache.ConnFor("runtime.test:1")
	require.NoError(t, err)
	require.Same(t, conn, again)
	allowed.Store(false)
	_, err = cache.ConnFor("runtime.test:1")
	require.ErrorIs(t, err, denied, "a cached connection cannot bypass current target validation")
	_, err = cache.ConnFor("foreign.test:1")
	require.ErrorIs(t, err, denied)
	require.NoError(t, cache.Close())
	allowed.Store(true)
	_, err = cache.ConnFor("runtime.test:1")
	require.ErrorContains(t, err, "protected gRPC connection cache is closed")
	legacy := &GRPCConnCache{}
	_, err = legacy.ConnFor("unused.test:1")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, legacy.Close()) })
	require.Error(t, legacy.SetDialOptions(provider), "an already used insecure cache cannot be repurposed")
}

func TestGRPCConnCacheCloseCannotPublishConcurrentProtectedDial(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	cache := NewGRPCConnCache(func(string) ([]grpc.DialOption, error) {
		close(entered)
		<-release
		return []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, nil
	})
	result := make(chan error, 1)
	go func() { _, err := cache.ConnFor("runtime.test:1"); result <- err }()
	<-entered
	require.NoError(t, cache.Close())
	close(release)
	require.ErrorContains(t, <-result, "cache closed during dial")
	cache.mu.Lock()
	require.Empty(t, cache.conns)
	cache.mu.Unlock()
}
