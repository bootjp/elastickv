package kv

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLeaderProxyCircuitBreakerOpensAndLimitsHalfOpenProbe(t *testing.T) {
	t.Parallel()

	var breaker leaderProxyCircuitBreaker
	identity := leaderProxyIdentity{id: "n1", address: "127.0.0.1:50051", term: 7}
	now := time.Unix(100, 0)

	for attempt := 1; attempt <= leaderProxyBreakerFailureThreshold; attempt++ {
		require.NoError(t, breaker.allow(identity, 1, now))
		breaker.record(identity, 1, ErrLeaderNotFound, now)
	}

	err := breaker.allow(identity, 1, now.Add(time.Millisecond))
	require.ErrorIs(t, err, ErrLeaderProxyCircuitOpen)
	require.True(t, breaker.owns(identity, 1))

	err = breaker.allow(identity, 2, now.Add(time.Millisecond))
	require.ErrorIs(t, err, ErrLeaderProxyCircuitOpen)
	require.False(t, breaker.owns(identity, 2))

	probeAt := now.Add(leaderProxyBreakerBaseBackoff)
	require.NoError(t, breaker.allow(identity, 2, probeAt))
	require.True(t, breaker.owns(identity, 2))
	require.ErrorIs(t, breaker.allow(identity, 3, probeAt), ErrLeaderProxyCircuitOpen)

	breaker.record(identity, 2, nil, probeAt)
	require.NoError(t, breaker.allow(identity, 3, probeAt))
}

func TestLeaderProxyCircuitBreakerResetsOnLeaderIdentityChange(t *testing.T) {
	t.Parallel()

	var breaker leaderProxyCircuitBreaker
	oldLeader := leaderProxyIdentity{id: "n1", address: "127.0.0.1:50051", term: 7}
	newLeader := leaderProxyIdentity{id: "n2", address: "127.0.0.1:50052", term: 8}
	now := time.Unix(100, 0)

	for range leaderProxyBreakerFailureThreshold {
		require.NoError(t, breaker.allow(oldLeader, 1, now))
		breaker.record(oldLeader, 1, ErrLeaderNotFound, now)
	}
	require.ErrorIs(t, breaker.allow(oldLeader, 2, now), ErrLeaderProxyCircuitOpen)
	require.True(t, breaker.mayRetryAfterOpen(newLeader, 2))
	require.NoError(t, breaker.allow(newLeader, 2, now))
	require.False(t, breaker.owns(oldLeader, 1))
}

func TestLeaderProxyCircuitBreakerIgnoresLateResultFromOldIdentity(t *testing.T) {
	t.Parallel()

	var breaker leaderProxyCircuitBreaker
	oldLeader := leaderProxyIdentity{id: "n1", address: "127.0.0.1:50051", term: 7}
	newLeader := leaderProxyIdentity{id: "n2", address: "127.0.0.1:50052", term: 8}
	now := time.Unix(100, 0)

	for range leaderProxyBreakerFailureThreshold - 1 {
		require.NoError(t, breaker.allow(newLeader, 2, now))
		breaker.record(newLeader, 2, ErrLeaderNotFound, now)
	}
	breaker.record(oldLeader, 1, nil, now)
	require.NoError(t, breaker.allow(newLeader, 2, now))
	breaker.record(newLeader, 2, ErrLeaderNotFound, now)

	require.ErrorIs(t, breaker.allow(newLeader, 3, now), ErrLeaderProxyCircuitOpen)
}

func TestLeaderProxyCircuitBreakerIgnoresLateFailureFromSuppressedRequest(t *testing.T) {
	t.Parallel()

	var breaker leaderProxyCircuitBreaker
	identity := leaderProxyIdentity{id: "n1", address: "127.0.0.1:50051", term: 7}
	now := time.Unix(100, 0)

	for range leaderProxyBreakerFailureThreshold {
		require.NoError(t, breaker.allow(identity, 1, now))
		breaker.record(identity, 1, ErrLeaderNotFound, now)
	}
	breaker.record(identity, 2, ErrLeaderNotFound, now.Add(time.Millisecond))
	breaker.record(identity, 2, ErrLeaderNotFound, now.Add(leaderProxyBreakerBaseBackoff))

	require.True(t, breaker.owns(identity, 1))
	require.NoError(t, breaker.allow(identity, 2, now.Add(leaderProxyBreakerBaseBackoff)))
	require.True(t, breaker.owns(identity, 2))
}

func TestLeaderProxyCircuitBreakerReleasesCanceledHalfOpenProbe(t *testing.T) {
	t.Parallel()

	var breaker leaderProxyCircuitBreaker
	identity := leaderProxyIdentity{id: "n1", address: "127.0.0.1:50051", term: 7}
	now := time.Unix(100, 0)

	for range leaderProxyBreakerFailureThreshold {
		require.NoError(t, breaker.allow(identity, 1, now))
		breaker.record(identity, 1, ErrLeaderNotFound, now)
	}
	probeAt := now.Add(leaderProxyBreakerBaseBackoff)
	require.NoError(t, breaker.allow(identity, 2, probeAt))

	breaker.release(identity, 2)

	require.False(t, breaker.owns(identity, 2))
	require.NoError(t, breaker.allow(identity, 3, probeAt))
	require.True(t, breaker.owns(identity, 3))
}

func TestLeaderProxyCircuitBreakerAllowsOneConcurrentHalfOpenProbe(t *testing.T) {
	t.Parallel()

	var breaker leaderProxyCircuitBreaker
	identity := leaderProxyIdentity{id: "n1", address: "127.0.0.1:50051", term: 7}
	now := time.Unix(100, 0)
	for range leaderProxyBreakerFailureThreshold {
		require.NoError(t, breaker.allow(identity, 1, now))
		breaker.record(identity, 1, ErrLeaderNotFound, now)
	}

	const requests = 32
	start := make(chan struct{})
	var allowed atomic.Int32
	var wg sync.WaitGroup
	for requestID := uint64(2); requestID < requests+2; requestID++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if breaker.allow(identity, requestID, now.Add(leaderProxyBreakerBaseBackoff)) == nil {
				allowed.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	require.Equal(t, int32(1), allowed.Load())
}

func TestLeaderProxyBreakerBackoffIsBounded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		failures int
		want     time.Duration
	}{
		{leaderProxyBreakerFailureThreshold, 100 * time.Millisecond},
		{leaderProxyBreakerFailureThreshold + 1, 200 * time.Millisecond},
		{leaderProxyBreakerFailureThreshold + 4, 1600 * time.Millisecond},
		{leaderProxyBreakerFailureThreshold + 5, leaderProxyBreakerMaxBackoff},
		{leaderProxyBreakerFailureThreshold + 20, leaderProxyBreakerMaxBackoff},
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, leaderProxyBreakerBackoff(tt.failures))
	}
}

func TestLeaderProxyBreakerFailureClassification(t *testing.T) {
	t.Parallel()

	require.True(t, isLeaderProxyBreakerFailure(ErrLeaderNotFound))
	require.True(t, isLeaderProxyBreakerFailure(status.Error(codes.Unavailable, "connection reset")))
	require.True(t, isLeaderProxyBreakerFailure(status.Error(codes.DeadlineExceeded, "forward timeout")))
	require.False(t, isLeaderProxyBreakerFailure(errors.New("write conflict")))
	require.False(t, isLeaderProxyBreakerFailure(nil))
}
