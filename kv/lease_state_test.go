package kv

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLeaseState_NilReceiverIsAlwaysExpired(t *testing.T) {
	t.Parallel()
	var s *leaseState
	require.False(t, s.valid(time.Now()))
	s.extend(time.Now().Add(time.Hour)) // must not panic
	s.invalidate()                      // must not panic
	require.False(t, s.valid(time.Now()))
}

func TestLeaseState_ZeroValueIsExpired(t *testing.T) {
	t.Parallel()
	var s leaseState
	require.False(t, s.valid(time.Now()))
}

func TestLeaseState_ExtendAndExpire(t *testing.T) {
	t.Parallel()
	var s leaseState
	now := time.Now()
	s.extend(now.Add(50 * time.Millisecond))

	require.True(t, s.valid(now))
	require.True(t, s.valid(now.Add(49*time.Millisecond)))
	require.False(t, s.valid(now.Add(50*time.Millisecond)))
	require.False(t, s.valid(now.Add(time.Hour)))
}

func TestLeaseState_InvalidateClears(t *testing.T) {
	t.Parallel()
	var s leaseState
	now := time.Now()
	s.extend(now.Add(time.Hour))
	require.True(t, s.valid(now))

	s.invalidate()
	require.False(t, s.valid(now))
}

func TestLeaseState_ExtendIsMonotonic(t *testing.T) {
	t.Parallel()
	var s leaseState
	now := time.Now()

	s.extend(now.Add(time.Hour))
	require.True(t, s.valid(now.Add(30*time.Minute)))

	// A shorter extension must NOT regress the lease: an out-of-order
	// writer that sampled time.Now() earlier could otherwise prematurely
	// expire a freshly extended lease and force callers into the slow
	// path while the leader is still confirmed.
	s.extend(now.Add(time.Minute))
	require.True(t, s.valid(now.Add(30*time.Minute)))

	// A strictly longer extension wins.
	s.extend(now.Add(2 * time.Hour))
	require.True(t, s.valid(now.Add(90*time.Minute)))
}

func TestLeaseState_InvalidateBeatsConcurrentExtend(t *testing.T) {
	t.Parallel()
	var s leaseState
	now := time.Now()
	s.extend(now.Add(time.Hour))

	// invalidate stores nil unconditionally, even when the current expiry
	// is in the future. Otherwise leadership-loss callbacks would be
	// powerless once a lease is in place.
	s.invalidate()
	require.False(t, s.valid(now))
}

func TestLeaseState_ConcurrentExtendAndRead(t *testing.T) {
	t.Parallel()
	var s leaseState
	stop := make(chan struct{})
	done := make(chan struct{}, 2)

	go func() {
		defer func() { done <- struct{}{} }()
		for {
			select {
			case <-stop:
				return
			default:
				s.extend(time.Now().Add(time.Second))
			}
		}
	}()
	go func() {
		defer func() { done <- struct{}{} }()
		for {
			select {
			case <-stop:
				return
			default:
				_ = s.valid(time.Now())
			}
		}
	}()

	time.Sleep(20 * time.Millisecond)
	close(stop)
	<-done
	<-done
}
