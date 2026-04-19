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

func TestLeaseState_ExtendOverwritesEarlierAndLater(t *testing.T) {
	t.Parallel()
	var s leaseState
	now := time.Now()

	s.extend(now.Add(time.Hour))
	require.True(t, s.valid(now.Add(30*time.Minute)))

	// Shorter extension overwrites — last writer wins, mirroring the
	// single-atomic-pointer semantics. Practically this is rare because
	// real callers always extend by LeaseDuration() relative to "now",
	// which monotonically advances; documenting the behavior here.
	s.extend(now.Add(time.Minute))
	require.False(t, s.valid(now.Add(30*time.Minute)))
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
