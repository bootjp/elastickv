package kv

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/monoclock"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
)

func TestIsLeadershipLossError(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"unrelated", errors.New("write conflict"), false},
		{"context canceled", context.Canceled, false},
		{"raftengine ErrNotLeader direct", raftengine.ErrNotLeader, true},
		{"raftengine ErrLeadershipLost direct", raftengine.ErrLeadershipLost, true},
		{"raftengine ErrLeadershipTransferInProgress direct", raftengine.ErrLeadershipTransferInProgress, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, isLeadershipLossError(tc.err))
		})
	}
}

func TestLeaseState_NilReceiverIsAlwaysExpired(t *testing.T) {
	t.Parallel()
	var s *leaseState
	require.False(t, s.valid(monoclock.Now()))
	s.extend(monoclock.Now().Add(time.Hour), s.generation()) // must not panic
	s.invalidate()                                           // must not panic
	require.False(t, s.valid(monoclock.Now()))
}

func TestLeaseState_ZeroValueIsExpired(t *testing.T) {
	t.Parallel()
	var s leaseState
	require.False(t, s.valid(monoclock.Now()))
}

func TestLeaseState_ExtendAndExpire(t *testing.T) {
	t.Parallel()
	var s leaseState
	now := monoclock.Now()
	s.extend(now.Add(50*time.Millisecond), s.generation())

	require.True(t, s.valid(now))
	require.True(t, s.valid(now.Add(49*time.Millisecond)))
	require.False(t, s.valid(now.Add(50*time.Millisecond)))
	require.False(t, s.valid(now.Add(time.Hour)))
}

func TestLeaseState_InvalidateClears(t *testing.T) {
	t.Parallel()
	var s leaseState
	now := monoclock.Now()
	s.extend(now.Add(time.Hour), s.generation())
	require.True(t, s.valid(now))

	s.invalidate()
	require.False(t, s.valid(now))
}

func TestLeaseState_ExtendIsMonotonic(t *testing.T) {
	t.Parallel()
	var s leaseState
	now := monoclock.Now()

	s.extend(now.Add(time.Hour), s.generation())
	require.True(t, s.valid(now.Add(30*time.Minute)))

	// A shorter extension must NOT regress the lease: an out-of-order
	// writer that sampled monoclock.Now() earlier could otherwise
	// prematurely expire a freshly extended lease and force callers
	// into the slow path while the leader is still confirmed.
	s.extend(now.Add(time.Minute), s.generation())
	require.True(t, s.valid(now.Add(30*time.Minute)))

	// A strictly longer extension wins.
	s.extend(now.Add(2*time.Hour), s.generation())
	require.True(t, s.valid(now.Add(90*time.Minute)))
}

func TestLeaseState_InvalidateBeatsConcurrentExtend(t *testing.T) {
	t.Parallel()
	var s leaseState
	now := monoclock.Now()
	s.extend(now.Add(time.Hour), s.generation())

	// invalidate stores the zero sentinel unconditionally, even when
	// the current expiry is in the future. Otherwise leadership-loss
	// callbacks would be powerless once a lease is in place.
	s.invalidate()
	require.False(t, s.valid(now))
}

// TestLeaseState_ExtendCannotResurrectAfterInvalidate exercises the
// generation-guard invariant: an extend that captured the pre-invalidate
// generation must not install a fresh lease after a concurrent
// invalidate has bumped the generation.
func TestLeaseState_ExtendCannotResurrectAfterInvalidate(t *testing.T) {
	t.Parallel()
	var s leaseState
	now := monoclock.Now()

	// Caller pattern: sample generation BEFORE the quorum operation.
	expectedGen := s.generation()

	// Leader-loss callback fires during the "quorum operation".
	s.invalidate()
	require.NotEqual(t, expectedGen, s.generation(),
		"invalidate must bump the generation")

	// Caller returns with success and calls extend with the stale
	// expected-generation. Must be a no-op.
	s.extend(now.Add(time.Hour), expectedGen)
	require.False(t, s.valid(now),
		"stale-generation extend must NOT resurrect the lease")
}

// TestLeaseState_ExtendWithFreshGenSucceedsAfterInvalidate verifies the
// dual to the above: a caller that captured the post-invalidate
// generation CAN install a fresh lease, so recovery from a brief
// leader-loss is possible.
func TestLeaseState_ExtendWithFreshGenSucceedsAfterInvalidate(t *testing.T) {
	t.Parallel()
	var s leaseState
	now := monoclock.Now()

	s.invalidate()
	freshGen := s.generation()
	s.extend(now.Add(time.Hour), freshGen)
	require.True(t, s.valid(now))
}

func TestLeaseState_ConcurrentExtendAndRead(t *testing.T) {
	t.Parallel()
	var s leaseState
	stop := make(chan struct{})
	done := make(chan struct{}, 2)

	// Cooperative scheduling: runtime.Gosched() between iterations keeps
	// the workers from pegging a core while still interleaving enough
	// extend/valid pairs under `-race` to exercise the atomic-int64
	// invariants.
	go func() {
		defer func() { done <- struct{}{} }()
		for {
			select {
			case <-stop:
				return
			default:
				gen := s.generation()
				s.extend(monoclock.Now().Add(time.Second), gen)
				runtime.Gosched()
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
				_ = s.valid(monoclock.Now())
				runtime.Gosched()
			}
		}
	}()

	time.Sleep(20 * time.Millisecond)
	close(stop)
	<-done
	<-done
}
