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

// TestLeaseState_ZeroNowFailsClosed pins the "clock unavailable"
// invariant: if the caller's monoclock.Now() read failed (e.g.
// clock_gettime denied under seccomp) and surfaced monoclock.Zero,
// valid() MUST return false even for a freshly warmed lease.
// Otherwise a persistent clock failure would keep the node serving
// fast-path reads off a stale lease indefinitely.
func TestLeaseState_ZeroNowFailsClosed(t *testing.T) {
	t.Parallel()
	var s leaseState
	s.extend(monoclock.Now().Add(time.Hour), s.generation())
	require.True(t, s.valid(monoclock.Now()), "sanity: warmed lease is valid for a real now")
	require.False(t, s.valid(monoclock.Zero),
		"a zero now signals clock failure; lease must fail closed")
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

// TestLeaseState_StaleExtendCannotClobberFreshLeaseSameExpiry pins the
// invariant that a stale-generation extend (a Dispatch that captured the
// pre-invalidate generation) must never clobber a fresh lease installed
// at a newer generation, even when both compute the SAME expiry value
// because their monoclock samples collided at clock granularity. The
// generation guard, not the expiry value, is what disambiguates the two
// writers: extend stores only when its captured generation still
// matches the live one.
//
// Scenario:
//  1. Extender A captures genA = 0 (BEFORE its quorum op).
//  2. invalidate fires (leader-loss callback), bumping gen to 1 and
//     clearing the expiry.
//  3. A concurrent extender B captures genB = 1 (after invalidate) and
//     installs a fresh lease with target T.
//  4. A's delayed extend(T, genA=0) runs LAST with the SAME target T.
//     It must be dropped by the generation guard, leaving B's lease
//     untouched.
func TestLeaseState_StaleExtendCannotClobberFreshLeaseSameExpiry(t *testing.T) {
	t.Parallel()
	var s leaseState
	now := monoclock.Now()
	target := now.Add(time.Hour)

	// Step 1: A captures the pre-invalidate generation.
	genA := s.generation()

	// Step 2: leader-loss invalidate fires during A's quorum op.
	s.invalidate()
	require.NotEqual(t, genA, s.generation(), "invalidate must bump the generation")

	// Step 3: B captures the post-invalidate generation and installs a
	// fresh lease with the exact same target value A will compute.
	genB := s.generation()
	s.extend(target, genB)
	require.True(t, s.valid(now), "B's fresh lease must be valid")
	expiryAfterB := s.expiryNanos.Load()

	// Step 4: A's delayed extend with the stale generation and the same
	// target. The generation guard must drop it.
	s.extend(target, genA)

	require.True(t, s.valid(now),
		"fresh lease with matching expiry must survive a stale extender's write")
	require.Equal(t, expiryAfterB, s.expiryNanos.Load(),
		"stale-generation extend must not overwrite the live expiry, even with an equal value")
}

// TestLeaseState_StaleExtendAfterInvalidateIsNoop is the negative
// control: when a stale-generation extend races an invalidate and NO
// concurrent fresh extender re-warms the lease, the lease stays cleared.
// A re-entered caller that captures the post-invalidate generation can
// then warm and clear it normally.
func TestLeaseState_StaleExtendAfterInvalidateIsNoop(t *testing.T) {
	t.Parallel()
	var s leaseState
	now := monoclock.Now()

	// Caller pattern: sample generation, then quorum op races with
	// invalidate, then extend with the now-stale generation.
	expectedGen := s.generation()
	s.invalidate() // concurrent leader-loss during the quorum op

	// extend with the pre-invalidate generation: the guard sees the
	// mismatch and drops the write. The invalidate's cleared state
	// remains.
	s.extend(now.Add(time.Hour), expectedGen)
	require.False(t, s.valid(now),
		"pre-invalidate extend must not resurrect the lease")

	// A re-entered caller samples a FRESH generation, warms the lease,
	// then a later invalidate clears it.
	freshGen := s.generation()
	s.extend(now.Add(time.Hour), freshGen)
	require.True(t, s.valid(now))
	s.invalidate()
	require.False(t, s.valid(now),
		"invalidate after a successful extend must clear the lease")
}

// TestLeaseState_InvalidateClearsExpiryBeforeBumpingGen pins the store
// ordering inside invalidate(): the expiry sentinel must be cleared
// BEFORE the generation is bumped. valid() reads only expiryNanos and
// generation() reads only gen, so if the generation were bumped first a
// reader could observe the post-invalidate generation together with the
// still-valid old expiry and serve a fast-path read after leadership
// loss was already detected.
//
// The intra-invalidate hook fires after the expiry store but before the
// generation bump; with the correct ordering valid() must already report
// false at that point. With the buggy ordering (gen bumped first) the
// hook would observe a still-valid lease and the assertion fails.
func TestLeaseState_InvalidateClearsExpiryBeforeBumpingGen(t *testing.T) {
	// Not parallel: mutates the package-level onInvalidateBetweenStores
	// hook.
	var s leaseState
	now := monoclock.Now()
	s.extend(now.Add(time.Hour), s.generation())
	require.True(t, s.valid(now), "sanity: warmed lease is valid before invalidate")

	genBefore := s.generation()

	var (
		validInWindow bool
		genInWindow   uint64
		hookFired     bool
	)
	onInvalidateBetweenStores = func() {
		hookFired = true
		// At this point invalidate() has cleared the expiry but has not
		// yet bumped the generation. A lock-free reader interleaving here
		// must already see the lease as invalid.
		validInWindow = s.valid(now)
		genInWindow = s.generation()
	}
	t.Cleanup(func() { onInvalidateBetweenStores = nil })

	s.invalidate()

	require.True(t, hookFired, "intra-invalidate hook must fire")
	require.False(t, validInWindow,
		"expiry must be cleared before the generation bump so valid() fails closed as early as the gen bump")
	require.Equal(t, genBefore, genInWindow,
		"generation must not be bumped before the expiry is cleared")
	require.False(t, s.valid(now), "lease stays invalid after invalidate completes")
	require.NotEqual(t, genBefore, s.generation(), "invalidate must bump the generation")
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

// BenchmarkLeaseStateExtend pins the zero-allocation acceptance
// criterion for the write path. Every successful extend installs a new
// expiry; the int64-atomic layout must not heap-allocate (the previous
// pointer-swap design allocated one slot per call, 1:1 with Dispatch
// throughput). Run with -benchmem; allocs/op MUST be 0.
//
// Each iteration advances the target by 1ns so the monotonic gate
// always takes the store branch (the worst case for allocation), rather
// than short-circuiting on a non-increasing target.
func BenchmarkLeaseStateExtend(b *testing.B) {
	var s leaseState
	base := monoclock.Now()
	gen := s.generation()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.extend(base.Add(time.Duration(i+1)), gen)
	}
}

// BenchmarkLeaseStateValid pins the hot read path: a single atomic load
// and comparison, zero allocations.
func BenchmarkLeaseStateValid(b *testing.B) {
	var s leaseState
	now := monoclock.Now()
	s.extend(now.Add(time.Hour), s.generation())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.valid(now)
	}
}
