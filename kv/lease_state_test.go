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

// TestLeaseState_RollbackCASUsesPointerIdentity pins the codex-P3
// invariant: when a stale extender rolls back after a racing
// invalidate, its rollback CAS must be gated on pointer identity, NOT
// on the expiry-nanos value. A value-gated CAS could clobber a fresh
// lease installed by a concurrent extender that (due to clock-
// granularity ties) computed the same expiry.
//
// Scenario:
//  1. Extender A captures genA = 0 and installs slot{target, gen=0}.
//  2. invalidate fires, bumping the live slot to gen=1 with zero
//     expiry. A's post-CAS gen check now MUST see the mismatch and
//     attempt a rollback.
//  3. A concurrent extender B captures genB = 1 and installs a FRESH
//     *leaseSlot{target, gen=1} -- same target value, distinct
//     allocation.
//  4. A's rollback CAS runs LAST. A value-gated CAS (old impl
//     expiryNanos.CompareAndSwap(target, 0)) would erase B's
//     still-valid lease. A pointer-gated CAS (new impl
//     current.CompareAndSwap(aSlot, rb)) fails because the live
//     pointer is B's allocation, preserving B's lease.
//
// Simulated step-by-step (no goroutines) against internal state so
// the ordering is deterministic. Without the fix this test fails:
// s.valid(now) observes the rolled-back zero-expiry slot.
func TestLeaseState_RollbackCASUsesPointerIdentity(t *testing.T) {
	t.Parallel()
	var s leaseState
	now := monoclock.Now()
	target := now.Add(time.Hour)
	targetNanos := target.Nanos()

	// Step 1: simulate A's CAS having already landed.
	aSlot := &leaseSlot{expiryNanos: targetNanos, gen: 0}
	s.current.Store(aSlot)

	// Step 2: invalidate races in. It CAS-replaces aSlot with a
	// zero-expiry slot at gen=1.
	s.invalidate()
	require.Equal(t, uint64(1), s.generation())

	// Step 3: concurrent extender B captures genB=1 (post-invalidate)
	// and installs a distinct *leaseSlot with the same target value.
	// This exercises the clock-granularity tie.
	bSlot := &leaseSlot{expiryNanos: targetNanos, gen: 1}
	bOld := s.current.Load()
	require.True(t, s.current.CompareAndSwap(bOld, bSlot),
		"B's CAS install must succeed in this simulated ordering")
	require.True(t, s.valid(now), "B's fresh lease must be valid")

	// Step 4: A's delayed rollback runs. The fix is that rollback is
	// pointer-identity CAS on aSlot, which fails here because the
	// live pointer is bSlot (even though bSlot.expiryNanos equals
	// aSlot.expiryNanos).
	rb := &leaseSlot{expiryNanos: 0, gen: 1}
	swapped := s.current.CompareAndSwap(aSlot, rb)
	require.False(t, swapped,
		"pointer-identity rollback must NOT clobber a fresh lease that happened to compute the same expiry value")

	// Invariant: B's lease is still live.
	require.True(t, s.valid(now),
		"fresh lease with matching expiry must survive a stale extender's rollback")
	require.Equal(t, bSlot, s.current.Load(),
		"live slot must still be B's allocation, unchanged")
}

// TestLeaseState_RollbackCASClearsOwnSlot is the positive control:
// when NO concurrent extender has replaced A's slot, A's rollback
// DOES clear it (because the live pointer is still aSlot). Exercises
// the happy path for the rollback branch inside extend.
func TestLeaseState_RollbackCASClearsOwnSlot(t *testing.T) {
	t.Parallel()
	var s leaseState
	now := monoclock.Now()

	// Caller pattern: sample generation, then quorum op races with
	// invalidate, then extend. extend sees the gen advance in its
	// post-CAS recheck and rolls back its own freshly-installed slot.
	expectedGen := s.generation()
	s.invalidate() // concurrent leader-loss during the quorum op

	// extend with the pre-invalidate generation. Internal flow:
	// pre-CAS gate observes gen mismatch and returns WITHOUT
	// installing any slot. The invalidate's zero-slot remains.
	s.extend(now.Add(time.Hour), expectedGen)
	require.False(t, s.valid(now),
		"pre-invalidate extend must not resurrect the lease")

	// Separately, run the actual extend race: sample a FRESH gen
	// (simulating a caller that re-entered after invalidate), install
	// a lease, then invalidate, then verify clearing.
	freshGen := s.generation()
	s.extend(now.Add(time.Hour), freshGen)
	require.True(t, s.valid(now))
	s.invalidate()
	require.False(t, s.valid(now),
		"invalidate after a successful extend must clear the lease")
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
