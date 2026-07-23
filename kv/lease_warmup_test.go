package kv

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/monoclock"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
)

// TestCoordinate_ProposeHLCLease_WarmsLease proves the background HLC
// ceiling renewal also extends the read lease: after a single successful
// ProposeHLCLease (no Dispatch, no LinearizableRead), the caller-side
// lease must be valid so the next LeaseRead serves from the fast path.
// This is the warm-up that flattens the read-only lease-expiry sawtooth.
func TestCoordinate_ProposeHLCLease_WarmsLease(t *testing.T) {
	t.Parallel()
	eng := &fakeLeaseEngine{applied: 11, leaseDur: time.Hour}
	c := NewCoordinatorWithEngine(nil, eng)

	require.False(t, c.lease.valid(monoclock.Now()),
		"lease must start cold so a fast-path hit is attributable to the renewal")

	require.NoError(t, c.ProposeHLCLease(context.Background(), time.Now().UnixMilli()+hlcPhysicalWindowMs))
	require.Equal(t, int32(1), eng.proposeCalls.Load())

	require.True(t, c.lease.valid(monoclock.Now()),
		"a successful HLC renewal propose must warm the read lease")

	// The next LeaseRead must hit the warmed lease without a
	// LinearizableRead round-trip.
	idx, err := c.LeaseRead(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(11), idx)
	require.Equal(t, int32(0), eng.linearizableCalls.Load(),
		"warmed lease must serve the read without LinearizableRead")
}

// TestCoordinate_ProposeHLCLease_FailedProposeDoesNotWarmLease is the
// CRITICAL safety case: a propose that fails (no quorum confirmation)
// must NOT extend the lease, otherwise the lease-read freshness window
// would widen on an unconfirmed renewal.
func TestCoordinate_ProposeHLCLease_FailedProposeDoesNotWarmLease(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("propose rejected: no quorum")
	eng := &fakeLeaseEngine{applied: 11, leaseDur: time.Hour, proposeErr: sentinel}
	c := NewCoordinatorWithEngine(nil, eng)

	err := c.ProposeHLCLease(context.Background(), time.Now().UnixMilli()+hlcPhysicalWindowMs)
	require.ErrorIs(t, err, sentinel)
	require.False(t, c.lease.valid(monoclock.Now()),
		"a failed renewal propose must NOT warm the read lease")

	// The next LeaseRead must still take the slow path.
	_, err = c.LeaseRead(context.Background())
	require.NoError(t, err)
	require.Equal(t, int32(1), eng.linearizableCalls.Load(),
		"cold lease after a failed renewal must fall through to LinearizableRead")
}

// TestCoordinate_ProposeHLCLease_LeadershipLossErrorInvalidatesLease is
// the SEQUENTIAL leadership-loss case (distinct from the concurrent
// proposeHook race below). When Propose itself returns a
// leadership-loss error -- this node lost leadership BEFORE the propose
// completed, before any async RegisterLeaderLossCallback could fire --
// the warm-up path must EAGERLY invalidate an already-warm lease, so no
// stale-warm lease survives on a non-leader node for the callback
// latency window. This is the exact-parity guarantee with
// refreshLeaseAfterDispatch's leadership-loss branch.
func TestCoordinate_ProposeHLCLease_LeadershipLossErrorInvalidatesLease(t *testing.T) {
	t.Parallel()
	eng := &fakeLeaseEngine{applied: 11, leaseDur: time.Hour}
	c := NewCoordinatorWithEngine(nil, eng)

	// Warm the lease so a regression (no eager invalidation) leaves it
	// valid -- the assertion below would then fail.
	require.NoError(t, c.ProposeHLCLease(context.Background(), time.Now().UnixMilli()+hlcPhysicalWindowMs))
	require.True(t, c.lease.valid(monoclock.Now()), "precondition: lease must be warm")

	// Next propose loses leadership before completing.
	eng.proposeErr = raftengine.ErrNotLeader
	err := c.ProposeHLCLease(context.Background(), time.Now().UnixMilli()+hlcPhysicalWindowMs)
	require.ErrorIs(t, err, raftengine.ErrNotLeader)
	require.False(t, c.lease.valid(monoclock.Now()),
		"a leadership-loss propose error must EAGERLY invalidate the warm lease")
}

// TestCoordinate_ProposeHLCLease_NonLeadershipErrorKeepsLease proves the
// invalidation is narrow: a non-leadership propose failure (e.g. no
// quorum) must NOT tear down an already-warm lease, otherwise every
// subsequent read would be forced onto the slow LinearizableRead path.
func TestCoordinate_ProposeHLCLease_NonLeadershipErrorKeepsLease(t *testing.T) {
	t.Parallel()
	eng := &fakeLeaseEngine{applied: 11, leaseDur: time.Hour}
	c := NewCoordinatorWithEngine(nil, eng)

	require.NoError(t, c.ProposeHLCLease(context.Background(), time.Now().UnixMilli()+hlcPhysicalWindowMs))
	require.True(t, c.lease.valid(monoclock.Now()), "precondition: lease must be warm")

	eng.proposeErr = errors.New("propose rejected: no quorum")
	err := c.ProposeHLCLease(context.Background(), time.Now().UnixMilli()+hlcPhysicalWindowMs)
	require.Error(t, err)
	require.True(t, c.lease.valid(monoclock.Now()),
		"a non-leadership propose error must NOT invalidate the warm lease")
}

// TestCoordinate_ProposeHLCLease_LeaderLossDuringProposeDoesNotWarmLease
// pins the generation guard: if a leader-loss callback fires DURING the
// propose (between sampling the generation and the post-propose extend),
// extend must observe the advanced generation and refuse to resurrect
// the lease -- even though the propose itself "succeeded". This mirrors
// the Dispatch hook's expectedGen-sampled-before-propose semantics.
func TestCoordinate_ProposeHLCLease_LeaderLossDuringProposeDoesNotWarmLease(t *testing.T) {
	t.Parallel()
	eng := &fakeLeaseEngine{applied: 11, leaseDur: time.Hour}
	c := NewCoordinatorWithEngine(nil, eng)
	// Fire leader-loss inside the propose so the lease generation advances
	// after ProposeHLCLease sampled it but before the extend.
	eng.proposeHook = func() { eng.fireLeaderLoss() }

	require.NoError(t, c.ProposeHLCLease(context.Background(), time.Now().UnixMilli()+hlcPhysicalWindowMs))
	require.False(t, c.lease.valid(monoclock.Now()),
		"a leader-loss racing the propose must prevent the lease warm-up")
}

// TestCoordinate_ProposeHLCLease_NoLeaseProviderNoPanic covers an engine
// without LeaseProvider: the propose still succeeds and the lease is
// simply never warmed (there is no lease infrastructure to warm).
func TestCoordinate_ProposeHLCLease_NoLeaseProviderNoPanic(t *testing.T) {
	t.Parallel()
	eng := &nonLeaseEngine{}
	c := NewCoordinatorWithEngine(nil, eng)

	require.NoError(t, c.ProposeHLCLease(context.Background(), time.Now().UnixMilli()+hlcPhysicalWindowMs))
	require.False(t, c.lease.valid(monoclock.Now()))
}

func TestCoordinate_RunHLCLeaseRenewal_BlockerSuppressesProposals(t *testing.T) {
	eng := &fakeLeaseEngine{applied: 11, leaseDur: time.Hour}
	c := NewCoordinatorWithEngine(nil, eng)
	var blocked atomic.Bool
	blocked.Store(true)
	c.SetHLCLeaseRenewalBlocker(blocked.Load)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		c.RunHLCLeaseRenewal(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	time.Sleep(hlcRenewalInterval + 100*time.Millisecond)
	require.Equal(t, int32(0), eng.proposeCalls.Load(),
		"blocked HLC renewal must not propose while startup rotation is active")

	blocked.Store(false)
	require.Eventually(t, func() bool {
		return eng.proposeCalls.Load() > 0
	}, 2*time.Second, 10*time.Millisecond,
		"HLC renewal should resume after startup rotation blocker clears")
}

// TestShardedCoordinator_RenewHLCLease_WarmsGroupLease proves the
// sharded renewal path warms the target group's lease on a successful
// propose, so LeaseReadForKey on a key owned by that group serves from the
// fast path without a per-shard LinearizableRead.
func TestShardedCoordinator_RenewHLCLease_WarmsGroupLease(t *testing.T) {
	t.Parallel()
	eng1 := newShardedLeaseEngine(100)
	eng2 := newShardedLeaseEngine(200)
	coord := mustShardedLeaseCoord(t, eng1, eng2)
	g1 := coord.groups[1]
	require.False(t, g1.lease.valid(monoclock.Now()))

	coord.renewHLCLease(context.Background(), 1, g1)
	require.Equal(t, int32(1), eng1.proposeCalls.Load())
	require.True(t, g1.lease.valid(monoclock.Now()),
		"a successful renewal propose must warm the target group's lease")

	// LeaseReadForKey on a default-group key ("apple" -> group 1) now
	// hits the warmed lease.
	idx, err := coord.LeaseReadForKey(context.Background(), []byte("apple"))
	require.NoError(t, err)
	require.Equal(t, uint64(100), idx)
	require.Equal(t, int32(0), eng1.linearizableCalls.Load(),
		"warmed default-group lease must serve the read without LinearizableRead")
}

// TestShardedCoordinator_RenewHLCLease_FailedProposeDoesNotWarmLease is
// the sharded counterpart of the single-shard safety case: a failed
// propose must leave the target group's lease cold.
func TestShardedCoordinator_RenewHLCLease_FailedProposeDoesNotWarmLease(t *testing.T) {
	t.Parallel()
	eng1 := newShardedLeaseEngine(100)
	eng2 := newShardedLeaseEngine(200)
	eng1.proposeErr = errors.New("propose rejected: no quorum")
	coord := mustShardedLeaseCoord(t, eng1, eng2)
	g1 := coord.groups[1]

	coord.renewHLCLease(context.Background(), 1, g1)
	require.Equal(t, int32(1), eng1.proposeCalls.Load())
	require.False(t, g1.lease.valid(monoclock.Now()),
		"a failed renewal propose must NOT warm the target group's lease")

	// LeaseReadForKey on a default-group key must still take the slow path.
	_, err := coord.LeaseReadForKey(context.Background(), []byte("apple"))
	require.NoError(t, err)
	require.Equal(t, int32(1), eng1.linearizableCalls.Load(),
		"cold default-group lease after a failed renewal must fall through to LinearizableRead")
}

// TestShardedCoordinator_RenewHLCLease_LeadershipLossErrorInvalidatesLease
// is the sharded counterpart of the sequential leadership-loss case: if
// the target group's Propose returns a leadership-loss error, the
// already-warm group lease must be eagerly invalidated, matching
// leaseRefreshingTxn's leadership-loss branch on that same group.
func TestShardedCoordinator_RenewHLCLease_LeadershipLossErrorInvalidatesLease(t *testing.T) {
	t.Parallel()
	eng1 := newShardedLeaseEngine(100)
	eng2 := newShardedLeaseEngine(200)
	coord := mustShardedLeaseCoord(t, eng1, eng2)
	g1 := coord.groups[1]

	coord.renewHLCLease(context.Background(), 1, g1)
	require.True(t, g1.lease.valid(monoclock.Now()), "precondition: target group lease must be warm")

	eng1.proposeErr = raftengine.ErrNotLeader
	coord.renewHLCLease(context.Background(), 1, g1)
	require.False(t, g1.lease.valid(monoclock.Now()),
		"a leadership-loss propose error must EAGERLY invalidate the target group's warm lease")
}

// TestShardedCoordinator_RenewHLCLease_NonLeadershipErrorKeepsLease
// proves the sharded invalidation is narrow: a non-leadership propose
// failure must NOT tear down the already-warm target group lease.
func TestShardedCoordinator_RenewHLCLease_NonLeadershipErrorKeepsLease(t *testing.T) {
	t.Parallel()
	eng1 := newShardedLeaseEngine(100)
	eng2 := newShardedLeaseEngine(200)
	coord := mustShardedLeaseCoord(t, eng1, eng2)
	g1 := coord.groups[1]

	coord.renewHLCLease(context.Background(), 1, g1)
	require.True(t, g1.lease.valid(monoclock.Now()), "precondition: target group lease must be warm")

	eng1.proposeErr = errors.New("propose rejected: no quorum")
	coord.renewHLCLease(context.Background(), 1, g1)
	require.True(t, g1.lease.valid(monoclock.Now()),
		"a non-leadership propose error must NOT invalidate the target group's warm lease")
}

// TestShardedCoordinator_RenewHLCLease_LeaderLossDuringProposeDoesNotWarm
// pins the generation guard for the sharded path.
func TestShardedCoordinator_RenewHLCLease_LeaderLossDuringProposeDoesNotWarm(t *testing.T) {
	t.Parallel()
	eng1 := newShardedLeaseEngine(100)
	eng2 := newShardedLeaseEngine(200)
	coord := mustShardedLeaseCoord(t, eng1, eng2)
	g1 := coord.groups[1]
	eng1.proposeHook = func() { eng1.fireLeaderLoss() }

	coord.renewHLCLease(context.Background(), 1, g1)
	require.False(t, g1.lease.valid(monoclock.Now()),
		"a leader-loss racing the propose must prevent the target group's lease warm-up")
}

func TestShardedCoordinator_RenewHLCLeases_ProposesToEveryLedGroup(t *testing.T) {
	t.Parallel()
	eng1 := newShardedLeaseEngine(100)
	eng2 := newShardedLeaseEngine(200)
	coord := mustShardedLeaseCoord(t, eng1, eng2)

	done := coord.renewHLCLeases(context.Background())
	requireRenewalDone(t, done)

	require.Equal(t, int32(1), eng1.proposeCalls.Load())
	require.Equal(t, int32(1), eng2.proposeCalls.Load())
	require.True(t, coord.groups[1].lease.valid(monoclock.Now()))
	require.True(t, coord.groups[2].lease.valid(monoclock.Now()))

	idx, err := coord.LeaseReadForKey(context.Background(), []byte("zebra"))
	require.NoError(t, err)
	require.Equal(t, uint64(200), idx)
	require.Equal(t, int32(0), eng2.linearizableCalls.Load(),
		"the non-default group lease must be warmed by all-group renewal")
}

func TestShardedCoordinator_RecoverHLCLease_ProposesToEveryLedGroup(t *testing.T) {
	t.Parallel()
	clock := NewHLC()
	clock.SetPhysicalCeiling(time.Now().Add(-time.Millisecond).UnixMilli())
	eng1 := newShardedLeaseEngine(100)
	eng2 := newShardedLeaseEngine(200)
	eng1.proposeApply = applyHLCLeaseEntryToClock(t, clock)
	eng2.proposeApply = applyHLCLeaseEntryToClock(t, clock)
	coord := mustShardedLeaseCoord(t, eng1, eng2)
	coord.clock = clock

	require.NoError(t, coord.RecoverHLCLease(context.Background()))
	require.Equal(t, int32(1), eng1.proposeCalls.Load())
	require.Equal(t, int32(1), eng2.proposeCalls.Load())

	got, err := clock.NextFenced()
	require.NoError(t, err)
	require.NotZero(t, got)
}

func TestShardedCoordinator_RecoverHLCLease_SucceedsWhenAnyTargetAdvancesCeiling(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		failFirst  bool
		failSecond bool
	}{
		{name: "first fails then second advances", failFirst: true},
		{name: "first advances then second fails", failSecond: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			clock := NewHLC()
			clock.SetPhysicalCeiling(time.Now().Add(-time.Millisecond).UnixMilli())
			eng1 := newShardedLeaseEngine(100)
			eng2 := newShardedLeaseEngine(200)
			eng1.proposeApply = applyHLCLeaseEntryToClock(t, clock)
			eng2.proposeApply = applyHLCLeaseEntryToClock(t, clock)
			if tc.failFirst {
				eng1.proposeErr = errors.New("group 1 unavailable")
			}
			if tc.failSecond {
				eng2.proposeErr = errors.New("group 2 unavailable")
			}
			coord := mustShardedLeaseCoord(t, eng1, eng2)
			coord.clock = clock

			require.NoError(t, coord.RecoverHLCLease(context.Background()))
			require.Equal(t, int32(1), eng1.proposeCalls.Load())
			require.Equal(t, int32(1), eng2.proposeCalls.Load())

			got, err := clock.NextFenced()
			require.NoError(t, err)
			require.NotZero(t, got)
		})
	}
}

func TestShardedCoordinator_RenewHLCLeases_SkipsNonLeaders(t *testing.T) {
	t.Parallel()
	eng1 := newShardedLeaseEngine(100)
	eng2 := newShardedLeaseEngine(200)
	eng2.state.Store(raftengine.StateFollower)
	coord := mustShardedLeaseCoord(t, eng1, eng2)

	done := coord.renewHLCLeases(context.Background())
	requireRenewalDone(t, done)

	require.Equal(t, int32(1), eng1.proposeCalls.Load())
	require.Equal(t, int32(0), eng2.proposeCalls.Load())
	require.True(t, coord.groups[1].lease.valid(monoclock.Now()))
	require.False(t, coord.groups[2].lease.valid(monoclock.Now()))
}

func TestShardedCoordinator_RenewHLCLeases_SlowGroupDoesNotBlockPeers(t *testing.T) {
	t.Parallel()
	eng1 := newShardedLeaseEngine(100)
	eng2 := newShardedLeaseEngine(200)
	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce sync.Once
	eng1.proposeHook = func() {
		enteredOnce.Do(func() { close(entered) })
		<-release
	}
	coord := mustShardedLeaseCoord(t, eng1, eng2)

	done := coord.renewHLCLeases(context.Background())
	<-entered
	require.Eventually(t, func() bool {
		return eng2.proposeCalls.Load() == 1
	}, time.Second, 10*time.Millisecond,
		"a slow led group must not delay renewal for another led group")
	close(release)
	requireRenewalDone(t, done)
}

func TestShardedCoordinator_RenewHLCLeases_SkipsInFlightGroup(t *testing.T) {
	eng1 := newShardedLeaseEngine(100)
	eng2 := newShardedLeaseEngine(200)
	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce sync.Once
	eng1.proposeHook = func() {
		enteredOnce.Do(func() { close(entered) })
		<-release
	}
	coord := mustShardedLeaseCoord(t, eng1, eng2)

	first := coord.renewHLCLeases(context.Background())
	<-entered
	require.Eventually(t, func() bool {
		return eng2.proposeCalls.Load() == 1 && !hlcRenewalInFlight(coord, 2)
	}, time.Second, 10*time.Millisecond,
		"precondition: the first round must fully finish for the non-blocked group")

	second := coord.renewHLCLeases(context.Background())
	requireRenewalDone(t, second)

	require.Equal(t, int32(1), eng1.proposeCalls.Load(),
		"an in-flight group must not receive a second concurrent renewal proposal")
	require.Equal(t, int32(2), eng2.proposeCalls.Load(),
		"other led groups must still renew while one group is in flight")

	close(release)
	requireRenewalDone(t, first)

	third := coord.renewHLCLeases(context.Background())
	requireRenewalDone(t, third)
	require.Equal(t, int32(2), eng1.proposeCalls.Load(),
		"the group must be eligible for renewal after the in-flight proposal finishes")
}

func TestShardedCoordinator_ProposeHLCLease_UsesDedicatedTimestampGroup(t *testing.T) {
	t.Parallel()
	eng0 := newShardedLeaseEngine(300)
	eng1 := newShardedLeaseEngine(100)
	distEngine := distribution.NewEngine()
	distEngine.UpdateRoute([]byte(""), nil, 1)
	coord := NewShardedCoordinator(distEngine, map[uint64]*ShardGroup{
		0: {Engine: eng0},
		1: {Engine: eng1},
	}, 1, NewHLC(), nil).WithTimestampGroup(0)

	err := coord.ProposeHLCLease(context.Background(), time.Now().UnixMilli()+hlcPhysicalWindowMs)
	require.NoError(t, err)
	require.Equal(t, int32(1), eng0.proposeCalls.Load())
	require.Equal(t, int32(0), eng1.proposeCalls.Load())
	require.True(t, coord.groups[0].lease.valid(monoclock.Now()),
		"a synchronous timestamp renewal must warm the timestamp group's lease")
}

func hlcRenewalInFlight(coord *ShardedCoordinator, gid uint64) bool {
	coord.hlcRenewalMu.Lock()
	defer coord.hlcRenewalMu.Unlock()
	_, ok := coord.hlcRenewalInFlight[gid]
	return ok
}

func requireRenewalDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for HLC lease renewals")
	}
}
