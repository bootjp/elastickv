package kv

import (
	"context"
	"errors"
	"testing"
	"time"

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

// TestShardedCoordinator_RenewHLCLease_WarmsDefaultGroupLease proves the
// sharded renewal path warms the DEFAULT group's lease on a successful
// propose, so LeaseReadForKey on a default-group key serves from the
// fast path without a per-shard LinearizableRead.
func TestShardedCoordinator_RenewHLCLease_WarmsDefaultGroupLease(t *testing.T) {
	t.Parallel()
	eng1 := newShardedLeaseEngine(100)
	eng2 := newShardedLeaseEngine(200)
	coord := mustShardedLeaseCoord(t, eng1, eng2)
	// defaultGroup is 1 (see mustShardedLeaseCoord).
	g1 := coord.groups[1]
	require.False(t, g1.lease.valid(monoclock.Now()))

	coord.renewHLCLease(context.Background(), g1)
	require.Equal(t, int32(1), eng1.proposeCalls.Load())
	require.True(t, g1.lease.valid(monoclock.Now()),
		"a successful renewal propose must warm the default group's lease")

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
// propose must leave the default group's lease cold.
func TestShardedCoordinator_RenewHLCLease_FailedProposeDoesNotWarmLease(t *testing.T) {
	t.Parallel()
	eng1 := newShardedLeaseEngine(100)
	eng2 := newShardedLeaseEngine(200)
	eng1.proposeErr = errors.New("propose rejected: no quorum")
	coord := mustShardedLeaseCoord(t, eng1, eng2)
	g1 := coord.groups[1]

	coord.renewHLCLease(context.Background(), g1)
	require.Equal(t, int32(1), eng1.proposeCalls.Load())
	require.False(t, g1.lease.valid(monoclock.Now()),
		"a failed renewal propose must NOT warm the default group's lease")

	// LeaseReadForKey on a default-group key must still take the slow path.
	_, err := coord.LeaseReadForKey(context.Background(), []byte("apple"))
	require.NoError(t, err)
	require.Equal(t, int32(1), eng1.linearizableCalls.Load(),
		"cold default-group lease after a failed renewal must fall through to LinearizableRead")
}

// TestShardedCoordinator_RenewHLCLease_LeadershipLossErrorInvalidatesLease
// is the sharded counterpart of the sequential leadership-loss case: if
// the default group's Propose returns a leadership-loss error, the
// already-warm group lease must be eagerly invalidated, matching
// leaseRefreshingTxn's leadership-loss branch on that same group.
func TestShardedCoordinator_RenewHLCLease_LeadershipLossErrorInvalidatesLease(t *testing.T) {
	t.Parallel()
	eng1 := newShardedLeaseEngine(100)
	eng2 := newShardedLeaseEngine(200)
	coord := mustShardedLeaseCoord(t, eng1, eng2)
	g1 := coord.groups[1]

	coord.renewHLCLease(context.Background(), g1)
	require.True(t, g1.lease.valid(monoclock.Now()), "precondition: default group lease must be warm")

	eng1.proposeErr = raftengine.ErrNotLeader
	coord.renewHLCLease(context.Background(), g1)
	require.False(t, g1.lease.valid(monoclock.Now()),
		"a leadership-loss propose error must EAGERLY invalidate the default group's warm lease")
}

// TestShardedCoordinator_RenewHLCLease_NonLeadershipErrorKeepsLease
// proves the sharded invalidation is narrow: a non-leadership propose
// failure must NOT tear down the already-warm default group lease.
func TestShardedCoordinator_RenewHLCLease_NonLeadershipErrorKeepsLease(t *testing.T) {
	t.Parallel()
	eng1 := newShardedLeaseEngine(100)
	eng2 := newShardedLeaseEngine(200)
	coord := mustShardedLeaseCoord(t, eng1, eng2)
	g1 := coord.groups[1]

	coord.renewHLCLease(context.Background(), g1)
	require.True(t, g1.lease.valid(monoclock.Now()), "precondition: default group lease must be warm")

	eng1.proposeErr = errors.New("propose rejected: no quorum")
	coord.renewHLCLease(context.Background(), g1)
	require.True(t, g1.lease.valid(monoclock.Now()),
		"a non-leadership propose error must NOT invalidate the default group's warm lease")
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

	coord.renewHLCLease(context.Background(), g1)
	require.False(t, g1.lease.valid(monoclock.Now()),
		"a leader-loss racing the propose must prevent the default group's lease warm-up")
}
