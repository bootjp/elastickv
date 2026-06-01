package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// applyComposed1Snapshot is a small helper that wraps the boilerplate
// of pushing a CatalogSnapshot into an Engine with a single
// key-range → group mapping (the typical M3 test shape).  Returns the
// version applied so the caller can pin ObservedRouteVersion on
// crafted pb.Requests.
func applyComposed1Snapshot(t *testing.T, e *distribution.Engine, version uint64, routes []distribution.RouteDescriptor) {
	t.Helper()
	require.NoError(t, e.ApplySnapshot(distribution.CatalogSnapshot{
		Version: version,
		Routes:  routes,
	}))
}

// newComposed1FSM constructs a kvFSM wired with the engine + the
// shard group ID the gate compares against.  Production wiring lives
// in main.go's buildShardGroups; this helper short-circuits to the
// test-only fixture without spinning up a real Raft group.
//
// the helper keeps it as a parameter so a future test can exercise
// the "wrong-group" case without re-deriving the boilerplate.
//
//nolint:unparam // shardGroupID is currently always 1 in tests but
func newComposed1FSM(t *testing.T, e *distribution.Engine, shardGroupID uint64) *kvFSM {
	t.Helper()
	fsmIface := NewKvFSMWithHLC(store.NewMVCCStore(), NewHLC(),
		WithRouteHistory(WrapDistributionEngine(e), shardGroupID))
	fsm, ok := fsmIface.(*kvFSM)
	require.True(t, ok)
	return fsm
}

// commitTxnRequest builds a single-shard one-phase pb.Request for the
// Composed-1 gate tests.  Only the fields the gate consults
// (ObservedRouteVersion + Mutations[].Key) need to be set; the
// downstream phase handlers are NOT exercised here — we only assert
// the gate's behaviour, so a malformed-from-the-handler-view request
// is fine.
func commitTxnRequest(observedVer uint64, keys ...string) *pb.Request {
	muts := make([]*pb.Mutation, 0, len(keys))
	for _, k := range keys {
		muts = append(muts, &pb.Mutation{Op: pb.Op_PUT, Key: []byte(k), Value: []byte("v")})
	}
	return &pb.Request{
		IsTxn:                true,
		Phase:                pb.Phase_NONE,
		ObservedRouteVersion: observedVer,
		Mutations:            muts,
	}
}

// TestVerifyComposed1_StaleObservedVersionWithMovedKeyFails is the M3
// "Done when" criterion (i) from the design doc: a txn that observed
// catalog version v_obs, where the key was owned by g1, now committing
// on g1 AFTER ApplySnapshot moved the key to g2 at v_obs+1 — the
// observed-version owner check at v_obs+1 (this group is g1 but the
// snapshot at v_obs+1 says the owner is g2) fails closed with
// ErrComposed1Violation.
//
// Wait — re-read the design doc: criterion (i) is "stale
// ObservedRouteVersion with the key moved → ErrComposed1Violation",
// meaning the OBSERVED version's snapshot resolves the key to a
// DIFFERENT group than the FSM's shardGroupID.  The classic
// MoveRange scenario from the doc's §3 isn't this — that's
// criterion (iii).  Criterion (i) is the spec-level Composed-1
// straight-up: txn pinned v=N, but routes[N][k] ≠ this group.
func TestVerifyComposed1_StaleObservedVersionWithMovedKeyFails(t *testing.T) {
	t.Parallel()

	// At v=1, key "k" is owned by group 2.  But this FSM serves
	// group 1 — a commit pinned at v=1 with key "k" must fail
	// the observed-version owner check.
	e := distribution.NewEngine()
	applyComposed1Snapshot(t, e, 1, []distribution.RouteDescriptor{
		{RouteID: 100, Start: []byte(""), End: nil, GroupID: 2, State: distribution.RouteStateActive},
	})
	fsm := newComposed1FSM(t, e, 1) // this FSM is for group 1

	err := fsm.handleTxnRequest(context.Background(), commitTxnRequest(1, "k"), 0)
	require.ErrorIs(t, err, ErrComposed1Violation,
		"observed-version owner check must reject a commit that lands on a group different from the historical owner")
	require.Contains(t, err.Error(), "observed-version",
		"the wrapped diagnostic must identify which check fired (observed vs current) so M4 retry can pick the right strategy")
}

// TestVerifyComposed1_ObservedVersionOlderThanRingFails is the M3
// "Done when" criterion (ii) from the design doc: a txn that observed
// a catalog version no longer in the ring (because the FIFO
// evicted it) surfaces as ErrComposed1VersionGCd, not
// ErrComposed1Violation.  The distinction matters because the M4
// coordinator retry path may want to treat the two differently
// (the violation is "route shifted, re-route"; the GCd is "version
// evicted, re-read catalog and re-issue").
func TestVerifyComposed1_ObservedVersionOlderThanRingFails(t *testing.T) {
	t.Parallel()

	e := distribution.NewEngine()
	// Tiny depth so the eviction trigger is bounded.  Safe direct
	// write: e is local to this goroutine and the depth is set
	// before any ApplySnapshot fires.
	e.SetHistoryDepthForTest(2)
	for v := uint64(1); v <= 5; v++ {
		applyComposed1Snapshot(t, e, v, []distribution.RouteDescriptor{
			{RouteID: 100, Start: []byte(""), End: nil, GroupID: 1, State: distribution.RouteStateActive},
		})
	}
	// At depth=2, only versions 4 and 5 are retained; v=2 has been
	// evicted long ago.
	fsm := newComposed1FSM(t, e, 1)

	err := fsm.handleTxnRequest(context.Background(), commitTxnRequest(2, "k"), 0)
	require.ErrorIs(t, err, ErrComposed1VersionGCd,
		"a txn observing a version outside the retention ring must surface ErrComposed1VersionGCd so M4 retry can re-read the catalog")
}

// TestVerifyComposed1_ObservedPassesButCurrentDiffersFails is the M3
// "Done when" criterion (iii) — the §3 codex P1 trace surfaced on
// PR #870.  Step-by-step:
//
//  1. At v=1, key "k1" is owned by g1.  Txn pins observedVer=1.
//  2. ApplySnapshot lands v=2 with k1 owned by g2.
//  3. Txn commits on g1 (it routed via its observed catalog).
//  4. Observed-version check at v=1 passes (routes[1][k1] = g1, this FSM
//     serves g1).
//  5. Current-version check at v=2 fails (routes[2][k1] = g2, this FSM
//     serves g1) — ErrComposed1Violation.
//
// Without the (b) cross-version fence, the commit would land on g1
// while readers at v=2 route to g2 and miss the write — exactly the
// G1c anomaly Composed-1a in the TLA+ spec (and PR #878) closes.
func TestVerifyComposed1_ObservedPassesButCurrentDiffersFails(t *testing.T) {
	t.Parallel()

	e := distribution.NewEngine()
	// v=1: k1 owned by g1
	applyComposed1Snapshot(t, e, 1, []distribution.RouteDescriptor{
		{RouteID: 100, Start: []byte(""), End: nil, GroupID: 1, State: distribution.RouteStateActive},
	})
	// v=2: k1 moved to g2
	applyComposed1Snapshot(t, e, 2, []distribution.RouteDescriptor{
		{RouteID: 101, Start: []byte(""), End: nil, GroupID: 2, State: distribution.RouteStateActive},
	})
	// This FSM serves g1 — the observed-version snapshot at v=1
	// agrees (the txn legitimately routed here at txn-begin time)
	// but the current snapshot at v=2 says the key has moved off.
	fsm := newComposed1FSM(t, e, 1)

	err := fsm.handleTxnRequest(context.Background(), commitTxnRequest(1, "k1"), 0)
	require.ErrorIs(t, err, ErrComposed1Violation,
		"the §4.4 current-version fence must reject the codex P1 trace: observed-version check passes (routes[1][k1]=g1) but the current snapshot at v=2 has moved k1 to g2")
	require.Contains(t, err.Error(), "current-version",
		"the wrapped diagnostic must identify the current-version fence as the rejecting check so M4 retry knows to re-route, not re-read the catalog")
}

// TestVerifyComposed1_ObservedVersionZeroSkipsGate documents the
// legacy-default behaviour: a txn with ObservedRouteVersion == 0
// (no M1 wiring) skips the gate entirely.  Combined with M1's
// behaviour-neutral default (every existing caller leaves the
// field at zero), this is what keeps M3 from regressing the
// pre-feature posture for any caller that hasn't migrated yet.
func TestVerifyComposed1_ObservedVersionZeroSkipsGate(t *testing.T) {
	t.Parallel()

	e := distribution.NewEngine()
	applyComposed1Snapshot(t, e, 1, []distribution.RouteDescriptor{
		// Routes[1] says owner is g2 — if the gate ran, this would
		// trip Composed1Violation for an FSM serving g1.  But the
		// gate short-circuits on ObservedRouteVersion==0.
		{RouteID: 100, Start: []byte(""), End: nil, GroupID: 2, State: distribution.RouteStateActive},
	})
	fsm := newComposed1FSM(t, e, 1)

	err := fsm.handleTxnRequest(context.Background(), commitTxnRequest(0, "k"), 0)
	// We expect either nil (gate skipped) or some non-Composed-1
	// error from the downstream phase handler (which we did not
	// fully set up).  What we MUST NOT see is Composed1Violation,
	// because the gate must have short-circuited.
	if err != nil {
		require.False(t, errors.Is(err, ErrComposed1Violation),
			"ObservedRouteVersion=0 (legacy caller) must skip the Composed-1 gate")
		require.False(t, errors.Is(err, ErrComposed1VersionGCd),
			"ObservedRouteVersion=0 must not surface ErrComposed1VersionGCd")
	}
}

// TestVerifyComposed1_NilRouteHistorySkipsGate documents the
// unwired-FSM default: a kvFSM constructed without WithRouteHistory
// has routes=nil and the gate short-circuits.  Matches the
// pre-feature posture byte-for-byte for callers (test harnesses,
// the pre-M2 single-binary demo) that have not been updated.
func TestVerifyComposed1_NilRouteHistorySkipsGate(t *testing.T) {
	t.Parallel()

	// No WithRouteHistory option — routes stays nil.
	fsmIface := NewKvFSMWithHLC(store.NewMVCCStore(), NewHLC())
	fsm, ok := fsmIface.(*kvFSM)
	require.True(t, ok)
	require.Nil(t, fsm.routes)

	err := fsm.handleTxnRequest(context.Background(), commitTxnRequest(42, "k"), 0)
	if err != nil {
		require.False(t, errors.Is(err, ErrComposed1Violation),
			"unwired FSM must not surface Composed1Violation")
		require.False(t, errors.Is(err, ErrComposed1VersionGCd),
			"unwired FSM must not surface ErrComposed1VersionGCd")
	}
}

// TestVerifyComposed1_ShardGroupIDZeroSkipsGate is the regression
// for the coderabbit MAJOR finding on PR #895.  A kvFSM with
// `routes` installed but `shardGroupID == 0` is a partially-wired
// FSM (the caller set routes before group ID, or the test fixture
// passed a non-zero engine but a zero group).  The doc comment on
// shardGroupID says "Zero at M2 means 'unset' — the M3 gate will
// short-circuit (matches the pre-feature behaviour)", but
// verifyComposed1 only checked `routes == nil`.  Without the
// shardGroupID bypass, every pinned txn against this FSM would be
// rejected with ErrComposed1Violation because no real group has
// ID 0 and the snapshot's OwnerOf would never return 0 as the
// owner.
//
// The fix: bypass when EITHER routes is nil OR shardGroupID is 0.
func TestVerifyComposed1_ShardGroupIDZeroSkipsGate(t *testing.T) {
	t.Parallel()

	// Build an engine where the (only) route IS owned by some
	// non-zero group, then construct the FSM with routes set but
	// shardGroupID = 0.  Under the pre-fix code the gate would
	// reject the txn (because OwnerOf returns the route's
	// non-zero owner, which does not equal the FSM's group=0).
	// Under the fix the gate short-circuits and returns nil —
	// matching the documented "unset = bypass" semantics.
	e := distribution.NewEngine()
	applyComposed1Snapshot(t, e, 1, []distribution.RouteDescriptor{
		{RouteID: 100, Start: []byte(""), End: nil, GroupID: 5, State: distribution.RouteStateActive},
	})
	fsm := newComposed1FSM(t, e, 0) // partially-wired: routes set, group ID unset

	err := fsm.handleTxnRequest(context.Background(), commitTxnRequest(1, "k"), 0)
	if err != nil {
		require.False(t, errors.Is(err, ErrComposed1Violation),
			"shardGroupID=0 must bypass the Composed-1 gate even when routes is non-nil; the documented 'unset = bypass' semantics protects partially-wired callers")
		require.False(t, errors.Is(err, ErrComposed1VersionGCd),
			"shardGroupID=0 must bypass before any ring lookup, so ErrComposed1VersionGCd must not surface either")
	}
}
