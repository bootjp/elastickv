package kv

import (
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

// TestKvFSMWithRouteHistory_StoresFields verifies the M2 plumbing:
// the WithRouteHistory option stores the RouteHistory provider and
// the shardGroupID on the FSM struct.  At M2 the fields are not
// consulted by the apply path; M3's verifyComposed1 will read them.
// This is the design doc §M2 "Done when" criterion — the struct
// extension is in place and compiles with the constructor wiring.
func TestKvFSMWithRouteHistory_StoresFields(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngineWithDefaultRoute()
	st := store.NewMVCCStore()

	fsmIface := NewKvFSMWithHLC(st, NewHLC(),
		WithRouteHistory(WrapDistributionEngine(engine), 42),
	)
	fsm, ok := fsmIface.(*kvFSM)
	require.True(t, ok, "NewKvFSMWithHLC must return *kvFSM")

	require.NotNil(t, fsm.routes,
		"WithRouteHistory must install the RouteHistory provider on the FSM")
	require.Equal(t, uint64(42), fsm.shardGroupID,
		"WithRouteHistory must record the owning Raft group ID")

	// Round-trip a known route through the provider so the adapter +
	// the M2 ring + the v0 seed are exercised end-to-end.
	snap, ok := fsm.routes.SnapshotAt(0)
	require.True(t, ok,
		"WrapDistributionEngine + NewEngineWithDefaultRoute must seed v=0")
	require.Equal(t, uint64(0), snap.Version())

	owner, found := snap.OwnerOf([]byte("anything"))
	require.True(t, found)
	require.NotZero(t, owner,
		"the default route must own the keyspace at v=0 so M3 can resolve every key for unpinned txns")
}

// TestKvFSM_NilRouteHistoryByDefault documents the M2 legacy-default:
// constructors that do NOT pass WithRouteHistory leave routes=nil and
// shardGroupID=0.  M3's verifyComposed1 will short-circuit on the nil
// provider and behave exactly like the pre-feature FSM.
func TestKvFSM_NilRouteHistoryByDefault(t *testing.T) {
	t.Parallel()

	fsmIface := NewKvFSMWithHLC(store.NewMVCCStore(), NewHLC())
	fsm, ok := fsmIface.(*kvFSM)
	require.True(t, ok)

	require.Nil(t, fsm.routes,
		"FSM constructed without WithRouteHistory must keep routes nil — M3 reads this as 'unpinned' and skips the Composed-1 gate")
	require.Equal(t, uint64(0), fsm.shardGroupID,
		"shardGroupID must default to 0 (the 'unset' sentinel) when WithRouteHistory is not supplied")
}

// TestWrapDistributionEngine_NilEngineReturnsNil verifies the
// adapter's defensive shape: passing nil yields nil, so a caller
// that conditionally builds the option can safely pass either a
// real engine or nil without an explicit guard at the FSM
// construction site.
func TestWrapDistributionEngine_NilEngineReturnsNil(t *testing.T) {
	t.Parallel()

	require.Nil(t, WrapDistributionEngine(nil))
}

// TestKvFSM_WithRouteHistory_NilProviderTreatedAsUnwired verifies that
// passing a nil RouteHistory through WithRouteHistory is equivalent
// to not passing the option at all.  Production main.go uses
// WrapDistributionEngine(nil) → nil for nodes that have not yet
// constructed the engine (e.g. early bootstrap); the FSM should
// treat this exactly like the legacy default.
func TestKvFSM_WithRouteHistory_NilProviderTreatedAsUnwired(t *testing.T) {
	t.Parallel()

	fsmIface := NewKvFSMWithHLC(store.NewMVCCStore(), NewHLC(),
		WithRouteHistory(nil, 7),
	)
	fsm, ok := fsmIface.(*kvFSM)
	require.True(t, ok)

	require.Nil(t, fsm.routes,
		"nil RouteHistory must remain nil; the M3 gate will short-circuit and behave like an unwired FSM")
	// shardGroupID is still recorded — the design doc says zero is
	// the "unset" sentinel but a caller that explicitly passes a
	// non-zero shardGroupID with a nil provider gets the gate
	// short-circuited anyway via the nil routes check.  Assert the
	// non-zero ID survives so a future refactor that accidentally
	// zeroes it would be caught (claude review on PR #894).
	require.Equal(t, uint64(7), fsm.shardGroupID,
		"WithRouteHistory(nil, 7) must still record shardGroupID=7; the M3 nil-routes short-circuit fires before the ID is consulted")
}
