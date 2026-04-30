package etcd

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFireLeaderAcquiredCallbacks_ContainsPanic mirrors the
// leader-loss panic-containment test. A panicking callback MUST
// NOT take down the engine loop or block subsequent callbacks.
func TestFireLeaderAcquiredCallbacks_ContainsPanic(t *testing.T) {
	t.Parallel()

	e := &Engine{}
	var before, after atomic.Int32
	e.RegisterLeaderAcquiredCallback(func() { before.Add(1) })
	e.RegisterLeaderAcquiredCallback(func() { panic("policy hook bug") })
	e.RegisterLeaderAcquiredCallback(func() { after.Add(1) })

	require.NotPanics(t, e.fireLeaderAcquiredCallbacks)

	require.Equal(t, int32(1), before.Load(),
		"callbacks registered before the panicking one must have fired")
	require.Equal(t, int32(1), after.Load(),
		"callbacks registered after the panicking one must still fire")
}

// TestFireLeaderAcquiredCallbacks_NoCallbacksIsSafe pins the
// empty-list fast path so refreshStatus can fire unconditionally
// without a guard.
func TestFireLeaderAcquiredCallbacks_NoCallbacksIsSafe(t *testing.T) {
	t.Parallel()
	e := &Engine{}
	require.NotPanics(t, e.fireLeaderAcquiredCallbacks)
}

// TestRegisterLeaderAcquiredCallback_DeregisterRemoves pins the
// returned deregister function: calling it removes the
// registration so subsequent fires no longer invoke fn. Mirrors
// the leader-loss deregister contract.
func TestRegisterLeaderAcquiredCallback_DeregisterRemoves(t *testing.T) {
	t.Parallel()

	e := &Engine{}
	var fired atomic.Int32
	deregister := e.RegisterLeaderAcquiredCallback(func() { fired.Add(1) })

	e.fireLeaderAcquiredCallbacks()
	require.Equal(t, int32(1), fired.Load(),
		"first fire must invoke the registered callback")

	deregister()

	e.fireLeaderAcquiredCallbacks()
	require.Equal(t, int32(1), fired.Load(),
		"after deregister, fire must not invoke the callback again")
}

// TestRegisterLeaderAcquiredCallback_DeregisterIdempotent pins
// that calling the returned deregister multiple times is safe and
// does not affect other registrations.
func TestRegisterLeaderAcquiredCallback_DeregisterIdempotent(t *testing.T) {
	t.Parallel()

	e := &Engine{}
	var fired atomic.Int32
	dereg1 := e.RegisterLeaderAcquiredCallback(func() { fired.Add(1) })
	e.RegisterLeaderAcquiredCallback(func() { fired.Add(10) })

	dereg1()
	dereg1() // second call must be a no-op
	dereg1() // third call must be a no-op

	e.fireLeaderAcquiredCallbacks()
	require.Equal(t, int32(10), fired.Load(),
		"only the second callback survives — the first's deregister "+
			"must have removed exactly one entry, not all of them, "+
			"even when deregister is called repeatedly")
}

// TestRegisterLeaderAcquiredCallback_NilFnIsSafe pins that
// passing nil for fn does not register anything and the returned
// deregister is a no-op.
func TestRegisterLeaderAcquiredCallback_NilFnIsSafe(t *testing.T) {
	t.Parallel()
	e := &Engine{}
	dereg := e.RegisterLeaderAcquiredCallback(nil)
	require.NotPanics(t, dereg)
	require.NotPanics(t, e.fireLeaderAcquiredCallbacks)
}

// TestRegisterLeaderAcquiredCallback_NilEngineIsSafe pins the
// typed-nil receiver guard so a coordinator constructed before
// the engine is wired does not crash on registration.
func TestRegisterLeaderAcquiredCallback_NilEngineIsSafe(t *testing.T) {
	t.Parallel()
	var e *Engine
	dereg := e.RegisterLeaderAcquiredCallback(func() {})
	require.NotPanics(t, dereg)
}

// TestRegisterLeaderAcquiredCallback_DistinguishesIdenticalFns
// pins that two registrations of the SAME function are treated as
// distinct slots — deregistering one leaves the other live. The
// sentinel-pointer design exists for exactly this case.
func TestRegisterLeaderAcquiredCallback_DistinguishesIdenticalFns(t *testing.T) {
	t.Parallel()

	e := &Engine{}
	var fired atomic.Int32
	fn := func() { fired.Add(1) }
	dereg1 := e.RegisterLeaderAcquiredCallback(fn)
	e.RegisterLeaderAcquiredCallback(fn)

	dereg1()

	e.fireLeaderAcquiredCallbacks()
	require.Equal(t, int32(1), fired.Load(),
		"deregistering one of two identical-fn registrations must "+
			"leave the other active — sentinel pointer disambiguates")
}
