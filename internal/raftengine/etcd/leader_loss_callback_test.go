package etcd

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFireLeaderLossCallbacks_ContainsPanic verifies that a panicking
// callback does NOT take down the raft engine loop: the remaining
// callbacks still fire and the method returns normally.
func TestFireLeaderLossCallbacks_ContainsPanic(t *testing.T) {
	t.Parallel()

	e := &Engine{}
	var before, after atomic.Int32
	e.RegisterLeaderLossCallback(func() { before.Add(1) })
	e.RegisterLeaderLossCallback(func() { panic("lease holder bug") })
	e.RegisterLeaderLossCallback(func() { after.Add(1) })

	// Must not panic out of the call.
	require.NotPanics(t, e.fireLeaderLossCallbacks)

	require.Equal(t, int32(1), before.Load(),
		"callbacks registered before the panicking one must have fired")
	require.Equal(t, int32(1), after.Load(),
		"callbacks registered after the panicking one must still fire")
}

// TestFireLeaderLossCallbacks_NoCallbacksIsSafe exercises the empty-list
// fast path so the helper can be called unconditionally from shutdown
// and refreshStatus paths without a guard.
func TestFireLeaderLossCallbacks_NoCallbacksIsSafe(t *testing.T) {
	t.Parallel()
	e := &Engine{}
	require.NotPanics(t, e.fireLeaderLossCallbacks)
}
