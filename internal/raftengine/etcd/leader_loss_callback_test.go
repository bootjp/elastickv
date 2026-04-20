package etcd

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestFireLeaderLossCallbacks_ContainsPanic verifies that a panicking
// callback does NOT take down the raft engine loop: the remaining
// callbacks still fire and the method returns normally.
// Callbacks now run on detached goroutines, so we wait (with a bounded
// timeout) for the two sibling counters to update before asserting.
func TestFireLeaderLossCallbacks_ContainsPanic(t *testing.T) {
	t.Parallel()

	e := &Engine{}
	var before, after atomic.Int32
	e.RegisterLeaderLossCallback(func() { before.Add(1) })
	e.RegisterLeaderLossCallback(func() { panic("lease holder bug") })
	e.RegisterLeaderLossCallback(func() { after.Add(1) })

	require.NotPanics(t, e.fireLeaderLossCallbacks)

	require.Eventually(t, func() bool {
		return before.Load() == 1 && after.Load() == 1
	}, time.Second, time.Millisecond,
		"both non-panicking callbacks must fire on detached goroutines")
}

// TestFireLeaderLossCallbacks_NoCallbacksIsSafe exercises the empty-list
// fast path so the helper can be called unconditionally from shutdown
// and refreshStatus paths without a guard.
func TestFireLeaderLossCallbacks_NoCallbacksIsSafe(t *testing.T) {
	t.Parallel()
	e := &Engine{}
	require.NotPanics(t, e.fireLeaderLossCallbacks)
}
