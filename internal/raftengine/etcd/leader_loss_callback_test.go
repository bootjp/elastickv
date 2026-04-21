package etcd

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFireLeaderLossCallbacks_ContainsPanic verifies that a panicking
// callback does NOT take down the raft engine loop: the remaining
// callbacks still fire synchronously and the method returns normally.
func TestFireLeaderLossCallbacks_ContainsPanic(t *testing.T) {
	t.Parallel()

	e := &Engine{}
	var before, after atomic.Int32
	e.RegisterLeaderLossCallback(func() { before.Add(1) })
	e.RegisterLeaderLossCallback(func() { panic("lease holder bug") })
	e.RegisterLeaderLossCallback(func() { after.Add(1) })

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

// TestAppliedIndex_LockFreeLoad confirms that AppliedIndex() reads the
// atomic mirror and does NOT acquire the engine's read-lock.
// Acquiring e.mu for write before calling AppliedIndex would deadlock
// if it were still RLock-based; the atomic path must return
// immediately regardless of lock state.
func TestAppliedIndex_LockFreeLoad(t *testing.T) {
	t.Parallel()
	e := &Engine{}
	e.appliedIndex.Store(42)

	// Hold the engine mutex exclusively. The atomic reader must not
	// block on this.
	e.mu.Lock()
	defer e.mu.Unlock()

	got := e.AppliedIndex()
	require.Equal(t, uint64(42), got)
}

// TestAppliedIndex_NilReceiver mirrors the other lease-related
// nil-receiver guards.
func TestAppliedIndex_NilReceiver(t *testing.T) {
	t.Parallel()
	var e *Engine
	require.Equal(t, uint64(0), e.AppliedIndex())
}
