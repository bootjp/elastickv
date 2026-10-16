package adapter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestInflightGateRefusesEntryOnceDraining(t *testing.T) {
	gate := newInflightGate()
	require.True(t, gate.enter())
	gate.exit()
	require.True(t, gate.drain(time.Second))
	require.False(t, gate.enter(), "no operation may start once the drain began")
	require.Zero(t, gate.remaining())

	var nilGate *inflightGate
	require.True(t, nilGate.enter())
	nilGate.exit()
	require.True(t, nilGate.drain(time.Second))
}

// An operation that entered before the drain keeps it waiting until it
// exits; only the first drain waits.
func TestInflightGateDrainWaitsForRunningOperation(t *testing.T) {
	gate := newInflightGate()
	require.True(t, gate.enter())
	go func() {
		time.Sleep(100 * time.Millisecond)
		gate.exit()
	}()
	start := time.Now()
	require.True(t, gate.drain(5*time.Second))
	require.GreaterOrEqual(t, time.Since(start), 80*time.Millisecond)

	stuck := newInflightGate()
	require.True(t, stuck.enter())
	start = time.Now()
	require.False(t, stuck.drain(100*time.Millisecond))
	require.GreaterOrEqual(t, time.Since(start), 80*time.Millisecond)
	start = time.Now()
	require.False(t, stuck.drain(5*time.Second), "a second drain reports without waiting again")
	require.Less(t, time.Since(start), 50*time.Millisecond)
}

// join waits, once, for an operation that outlived the drain to exit.
func TestInflightGateJoinWaitsAfterDrainGaveUp(t *testing.T) {
	gate := newInflightGate()
	require.True(t, gate.enter())
	require.False(t, gate.drain(10*time.Millisecond))
	go func() {
		time.Sleep(100 * time.Millisecond)
		gate.exit()
	}()
	start := time.Now()
	require.True(t, gate.join(5*time.Second))
	require.GreaterOrEqual(t, time.Since(start), 80*time.Millisecond)
	require.True(t, gate.join(5*time.Second), "an idle gate joins at once")

	stuck := newInflightGate()
	require.True(t, stuck.enter())
	require.False(t, stuck.drain(10*time.Millisecond))
	start = time.Now()
	require.False(t, stuck.join(100*time.Millisecond))
	require.GreaterOrEqual(t, time.Since(start), 80*time.Millisecond)
	start = time.Now()
	require.False(t, stuck.join(5*time.Second), "a second join reports without waiting again")
	require.Less(t, time.Since(start), 50*time.Millisecond)

	var nilGate *inflightGate
	require.True(t, nilGate.join(time.Second))
}
