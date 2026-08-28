package kv

import (
	"context"
	"errors"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/stretchr/testify/require"
)

// countingAbortAllocator hands out consecutive values the way BatchAllocator
// does from a reserved window, so the value adjacent to a commit timestamp is
// the one the next caller would receive.
type countingAbortAllocator struct {
	next  uint64
	calls int
	err   error
}

func (a *countingAbortAllocator) Next(context.Context) (uint64, error) {
	a.calls++
	if a.err != nil {
		return 0, a.err
	}
	a.next++
	return a.next, nil
}

func (a *countingAbortAllocator) NextAfter(_ context.Context, minTS uint64) (uint64, error) {
	a.calls++
	if a.err != nil {
		return 0, a.err
	}
	a.next++
	if a.next <= minTS {
		a.next = minTS + 1
	}
	return a.next, nil
}

// A rollback record written at commitTS+1 claims a value the allocator never
// handed out. With a window allocator that neighbour goes to the next
// transaction, so the record and that transaction would share one supposedly
// global timestamp.
func TestAbortTimestampIsAllocatedNotDerived(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)
	// The allocator's window is well past the commit timestamp, so an allocated
	// abort timestamp is plainly distinct from the derived neighbour.
	alloc := &countingAbortAllocator{next: 900}
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{1: {}}, 1, NewHLC(), nil).
		WithTSOAllocator(alloc)

	const (
		startTS  = uint64(400)
		commitTS = uint64(500)
	)
	abortTS := coord.abortTimestamp(context.Background(), startTS, commitTS)

	require.Positive(t, alloc.calls, "the abort timestamp must come from the allocator")
	require.Greater(t, abortTS, commitTS)
	require.NotEqual(t, abortTSFrom(startTS, commitTS), abortTS,
		"the derived neighbour is a value the allocator never handed out")

	// Whatever the allocator hands out next must not be the abort timestamp.
	nextForSomeoneElse, err := alloc.Next(context.Background())
	require.NoError(t, err)
	require.NotEqual(t, abortTS, nextForSomeoneElse)
}

// The cleanup this feeds releases prewrite intents, so it must still run when
// allocation fails. The derived value stays as the fallback -- which is what the
// code used unconditionally before.
func TestAbortTimestampFallsBackWhenAllocationFails(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)
	alloc := &countingAbortAllocator{next: 900, err: errors.New("allocator unavailable")}
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{1: {}}, 1, NewHLC(), nil).
		WithTSOAllocator(alloc)

	const (
		startTS  = uint64(400)
		commitTS = uint64(500)
	)
	abortTS := coord.abortTimestamp(context.Background(), startTS, commitTS)
	require.Equal(t, abortTSFrom(startTS, commitTS), abortTS)
	require.Greater(t, abortTS, startTS, "abortPreparedTxn drops anything at or below startTS")
}
