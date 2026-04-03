package kv

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type fakeLinearizableFuture struct {
	err error
}

func (f fakeLinearizableFuture) Error() error {
	return f.err
}

type fakeLinearizableRaft struct {
	state atomic.Uint32
	calls atomic.Int64

	mu          sync.Mutex
	commitIndex uint64
	err         error
	stats       map[string]string
}

func newFakeLinearizableRaft(state raft.RaftState, commitIndex uint64, stats map[string]string) *fakeLinearizableRaft {
	f := &fakeLinearizableRaft{
		commitIndex: commitIndex,
		stats:       make(map[string]string, len(stats)),
	}
	for k, v := range stats {
		f.stats[k] = v
	}
	f.state.Store(uint32(state))
	return f
}

func (f *fakeLinearizableRaft) State() raft.RaftState {
	return raft.RaftState(f.state.Load())
}

func (f *fakeLinearizableRaft) setState(state raft.RaftState) {
	f.state.Store(uint32(state))
}

func (f *fakeLinearizableRaft) CommitIndex() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.commitIndex
}

func (f *fakeLinearizableRaft) VerifyLeader() raft.Future {
	f.calls.Add(1)

	f.mu.Lock()
	defer f.mu.Unlock()
	return fakeLinearizableFuture{err: f.err}
}

func (f *fakeLinearizableRaft) Stats() map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make(map[string]string, len(f.stats))
	for k, v := range f.stats {
		out[k] = v
	}
	return out
}

func (f *fakeLinearizableRaft) setStats(stats map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.stats = make(map[string]string, len(stats))
	for k, v := range stats {
		f.stats[k] = v
	}
}

func (f *fakeLinearizableRaft) verifyCalls() int64 {
	return f.calls.Load()
}

func TestAppliedIndexTrackerWaitsForTarget(t *testing.T) {
	t.Parallel()

	tracker := newAppliedIndexTracker()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- tracker.WaitForAppliedIndex(ctx, 5)
	}()

	tracker.markAppliedIndex(4)
	select {
	case err := <-done:
		t.Fatalf("wait returned early: %v", err)
	default:
	}

	tracker.markAppliedIndex(5)
	require.NoError(t, <-done)
}

func TestLinearizableReadIndexWithWaiterWaitsForFSMApply(t *testing.T) {
	t.Parallel()

	r := newFakeLinearizableRaft(raft.Leader, 7, map[string]string{
		"applied_index": "7",
		"fsm_pending":   "1",
	})
	waiter := newAppliedIndexTracker()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	go func() {
		time.Sleep(20 * time.Millisecond)
		waiter.markAppliedIndex(7)
	}()

	index, err := linearizableReadIndexWithWaiter(ctx, r, waiter)
	require.NoError(t, err)
	require.Equal(t, uint64(7), index)
	require.Equal(t, int64(1), r.verifyCalls())
}

func TestLinearizableReadIndexWithWaiterUsesBootstrapFallbackBeforeFirstFSMApply(t *testing.T) {
	t.Parallel()

	r := newFakeLinearizableRaft(raft.Leader, 7, map[string]string{
		"applied_index": "7",
		"fsm_pending":   "0",
	})
	waiter := newAppliedIndexTracker()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	index, err := linearizableReadIndexWithWaiter(ctx, r, waiter)
	require.NoError(t, err)
	require.Equal(t, uint64(7), index)
}

func TestLinearizableReadIndexWithWaiterUsesBootstrapFallback(t *testing.T) {
	t.Parallel()

	r := newFakeLinearizableRaft(raft.Leader, 11, map[string]string{
		"applied_index": "11",
		"fsm_pending":   "0",
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	index, err := linearizableReadIndexWithWaiter(ctx, r, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(11), index)
}

func TestLinearizableReadIndexWithWaiterRejectsFollowers(t *testing.T) {
	t.Parallel()

	r := newFakeLinearizableRaft(raft.Follower, 3, nil)

	_, err := linearizableReadIndexWithWaiter(context.Background(), r, nil)
	require.ErrorIs(t, err, raft.ErrNotLeader)
	require.Equal(t, int64(0), r.verifyCalls())
}

func TestLinearizableReadIndexWithWaiterRejectsLeaderLossAfterTrackedWait(t *testing.T) {
	t.Parallel()

	r := newFakeLinearizableRaft(raft.Leader, 7, map[string]string{
		"applied_index": "7",
		"fsm_pending":   "1",
	})
	waiter := newAppliedIndexTracker()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	go func() {
		time.Sleep(20 * time.Millisecond)
		r.setState(raft.Follower)
		waiter.markAppliedIndex(7)
	}()

	_, err := linearizableReadIndexWithWaiter(ctx, r, waiter)
	require.ErrorIs(t, err, raft.ErrNotLeader)
}

func TestLinearizableReadIndexWithWaiterRejectsInvalidBootstrapStats(t *testing.T) {
	t.Parallel()

	r := newFakeLinearizableRaft(raft.Leader, 11, map[string]string{
		"applied_index": "bad",
		"fsm_pending":   "0",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	_, err := linearizableReadIndexWithWaiter(ctx, r, nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled))
}

func TestLinearizableReadIndexWithWaiterRejectsLeaderLossDuringBootstrapWait(t *testing.T) {
	t.Parallel()

	r := newFakeLinearizableRaft(raft.Leader, 11, map[string]string{
		"applied_index": "0",
		"fsm_pending":   "1",
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	go func() {
		time.Sleep(20 * time.Millisecond)
		r.setState(raft.Follower)
		r.setStats(map[string]string{
			"applied_index": "11",
			"fsm_pending":   "0",
		})
	}()

	_, err := linearizableReadIndexWithWaiter(ctx, r, nil)
	require.ErrorIs(t, err, raft.ErrNotLeader)
}
