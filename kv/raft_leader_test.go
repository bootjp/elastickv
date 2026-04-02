package kv

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type fakeRaftLeaderFuture struct {
	err error
}

func (f fakeRaftLeaderFuture) Error() error {
	return f.err
}

type fakeRaftLeader struct {
	state atomic.Uint32
	calls atomic.Int64

	mu         sync.Mutex
	verifyErr  error
	verifyWait time.Duration
}

func newFakeRaftLeader() *fakeRaftLeader {
	f := &fakeRaftLeader{}
	f.state.Store(uint32(raft.Leader))
	return f
}

func (f *fakeRaftLeader) State() raft.RaftState {
	return raft.RaftState(f.state.Load())
}

func (f *fakeRaftLeader) VerifyLeader() raft.Future {
	f.calls.Add(1)

	f.mu.Lock()
	wait := f.verifyWait
	err := f.verifyErr
	f.mu.Unlock()

	if wait > 0 {
		time.Sleep(wait)
	}
	return fakeRaftLeaderFuture{err: err}
}

func (f *fakeRaftLeader) setState(state raft.RaftState) {
	f.state.Store(uint32(state))
}

func (f *fakeRaftLeader) setVerifyError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.verifyErr = err
}

func (f *fakeRaftLeader) setVerifyWait(wait time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.verifyWait = wait
}

func (f *fakeRaftLeader) verifyCalls() int64 {
	return f.calls.Load()
}

func TestRaftLeaderVerifyCache_ReusesRecentSuccess(t *testing.T) {
	t.Parallel()

	cache := newRaftLeaderVerifyCache(50 * time.Millisecond)
	r := newFakeRaftLeader()

	require.NoError(t, cache.verify(r))
	require.NoError(t, cache.verify(r))
	require.Equal(t, int64(1), r.verifyCalls())
}

func TestRaftLeaderVerifyCache_ExpiresAndRechecks(t *testing.T) {
	t.Parallel()

	cache := newRaftLeaderVerifyCache(10 * time.Millisecond)
	r := newFakeRaftLeader()

	require.NoError(t, cache.verify(r))
	time.Sleep(15 * time.Millisecond)
	require.NoError(t, cache.verify(r))
	require.Equal(t, int64(2), r.verifyCalls())
}

func TestRaftLeaderVerifyCache_DeduplicatesConcurrentVerify(t *testing.T) {
	t.Parallel()

	cache := newRaftLeaderVerifyCache(0)
	r := newFakeRaftLeader()
	r.setVerifyWait(25 * time.Millisecond)

	var wg sync.WaitGroup
	errCh := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- cache.verify(r)
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}

	require.Equal(t, int64(1), r.verifyCalls())
}

func TestRaftLeaderVerifyCache_ClearsOnFollowerAndFailure(t *testing.T) {
	t.Parallel()

	cache := newRaftLeaderVerifyCache(time.Minute)
	r := newFakeRaftLeader()

	require.NoError(t, cache.verify(r))
	require.Equal(t, int64(1), r.verifyCalls())

	r.setState(raft.Follower)
	require.ErrorIs(t, cache.verify(r), raft.ErrNotLeader)
	require.Equal(t, int64(1), r.verifyCalls())

	r.setState(raft.Leader)
	r.setVerifyError(raft.ErrLeadershipLost)
	require.ErrorIs(t, cache.verify(r), raft.ErrLeadershipLost)
	require.Equal(t, int64(2), r.verifyCalls())

	r.setVerifyError(nil)
	require.NoError(t, cache.verify(r))
	require.Equal(t, int64(3), r.verifyCalls())
}

func TestRaftLeaderVerifyCache_UnregisterRemovesState(t *testing.T) {
	t.Parallel()

	cache := newRaftLeaderVerifyCache(time.Minute)
	r := newFakeRaftLeader()

	require.NoError(t, cache.verify(r))

	_, ok := cache.states.Load(r)
	require.True(t, ok)

	cache.unregister(r)

	_, ok = cache.states.Load(r)
	require.False(t, ok)
}
