package kv

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
)

// fakeLeaseEngine implements raftengine.Engine + raftengine.LeaseProvider
// with controllable applied index, lease duration, and LinearizableRead
// behaviour, plus call counters for assertions.
type fakeLeaseEngine struct {
	applied                  uint64
	leaseDur                 time.Duration
	linearizableErr          error
	linearizableCalls        atomic.Int32
	state                    atomic.Value // stores raftengine.State; default Leader
	lastQuorumAckUnixNano    atomic.Int64 // 0 = no ack yet. Updated by ackNow().
	leaderLossCallbacksMu    sync.Mutex
	leaderLossCallbacks      []fakeLeaseEngineCb
	registerLeaderLossCalled atomic.Int32
}


// fakeLeaseEngineCb pairs a callback with a unique sentinel pointer so
// deregister can target THIS specific registration even when callbacks
// are removed out of order, matching the production etcd engine.
type fakeLeaseEngineCb struct {
	id *struct{}
	fn func()
}

func (e *fakeLeaseEngine) State() raftengine.State {
	if v := e.state.Load(); v != nil {
		return v.(raftengine.State) //nolint:forcetypeassert
	}
	return raftengine.StateLeader
}
func (e *fakeLeaseEngine) Leader() raftengine.LeaderInfo {
	return raftengine.LeaderInfo{ID: "n1", Address: "127.0.0.1:0"}
}
func (e *fakeLeaseEngine) VerifyLeader(context.Context) error { return nil }
func (e *fakeLeaseEngine) LinearizableRead(context.Context) (uint64, error) {
	e.linearizableCalls.Add(1)
	if e.linearizableErr != nil {
		return 0, e.linearizableErr
	}
	return e.applied, nil
}
func (e *fakeLeaseEngine) Status() raftengine.Status {
	return raftengine.Status{State: raftengine.StateLeader, AppliedIndex: e.applied}
}
func (e *fakeLeaseEngine) Configuration(context.Context) (raftengine.Configuration, error) {
	return raftengine.Configuration{}, nil
}
func (e *fakeLeaseEngine) Propose(context.Context, []byte) (*raftengine.ProposalResult, error) {
	return &raftengine.ProposalResult{}, nil
}
func (e *fakeLeaseEngine) Close() error                 { return nil }
func (e *fakeLeaseEngine) LeaseDuration() time.Duration { return e.leaseDur }
func (e *fakeLeaseEngine) AppliedIndex() uint64         { return e.applied }
func (e *fakeLeaseEngine) LastQuorumAck() time.Time {
	ns := e.lastQuorumAckUnixNano.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}
func (e *fakeLeaseEngine) RegisterLeaderLossCallback(fn func()) func() {
	e.registerLeaderLossCalled.Add(1)
	// Unique sentinel per registration so deregister can target THIS
	// entry even after earlier entries were removed. Mirrors the
	// production etcd engine semantics; a naive index-based remover
	// would drop the wrong callback under out-of-order deregister.
	slot := &struct{}{}
	e.leaderLossCallbacksMu.Lock()
	e.leaderLossCallbacks = append(e.leaderLossCallbacks, fakeLeaseEngineCb{id: slot, fn: fn})
	e.leaderLossCallbacksMu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			e.leaderLossCallbacksMu.Lock()
			defer e.leaderLossCallbacksMu.Unlock()
			for i, c := range e.leaderLossCallbacks {
				if c.id != slot {
					continue
				}
				// Zero the tail before truncating so the removed
				// callback's captured *Coordinate can be GC'd.
				// Mirrors the production etcd engine.
				last := len(e.leaderLossCallbacks) - 1
				copy(e.leaderLossCallbacks[i:], e.leaderLossCallbacks[i+1:])
				e.leaderLossCallbacks[last] = fakeLeaseEngineCb{}
				e.leaderLossCallbacks = e.leaderLossCallbacks[:last]
				return
			}
		})
	}
}

func (e *fakeLeaseEngine) fireLeaderLoss() {
	e.leaderLossCallbacksMu.Lock()
	cbs := make([]func(), len(e.leaderLossCallbacks))
	for i, c := range e.leaderLossCallbacks {
		cbs[i] = c.fn
	}
	e.leaderLossCallbacksMu.Unlock()
	for _, cb := range cbs {
		cb()
	}
}

// nonLeaseEngine implements only raftengine.Engine, not LeaseProvider.
// Used to verify the type-assertion fallback.
type nonLeaseEngine struct {
	linearizableCalls atomic.Int32
	linearizableErr   error
}

func (e *nonLeaseEngine) State() raftengine.State { return raftengine.StateLeader }
func (e *nonLeaseEngine) Leader() raftengine.LeaderInfo {
	return raftengine.LeaderInfo{ID: "n1", Address: "127.0.0.1:0"}
}
func (e *nonLeaseEngine) VerifyLeader(context.Context) error { return nil }
func (e *nonLeaseEngine) LinearizableRead(context.Context) (uint64, error) {
	e.linearizableCalls.Add(1)
	if e.linearizableErr != nil {
		return 0, e.linearizableErr
	}
	return 42, nil
}
func (e *nonLeaseEngine) Status() raftengine.Status {
	return raftengine.Status{State: raftengine.StateLeader, AppliedIndex: 42}
}
func (e *nonLeaseEngine) Configuration(context.Context) (raftengine.Configuration, error) {
	return raftengine.Configuration{}, nil
}
func (e *nonLeaseEngine) Propose(context.Context, []byte) (*raftengine.ProposalResult, error) {
	return &raftengine.ProposalResult{}, nil
}
func (e *nonLeaseEngine) Close() error { return nil }

// --- Coordinate.LeaseRead -----------------------------------------------

func TestCoordinate_LeaseRead_FastPathSkipsEngine(t *testing.T) {
	t.Parallel()
	eng := &fakeLeaseEngine{applied: 100, leaseDur: time.Hour}
	c := NewCoordinatorWithEngine(nil, eng)

	c.lease.extend(time.Now().Add(time.Hour), c.lease.generation())

	idx, err := c.LeaseRead(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(100), idx)
	require.Equal(t, int32(0), eng.linearizableCalls.Load())
}

func TestCoordinate_LeaseRead_SlowPathRefreshesLease(t *testing.T) {
	t.Parallel()
	eng := &fakeLeaseEngine{applied: 50, leaseDur: time.Hour}
	c := NewCoordinatorWithEngine(nil, eng)

	idx, err := c.LeaseRead(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(50), idx)
	require.Equal(t, int32(1), eng.linearizableCalls.Load())

	require.True(t, c.lease.valid(time.Now()))

	idx2, err := c.LeaseRead(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(50), idx2)
	require.Equal(t, int32(1), eng.linearizableCalls.Load(), "second read should hit fast path")
}

func TestCoordinate_LeaseRead_ErrorInvalidatesLease(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("read-index failed")
	eng := &fakeLeaseEngine{applied: 7, leaseDur: time.Hour, linearizableErr: sentinel}
	c := NewCoordinatorWithEngine(nil, eng)

	c.lease.extend(time.Now().Add(time.Hour), c.lease.generation())
	c.lease.invalidate() // force slow path

	_, err := c.LeaseRead(context.Background())
	require.ErrorIs(t, err, sentinel)
	require.False(t, c.lease.valid(time.Now()))
	require.Equal(t, int32(1), eng.linearizableCalls.Load())

	// Subsequent call also takes slow path because lease is invalidated.
	_, err = c.LeaseRead(context.Background())
	require.ErrorIs(t, err, sentinel)
	require.Equal(t, int32(2), eng.linearizableCalls.Load())
}

func TestCoordinate_LeaseRead_FallbackWhenEngineNotLeader(t *testing.T) {
	t.Parallel()
	// Even with a currently-valid lease, if the engine already reports
	// a non-leader state (e.g. a leader-loss transition that has not
	// yet triggered the async invalidation callback), LeaseRead must
	// NOT return the fast-path AppliedIndex -- it must fall through
	// to LinearizableRead, which will fail fast on a non-leader.
	sentinel := errors.New("not leader")
	eng := &fakeLeaseEngine{applied: 7, leaseDur: time.Hour, linearizableErr: sentinel}
	c := NewCoordinatorWithEngine(nil, eng)

	// Warm the lease so valid() returns true.
	c.lease.extend(time.Now().Add(time.Hour), c.lease.generation())
	require.True(t, c.lease.valid(time.Now()))

	// Engine transitioned to follower (or unknown); async invalidate
	// hasn't run yet.
	eng.state.Store(raftengine.StateFollower)

	_, err := c.LeaseRead(context.Background())
	require.ErrorIs(t, err, sentinel,
		"fast path must not hide an already-known non-leader state")
	require.Equal(t, int32(1), eng.linearizableCalls.Load(),
		"non-leader state must force the slow path")
}

func TestCoordinate_LeaseRead_FallbackWhenLeaseDurationZero(t *testing.T) {
	t.Parallel()
	// Misconfigured tick settings can produce LeaseDuration <= 0.
	// The implementation must short-circuit to LinearizableRead
	// without touching lease state; otherwise extend(now+0, ...) would
	// run on every slow-path call for no benefit.
	eng := &fakeLeaseEngine{applied: 3, leaseDur: 0}
	c := NewCoordinatorWithEngine(nil, eng)

	idx, err := c.LeaseRead(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(3), idx)
	require.Equal(t, int32(1), eng.linearizableCalls.Load())
	require.False(t, c.lease.valid(time.Now()),
		"lease must not have been extended when LeaseDuration <= 0")

	// Every subsequent call must still take the slow path.
	_, err = c.LeaseRead(context.Background())
	require.NoError(t, err)
	require.Equal(t, int32(2), eng.linearizableCalls.Load())
}

func TestCoordinate_LeaseRead_FallbackWhenEngineLacksLeaseProvider(t *testing.T) {
	t.Parallel()
	eng := &nonLeaseEngine{}
	c := NewCoordinatorWithEngine(nil, eng)

	idx, err := c.LeaseRead(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(42), idx)
	require.Equal(t, int32(1), eng.linearizableCalls.Load())

	// Without LeaseProvider the lease never becomes valid; every call
	// goes through LinearizableRead.
	_, err = c.LeaseRead(context.Background())
	require.NoError(t, err)
	require.Equal(t, int32(2), eng.linearizableCalls.Load())
}

// --- Leader-loss invalidation hook --------------------------------------

func TestCoordinate_CloseDeregistersLeaderLossCallback(t *testing.T) {
	t.Parallel()
	eng := &fakeLeaseEngine{applied: 1, leaseDur: time.Hour}
	c := NewCoordinatorWithEngine(nil, eng)
	require.Equal(t, int32(1), eng.registerLeaderLossCalled.Load())

	require.NoError(t, c.Close())

	// After Close, firing leader-loss must NOT invoke this Coordinate's
	// invalidate (it must have been removed from the engine's slice).
	c.lease.extend(time.Now().Add(time.Hour), c.lease.generation())
	require.True(t, c.lease.valid(time.Now()))
	eng.fireLeaderLoss()
	require.True(t, c.lease.valid(time.Now()),
		"Close must remove the callback so subsequent leader-loss firings do NOT touch this Coordinate's lease")

	// Close is idempotent.
	require.NoError(t, c.Close())
}

func TestCoordinate_RegistersLeaderLossCallback(t *testing.T) {
	t.Parallel()
	eng := &fakeLeaseEngine{applied: 1, leaseDur: time.Hour}
	c := NewCoordinatorWithEngine(nil, eng)
	require.Equal(t, int32(1), eng.registerLeaderLossCalled.Load())

	c.lease.extend(time.Now().Add(time.Hour), c.lease.generation())
	require.True(t, c.lease.valid(time.Now()))

	eng.fireLeaderLoss()
	require.False(t, c.lease.valid(time.Now()),
		"leader-loss callback must invalidate the lease")
}

// --- Amortization end-to-end ---------------------------------------------

// TestCoordinate_LeaseRead_AmortizesLinearizableRead is the Phase-4 design
// item proving the lease actually amortizes the cost: N calls within a
// single lease window must trigger only the first slow-path
// LinearizableRead and N-1 fast-path returns.
func TestCoordinate_LeaseRead_AmortizesLinearizableRead(t *testing.T) {
	t.Parallel()
	const N = 100
	eng := &fakeLeaseEngine{applied: 9, leaseDur: time.Hour}
	c := NewCoordinatorWithEngine(nil, eng)

	for i := 0; i < N; i++ {
		idx, err := c.LeaseRead(context.Background())
		require.NoError(t, err)
		require.Equal(t, uint64(9), idx)
	}

	require.Equal(t, int32(1), eng.linearizableCalls.Load(),
		"100 LeaseRead calls inside the lease window should trigger exactly 1 LinearizableRead")
}
