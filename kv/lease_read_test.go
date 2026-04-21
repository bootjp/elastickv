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
	// Honor the raftengine.LeaseProvider contract that non-leaders
	// return the zero time, mirroring the production etcd engine. A
	// test that sets a fresh ack and a non-leader state MUST still
	// see the slow path taken; a divergent fake would hide regressions
	// where production code stops gating on engine.State() before
	// consulting LastQuorumAck.
	if e.State() != raftengine.StateLeader {
		return time.Time{}
	}
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

// setQuorumAck is a test helper that drives the engine-driven lease
// anchor on the fake engine so tests can exercise the new PRIMARY
// fast path (LastQuorumAck + State==Leader) independently of the
// caller-side lease state.
func (e *fakeLeaseEngine) setQuorumAck(t time.Time) {
	if t.IsZero() {
		e.lastQuorumAckUnixNano.Store(0)
		return
	}
	e.lastQuorumAckUnixNano.Store(t.UnixNano())
}

// --- Coordinate.LeaseRead -----------------------------------------------

// TestCoordinate_LeaseRead_EngineAckFastPath covers the engine-driven
// primary path introduced in feat/engine-driven-lease: a fresh
// LastQuorumAck alone (cold caller-side lease, no prior
// LinearizableRead) must satisfy LeaseRead without consulting the
// engine's slow-path read API.
func TestCoordinate_LeaseRead_EngineAckFastPath(t *testing.T) {
	t.Parallel()
	eng := &fakeLeaseEngine{applied: 123, leaseDur: time.Hour}
	eng.setQuorumAck(time.Now())
	c := NewCoordinatorWithEngine(nil, eng)

	require.False(t, c.lease.valid(time.Now()),
		"caller-side lease must start cold so the fast-path hit is attributable to the engine ack")

	idx, err := c.LeaseRead(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(123), idx)
	require.Equal(t, int32(0), eng.linearizableCalls.Load(),
		"engine-driven ack alone must skip LinearizableRead")
	require.False(t, c.lease.valid(time.Now()),
		"engine-driven fast path must not warm the caller-side lease")
}

// TestCoordinate_LeaseRead_EngineAckStaleFallsThrough covers the
// stale-ack case: if the engine's ack has aged past LeaseDuration we
// must NOT serve from AppliedIndex alone, and instead take the slow
// path through LinearizableRead.
func TestCoordinate_LeaseRead_EngineAckStaleFallsThrough(t *testing.T) {
	t.Parallel()
	eng := &fakeLeaseEngine{applied: 7, leaseDur: 50 * time.Millisecond}
	// Set the ack far enough in the past that time.Since(ack) > leaseDur.
	eng.setQuorumAck(time.Now().Add(-time.Hour))
	c := NewCoordinatorWithEngine(nil, eng)

	_, err := c.LeaseRead(context.Background())
	require.NoError(t, err)
	require.Equal(t, int32(1), eng.linearizableCalls.Load(),
		"stale engine ack must fall through to LinearizableRead")
}

// TestCoordinate_LeaseRead_EngineAckIgnoredWhenNotLeader covers the
// engine-state guard: even with a fresh ack, if the engine reports a
// non-leader role the fast path must NOT fire -- the ack could be
// inherited state from a just-lost leader term.
func TestCoordinate_LeaseRead_EngineAckIgnoredWhenNotLeader(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("not leader")
	eng := &fakeLeaseEngine{applied: 7, leaseDur: time.Hour, linearizableErr: sentinel}
	eng.setQuorumAck(time.Now())
	eng.state.Store(raftengine.StateFollower)
	c := NewCoordinatorWithEngine(nil, eng)

	_, err := c.LeaseRead(context.Background())
	require.ErrorIs(t, err, sentinel)
	require.Equal(t, int32(1), eng.linearizableCalls.Load(),
		"non-leader state must bypass the engine ack fast path")
}

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

// countingLeaseObserver records hits and misses for assertion purposes.
type countingLeaseObserver struct {
	hits   atomic.Int32
	misses atomic.Int32
}

func (o *countingLeaseObserver) ObserveLeaseRead(hit bool) {
	if hit {
		o.hits.Add(1)
		return
	}
	o.misses.Add(1)
}

func TestCoordinate_LeaseRead_ObserverSeparatesHitsFromMisses(t *testing.T) {
	t.Parallel()
	eng := &fakeLeaseEngine{applied: 77, leaseDur: time.Hour}
	obs := &countingLeaseObserver{}
	c := NewCoordinatorWithEngine(nil, eng, WithLeaseReadObserver(obs))

	// First call: lease not yet extended → slow path, MISS.
	_, err := c.LeaseRead(context.Background())
	require.NoError(t, err)
	require.Equal(t, int32(0), obs.hits.Load())
	require.Equal(t, int32(1), obs.misses.Load())

	// The slow path refreshed the lease; the next N calls must all be hits.
	for i := 0; i < 5; i++ {
		_, err := c.LeaseRead(context.Background())
		require.NoError(t, err)
	}
	require.Equal(t, int32(5), obs.hits.Load())
	require.Equal(t, int32(1), obs.misses.Load())
}

// --- leaseReadEngineCtx -------------------------------------------------

// TestLeaseReadEngineCtx_FastPath_SkipsLinearizableRead covers the
// core invariant for ShardStore.GetAt: a fresh LastQuorumAck with
// Leader state must return AppliedIndex directly, without calling
// engine.LinearizableRead. This is the fix for the post-#573
// read-index dispatcher saturation (each in-script redis.call was
// going through LinearizableRead every time).
func TestLeaseReadEngineCtx_FastPath_SkipsLinearizableRead(t *testing.T) {
	t.Parallel()
	eng := &fakeLeaseEngine{applied: 42, leaseDur: time.Hour}
	eng.setQuorumAck(time.Now())

	idx, err := leaseReadEngineCtx(context.Background(), eng)
	require.NoError(t, err)
	require.Equal(t, uint64(42), idx)
	require.Equal(t, int32(0), eng.linearizableCalls.Load(),
		"fresh engine-driven lease must skip the LinearizableRead round-trip")
}

// TestLeaseReadEngineCtx_StaleAck_FallsThroughToLinearizable makes
// sure the fast path isn't taken when the engine's last quorum ack
// has aged past LeaseDuration.
func TestLeaseReadEngineCtx_StaleAck_FallsThroughToLinearizable(t *testing.T) {
	t.Parallel()
	eng := &fakeLeaseEngine{applied: 7, leaseDur: 50 * time.Millisecond}
	eng.setQuorumAck(time.Now().Add(-time.Hour))

	idx, err := leaseReadEngineCtx(context.Background(), eng)
	require.NoError(t, err)
	require.Equal(t, uint64(7), idx)
	require.Equal(t, int32(1), eng.linearizableCalls.Load(),
		"stale ack must take the slow path exactly once")
}

// TestLeaseReadEngineCtx_NotLeader_FallsThrough verifies that a
// fresh ack alone is not sufficient — the engine must currently be
// Leader. Mirrors engineLeaseAckValid's state guard.
func TestLeaseReadEngineCtx_NotLeader_FallsThrough(t *testing.T) {
	t.Parallel()
	eng := &fakeLeaseEngine{applied: 99, leaseDur: time.Hour}
	eng.setQuorumAck(time.Now())
	eng.state.Store(raftengine.StateFollower)
	// On a non-leader the fake honours the LeaseProvider contract and
	// returns zero ack, so the engineLeaseAckValid state guard also
	// fires — both layers must fail closed.
	eng.linearizableErr = errors.New("not leader")

	_, err := leaseReadEngineCtx(context.Background(), eng)
	require.Error(t, err)
	require.Equal(t, int32(1), eng.linearizableCalls.Load(),
		"non-leader must take the slow path; leader-only fast path must NOT serve")
}

// TestLeaseReadEngineCtx_NoLeaseProvider_FallsThrough covers engines
// that do not implement LeaseProvider. The helper must still work by
// routing every call through LinearizableRead.
func TestLeaseReadEngineCtx_NoLeaseProvider_FallsThrough(t *testing.T) {
	t.Parallel()
	eng := &nonLeaseEngine{}

	idx, err := leaseReadEngineCtx(context.Background(), eng)
	require.NoError(t, err)
	require.Equal(t, uint64(42), idx)
}

// TestLeaseReadEngineCtx_NilEngine returns ErrLeaderNotFound without
// panicking. Protects against a regression where the nil guard is
// removed.
func TestLeaseReadEngineCtx_NilEngine(t *testing.T) {
	t.Parallel()
	_, err := leaseReadEngineCtx(context.Background(), nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrLeaderNotFound)
}
