package kv

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/raftengine/raftenginetest"
	"github.com/bootjp/elastickv/internal/testutil/grpctest"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeInternal struct {
	pb.UnimplementedInternalServer

	mu      sync.Mutex
	calls   int
	lastReq *pb.ForwardRequest
	resp    *pb.ForwardResponse
	err     error
	failFor int
}

func (f *fakeInternal) Forward(_ context.Context, req *pb.ForwardRequest) (*pb.ForwardResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastReq = req
	if f.err != nil && (f.failFor == 0 || f.calls <= f.failFor) {
		return nil, f.err
	}
	if f.resp != nil {
		return f.resp, nil
	}
	return &pb.ForwardResponse{Success: true, CommitIndex: 0}, nil
}

func (f *fakeInternal) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func startLeaderProxyBlackhole(t *testing.T) string {
	t.Helper()
	var lc net.ListenConfig
	lis, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	stop := make(chan struct{})
	go func() {
		for {
			conn, acceptErr := lis.Accept()
			if acceptErr != nil {
				return
			}
			go func() {
				<-stop
				_ = conn.Close()
			}()
		}
	}()
	t.Cleanup(func() {
		close(stop)
		_ = lis.Close()
	})
	return lis.Addr().String()
}

// newStubFollowerEngine reports the local node as a follower of a leader at
// leaderAddr and rejects proposals and reads with ErrNotLeader. It exercises
// the forwarding code path without running a real two-node raft cluster.
func newStubFollowerEngine(leaderAddr string) *testEngine {
	leader := raftengine.LeaderInfo{ID: "leader", Address: leaderAddr}
	return &testEngine{
		StaticLeaderView: raftenginetest.StaticLeaderView{
			StateValue:  raftengine.StateFollower,
			LeaderValue: leader,
			ReadErr:     raftengine.ErrNotLeader,
		},
		ProposeFunc: func(context.Context, []byte) (*raftengine.ProposalResult, error) {
			return nil, raftengine.ErrNotLeader
		},
		StatusValue:  raftengine.Status{State: raftengine.StateFollower, Leader: leader},
		SnapshotStep: 10_000,
	}
}

func TestLeaderProxy_CommitLocalWhenLeader(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	r, stop := newSingleRaft(t, "lp-local", NewKvFSMWithHLC(st, NewHLC()))
	defer stop()

	p := NewLeaderProxyWithEngine(r)

	reqs := []*pb.Request{
		{
			IsTxn: false,
			Phase: pb.Phase_NONE,
			Ts:    10,
			Mutations: []*pb.Mutation{
				{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("v")},
			},
		},
	}
	resp, err := p.Commit(context.Background(), reqs)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Greater(t, resp.CommitIndex, uint64(0))

	got, err := st.GetAt(context.Background(), []byte("k"), ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, []byte("v"), got)
}

func TestLeaderProxy_ForwardsWhenFollower(t *testing.T) {
	t.Parallel()

	svc := &fakeInternal{resp: &pb.ForwardResponse{Success: true, CommitIndex: 123, CommitTs: 456}}
	addr := grpctest.Serve(t, func(srv *grpc.Server) { pb.RegisterInternalServer(srv, svc) })

	// Wait briefly so the gRPC server is ready to serve.
	dialer := &net.Dialer{Timeout: 100 * time.Millisecond}
	require.Eventually(t, func() bool {
		dialCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		c, err := dialer.DialContext(dialCtx, "tcp", addr)
		if err != nil {
			return false
		}
		_ = c.Close()
		return true
	}, 2*time.Second, 10*time.Millisecond)

	follower := newStubFollowerEngine(addr)
	p := NewLeaderProxyWithEngine(follower)
	t.Cleanup(func() { _ = p.connCache.Close() })

	reqs := []*pb.Request{
		{
			IsTxn: false,
			Phase: pb.Phase_NONE,
			Ts:    10,
			Mutations: []*pb.Mutation{
				{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("v")},
			},
		},
	}

	resp, err := p.Commit(context.Background(), reqs)
	require.NoError(t, err)
	require.Equal(t, uint64(123), resp.CommitIndex)
	require.Equal(t, uint64(456), resp.CommitTS)

	svc.mu.Lock()
	defer svc.mu.Unlock()
	require.Equal(t, 1, svc.calls)
	require.NotNil(t, svc.lastReq)
	require.Len(t, svc.lastReq.Requests, 1)
}

// togglingFollowerEngine starts out reporting no resolvable leader and
// flips to a caller-supplied address after setLeader() is invoked. It
// models the re-election window during which forward() must poll rather
// than return ErrLeaderNotFound immediately.
type togglingFollowerEngine struct {
	addr atomic.Pointer[string]
}

func (e *togglingFollowerEngine) setLeader(addr string) {
	s := addr
	e.addr.Store(&s)
}

func (e *togglingFollowerEngine) Propose(context.Context, []byte) (*raftengine.ProposalResult, error) {
	return nil, raftengine.ErrNotLeader
}
func (e *togglingFollowerEngine) ProposeAdmin(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	return e.Propose(ctx, data)
}
func (e *togglingFollowerEngine) State() raftengine.State { return raftengine.StateFollower }
func (e *togglingFollowerEngine) Leader() raftengine.LeaderInfo {
	p := e.addr.Load()
	if p == nil {
		return raftengine.LeaderInfo{}
	}
	return raftengine.LeaderInfo{ID: "leader", Address: *p}
}
func (e *togglingFollowerEngine) VerifyLeader(context.Context) error { return raftengine.ErrNotLeader }
func (e *togglingFollowerEngine) LinearizableRead(context.Context) (uint64, error) {
	return 0, raftengine.ErrNotLeader
}
func (e *togglingFollowerEngine) Status() raftengine.Status {
	return raftengine.Status{State: raftengine.StateFollower, Leader: e.Leader()}
}
func (e *togglingFollowerEngine) Configuration(context.Context) (raftengine.Configuration, error) {
	return raftengine.Configuration{}, nil
}
func (e *togglingFollowerEngine) SnapshotEvery() uint64 { return 10_000 }
func (e *togglingFollowerEngine) Close() error          { return nil }

func TestLeaderProxy_ForwardsAfterLeaderPublishes(t *testing.T) {
	t.Parallel()

	// Bring up a real fake gRPC Forward server; LeaderProxy.forward()
	// dials it via the connCache. We leave the engine's leader address
	// empty initially so the first few forward() attempts fail with
	// ErrLeaderNotFound, then flip to the real address after a brief
	// delay. forwardWithRetry must absorb the empty-address window and
	// succeed once the engine publishes.

	svc := &fakeInternal{resp: &pb.ForwardResponse{Success: true, CommitIndex: 42}}
	addr := grpctest.Serve(t, func(srv *grpc.Server) { pb.RegisterInternalServer(srv, svc) })

	// Wait briefly so the gRPC server is ready.
	dialer := &net.Dialer{Timeout: 100 * time.Millisecond}
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		c, err := dialer.DialContext(ctx, "tcp", addr)
		if err != nil {
			return false
		}
		_ = c.Close()
		return true
	}, 2*time.Second, 10*time.Millisecond)

	eng := &togglingFollowerEngine{}
	p := NewLeaderProxyWithEngine(eng)
	t.Cleanup(func() { _ = p.connCache.Close() })

	reqs := []*pb.Request{
		{
			IsTxn: false,
			Phase: pb.Phase_NONE,
			Ts:    10,
			Mutations: []*pb.Mutation{
				{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("v")},
			},
		},
	}

	// Anchor start BEFORE launching the publish goroutine so the
	// publishDelay lower bound on elapsed is measured from the same
	// instant as the Commit call. Capturing start after the goroutine
	// launch would subtract the goroutine-scheduling time from
	// elapsed and can let the GreaterOrEqual(elapsed, publishDelay)
	// assertion flake when the proxy really did wait the full delay.
	publishDelay := 100 * time.Millisecond
	start := time.Now()
	go func() {
		time.Sleep(publishDelay)
		eng.setLeader(addr)
	}()

	resp, err := p.Commit(context.Background(), reqs)
	elapsed := time.Since(start)
	require.NoError(t, err)
	require.Equal(t, uint64(42), resp.CommitIndex)
	// The proxy must have waited at least until setLeader fired;
	// otherwise it did not actually poll the missing-leader window.
	require.GreaterOrEqual(t, elapsed, publishDelay)
	// And it must have stopped polling well before the budget expires.
	require.Less(t, elapsed, leaderProxyRetryBudget)
}

func TestLeaderProxy_FailsAfterLeaderBudgetElapses(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("exhausts the full leaderProxyRetryBudget (5s); skipped in -short mode")
	}

	// No gRPC server, no address ever published → every forward()
	// returns ErrLeaderNotFound immediately. forwardWithRetry must loop
	// until leaderProxyRetryBudget elapses and then surface the final
	// ErrLeaderNotFound instead of hanging forever. To keep the test
	// snappy we only need to assert that (a) the call eventually returns
	// with an error, (b) the error chain still contains ErrLeaderNotFound
	// after the budget is exhausted.
	//
	// We cap the retry budget at its package default (5s); that is the
	// contract we want to pin. Running the full 5s here is acceptable as
	// a single pinned test, especially since it runs in t.Parallel.
	eng := &togglingFollowerEngine{}
	p := NewLeaderProxyWithEngine(eng)
	t.Cleanup(func() { _ = p.connCache.Close() })

	reqs := []*pb.Request{
		{
			IsTxn: false,
			Phase: pb.Phase_NONE,
			Ts:    10,
			Mutations: []*pb.Mutation{
				{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("v")},
			},
		},
	}

	start := time.Now()
	_, err := p.Commit(context.Background(), reqs)
	elapsed := time.Since(start)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrLeaderNotFound)
	require.GreaterOrEqual(t, elapsed, leaderProxyRetryBudget)
}

func TestLeaderProxy_CanceledForwardReleasesHalfOpenProbe(t *testing.T) {
	t.Parallel()

	eng := newStubFollowerEngine(startLeaderProxyBlackhole(t))
	p := NewLeaderProxyWithEngine(eng)
	t.Cleanup(func() { _ = p.connCache.Close() })

	identity := leaderProxyIdentityFromEngine(eng)
	openedAt := time.Now().Add(-2 * leaderProxyBreakerBaseBackoff)
	for range leaderProxyBreakerFailureThreshold {
		require.NoError(t, p.forwardBreaker.allow(identity, 1, openedAt))
		p.forwardBreaker.record(identity, 1, ErrLeaderNotFound, openedAt)
	}

	callerCtx, cancelCaller := context.WithCancel(context.Background())
	proxyCtx, cancelProxy := context.WithTimeout(callerCtx, time.Second)
	time.AfterFunc(50*time.Millisecond, cancelCaller)
	_, err := p.forward(callerCtx, proxyCtx, []*pb.Request{{IsTxn: false}}, 1)
	cancelProxy()
	require.ErrorIs(t, err, ErrTransactionOutcomeUnknown)

	require.NoError(t, p.forwardBreaker.allow(identity, 3, time.Now()))
	require.True(t, p.forwardBreaker.owns(identity, 3))
}

func TestLeaderProxy_ProxyBudgetDeadlineCountsTowardBreaker(t *testing.T) {
	t.Parallel()

	eng := newStubFollowerEngine(startLeaderProxyBlackhole(t))
	p := NewLeaderProxyWithEngine(eng)
	t.Cleanup(func() { _ = p.connCache.Close() })
	identity := leaderProxyIdentityFromEngine(eng)
	reqs := []*pb.Request{{IsTxn: false}}

	for range leaderProxyBreakerFailureThreshold {
		proxyCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		_, forwardErr := p.forward(context.Background(), proxyCtx, reqs, 1)
		cancel()
		require.Equal(t, codes.DeadlineExceeded, status.Code(forwardErr))
	}

	require.ErrorIs(t, p.forwardBreaker.allow(identity, 2, time.Now()), ErrLeaderProxyCircuitOpen)
}

func TestLeaderProxy_CanceledRecoveryOwnerStopsRetrying(t *testing.T) {
	t.Parallel()

	eng := newStubFollowerEngine("127.0.0.1:1")
	p := NewLeaderProxyWithEngine(eng)
	t.Cleanup(func() { _ = p.connCache.Close() })
	identity := leaderProxyIdentityFromEngine(eng)
	const requestID = uint64(7)
	openedAt := time.Now()
	for range leaderProxyBreakerFailureThreshold {
		require.NoError(t, p.forwardBreaker.allow(identity, requestID, openedAt))
		p.forwardBreaker.record(identity, requestID, ErrLeaderNotFound, openedAt)
	}
	p.forwardSeq.Store(requestID - 1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := p.forwardWithRetry(ctx, []*pb.Request{{IsTxn: false}})

	require.ErrorIs(t, err, context.Canceled)
	require.False(t, p.forwardBreaker.owns(identity, requestID))
}

func TestLeaderProxy_LostForwardResponseDoesNotReplay(t *testing.T) {
	t.Parallel()

	svc := &fakeInternal{
		resp:    &pb.ForwardResponse{Success: true, CommitIndex: 77},
		err:     status.Error(codes.Unavailable, "startup gate closed"),
		failFor: leaderProxyBreakerFailureThreshold,
	}
	addr := grpctest.Serve(t, func(srv *grpc.Server) { pb.RegisterInternalServer(srv, svc) })

	eng := newStubFollowerEngine(addr)
	p := NewLeaderProxyWithEngine(eng)
	t.Cleanup(func() { _ = p.connCache.Close() })

	resp, err := p.Commit(context.Background(), []*pb.Request{{IsTxn: false}})
	require.Nil(t, resp)
	require.ErrorIs(t, err, ErrTransactionOutcomeUnknown)
	require.Equal(t, 1, svc.callCount())
}

func TestForwardDecisionEvidence(t *testing.T) {
	t.Parallel()

	rejected := ForwardWriteConflictStatus()
	require.True(t, isDefinitiveForwardConflict(rejected))
	require.True(t, isDefinitiveForwardConflict(errors.WithStack(ForwardWriteConflictStatus())))
	require.False(t, isDefinitiveForwardConflict(status.Error(codes.Aborted, store.ErrWriteConflict.Error())))
	require.False(t, isDefinitiveForwardNotLeader(rejected))
	require.False(t, isDefinitiveForwardConflict(status.Error(codes.Unknown, store.NewWriteConflictError([]byte("key")).Error())))

	beforeSubmit := ForwardNotLeaderStatus()
	require.True(t, isDefinitiveForwardNotLeader(beforeSubmit))
	require.False(t, isDefinitiveForwardNotLeader(status.Error(codes.Unknown, "not leader")))

	for _, err := range []error{
		status.Error(codes.Unknown, "raft engine: leadership lost"),
		status.Error(codes.Unavailable, "not leader"),
		status.Error(codes.DeadlineExceeded, "context deadline exceeded"),
	} {
		require.False(t, isDefinitiveForwardNotLeader(err))
		require.False(t, isDefinitiveForwardConflict(err))
	}
}

func TestLeaderProxy_MultiPhaseConflictHasUnknownOutcome(t *testing.T) {
	t.Parallel()

	svc := &fakeInternal{err: ForwardWriteConflictStatus()}
	addr := grpctest.Serve(t, func(srv *grpc.Server) { pb.RegisterInternalServer(srv, svc) })

	p := NewLeaderProxyWithEngine(newStubFollowerEngine(addr))
	t.Cleanup(func() { _ = p.connCache.Close() })
	_, err := p.Commit(context.Background(), []*pb.Request{{IsTxn: true, Phase: pb.Phase_COMMIT}})
	require.ErrorIs(t, err, ErrTransactionOutcomeUnknown)
	require.Equal(t, 1, svc.callCount())
}

func TestLeaderProxy_SinglePhaseConflictUsesStructuredStatus(t *testing.T) {
	t.Parallel()

	svc := &fakeInternal{err: ForwardWriteConflictStatus()}
	addr := grpctest.Serve(t, func(srv *grpc.Server) { pb.RegisterInternalServer(srv, svc) })

	p := NewLeaderProxyWithEngine(newStubFollowerEngine(addr))
	t.Cleanup(func() { _ = p.connCache.Close() })
	_, err := p.Commit(context.Background(), []*pb.Request{{IsTxn: false, Phase: pb.Phase_NONE}})
	require.ErrorIs(t, err, store.ErrWriteConflict)
	require.NotErrorIs(t, err, ErrTransactionOutcomeUnknown)
	require.Equal(t, 1, svc.callCount())
}

func TestLeaderProxy_SinglePhaseLockUsesStructuredStatus(t *testing.T) {
	t.Parallel()

	svc := &fakeInternal{err: ForwardTxnLockedStatus()}
	addr := grpctest.Serve(t, func(srv *grpc.Server) { pb.RegisterInternalServer(srv, svc) })

	p := NewLeaderProxyWithEngine(newStubFollowerEngine(addr))
	t.Cleanup(func() { _ = p.connCache.Close() })
	_, err := p.Commit(context.Background(), []*pb.Request{{IsTxn: false, Phase: pb.Phase_NONE}})
	require.ErrorIs(t, err, ErrTxnLocked)
	require.NotErrorIs(t, err, ErrTransactionOutcomeUnknown)
	require.True(t, IsForwardTxnLocked(err))
	require.Equal(t, 1, svc.callCount())
}

func TestClassifyForwardConflictByPhase(t *testing.T) {
	t.Parallel()
	proof := ForwardWriteConflictStatus()
	for _, phase := range []pb.Phase{pb.Phase_NONE, pb.Phase_PREPARE} {
		err := classifyForwardRPCError(proof, []*pb.Request{{IsTxn: true, Phase: phase}})
		require.ErrorIs(t, err, store.ErrWriteConflict)
		require.NotErrorIs(t, err, ErrTransactionOutcomeUnknown)
		wrapped := errors.WithStack(proof)
		err = classifyForwardRPCError(wrapped, []*pb.Request{{IsTxn: true, Phase: phase}})
		require.ErrorIs(t, err, store.ErrWriteConflict)
	}
	for _, phase := range []pb.Phase{pb.Phase_COMMIT, pb.Phase_ABORT} {
		err := classifyForwardRPCError(proof, []*pb.Request{{IsTxn: true, Phase: phase}})
		require.ErrorIs(t, err, ErrTransactionOutcomeUnknown)
	}
	legacy := status.Error(codes.Unknown, store.NewWriteConflictError([]byte("key")).Error())
	err := classifyForwardRPCError(legacy, []*pb.Request{{IsTxn: true, Phase: pb.Phase_NONE}})
	require.ErrorIs(t, err, ErrTransactionOutcomeUnknown)
}

func TestClassifyForwardLockAndRouteByPhase(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		proof  error
		target error
	}{
		{name: "lock", proof: ForwardTxnLockedStatus(), target: ErrTxnLocked},
		{name: "route", proof: ForwardComposed1Status(ErrComposed1Violation), target: ErrComposed1Violation},
		{name: "route gc", proof: ForwardComposed1Status(ErrComposed1VersionGCd), target: ErrComposed1VersionGCd},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, phase := range []pb.Phase{pb.Phase_NONE, pb.Phase_PREPARE} {
				err := classifyForwardRPCError(tc.proof, []*pb.Request{{IsTxn: true, Phase: phase}})
				require.ErrorIs(t, err, tc.target)
				require.NotErrorIs(t, err, ErrTransactionOutcomeUnknown)
				require.True(t, isTerminalForwardError(err))
			}
			for _, phase := range []pb.Phase{pb.Phase_COMMIT, pb.Phase_ABORT} {
				err := classifyForwardRPCError(tc.proof, []*pb.Request{{IsTxn: true, Phase: phase}})
				require.ErrorIs(t, err, ErrTransactionOutcomeUnknown)
			}
		})
	}
}
