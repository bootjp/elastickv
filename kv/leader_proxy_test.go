package kv

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
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

// stubFollowerEngine is a minimal raftengine.Engine stub that reports the
// local node as a follower and returns a configured leader address. It is
// used by TestLeaderProxy_ForwardsWhenFollower to exercise the forwarding
// code path without running a real two-node raft cluster.
type stubFollowerEngine struct {
	leaderAddr string
}

func (s *stubFollowerEngine) Propose(context.Context, []byte) (*raftengine.ProposalResult, error) {
	return nil, raftengine.ErrNotLeader
}
func (s *stubFollowerEngine) ProposeAdmin(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	return s.Propose(ctx, data)
}
func (s *stubFollowerEngine) State() raftengine.State { return raftengine.StateFollower }
func (s *stubFollowerEngine) Leader() raftengine.LeaderInfo {
	return raftengine.LeaderInfo{ID: "leader", Address: s.leaderAddr}
}
func (s *stubFollowerEngine) VerifyLeader(context.Context) error { return raftengine.ErrNotLeader }
func (s *stubFollowerEngine) LinearizableRead(context.Context) (uint64, error) {
	return 0, raftengine.ErrNotLeader
}
func (s *stubFollowerEngine) Status() raftengine.Status {
	return raftengine.Status{State: raftengine.StateFollower, Leader: s.Leader()}
}
func (s *stubFollowerEngine) Configuration(context.Context) (raftengine.Configuration, error) {
	return raftengine.Configuration{}, nil
}
func (s *stubFollowerEngine) Close() error { return nil }

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

	var lc net.ListenConfig
	lis, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	svc := &fakeInternal{resp: &pb.ForwardResponse{Success: true, CommitIndex: 123}}
	srv := grpc.NewServer()
	pb.RegisterInternalServer(srv, svc)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.Stop()
		_ = lis.Close()
	})

	// Wait briefly so the gRPC server is ready to serve.
	dialer := &net.Dialer{Timeout: 100 * time.Millisecond}
	require.Eventually(t, func() bool {
		dialCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		c, err := dialer.DialContext(dialCtx, "tcp", lis.Addr().String())
		if err != nil {
			return false
		}
		_ = c.Close()
		return true
	}, 2*time.Second, 10*time.Millisecond)

	follower := &stubFollowerEngine{leaderAddr: lis.Addr().String()}
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
func (e *togglingFollowerEngine) Close() error { return nil }

func TestLeaderProxy_ForwardsAfterLeaderPublishes(t *testing.T) {
	t.Parallel()

	// Bring up a real fake gRPC Forward server; LeaderProxy.forward()
	// dials it via the connCache. We leave the engine's leader address
	// empty initially so the first few forward() attempts fail with
	// ErrLeaderNotFound, then flip to the real address after a brief
	// delay. forwardWithRetry must absorb the empty-address window and
	// succeed once the engine publishes.
	var lc net.ListenConfig
	lis, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	svc := &fakeInternal{resp: &pb.ForwardResponse{Success: true, CommitIndex: 42}}
	srv := grpc.NewServer()
	pb.RegisterInternalServer(srv, svc)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.Stop()
		_ = lis.Close()
	})

	// Wait briefly so the gRPC server is ready.
	dialer := &net.Dialer{Timeout: 100 * time.Millisecond}
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		c, err := dialer.DialContext(ctx, "tcp", lis.Addr().String())
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
		eng.setLeader(lis.Addr().String())
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

	eng := &stubFollowerEngine{leaderAddr: startLeaderProxyBlackhole(t)}
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
	require.ErrorIs(t, err, context.Canceled)

	require.NoError(t, p.forwardBreaker.allow(identity, 3, time.Now()))
	require.True(t, p.forwardBreaker.owns(identity, 3))
}

func TestLeaderProxy_ProxyBudgetDeadlineCountsTowardBreaker(t *testing.T) {
	t.Parallel()

	eng := &stubFollowerEngine{leaderAddr: startLeaderProxyBlackhole(t)}
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

	eng := &stubFollowerEngine{leaderAddr: "127.0.0.1:1"}
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

func TestLeaderProxy_TransportFailureOwnerPerformsHalfOpenProbe(t *testing.T) {
	t.Parallel()

	var lc net.ListenConfig
	lis, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	svc := &fakeInternal{
		resp:    &pb.ForwardResponse{Success: true, CommitIndex: 77},
		err:     status.Error(codes.Unavailable, "startup gate closed"),
		failFor: leaderProxyBreakerFailureThreshold,
	}
	srv := grpc.NewServer()
	pb.RegisterInternalServer(srv, svc)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.Stop()
		_ = lis.Close()
	})

	eng := &stubFollowerEngine{leaderAddr: lis.Addr().String()}
	p := NewLeaderProxyWithEngine(eng)
	t.Cleanup(func() { _ = p.connCache.Close() })

	resp, err := p.Commit(context.Background(), []*pb.Request{{IsTxn: false}})
	require.NoError(t, err)
	require.Equal(t, uint64(77), resp.CommitIndex)
	require.Equal(t, leaderProxyBreakerFailureThreshold+1, svc.callCount())
}
