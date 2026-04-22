package kv

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type fakeInternal struct {
	pb.UnimplementedInternalServer

	mu      sync.Mutex
	calls   int
	lastReq *pb.ForwardRequest
	resp    *pb.ForwardResponse
}

func (f *fakeInternal) Forward(_ context.Context, req *pb.ForwardRequest) (*pb.ForwardResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastReq = req
	if f.resp != nil {
		return f.resp, nil
	}
	return &pb.ForwardResponse{Success: true, CommitIndex: 0}, nil
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
	resp, err := p.Commit(reqs)
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
	require.Eventually(t, func() bool {
		c, err := net.DialTimeout("tcp", lis.Addr().String(), 100*time.Millisecond)
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

	resp, err := p.Commit(reqs)
	require.NoError(t, err)
	require.Equal(t, uint64(123), resp.CommitIndex)

	svc.mu.Lock()
	defer svc.mu.Unlock()
	require.Equal(t, 1, svc.calls)
	require.NotNil(t, svc.lastReq)
	require.Len(t, svc.lastReq.Requests, 1)
}
