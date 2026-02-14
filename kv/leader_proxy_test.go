package kv

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/hashicorp/raft"
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

func TestLeaderProxy_CommitLocalWhenLeader(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	r, stop := newSingleRaft(t, "lp-local", NewKvFSM(st))
	defer stop()

	p := NewLeaderProxy(r)

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

	leaderAddr, leaderTrans := raft.NewInmemTransport(raft.ServerAddress(lis.Addr().String()))
	followerAddr, followerTrans := raft.NewInmemTransport("follower")
	leaderTrans.Connect(followerAddr, followerTrans)
	followerTrans.Connect(leaderAddr, leaderTrans)

	raftCfg := raft.Configuration{
		Servers: []raft.Server{
			{Suffrage: raft.Voter, ID: "leader", Address: leaderAddr},
			{Suffrage: raft.Voter, ID: "follower", Address: followerAddr},
		},
	}

	leader := func() *raft.Raft {
		c := raft.DefaultConfig()
		c.LocalID = "leader"
		c.HeartbeatTimeout = 50 * time.Millisecond
		c.ElectionTimeout = 100 * time.Millisecond
		c.LeaderLeaseTimeout = 50 * time.Millisecond

		ldb := raft.NewInmemStore()
		sdb := raft.NewInmemStore()
		fss := raft.NewInmemSnapshotStore()
		r, err := raft.NewRaft(c, NewKvFSM(store.NewMVCCStore()), ldb, sdb, fss, leaderTrans)
		require.NoError(t, err)
		require.NoError(t, r.BootstrapCluster(raftCfg).Error())
		t.Cleanup(func() { _ = r.Shutdown().Error() })
		return r
	}()

	follower := func() *raft.Raft {
		c := raft.DefaultConfig()
		c.LocalID = "follower"
		c.HeartbeatTimeout = 250 * time.Millisecond
		c.ElectionTimeout = 500 * time.Millisecond
		c.LeaderLeaseTimeout = 250 * time.Millisecond

		ldb := raft.NewInmemStore()
		sdb := raft.NewInmemStore()
		fss := raft.NewInmemSnapshotStore()
		r, err := raft.NewRaft(c, NewKvFSM(store.NewMVCCStore()), ldb, sdb, fss, followerTrans)
		require.NoError(t, err)
		require.NoError(t, r.BootstrapCluster(raftCfg).Error())
		t.Cleanup(func() { _ = r.Shutdown().Error() })
		return r
	}()

	require.Eventually(t, func() bool { return leader.State() == raft.Leader }, 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool { return follower.State() == raft.Follower }, 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		addr, _ := follower.LeaderWithID()
		return addr == leaderAddr
	}, 5*time.Second, 10*time.Millisecond)

	p := NewLeaderProxy(follower)
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
