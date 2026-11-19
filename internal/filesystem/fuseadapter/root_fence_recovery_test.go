package fuseadapter

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/filesystem"
	"github.com/bootjp/elastickv/internal/filesystem/fstest"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/raftengine/raftenginetest"
	"github.com/bootjp/elastickv/internal/testutil"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Capture the first transport so it can be reset after the leader receives
// ForwardLeaseRead. The listener keeps accepting replacement connections.
type firstFenceConnection struct {
	net.Listener
	once  sync.Once
	first chan net.Conn
}

func (l *firstFenceConnection) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		l.once.Do(func() { l.first <- c })
	}
	return c, err
}

type recoveryFenceLeader struct {
	pb.UnimplementedInternalServer
	listener *firstFenceConnection
	reset    bool
	code     codes.Code
	ts       uint64
	calls    atomic.Int32
}

func (s *recoveryFenceLeader) ForwardLeaseRead(context.Context, *pb.ForwardLeaseReadRequest) (*pb.ForwardLeaseReadResponse, error) {
	if s.calls.Add(1) == 1 && s.reset {
		c := <-s.listener.first
		tcp, ok := c.(*net.TCPConn)
		if !ok {
			return nil, status.Error(codes.Internal, "expected a TCP connection")
		}
		if err := tcp.SetLinger(0); err != nil {
			return nil, err
		}
		if err := tcp.Close(); err != nil {
			return nil, err
		}
		return nil, status.Error(codes.Unavailable, "connection closed during fence")
	}
	if s.code != codes.OK {
		return nil, status.Error(s.code, "fence rejected")
	}
	return &pb.ForwardLeaseReadResponse{AppliedIndex: 12, LastCommitTs: s.ts}, nil
}

func TestRootGetAttrAfterPartition(t *testing.T) {
	for _, tc := range []struct {
		name           string
		reset, expired bool
		code           codes.Code
		want           fuse.Status
		calls          int32
	}{
		{"healthy", false, false, codes.OK, fuse.OK, 1},
		{"expired HLC only", false, true, codes.OK, fuse.OK, 1},
		{"reset only", true, false, codes.OK, fuse.OK, 2},
		{"reset and expired HLC", true, true, codes.OK, fuse.OK, 2},
		{"persistent unavailability", false, false, codes.Unavailable, fuse.EIO, 2},
		{"access denied", false, false, codes.PermissionDenied, fuse.EIO, 1},
		{"reconnected but denied", true, true, codes.PermissionDenied, fuse.EIO, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			st := store.NewMVCCStore()
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			seed, err := filesystem.NewService(st, fstest.NewMemoryDispatcher(st))
			require.NoError(t, err)
			require.NoError(t, seed.InitializeRoot(ctx, 0o755, 0, 0))
			l := &firstFenceConnection{Listener: testutil.Listen(t), first: make(chan net.Conn, 1)}
			server := grpc.NewServer()
			leader := &recoveryFenceLeader{listener: l, reset: tc.reset, code: tc.code, ts: st.LastCommitTS()}
			pb.RegisterInternalServer(server, leader)
			go func() { _ = server.Serve(l) }()
			t.Cleanup(server.Stop)

			group := &kv.ShardGroup{Store: st, Engine: &raftenginetest.StubEngine{StaticLeaderView: raftenginetest.StaticLeaderView{
				StateValue: raftengine.StateFollower, LeaderValue: raftengine.LeaderInfo{Address: l.Addr().String()},
			}}}
			proxy := kv.NewLeaderProxyForShardGroup(group)
			t.Cleanup(func() { require.NoError(t, proxy.Close()) })
			group.Txn = proxy
			clock := kv.NewHLC()
			coord := kv.NewShardedCoordinator(distribution.NewEngine(), map[uint64]*kv.ShardGroup{1: group}, 1, clock, st)
			t.Cleanup(func() { require.NoError(t, coord.Close()) })
			if tc.expired {
				clock.SetPhysicalCeiling(1)
				_, err = clock.NextFenced()
				require.ErrorIs(t, err, kv.ErrCeilingExpired)
				require.ErrorContains(t, coord.RecoverHLCLease(ctx), "hlc lease recovery unavailable")
			}
			rejections := clock.NextFencedRejections()
			svc, err := filesystem.NewService(st, coord)
			require.NoError(t, err)
			raw, _ := newRawFileSystem(t, svc)
			var out fuse.AttrOut
			got := raw.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: filesystem.RootInode}}, &out)
			require.Equal(t, tc.want, got)
			require.Equal(t, tc.calls, leader.calls.Load())
			require.Equal(t, rejections, clock.NextFencedRejections())
			if got == fuse.OK {
				require.EqualValues(t, filesystem.RootInode, out.Ino)
			}
		})
	}
}
