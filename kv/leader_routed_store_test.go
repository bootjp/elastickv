package kv

import (
	"context"
	"net"
	"sync"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type stubLeaderCoordinator struct {
	isLeader bool
	verify   error
	leader   raft.ServerAddress
	clock    *HLC
}

func (s *stubLeaderCoordinator) Dispatch(context.Context, *OperationGroup[OP]) (*CoordinateResponse, error) {
	return &CoordinateResponse{}, nil
}

func (s *stubLeaderCoordinator) IsLeader() bool {
	return s.isLeader
}

func (s *stubLeaderCoordinator) VerifyLeader() error {
	return s.verify
}

func (s *stubLeaderCoordinator) RaftLeader() raft.ServerAddress {
	return s.leader
}

func (s *stubLeaderCoordinator) IsLeaderForKey([]byte) bool {
	return s.isLeader
}

func (s *stubLeaderCoordinator) VerifyLeaderForKey([]byte) error {
	return s.verify
}

func (s *stubLeaderCoordinator) RaftLeaderForKey([]byte) raft.ServerAddress {
	return s.leader
}

func (s *stubLeaderCoordinator) Clock() *HLC {
	if s.clock == nil {
		s.clock = NewHLC()
	}
	return s.clock
}

type fakeRawKVServer struct {
	pb.UnimplementedRawKVServer

	mu sync.Mutex

	getCalls    int
	scanCalls   int
	latestCalls int

	getResp    *pb.RawGetResponse
	scanResp   *pb.RawScanAtResponse
	latestResp *pb.RawLatestCommitTSResponse
}

func (f *fakeRawKVServer) RawGet(context.Context, *pb.RawGetRequest) (*pb.RawGetResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getCalls++
	if f.getResp != nil {
		return f.getResp, nil
	}
	return &pb.RawGetResponse{}, nil
}

func (f *fakeRawKVServer) RawScanAt(context.Context, *pb.RawScanAtRequest) (*pb.RawScanAtResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scanCalls++
	if f.scanResp != nil {
		return f.scanResp, nil
	}
	return &pb.RawScanAtResponse{}, nil
}

func (f *fakeRawKVServer) RawLatestCommitTS(context.Context, *pb.RawLatestCommitTSRequest) (*pb.RawLatestCommitTSResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.latestCalls++
	if f.latestResp != nil {
		return f.latestResp, nil
	}
	return &pb.RawLatestCommitTSResponse{}, nil
}

func startRawKVServer(t *testing.T, svc pb.RawKVServer) (raft.ServerAddress, func()) {
	t.Helper()

	var lc net.ListenConfig
	lis, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	grpcServer := grpc.NewServer()
	pb.RegisterRawKVServer(grpcServer, svc)
	go func() {
		_ = grpcServer.Serve(lis)
	}()

	stop := func() {
		grpcServer.Stop()
		_ = lis.Close()
	}
	return raft.ServerAddress(lis.Addr().String()), stop
}

func TestLeaderRoutedStore_UsesLocalStoreWhenLeaderVerified(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	local := store.NewMVCCStore()
	require.NoError(t, local.PutAt(ctx, []byte("k"), []byte("v"), 10, 0))

	coord := &stubLeaderCoordinator{
		isLeader: true,
		clock:    NewHLC(),
	}
	s := NewLeaderRoutedStore(local, coord)
	t.Cleanup(func() { _ = s.Close() })

	val, err := s.GetAt(ctx, []byte("k"), 10)
	require.NoError(t, err)
	require.Equal(t, []byte("v"), val)

	kvs, err := s.ScanAt(ctx, []byte("k"), []byte("kz"), 10, 10)
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, []byte("k"), kvs[0].Key)

	ts, exists, err := s.LatestCommitTS(ctx, []byte("k"))
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, uint64(10), ts)
}

func TestLeaderRoutedStore_ProxiesReadsWhenFollower(t *testing.T) {
	t.Parallel()

	fake := &fakeRawKVServer{
		getResp: &pb.RawGetResponse{
			Exists: true,
			Value:  []byte("remote-v"),
		},
		scanResp: &pb.RawScanAtResponse{
			Kv: []*pb.RawKVPair{
				{Key: []byte("a"), Value: []byte("1")},
				{Key: []byte("b"), Value: []byte("2")},
			},
		},
		latestResp: &pb.RawLatestCommitTSResponse{
			Ts:     42,
			Exists: true,
		},
	}
	addr, stop := startRawKVServer(t, fake)
	t.Cleanup(stop)

	coord := &stubLeaderCoordinator{
		isLeader: false,
		leader:   addr,
		clock:    NewHLC(),
	}
	s := NewLeaderRoutedStore(store.NewMVCCStore(), coord)
	t.Cleanup(func() { _ = s.Close() })

	ctx := context.Background()

	val, err := s.GetAt(ctx, []byte("k"), 10)
	require.NoError(t, err)
	require.Equal(t, []byte("remote-v"), val)

	kvs, err := s.ScanAt(ctx, []byte("a"), []byte("z"), 10, 10)
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, []byte("a"), kvs[0].Key)
	require.Equal(t, []byte("1"), kvs[0].Value)
	require.Equal(t, []byte("b"), kvs[1].Key)
	require.Equal(t, []byte("2"), kvs[1].Value)

	ts, exists, err := s.LatestCommitTS(ctx, []byte("k"))
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, uint64(42), ts)

	fake.mu.Lock()
	defer fake.mu.Unlock()
	require.Equal(t, 1, fake.getCalls)
	require.Equal(t, 1, fake.scanCalls)
	require.Equal(t, 1, fake.latestCalls)
}

func TestLeaderRoutedStore_ReturnsLeaderNotFoundWhenNoLeaderAddr(t *testing.T) {
	t.Parallel()

	coord := &stubLeaderCoordinator{
		isLeader: false,
		leader:   "",
		clock:    NewHLC(),
	}
	s := NewLeaderRoutedStore(store.NewMVCCStore(), coord)
	t.Cleanup(func() { _ = s.Close() })

	ctx := context.Background()

	_, err := s.GetAt(ctx, []byte("k"), 1)
	require.ErrorIs(t, err, ErrLeaderNotFound)

	_, err = s.ScanAt(ctx, []byte("a"), []byte("z"), 10, 1)
	require.ErrorIs(t, err, ErrLeaderNotFound)

	_, _, err = s.LatestCommitTS(ctx, []byte("k"))
	require.ErrorIs(t, err, ErrLeaderNotFound)
}
