package kv

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

func TestStartupOnlyControlRPCSelectsEveryCallAndNeverFallsBackOnError(t *testing.T) {
	t.Parallel()
	var ready atomic.Bool
	var calls int
	denied := errors.New("typed private transport denied")
	private := func(context.Context, uint64, string, string, any, any) error { calls++; return denied }
	rpc := StartupOnlyControlRPC(private, ready.Load)
	for _, state := range []bool{false, true, false} {
		ready.Store(state)
		err := invokeStartupControlRPC(t.Context(), rpc, nil, 17, "peer.test:1", pb.RawKV_RawLatestCommitTS_FullMethodName,
			&pb.RawLatestCommitTSRequest{GroupId: 17}, &pb.RawLatestCommitTSResponse{})
		if state {
			require.ErrorContains(t, err, "public connection cache is unavailable")
		} else {
			require.ErrorIs(t, err, denied, "private errors cannot select public transport")
		}
	}
	require.Equal(t, 2, calls)
	for _, invalid := range []StartupControlRPC{StartupOnlyControlRPC(nil, ready.Load), StartupOnlyControlRPC(private, nil)} {
		err := invokeStartupControlRPC(t.Context(), invalid, nil, 17, "peer.test:1", "method", nil, nil)
		require.ErrorContains(t, err, "requires private invoker and process readiness")
	}
	// Even an unhandled selector error cannot invoke the public cache.
	err := invokeStartupControlRPC(t.Context(), func(context.Context, uint64, string, string, any, any) (bool, error) {
		return false, denied
	}, nil, 17, "peer.test:1", "method", nil, nil)
	require.ErrorIs(t, err, denied)
}

type startupControlPublicTSO struct {
	pb.UnimplementedDistributionServer
	calls atomic.Int64
}

func (s *startupControlPublicTSO) GetTimestamp(ctx context.Context, request *pb.GetTimestampRequest) (*pb.GetTimestampResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if len(md.Get("x-elastickv-startup-token")) != 0 || len(md.Get("x-elastickv-peer-session")) != 0 || len(md.Get("x-elastickv-peer-proof")) != 0 {
		return nil, errors.New("startup credentials leaked into public timestamp RPC")
	}
	if got := md.Get("authorization"); len(got) != 1 || got[0] != "Bearer ordinary-auth" {
		return nil, errors.New("ordinary public authorization was lost")
	}
	s.calls.Add(1)
	return &pb.GetTimestampResponse{Timestamp: 100, Count: request.Count, CommittedByDedicatedTso: true}, nil
}

func startupControlPublicCache(t *testing.T, service pb.DistributionServer) *GRPCConnCache {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	pb.RegisterDistributionServer(server, service)
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	conn, err := grpc.NewClient("passthrough:///public.test:1", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			connection, err := listener.Dial()
			return connection, errors.WithStack(err)
		}))
	require.NoError(t, err)
	cache := &GRPCConnCache{}
	cache.storeConn("public.test:1", conn)
	t.Cleanup(func() {
		require.NoError(t, cache.Close())
		server.Stop()
		require.NoError(t, listener.Close())
		<-done
	})
	return cache
}

func startupControlBlockedTimestamp(entered chan<- struct{}, release <-chan struct{}) func(context.Context, uint64, string, string, any, any) error {
	return func(ctx context.Context, group uint64, address, method string, _, reply any) error {
		if group != 0 || address != "public.test:1" || method != pb.Distribution_GetTimestamp_FullMethodName {
			return errors.New("startup timestamp binding changed")
		}
		close(entered)
		select {
		case <-release:
			response, ok := reply.(*pb.GetTimestampResponse)
			if !ok {
				return errors.New("unexpected timestamp reply type")
			}
			*response = pb.GetTimestampResponse{Timestamp: 200, Count: 1, CommittedByDedicatedTso: true}
			return nil
		case <-ctx.Done():
			return errors.WithStack(ctx.Err())
		}
	}
}

func TestStartupControlTimestampReadyDoesNotWaitForPrivateSerialLane(t *testing.T) {
	t.Parallel()
	service := &startupControlPublicTSO{}
	cache := startupControlPublicCache(t, service)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer ordinary-auth")
	var ready atomic.Bool
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	rpc := StartupOnlyControlRPC(startupControlBlockedTimestamp(entered, release), ready.Load)
	allocator := &LeaderRoutedTSOAllocator{}
	WithTSOStartupControlRPC(rpc)(allocator)
	// Use the exact routed timestamp hook; its legacy cache is the existing one.
	allocator.connCache.conns = cache.conns
	privateDone := make(chan error, 1)
	go func() {
		_, err := allocator.requestRemoteBatch(ctx, "public.test:1", 1, 0, false, false)
		privateDone <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("private timestamp call did not enter")
	}
	ready.Store(true)
	publicDone := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := allocator.requestRemoteBatch(ctx, "public.test:1", 1, 0, false, false)
			publicDone <- err
		}()
	}
	for range 2 {
		select {
		case err := <-publicDone:
			require.NoError(t, err)
		case <-ctx.Done():
			t.Fatal("steady-state timestamp RPC waited for the private startup lane")
		}
	}
	require.EqualValues(t, 2, service.calls.Load())
	releaseOnce.Do(func() { close(release) })
	require.NoError(t, <-privateDone)
}

func TestStartupControlShardAndPrepareHooksKeepExactBinding(t *testing.T) {
	t.Parallel()
	var calls int
	rpc := StartupOnlyControlRPC(func(_ context.Context, group uint64, address, method string, request, _ any) error {
		calls++
		require.EqualValues(t, 17, group)
		require.Equal(t, "peer.test:1", address)
		switch value := request.(type) {
		case *pb.RawLatestCommitTSRequest:
			require.Equal(t, pb.RawKV_RawLatestCommitTS_FullMethodName, method)
			require.Equal(t, group, value.GroupId)
			require.Empty(t, value.Key)
		case *pb.PrepareEvidenceStateRequest:
			require.Equal(t, pb.Internal_GetPrepareEvidenceState_FullMethodName, method)
			require.True(t, value.CapabilityOnly)
		case *pb.ForwardAdminProposalRequest:
			require.Equal(t, pb.Internal_ForwardAdminProposal_FullMethodName, method)
			require.False(t, value.ForceEntry)
		default:
			return errors.New("unexpected startup control request")
		}
		return nil
	}, func() bool { return false })
	shards := &ShardStore{}
	shards.SetStartupControlRPC(rpc)
	_, err := shards.remoteStartupGroupFloor(t.Context(), 17, "peer.test:1")
	require.NoError(t, err)
	_, err = shards.remoteStartupPrepareState(t.Context(), 17, "peer.test:1", &pb.PrepareEvidenceStateRequest{CapabilityOnly: true})
	require.NoError(t, err)
	proposer := &leaderAdminProposer{}
	WithLeaderAdminStartupControlRPC(17, rpc)(proposer)
	_, err = proposer.remoteStartupAdminProposal(t.Context(), "peer.test:1", &pb.ForwardAdminProposalRequest{Payload: EncodePrepareEvidenceSeal()})
	require.NoError(t, err)
	require.Equal(t, 3, calls)
}
