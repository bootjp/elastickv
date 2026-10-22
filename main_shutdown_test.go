package main

import (
	"context"
	"errors"
	"math"
	"net"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"
)

type shutdownEngineStub struct {
	raftengine.Engine
	state    raftengine.State
	servers  []raftengine.Server
	transfer func(context.Context, []raftengine.TransferTarget, uint64) error
}

func (e *shutdownEngineStub) State() raftengine.State { return e.state }
func (e *shutdownEngineStub) Leader() raftengine.LeaderInfo {
	return raftengine.LeaderInfo{ID: "local"}
}
func (e *shutdownEngineStub) Configuration(context.Context) (raftengine.Configuration, error) {
	return raftengine.Configuration{Servers: e.servers}, nil
}
func (e *shutdownEngineStub) TransferLeadershipToServerIfEligible(ctx context.Context, candidates []raftengine.TransferTarget, maxLag uint64) error {
	return e.transfer(ctx, candidates, maxLag)
}

func shutdownTestEngine(transfer func(context.Context, []raftengine.TransferTarget, uint64) error) *shutdownEngineStub {
	return &shutdownEngineStub{
		state: raftengine.StateLeader,
		servers: []raftengine.Server{
			{ID: "local", Address: "local:1", Suffrage: "voter"},
			{ID: "learner", Address: "learner:1", Suffrage: "learner"},
			{ID: "voter", Address: "voter:1", Suffrage: "voter"},
		},
		transfer: transfer,
	}
}

func TestShutdownSignalTransfersAllLeadersBeforeCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls atomic.Int32
	transfer := func(transferCtx context.Context, candidates []raftengine.TransferTarget, maxLag uint64) error {
		require.NoError(t, ctx.Err(), "the Raft transport must stay alive during transfer")
		require.NoError(t, transferCtx.Err())
		require.Equal(t, []raftengine.TransferTarget{{ID: "voter", Address: "voter:1"}}, candidates)
		require.Equal(t, uint64(math.MaxUint64), maxLag)
		calls.Add(1)
		return nil
	}
	follower := shutdownTestEngine(func(context.Context, []raftengine.TransferTarget, uint64) error {
		t.Error("a follower must not initiate transfer")
		return nil
	})
	follower.state = raftengine.StateFollower
	runtimes := []*raftGroupRuntime{
		nil,
		{},
		{spec: groupSpec{id: 1}, engine: shutdownTestEngine(transfer)},
		{spec: groupSpec{id: 2}, engine: shutdownTestEngine(transfer)},
		{spec: groupSpec{id: 3}, engine: follower},
	}
	signals := make(chan os.Signal, 1)
	signals <- syscall.SIGTERM
	shutdownOnSignal(ctx, cancel, runtimes, signals, time.Second)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.Equal(t, int32(2), calls.Load())
}

func TestShutdownSignalSharesDeadlineAcrossGroups(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{}, 2)
	transfer := func(transferCtx context.Context, _ []raftengine.TransferTarget, _ uint64) error {
		started <- struct{}{}
		<-transferCtx.Done()
		return transferCtx.Err()
	}
	runtimes := []*raftGroupRuntime{
		{spec: groupSpec{id: 1}, engine: shutdownTestEngine(transfer)},
		{spec: groupSpec{id: 2}, engine: shutdownTestEngine(transfer)},
	}
	signals := make(chan os.Signal, 1)
	signals <- syscall.SIGTERM
	done := make(chan struct{})
	go func() {
		shutdownOnSignal(ctx, cancel, runtimes, signals, 200*time.Millisecond)
		close(done)
	}()
	for range runtimes {
		select {
		case <-started:
		case <-done:
			t.Fatal("all transfers must start before the shared deadline")
		}
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not finish after the transfer deadline")
	}
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

func TestShutdownTransferRetriesTransientErrors(t *testing.T) {
	t.Parallel()
	for _, transientErr := range []error{
		raftengine.ErrLeadershipTransferConfChangePending,
		raftengine.ErrLeadershipTransferInProgress,
		raftengine.ErrLeadershipTransferNoHealthyTarget,
	} {
		t.Run(transientErr.Error(), func(t *testing.T) {
			t.Parallel()
			calls := 0
			engine := shutdownTestEngine(func(context.Context, []raftengine.TransferTarget, uint64) error {
				calls++
				if calls == 1 {
					return transientErr
				}
				return nil
			})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			require.NoError(t, transferGroupLeadershipOnShutdown(ctx, engine))
			require.Equal(t, 2, calls)
		})
	}
}

func TestShutdownTransferWithoutAnotherVoter(t *testing.T) {
	t.Parallel()
	engine := shutdownTestEngine(func(context.Context, []raftengine.TransferTarget, uint64) error {
		t.Error("a single voter must not initiate transfer")
		return nil
	})
	engine.servers = engine.servers[:2] // local voter and a learner
	err := transferGroupLeadershipOnShutdown(t.Context(), engine)
	require.ErrorIs(t, err, raftengine.ErrLeadershipTransferNoHealthyTarget)
}

func TestShutdownTransferLosingLeadershipIsSuccess(t *testing.T) {
	t.Parallel()
	engine := shutdownTestEngine(func(context.Context, []raftengine.TransferTarget, uint64) error {
		return errors.Join(errors.New("transfer raced an election"), raftengine.ErrNotLeader)
	})
	require.NoError(t, transferGroupLeadershipOnShutdown(t.Context(), engine))
}

func TestShutdownAlreadyCanceledDoesNotTransfer(t *testing.T) {
	t.Parallel()
	engine := shutdownTestEngine(func(context.Context, []raftengine.TransferTarget, uint64) error {
		t.Error("an already stopped server must not initiate transfer")
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	shutdownOnSignal(ctx, cancel, []*raftGroupRuntime{{engine: engine}}, make(chan os.Signal), time.Second)
}

func TestShutdownGRPCServerClosesPersistentStreams(t *testing.T) {
	t.Parallel()
	const bufferSize = 1 << 20
	listener := bufconn.Listen(bufferSize)
	server := grpc.NewServer()
	healthpb.RegisterHealthServer(server, health.NewServer())
	t.Cleanup(server.Stop)
	go func() { _ = server.Serve(listener) }()
	conn, err := grpc.NewClient("passthrough:///shutdown-test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	stream, err := healthpb.NewHealthClient(conn).Watch(t.Context(), &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.NoError(t, err, "establish an active stream before stopping the server")
	done := make(chan struct{})
	go func() {
		stopGRPCServer(server, shutdownTransferRetryDelay)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("a persistent stream blocked gRPC shutdown")
	}
	_, err = stream.Recv()
	require.Error(t, err)
}
