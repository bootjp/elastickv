package main

import (
	"context"
	"errors"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/testutil"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
)

type shutdownEngineStub struct {
	raftengine.Engine
	leader   atomic.Bool
	servers  []raftengine.Server
	transfer func(context.Context, []raftengine.TransferTarget, uint64) error
}

func (e *shutdownEngineStub) State() raftengine.State {
	if e.leader.Load() {
		return raftengine.StateLeader
	}
	return raftengine.StateFollower
}
func (e *shutdownEngineStub) Leader() raftengine.LeaderInfo {
	return raftengine.LeaderInfo{ID: "local"}
}
func (e *shutdownEngineStub) Configuration(context.Context) (raftengine.Configuration, error) {
	return raftengine.Configuration{Servers: e.servers}, nil
}
func (e *shutdownEngineStub) TransferLeadershipToServerIfEligible(ctx context.Context, candidates []raftengine.TransferTarget, maxLag uint64) error {
	err := e.transfer(ctx, candidates, maxLag)
	if err == nil || errors.Is(err, raftengine.ErrNotLeader) {
		e.leader.Store(false)
	}
	return err
}

func shutdownTestEngine(transfer func(context.Context, []raftengine.TransferTarget, uint64) error) *shutdownEngineStub {
	engine := &shutdownEngineStub{
		servers: []raftengine.Server{
			{ID: "local", Address: "local:1", Suffrage: "voter"},
			{ID: "learner", Address: "learner:1", Suffrage: "learner"},
			{ID: "voter", Address: "voter:1", Suffrage: "voter"},
		},
		transfer: transfer,
	}
	engine.leader.Store(true)
	return engine
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
	follower.leader.Store(false)
	runtimes := []*raftGroupRuntime{
		nil,
		{},
		{spec: groupSpec{id: 1}, engine: shutdownTestEngine(transfer)},
		{spec: groupSpec{id: 2}, engine: shutdownTestEngine(transfer)},
		{spec: groupSpec{id: 3}, engine: follower},
	}
	requests := make(chan struct{})
	close(requests)
	shutdownAfterRequest(ctx, cancel, runtimes, requests, time.Second, nil)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.Equal(t, int32(2), calls.Load())
}

func TestShutdownSignalTransfersLeadershipAcquiredDuringHandoff(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var followerTransfers atomic.Int32
	follower := shutdownTestEngine(func(context.Context, []raftengine.TransferTarget, uint64) error {
		require.NoError(t, ctx.Err(), "newly acquired leadership must transfer before server cancellation")
		followerTransfers.Add(1)
		return nil
	})
	follower.leader.Store(false)
	leader := shutdownTestEngine(func(context.Context, []raftengine.TransferTarget, uint64) error {
		// The first runtime was already observed as a follower when this
		// group's transfer causes it to acquire leadership.
		follower.leader.Store(true)
		return nil
	})
	requests := make(chan struct{})
	close(requests)
	shutdownAfterRequest(ctx, cancel, []*raftGroupRuntime{
		{spec: groupSpec{id: 1}, engine: follower},
		{spec: groupSpec{id: 2}, engine: leader},
	}, requests, time.Second, nil)
	require.Equal(t, int32(1), followerTransfers.Load())
	require.Equal(t, raftengine.StateFollower, follower.State())
	require.ErrorIs(t, ctx.Err(), context.Canceled)
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
	requests := make(chan struct{})
	close(requests)
	done := make(chan struct{})
	go func() {
		shutdownAfterRequest(ctx, cancel, runtimes, requests, 200*time.Millisecond, nil)
		close(done)
	}()
	for range runtimes {
		select {
		case <-started:
		case <-done:
			t.Fatal("all transfers must start before the shared deadline")
		}
	}
	testutil.Closed(t, done, 2*time.Second, "shutdown did not finish after the transfer deadline")
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
	shutdownAfterRequest(ctx, cancel, []*raftGroupRuntime{{engine: engine}}, make(chan struct{}), time.Second, nil)
}
