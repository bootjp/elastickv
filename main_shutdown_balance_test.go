package main

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/testutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestShutdownJoinsBalancerBeforeHandoff(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	balanceCtx, stopBalance := context.WithCancel(ctx)
	defer stopBalance()
	stopping, balanceDone, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	go func() {
		<-balanceCtx.Done()
		close(stopping)
		<-release
		close(balanceDone)
	}()
	var transfers atomic.Int32
	engine := shutdownTestEngine(func(context.Context, []raftengine.TransferTarget, uint64) error {
		select {
		case <-balanceDone:
		default:
			t.Error("handoff started before balancing stopped")
		}
		require.NoError(t, ctx.Err(), "quiescing must keep Raft transport alive")
		transfers.Add(1)
		return nil
	})
	requests, finished := make(chan struct{}), make(chan struct{})
	close(requests)
	go func() {
		defer close(finished)
		shutdownAfterRequest(ctx, cancel, []*raftGroupRuntime{{engine: engine}}, requests, time.Second,
			func(ctx context.Context) error { return stopLeaderBalanceForShutdown(ctx, stopBalance, balanceDone) })
	}()
	testutil.Closed(t, stopping, shutdownTestTimeout, "balancing was not cancelled")
	require.NoError(t, ctx.Err())
	require.Zero(t, transfers.Load())
	unblock()
	testutil.Closed(t, finished, shutdownTestTimeout, "handoff did not finish after balancing joined")
	require.Equal(t, int32(1), transfers.Load())
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

func TestShutdownBalancerJoinSharesHandoffDeadline(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	balanceCtx, stopBalance := context.WithCancel(ctx)
	defer stopBalance()
	var transfers atomic.Int32
	engine := shutdownTestEngine(func(context.Context, []raftengine.TransferTarget, uint64) error {
		transfers.Add(1)
		return nil
	})
	requests, balanceDone := make(chan struct{}), make(chan struct{})
	close(requests)
	shutdownAfterRequest(ctx, cancel, []*raftGroupRuntime{{engine: engine}}, requests, 20*time.Millisecond,
		func(ctx context.Context) error { return stopLeaderBalanceForShutdown(ctx, stopBalance, balanceDone) })
	require.ErrorIs(t, balanceCtx.Err(), context.Canceled)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.Zero(t, transfers.Load(), "do not transfer concurrently with an unjoined balancing task")
}

func TestShutdownBeforeBalancerStartsCompletesJoin(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	done := make(chan struct{})
	eg := &errgroup.Group{}
	startLeaderBalanceScheduler(ctx, eg, nil, leaderBalanceConfig{enabled: true}, done)
	select {
	case <-done:
	default:
		t.Fatal("cancelled startup left balancing join pending")
	}
	require.NoError(t, eg.Wait())
	// A ready tick must not inspect engines or dispatch after cancellation.
	(&leaderBalanceLoop{}).tick(ctx, time.Now())
	require.ErrorIs(t, (&leaderBalanceLoop{}).executeMove(ctx, leaderBalanceMove{}), context.Canceled)
}
