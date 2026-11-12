package leaderbalance

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestShutdownBeforeBalancerStartsCompletesJoin(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	done := make(chan struct{})
	eg := &errgroup.Group{}
	start(ctx, eg, nil, leaderBalanceConfig{enabled: true}, nil, done)
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
