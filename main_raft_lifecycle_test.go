package main

import (
	"context"
	"errors"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

type lifecycleEngineStub struct {
	raftengine.Engine
	done chan struct{}
	err  error
}

func (e *lifecycleEngineStub) Done() <-chan struct{} { return e.done }
func (e *lifecycleEngineStub) Err() error            { return e.err }

func TestStartRaftEngineLifecycleWatchersReportsEngineFailure(t *testing.T) {
	t.Parallel()
	engineErr := errors.New("snapshot write failed")
	engine := &lifecycleEngineStub{done: make(chan struct{}), err: engineErr}
	runtimes := []*raftGroupRuntime{{spec: groupSpec{id: 7}, engine: engine}}

	eg, ctx := errgroup.WithContext(context.Background())
	startRaftEngineLifecycleWatchers(ctx, eg, runtimes)
	close(engine.done)

	err := eg.Wait()
	require.ErrorIs(t, err, engineErr)
	require.Contains(t, err.Error(), "raft group 7 engine stopped")
}

func TestStartRaftEngineLifecycleWatchersIgnoresContextCancellation(t *testing.T) {
	t.Parallel()
	engine := &lifecycleEngineStub{done: make(chan struct{})}
	runtimes := []*raftGroupRuntime{{spec: groupSpec{id: 8}, engine: engine}}
	ctx, cancel := context.WithCancel(context.Background())
	eg, runCtx := errgroup.WithContext(ctx)
	startRaftEngineLifecycleWatchers(runCtx, eg, runtimes)

	cancel()

	require.NoError(t, eg.Wait())
}
