package adapter

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestBackgroundReservationStartsLiveAfterShutdownBound(t *testing.T) {
	w := newBackgroundWorkers()
	r, ok := w.reserve()
	require.True(t, ok)
	done := make(chan struct{})
	go func() { w.stop("test", time.Millisecond, time.Second); close(done) }()
	<-w.ctx.Done()
	ran := make(chan error, 1)
	r.start(time.Second, func(ctx context.Context) { ran <- ctx.Err() })
	require.NoError(t, <-ran)
	testutil.Closed(t, done, time.Second, "stop did not join reserved cleanup")
}

func TestBackgroundReservationCannotStartAfterStopReturns(t *testing.T) {
	w := newBackgroundWorkers()
	r, ok := w.reserve()
	require.True(t, ok)
	w.stop("test", time.Millisecond, time.Millisecond)
	ran := make(chan struct{}, 1)
	r.start(time.Second, func(context.Context) { ran <- struct{}{} })
	r.release()
	require.Zero(t, w.gate.remaining())
	testutil.NoRecv(t, ran, time.Millisecond, "cleanup started after shutdown returned")
}
