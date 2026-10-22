package adapter

import (
	"context"
	"testing"
	"time"

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
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stop did not join reserved cleanup")
	}
}
