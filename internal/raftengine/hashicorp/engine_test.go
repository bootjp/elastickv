package hashicorp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWaitForFutureHonorsContextCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	block := make(chan struct{})
	started := make(chan struct{})
	waitDone := make(chan struct{})

	errCh := make(chan error, 1)
	go func() {
		errCh <- waitForFuture(ctx, func() error {
			close(started)
			defer close(waitDone)
			<-block
			return nil
		})
	}()

	<-started
	cancel()

	err := <-errCh
	require.ErrorIs(t, err, context.Canceled)
	close(block)

	select {
	case <-waitDone:
	case <-time.After(time.Second):
		t.Fatal("wait goroutine did not exit")
	}
}

func TestWaitForFutureReturnsUnderlyingError(t *testing.T) {
	t.Parallel()

	expected := errors.New("boom")
	err := waitForFuture(context.Background(), func() error {
		return expected
	})
	require.ErrorIs(t, err, expected)
}
