package kv

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/testutil"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
)

type crashBoundaryContext struct {
	context.Context
	waiting chan struct{}
	errRead chan error
	once    sync.Once
}

func (c *crashBoundaryContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func (c *crashBoundaryContext) Err() error {
	err := c.Context.Err()
	c.errRead <- err
	return err
}

func TestKillAfterPrimaryCommitAcceptedSignalCannotResume(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	observed := &crashBoundaryContext{Context: ctx, waiting: make(chan struct{}), errRead: make(chan error, 1)}
	primary := &stubTransactional{}
	type signalCall struct {
		pid    int
		signal syscall.Signal
	}
	signaled := make(chan signalCall, 1)
	marker := filepath.Join(t.TempDir(), "primary-committed")
	hook := killAfterPrimaryCommit{
		Transactional: primary,
		marker:        marker,
		kill: func(pid int, signal syscall.Signal) error {
			signaled <- signalCall{pid: pid, signal: signal}
			return nil // Accept the signal without terminating this test process.
		},
	}
	type commitResult struct {
		response *TransactionResponse
		err      error
	}
	resultCh := make(chan commitResult, 1)
	continued := make(chan struct{}, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		response, err := hook.Commit(observed, []*pb.Request{{Phase: pb.Phase_COMMIT}})
		if err == nil {
			continued <- struct{}{}
		}
		resultCh <- commitResult{response: response, err: err}
	}()
	t.Cleanup(func() {
		cancel()
		testutil.Closed(t, finished, time.Second, "crash hook did not join after cancellation")
	})
	call := testutil.Recv(t, signaled, time.Second, "crash hook did not request a signal")
	require.Equal(t, os.Getpid(), call.pid)
	require.Equal(t, syscall.SIGKILL, call.signal)
	require.Equal(t, 1, primary.commits)
	contents, err := os.ReadFile(marker)
	require.NoError(t, err)
	require.Equal(t, "primary committed", string(contents))
	testutil.Closed(t, observed.waiting, time.Second, "crash hook did not observe cancellation channel")
	require.NoError(t, ctx.Err(), "the hook must reach its wait before cancellation")
	cancel()
	result := testutil.Recv(t, resultCh, time.Second, "crash hook did not return after cancellation")
	require.Nil(t, result.response)
	require.ErrorIs(t, result.err, context.Canceled)
	testutil.Closed(t, finished, time.Second, "crash hook caller did not finish")
	require.Empty(t, continued, "a successful crash-hook return would resume secondary commit")
	// A premature context.Canceled return cannot pass just because waiting and
	// finished become ready together. Require the hook's actual post-wait Err read.
	require.Len(t, observed.errRead, 1, "the hook must obtain its returned error from the canceled context")
	require.ErrorIs(t, <-observed.errRead, context.Canceled)
}

func TestKillAfterPrimaryCommitSignalErrorCannotReturnSuccess(t *testing.T) {
	hook := killAfterPrimaryCommit{
		Transactional: &stubTransactional{},
		marker:        filepath.Join(t.TempDir(), "primary-committed"),
		kill: func(pid int, signal syscall.Signal) error {
			require.Equal(t, os.Getpid(), pid)
			require.Equal(t, syscall.SIGKILL, signal)
			return syscall.EPERM
		},
	}
	response, err := hook.Commit(t.Context(), []*pb.Request{{Phase: pb.Phase_COMMIT}})
	require.ErrorIs(t, err, syscall.EPERM)
	require.Nil(t, response)
}

func TestKillAfterPrimaryCommitOtherOutcomesPassThrough(t *testing.T) {
	for _, test := range []struct {
		name    string
		primary Transactional
		reqs    []*pb.Request
		wantErr error
	}{
		{name: "no-requests", primary: &stubTransactional{}},
		{name: "prepare", primary: &stubTransactional{}, reqs: []*pb.Request{{Phase: pb.Phase_PREPARE}}},
		{name: "multiple-requests", primary: &stubTransactional{}, reqs: []*pb.Request{{Phase: pb.Phase_COMMIT}, {Phase: pb.Phase_COMMIT}}},
		{name: "primary-error", primary: &recordingTransactional{errs: []error{syscall.EIO}}, reqs: []*pb.Request{{Phase: pb.Phase_COMMIT}}, wantErr: syscall.EIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			marker := filepath.Join(t.TempDir(), "primary-committed")
			killCalls := 0
			hook := killAfterPrimaryCommit{
				Transactional: test.primary,
				marker:        marker,
				kill: func(int, syscall.Signal) error {
					killCalls++
					return syscall.EPERM
				},
			}
			response, err := hook.Commit(t.Context(), test.reqs)
			if test.wantErr != nil {
				require.ErrorIs(t, err, test.wantErr)
				require.Nil(t, response)
			} else {
				require.NoError(t, err)
				require.NotNil(t, response)
			}
			require.Zero(t, killCalls)
			require.NoFileExists(t, marker)
		})
	}
}

func TestKillAfterPrimaryCommitMarkerErrorDoesNotSignal(t *testing.T) {
	killCalls := 0
	hook := killAfterPrimaryCommit{
		Transactional: &stubTransactional{},
		marker:        filepath.Join(t.TempDir(), "missing-directory", "primary-committed"),
		kill: func(int, syscall.Signal) error {
			killCalls++
			return syscall.EPERM
		},
	}
	response, err := hook.Commit(t.Context(), []*pb.Request{{Phase: pb.Phase_COMMIT}})
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Nil(t, response)
	require.Zero(t, killCalls)
}
