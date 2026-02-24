package adapter

import (
	"context"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestDispatchTransactWriteItemsWithRetry_RetryableError(t *testing.T) {
	t.Parallel()

	coord := &retryCoordinator{
		failures: 3,
		err:      errors.Wrapf(kv.ErrTxnLocked, "key: k"),
	}
	server := &DynamoDBServer{coordinator: coord}

	resp, err := server.dispatchTransactWriteItemsWithRetry(context.Background(), &kv.OperationGroup[kv.OP]{IsTxn: true})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, uint64(4), coord.DispatchCalls())
}

func TestDispatchTransactWriteItemsWithRetry_NonRetryableError(t *testing.T) {
	t.Parallel()

	coord := &retryCoordinator{
		failures: 1,
		err:      errors.New("boom"),
	}
	server := &DynamoDBServer{coordinator: coord}

	resp, err := server.dispatchTransactWriteItemsWithRetry(context.Background(), &kv.OperationGroup[kv.OP]{IsTxn: true})
	require.Error(t, err)
	require.Nil(t, resp)
	require.Equal(t, uint64(1), coord.DispatchCalls())
}

func TestDispatchTransactWriteItemsWithRetry_ContextCanceledIncludesLastError(t *testing.T) {
	t.Parallel()

	coord := &retryCoordinator{
		failures: 1_000,
		err:      errors.Wrapf(kv.ErrTxnLocked, "key: k"),
	}
	server := &DynamoDBServer{coordinator: coord}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	resp, err := server.dispatchTransactWriteItemsWithRetry(ctx, &kv.OperationGroup[kv.OP]{IsTxn: true})
	require.Error(t, err)
	require.Nil(t, resp)
	require.True(t, errors.Is(err, context.Canceled), "expected context cancellation in error chain")
	require.True(t, errors.Is(err, kv.ErrTxnLocked), "expected last dispatch error in error chain")
}

type retryCoordinator struct {
	mu       sync.Mutex
	failures uint64
	calls    uint64
	err      error
}

func (c *retryCoordinator) Dispatch(context.Context, *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.calls <= c.failures {
		return nil, c.err
	}
	return &kv.CoordinateResponse{CommitIndex: c.calls}, nil
}

func (c *retryCoordinator) DispatchCalls() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func (c *retryCoordinator) IsLeader() bool { return true }

func (c *retryCoordinator) VerifyLeader() error { return nil }

func (c *retryCoordinator) RaftLeader() raft.ServerAddress { return "" }

func (c *retryCoordinator) IsLeaderForKey([]byte) bool { return true }

func (c *retryCoordinator) VerifyLeaderForKey([]byte) error { return nil }

func (c *retryCoordinator) RaftLeaderForKey([]byte) raft.ServerAddress { return "" }

func (c *retryCoordinator) Clock() *kv.HLC { return kv.NewHLC() }
