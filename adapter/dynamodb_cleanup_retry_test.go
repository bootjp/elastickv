package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestDynamoDBCleanupDeletedTableGeneration_RetriesRouteFence(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	coord := &cleanupRouteFenceCoordinator{
		localAdapterCoordinator: newLocalAdapterCoordinator(st),
		failuresRemaining:       1,
	}
	server := NewDynamoDBServer(nil, st, coord)

	err := server.cleanupDeletedTableGeneration(context.Background(), "table-a", 7)
	require.NoError(t, err)
	require.Equal(t, 3, coord.calls, "first prefix is retried once, second prefix succeeds once")
}

type cleanupRouteFenceCoordinator struct {
	*localAdapterCoordinator
	failuresRemaining int
	calls             int
}

func (c *cleanupRouteFenceCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	if req != nil && operationGroupHasDelPrefix(req.Elems) {
		c.calls++
		if c.failuresRemaining > 0 {
			c.failuresRemaining--
			return nil, kv.ErrRouteWriteFenced
		}
	}
	return c.localAdapterCoordinator.Dispatch(ctx, req)
}
