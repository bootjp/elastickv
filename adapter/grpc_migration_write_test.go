package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
)

type migrationWriteCoordinator struct {
	kv.Coordinator
	lastError error
}

func (c *migrationWriteCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	response, err := c.Coordinator.Dispatch(ctx, req)
	c.lastError = err
	return response, err
}

// Exercise public RawKV routing over real migration FSM/store controls. The
// fixture runs the runner directly, so this does not wait for wall-clock ticks.
func TestRawKVWriteAfterSplitMigration(t *testing.T) {
	t.Parallel()
	r := newResplitAcceptanceRuntime(t)
	snapshot, err := r.server.catalog.Snapshot(t.Context())
	require.NoError(t, err)
	require.NoError(t, r.server.engine.ApplySnapshot(snapshot))
	coord := kv.NewShardedCoordinator(r.server.engine, r.groups, 1, kv.NewHLC(), r.reader)
	captured := &migrationWriteCoordinator{Coordinator: coord}
	server := NewGRPCServer(r.reader, captured)
	before, err := server.RawPut(t.Context(), &pb.RawPutRequest{Key: []byte("b"), Value: []byte("before")})
	require.NoError(t, err)
	require.True(t, before.GetSuccess())
	split, err := r.client.SplitRange(t.Context(), &pb.SplitRangeRequest{
		ExpectedCatalogVersion: snapshot.Version, RouteId: 1, SplitKey: []byte("m"),
	})
	require.NoError(t, err)
	r.migrate(t, split.GetRight().GetRouteId(), 2, "t")
	for _, key := range []string{"b", "n", "y"} {
		t.Run(key, func(t *testing.T) {
			value := []byte("after-" + key)
			put, putErr := server.RawPut(t.Context(), &pb.RawPutRequest{Key: []byte(key), Value: value})
			require.NoError(t, putErr, "coordinator: %+v", captured.lastError)
			require.True(t, put.GetSuccess())
			got, getErr := server.RawGet(t.Context(), &pb.RawGetRequest{Key: []byte(key)})
			require.NoError(t, getErr)
			require.True(t, got.GetExists())
			require.Equal(t, value, got.GetValue())
			deleted, deleteErr := server.RawDelete(t.Context(), &pb.RawDeleteRequest{Key: []byte(key)})
			require.NoError(t, deleteErr)
			require.True(t, deleted.GetSuccess())
			got, getErr = server.RawGet(t.Context(), &pb.RawGetRequest{Key: []byte(key)})
			require.NoError(t, getErr)
			require.False(t, got.GetExists())
		})
	}
	require.Error(t, r.put(t, 1, "y", "stale-owner", r.groups[2].Store.LastCommitTS()+1))
}
