package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestS3DeleteByPrefix_RetriesRouteFence(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := &s3CleanupRouteFenceCoordinator{
		localAdapterCoordinator: newLocalAdapterCoordinator(st),
		failuresRemaining:       1,
	}
	server := NewS3Server(nil, "", st, coord, nil)
	key := s3keys.BlobKey("bucket-a", 3, "object-a", "upload-a", 1, 0)
	require.NoError(t, st.PutAt(ctx, key, []byte("blob"), 1, 0))

	server.deleteByPrefix(ctx, s3keys.BlobPrefixForUpload("bucket-a", 3, "object-a", "upload-a"), "bucket-a", 3, "object-a", "upload-a")

	require.Equal(t, 2, coord.calls, "route-fenced cleanup batch should retry")
	_, err := st.GetAt(ctx, key, snapshotTS(coord.Clock(), st))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

type s3CleanupRouteFenceCoordinator struct {
	*localAdapterCoordinator
	failuresRemaining int
	calls             int
}

func (c *s3CleanupRouteFenceCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	if req != nil && operationGroupHasDel(req.Elems) {
		c.calls++
		if c.failuresRemaining > 0 {
			c.failuresRemaining--
			return nil, kv.ErrRouteWriteFenced
		}
	}
	return c.localAdapterCoordinator.Dispatch(ctx, req)
}

func operationGroupHasDel(elems []*kv.Elem[kv.OP]) bool {
	for _, elem := range elems {
		if elem != nil && elem.Op == kv.Del {
			return true
		}
	}
	return false
}
