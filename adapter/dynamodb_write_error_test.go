package adapter

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNormalizeDynamoWriteDispatchError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		err  error
		want error
	}{
		{"nil", nil, nil},
		{"local conflict", store.NewWriteConflictError([]byte("k")), store.ErrWriteConflict},
		{"unknown conflict", status.Error(codes.Unknown, "key: k: write conflict"), store.ErrWriteConflict},
		{"wrapped aborted conflict", errors.WithStack(status.Error(codes.Aborted, "key: k: write conflict")), store.ErrWriteConflict},
		{"local lock", kv.NewTxnLockedError([]byte("k")), kv.ErrTxnLocked},
		{"unknown lock", status.Error(codes.Unknown, "key: k: txn locked"), kv.ErrTxnLocked},
		{"wrapped aborted lock", errors.WithStack(status.Error(codes.Aborted, "key: k (pending): txn locked")), kv.ErrTxnLocked},
		{"plain text", errors.New("key: k: write conflict"), nil},
		{"missing key envelope", status.Error(codes.Unknown, "write conflict"), nil},
		{"wrong code", status.Error(codes.Internal, "key: k: write conflict"), nil},
		{"embedded sentinel", status.Error(codes.Unknown, "key: write conflict: invalid value"), nil},
		{"plain lock", status.Error(codes.Unknown, "txn locked"), nil},
		{"wrong lock code", status.Error(codes.Internal, "key: k: txn locked"), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			normalized := normalizeDynamoWriteDispatchError(tc.err)
			switch {
			case tc.want != nil:
				require.True(t, errors.Is(normalized, tc.want), "missing normalized sentinel %v: %v", tc.want, normalized)
				require.True(t, shouldPreserveTransactWriteAttempt(normalized))
			case tc.err != nil:
				require.False(t, isRetryableTransactWriteError(normalized))
				require.Same(t, tc.err, normalized)
			default:
				require.NoError(t, normalized)
			}
		})
	}
}

type forwardedDynamoLockCoordinator struct {
	kv.Coordinator
	rejected atomic.Bool
	failure  error
	seal     bool
}

func (c *forwardedDynamoLockCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	if req != nil && req.IsTxn && isDynamoSnapshotSealRequest(req) == c.seal && c.rejected.CompareAndSwap(false, true) {
		if c.failure != nil {
			return nil, c.failure
		}
		return nil, status.Error(codes.Unknown, "key: pending: txn locked")
	}
	return c.Coordinator.Dispatch(ctx, req)
}

func TestDynamoDBRetriesForwardedSnapshotSealConflict(t *testing.T) {
	t.Parallel()
	for _, sentinel := range []error{store.ErrWriteConflict, kv.ErrTxnLocked} {
		t.Run(sentinel.Error(), func(t *testing.T) {
			st := store.NewMVCCStore()
			defer st.Close()
			schema := dedupItemTable()
			seedDedupItem(t, st, schema, "1")
			coord := &forwardedDynamoLockCoordinator{
				Coordinator: newOCCAdapterCoordinator(st),
				seal:        true,
				failure:     status.Error(codes.Aborted, "key: !ddb|seal|pending: "+sentinel.Error()),
			}
			server := NewDynamoDBServer(nil, st, coord)
			_, err := server.updateItemWithRetry(context.Background(), appendListInput())
			require.NoError(t, err)
			require.True(t, coord.rejected.Load())
			require.Equal(t, []string{"1", "3"}, readListValues(t, server, schema))
		})
	}
}

func TestDynamoDBCreateTableRetriesForwardedConflict(t *testing.T) {
	t.Parallel()
	st := store.NewMVCCStore()
	defer st.Close()
	coord := &forwardedDynamoLockCoordinator{
		Coordinator: newOCCAdapterCoordinator(st),
		failure:     status.Error(codes.Unknown, "key: !ddb|meta|table|test: write conflict"),
	}
	server := NewDynamoDBServer(nil, st, coord)
	schema := dedupItemTable()
	require.NoError(t, server.createTableWithRetry(context.Background(), schema.TableName, schema))
	require.True(t, coord.rejected.Load())
	exists, err := server.tableExistsAt(context.Background(), schema.TableName, consistentReadLatestTS)
	require.NoError(t, err)
	require.True(t, exists)
}

func TestDynamoDBRetriesForwardedTxnLock(t *testing.T) {
	t.Parallel()
	for _, txn := range []bool{false, true} {
		t.Run(map[bool]string{false: "item", true: "transaction"}[txn], func(t *testing.T) {
			st := store.NewMVCCStore()
			defer st.Close()
			schema := dedupItemTable()
			seedDedupItem(t, st, schema, "1")
			coord := &forwardedDynamoLockCoordinator{Coordinator: newOCCAdapterCoordinator(st)}
			server := NewDynamoDBServer(nil, st, coord)
			if txn {
				require.NoError(t, server.transactWriteItemsWithRetry(context.Background(), transactWriteItemsInput{
					TransactItems: []transactWriteItem{sealRaftTxnUpdate(schema.TableName)},
				}))
			} else {
				_, err := server.updateItemWithRetry(context.Background(), appendListInput())
				require.NoError(t, err)
			}
			require.True(t, coord.rejected.Load())
			require.Equal(t, []string{"1", "3"}, readListValues(t, server, schema))
		})
	}
}
