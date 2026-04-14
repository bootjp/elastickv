package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

func TestRedisTxnValidateReadSetDetectsStaleListMeta(t *testing.T) {
	t.Parallel()

	server, st := newRedisStorageMigrationTestServer(t)
	key := []byte("list:stale")

	metaV1, err := store.MarshalListMeta(store.ListMeta{Len: 1})
	require.NoError(t, err)
	require.NoError(t, st.PutAt(context.Background(), store.ListMetaKey(key), metaV1, 10, 0))

	txn := &txnContext{
		server:     server,
		working:    map[string]*txnValue{},
		listStates: map[string]*listTxnState{},
		zsetStates: map[string]*zsetTxnState{},
		ttlStates:  map[string]*ttlTxnState{},
		readKeys:   map[string][]byte{},
		startTS:    10,
	}

	_, err = txn.loadListState(key)
	require.NoError(t, err)

	metaV2, err := store.MarshalListMeta(store.ListMeta{Len: 2})
	require.NoError(t, err)
	require.NoError(t, st.PutAt(context.Background(), store.ListMetaKey(key), metaV2, 11, 0))

	err = txn.validateReadSet(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, store.ErrWriteConflict)
}

// TestRedisTxnMULTIEXECRetriesOnCoordinatorConflict verifies that runTransaction
// retries the full transaction when the coordinator returns ErrWriteConflict,
// matching the retry behaviour of individual write commands.
func TestRedisTxnMULTIEXECRetriesOnCoordinatorConflict(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := newRetryOnceCoordinator(st)

	srv := &RedisServer{
		store:       st,
		coordinator: coord,
		scriptCache: map[string]string{},
	}

	// Simulate a queued MULTI/EXEC with a single SET command.
	queue := []redcon.Command{
		{Args: [][]byte{[]byte(cmdSet), []byte("txn:key"), []byte("v1")}},
	}

	results, err := srv.runTransaction(queue)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, 2, coord.dispatches) // first dispatch fails, second succeeds

	val, err := st.GetAt(ctx, redisStrKey([]byte("txn:key")), snapshotTS(coord.clock, st))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), val)
}
