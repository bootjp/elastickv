package adapter

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

type execPrepareCoordinator struct {
	*verifyHookCoordinator
	allocErr    error
	allocations int
	dispatches  int
}

func (c *execPrepareCoordinator) Next(ctx context.Context) (uint64, error) {
	return c.NextAfter(ctx, 0)
}

func (c *execPrepareCoordinator) NextAfter(ctx context.Context, floor uint64) (uint64, error) {
	c.allocations++
	if c.allocErr != nil {
		return 0, c.allocErr
	}
	return floor + 1, ctx.Err()
}

func (c *execPrepareCoordinator) Dispatch(ctx context.Context, group *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	c.dispatches++
	return c.localAdapterCoordinator.Dispatch(ctx, group)
}

type execPrepareValidationStore struct {
	store.MVCCStore
	onValidate func()
}

func (s *execPrepareValidationStore) LatestCommitTS(context.Context, []byte) (uint64, bool, error) {
	// The store is empty. Let validation finish with that answer even when
	// the client cancels, so preparation must observe cancellation itself.
	s.onValidate()
	return 0, false, nil
}

func TestRedisExecPrepareCommitTimestamp(t *testing.T) {
	t.Parallel()
	for _, mode := range []struct {
		name  string
		dedup bool
	}{{"ordinary", false}, {"dedup", true}} {
		t.Run(mode.name, func(t *testing.T) {
			ctx := t.Context()
			st := store.NewMVCCStore()
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			allocErr := errors.New("commit timestamp unavailable")
			coord := &execPrepareCoordinator{verifyHookCoordinator: newVerifyHookCoordinator(st), allocErr: allocErr}
			server := &RedisServer{store: st, coordinator: coord, onePhaseTxnDedup: mode.dedup}

			results, err := server.runTransaction(nil, ctx)
			require.NoError(t, err)
			require.Empty(t, results)
			require.Zero(t, coord.allocations)
			require.Zero(t, coord.dispatches)

			left, right := []byte("left"), []byte("right")
			coord.groupForKey = func(key []byte) uint64 {
				if bytes.HasSuffix(key, right) {
					return 2
				}
				return 1
			}
			require.NoError(t, st.PutAt(ctx, redisStrKey(left), encodeRedisStr([]byte("value"), nil), redisTxnTestStartTS, 0))
			results, err = server.runTransaction([]redcon.Command{
				{Args: [][]byte{[]byte(cmdGet), left}},
				{Args: [][]byte{[]byte(cmdGet), right}},
			}, ctx)
			require.NoError(t, err)
			require.Equal(t, []redisResult{{typ: resultBulk, bulk: []byte("value")}, {typ: resultNil}}, results)
			require.GreaterOrEqual(t, countReadKey(coord.leaseKeys, redisStrKey(left)), 2)
			require.GreaterOrEqual(t, countReadKey(coord.leaseKeys, redisStrKey(right)), 2)
			require.Zero(t, coord.allocations)
			require.Zero(t, coord.dispatches)

			// SET NX only needs a timestamp if the condition actually stages a write.
			results, err = server.runTransaction([]redcon.Command{
				{Args: [][]byte{[]byte(cmdSet), left, []byte("new"), []byte("NX")}},
			}, ctx)
			require.NoError(t, err)
			require.Equal(t, []redisResult{{typ: resultNil}}, results)
			require.Zero(t, coord.allocations)
			require.Zero(t, coord.dispatches)

			_, err = server.runTransaction([]redcon.Command{
				{Args: [][]byte{[]byte(cmdSet), right, []byte("new"), []byte("NX")}},
			}, ctx)
			require.ErrorIs(t, err, allocErr)
			require.Equal(t, 1, coord.allocations)
			require.Zero(t, coord.dispatches)

			// RPUSH has no string/key mutations. Its delta key must embed the
			// same timestamp under which the coordinator persists the batch.
			coord.allocErr = nil
			results, err = server.runTransaction([]redcon.Command{
				{Args: [][]byte{[]byte(cmdRPush), right, []byte("item")}},
			}, ctx)
			require.NoError(t, err)
			require.Equal(t, []redisResult{{typ: resultInt, integer: 1}}, results)
			require.Equal(t, 2, coord.allocations)
			require.Equal(t, 1, coord.dispatches)
			commitTS := uint64(redisTxnTestStartTS + 1)
			require.Equal(t, commitTS, st.LastCommitTS())
			delta, err := st.GetAt(ctx, store.ListMetaDeltaKey(right, commitTS, 0), commitTS)
			require.NoError(t, err)
			require.Equal(t, store.MarshalListMetaDelta(store.ListMetaDelta{LenDelta: 1}), delta)
		})
	}
}

func TestRedisReadOnlyPrepareCanceledAfterValidation(t *testing.T) {
	t.Parallel()
	for _, dedup := range []bool{false, true} {
		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)
		tracker := kv.NewActiveTimestampTracker()
		t.Cleanup(tracker.Close)
		st := &execPrepareValidationStore{MVCCStore: store.NewMVCCStore(), onValidate: func() {
			require.NotZero(t, tracker.Oldest())
			cancel()
		}}
		t.Cleanup(func() { require.NoError(t, st.Close()) })
		coord := &execPrepareCoordinator{verifyHookCoordinator: newVerifyHookCoordinator(st)}
		server := &RedisServer{store: st, coordinator: coord, onePhaseTxnDedup: dedup, readTracker: tracker}
		results, err := server.runTransaction([]redcon.Command{{Args: [][]byte{[]byte(cmdGet), []byte("missing")}}}, ctx)
		require.ErrorIs(t, ctx.Err(), context.Canceled)
		// The Redis retry layer normalizes client cancellation to this error.
		require.ErrorIs(t, err, kv.ErrTransactionOutcomeUnknown)
		require.Nil(t, results)
		require.Zero(t, tracker.Oldest())
		require.Zero(t, coord.allocations)
		require.Zero(t, coord.dispatches)
	}
}
