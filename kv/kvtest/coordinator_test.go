package kvtest_test

import (
	"context"
	"encoding/binary"
	"math"
	"reflect"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/kv/kvtest"
	"github.com/bootjp/elastickv/store"
	"github.com/bootjp/elastickv/store/storetest"
	"github.com/stretchr/testify/require"
)

func TestBaseCoordinatorsHaveNoOptionalCapabilities(t *testing.T) {
	for _, value := range []kv.Coordinator{&kvtest.NopCoordinator{}, kvtest.NewLocalCoordinator(store.NewMVCCStore(), kvtest.LocalOptions{})} {
		require.Equal(t, reflect.TypeFor[kv.Coordinator]().NumMethod(), reflect.TypeOf(value).NumMethod())
		_, lease := value.(kv.LeaseReadableCoordinator)
		_, group := value.(kv.GroupRoutableCoordinator)
		_, all := value.(kv.AllGroupsLeaseReadableCoordinator)
		_, timestamp := value.(kv.AllGroupsLeaseTimestampCoordinator)
		require.False(t, lease)
		require.False(t, group)
		require.False(t, all)
		require.False(t, timestamp)
	}
}

func TestNopCoordinator(t *testing.T) {
	c := &kvtest.NopCoordinator{}
	ctx := context.Background()
	require.True(t, c.IsLeader())
	require.True(t, c.IsLeaderForKey(nil))
	require.NoError(t, c.VerifyLeader(ctx))
	require.NoError(t, c.VerifyLeaderForKey(ctx, nil))
	require.Equal(t, c.RaftLeader(), c.RaftLeaderForKey(nil))
	result, err := c.Dispatch(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, &kv.CoordinateResponse{}, result)
	clock := c.Clock()
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			if c.Clock() != clock {
				t.Error("clock changed")
			}
		})
	}
	wg.Wait()
}

func put(key string) *kv.Elem[kv.OP] {
	return &kv.Elem[kv.OP]{Op: kv.Put, Key: []byte(key), Value: []byte("value")}
}

func TestLocalCoordinatorTimestampPolicies(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts kvtest.LocalOptions
		req  kv.OperationGroup[kv.OP]
		want uint64
		err  error
	}{
		{"store", kvtest.LocalOptions{CommitTimestamp: kvtest.TimestampStoreIncrement}, kv.OperationGroup[kv.OP]{}, 11, nil},
		{"pinned", kvtest.LocalOptions{}, kv.OperationGroup[kv.OP]{CommitTS: 20}, 20, nil},
		{"missing request timestamp", kvtest.LocalOptions{CommitTimestamp: kvtest.TimestampRequest}, kv.OperationGroup[kv.OP]{}, 0, kv.ErrTxnCommitTSRequired},
		{"invalid transaction timestamp", kvtest.LocalOptions{}, kv.OperationGroup[kv.OP]{IsTxn: true, StartTS: 20, CommitTS: 20}, 0, kv.ErrTxnCommitTSRequired},
		{"filesystem clamp", kvtest.LocalOptions{CommitTimestamp: kvtest.TimestampStoreIncrement, AdvanceCommitTS: true}, kv.OperationGroup[kv.OP]{IsTxn: true, StartTS: 20, CommitTS: 9}, 21, nil},
		{"overflow", kvtest.LocalOptions{AdvanceCommitTS: true}, kv.OperationGroup[kv.OP]{IsTxn: true, StartTS: math.MaxUint64, CommitTS: 9}, 0, kv.ErrTxnCommitTSRequired},
		{"hlc", kvtest.LocalOptions{}, kv.OperationGroup[kv.OP]{IsTxn: true, StartTS: 20}, 0, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := store.NewMVCCStore()
			require.NoError(t, st.PutAt(context.Background(), []byte("seed"), nil, 10, 0))
			c := kvtest.NewLocalCoordinator(st, tc.opts)
			tc.req.Elems = []*kv.Elem[kv.OP]{put("key")}
			result, err := c.Dispatch(context.Background(), &tc.req)
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			if tc.want != 0 {
				require.Equal(t, tc.want, result.CommitTS)
			} else {
				require.Greater(t, result.CommitTS, tc.req.StartTS)
			}
			ts, err := c.LinearizableRead(context.Background())
			require.NoError(t, err)
			require.Equal(t, result.CommitTS, ts)
		})
	}
}

func TestLocalCoordinatorOCCPolicies(t *testing.T) {
	for _, tc := range []struct {
		name     string
		mode     kvtest.OCCMode
		reads    bool
		txn      bool
		conflict bool
	}{
		{"disabled", kvtest.OCCDisabled, true, true, false},
		{"write only ignores read set", kvtest.OCCTransactions, false, true, false},
		{"transaction read conflict", kvtest.OCCTransactions, true, true, true},
		{"non txn bypass", kvtest.OCCTransactions, true, false, false},
		{"all group conflict", kvtest.OCCAllGroups, true, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := store.NewMVCCStore()
			require.NoError(t, st.PutAt(context.Background(), []byte("read"), nil, 5, 0))
			c := kvtest.NewLocalCoordinator(st, kvtest.LocalOptions{OCC: tc.mode, ValidateReadKeys: tc.reads})
			_, err := c.Dispatch(context.Background(), &kv.OperationGroup[kv.OP]{IsTxn: tc.txn, StartTS: 3, CommitTS: 6, ReadKeys: [][]byte{[]byte("read")}, Elems: []*kv.Elem[kv.OP]{put("key")}})
			if tc.conflict {
				require.ErrorIs(t, err, store.ErrWriteConflict)
				_, err = st.GetAt(context.Background(), []byte("key"), 6)
				require.ErrorIs(t, err, store.ErrKeyNotFound)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestLocalCoordinatorPrefixPolicies(t *testing.T) {
	for _, mode := range []kvtest.PrefixMode{kvtest.PrefixReject, kvtest.PrefixIgnore, kvtest.PrefixApply} {
		st := store.NewMVCCStore()
		require.NoError(t, st.PutAt(context.Background(), []byte("prefix:key"), []byte("value"), 1, 0))
		c := kvtest.NewLocalCoordinator(st, kvtest.LocalOptions{DelPrefix: mode})
		req := &kv.OperationGroup[kv.OP]{CommitTS: 2, Elems: []*kv.Elem[kv.OP]{{Op: kv.DelPrefix, Key: []byte("prefix:")}}}
		_, err := c.Dispatch(context.Background(), req)
		if mode == kvtest.PrefixReject {
			require.ErrorIs(t, err, store.ErrUnknownOp)
		} else {
			require.NoError(t, err)
		}
		_, err = st.GetAt(context.Background(), []byte("prefix:key"), 2)
		if mode == kvtest.PrefixApply {
			require.ErrorIs(t, err, store.ErrKeyNotFound)
		} else {
			require.NoError(t, err)
		}
	}
	c := kvtest.NewLocalCoordinator(store.NewMVCCStore(), kvtest.LocalOptions{DelPrefix: kvtest.PrefixApply})
	for _, req := range []*kv.OperationGroup[kv.OP]{
		{CommitTS: 2, Elems: []*kv.Elem[kv.OP]{put("key"), {Op: kv.DelPrefix}}},
		{IsTxn: true, CommitTS: 2, Elems: []*kv.Elem[kv.OP]{{Op: kv.DelPrefix}}},
	} {
		_, err := c.Dispatch(context.Background(), req)
		require.ErrorIs(t, err, kv.ErrInvalidRequest)
	}
}

func TestLocalCoordinatorPatchAndRejectBeforeWriting(t *testing.T) {
	for _, patch := range []bool{false, true} {
		st := storetest.NewPebble(t)
		c := kvtest.NewLocalCoordinator(st, kvtest.LocalOptions{PatchCommitTS: patch})
		value := make([]byte, 9)
		_, err := c.Dispatch(context.Background(), &kv.OperationGroup[kv.OP]{CommitTS: 7, Elems: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: []byte("key"), Value: value, CommitTSValueOffset: 1}}})
		require.NoError(t, err)
		got, err := st.GetAt(context.Background(), []byte("key"), 7)
		require.NoError(t, err)
		if patch {
			require.Equal(t, uint64(7), binary.BigEndian.Uint64(got[1:]))
		} else {
			require.Equal(t, value, got)
		}
		require.Equal(t, make([]byte, 9), value)
		if patch {
			_, err = c.Dispatch(context.Background(), &kv.OperationGroup[kv.OP]{CommitTS: 8, Elems: []*kv.Elem[kv.OP]{put("untouched"), {Op: kv.Put, Value: []byte("short"), CommitTSValueOffset: math.MaxUint64}}})
			require.ErrorIs(t, err, kv.ErrInvalidRequest)
			_, err = st.GetAt(context.Background(), []byte("untouched"), 8)
			require.ErrorIs(t, err, store.ErrKeyNotFound)
		}
	}
}

func TestLocalCoordinatorConcurrentDispatch(t *testing.T) {
	c := kvtest.NewLocalCoordinator(store.NewMVCCStore(), kvtest.LocalOptions{CommitTimestamp: kvtest.TimestampStoreIncrement})
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			if _, err := c.Dispatch(context.Background(), &kv.OperationGroup[kv.OP]{Elems: []*kv.Elem[kv.OP]{put("key")}}); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	ts, err := c.LinearizableRead(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(20), ts)
}

func TestLocalCoordinatorRejectsInvalidGroupsBeforeWriting(t *testing.T) {
	st := store.NewMVCCStore()
	c := kvtest.NewLocalCoordinator(st, kvtest.LocalOptions{PatchCommitTS: true})
	result, err := c.Dispatch(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, &kv.CoordinateResponse{}, result)
	for _, elems := range [][]*kv.Elem[kv.OP]{
		{put("untouched"), {Op: kv.OP(99)}},
		{put("untouched"), {Op: kv.Del, Value: make([]byte, 9), CommitTSValueOffset: 1}},
	} {
		_, err = c.Dispatch(context.Background(), &kv.OperationGroup[kv.OP]{CommitTS: 2, Elems: elems})
		require.Error(t, err)
		require.Zero(t, st.LastCommitTS())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = c.Dispatch(ctx, &kv.OperationGroup[kv.OP]{Elems: []*kv.Elem[kv.OP]{put("untouched")}})
	require.ErrorIs(t, err, context.Canceled)
	_, err = c.LinearizableRead(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, st.LastCommitTS())
	for _, opts := range []kvtest.LocalOptions{
		{DelPrefix: kvtest.PrefixMode(99)}, {CommitTimestamp: kvtest.TimestampSource(99)}, {OCC: kvtest.OCCMode(99)},
	} {
		_, err = kvtest.NewLocalCoordinator(st, opts).Dispatch(context.Background(), &kv.OperationGroup[kv.OP]{Elems: []*kv.Elem[kv.OP]{put("untouched")}})
		require.ErrorIs(t, err, kv.ErrInvalidRequest)
	}
}

func TestLocalCoordinatorOCCWriteConflictIsAtomic(t *testing.T) {
	st := store.NewMVCCStore()
	ctx := context.Background()
	require.NoError(t, st.PutAt(ctx, []byte("newer"), nil, 5, 0))
	c := kvtest.NewLocalCoordinator(st, kvtest.LocalOptions{OCC: kvtest.OCCTransactions})
	_, err := c.Dispatch(ctx, &kv.OperationGroup[kv.OP]{IsTxn: true, StartTS: 3, CommitTS: 6, Elems: []*kv.Elem[kv.OP]{put("untouched"), put("newer")}})
	require.ErrorIs(t, err, store.ErrWriteConflict)
	_, err = st.GetAt(ctx, []byte("untouched"), 6)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestLocalCoordinatorDelete(t *testing.T) {
	for _, mode := range []kvtest.OCCMode{kvtest.OCCDisabled, kvtest.OCCAllGroups} {
		st := store.NewMVCCStore()
		ctx := context.Background()
		require.NoError(t, st.PutAt(ctx, []byte("key"), nil, 1, 0))
		c := kvtest.NewLocalCoordinator(st, kvtest.LocalOptions{OCC: mode})
		_, err := c.Dispatch(ctx, &kv.OperationGroup[kv.OP]{StartTS: 1, CommitTS: 2, Elems: []*kv.Elem[kv.OP]{nil, {Op: kv.Del, Key: []byte("key")}}})
		require.NoError(t, err)
		_, err = st.GetAt(ctx, []byte("key"), 2)
		require.ErrorIs(t, err, store.ErrKeyNotFound)
	}
}

func TestLocalCoordinatorIgnoresNonTxnStartTS(t *testing.T) {
	c := kvtest.NewLocalCoordinator(store.NewMVCCStore(), kvtest.LocalOptions{})
	const unrelatedStartTS = uint64(1) << 63
	response, err := c.Dispatch(context.Background(), &kv.OperationGroup[kv.OP]{StartTS: unrelatedStartTS, Elems: []*kv.Elem[kv.OP]{put("key")}})
	require.NoError(t, err)
	require.Less(t, response.CommitTS, unrelatedStartTS, "non-transactional StartTS must not advance the HLC")
}

func TestLocalCoordinatorRejectsExhaustingTimestampsWithoutChangingClock(t *testing.T) {
	for _, tc := range []struct {
		name    string
		opts    kvtest.LocalOptions
		storeTS uint64
		req     kv.OperationGroup[kv.OP]
	}{
		{"explicit maximum", kvtest.LocalOptions{}, 0, kv.OperationGroup[kv.OP]{CommitTS: math.MaxUint64}},
		{"transaction start has no usable successor", kvtest.LocalOptions{}, 0, kv.OperationGroup[kv.OP]{IsTxn: true, StartTS: math.MaxUint64 - 1}},
		{"store increment reaches maximum", kvtest.LocalOptions{CommitTimestamp: kvtest.TimestampStoreIncrement}, math.MaxUint64 - 1, kv.OperationGroup[kv.OP]{}},
		{"clamp reaches maximum", kvtest.LocalOptions{AdvanceCommitTS: true}, math.MaxUint64 - 1, kv.OperationGroup[kv.OP]{CommitTS: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := store.NewMVCCStore()
			if tc.storeTS != 0 {
				require.NoError(t, st.PutAt(context.Background(), []byte("seed"), nil, tc.storeTS, 0))
			}
			c := kvtest.NewLocalCoordinator(st, tc.opts)
			tc.req.Elems = []*kv.Elem[kv.OP]{put("untouched")}
			_, err := c.Dispatch(context.Background(), &tc.req)
			require.ErrorIs(t, err, kv.ErrTxnCommitTSRequired)
			require.Zero(t, c.Clock().Current(), "invalid requests must not poison the HLC")
			require.Equal(t, tc.storeTS, st.LastCommitTS())
			_, err = st.GetAt(context.Background(), []byte("untouched"), math.MaxUint64)
			require.ErrorIs(t, err, store.ErrKeyNotFound)
		})
	}
}

func TestLocalCoordinatorRejectsAutomaticTimestampAtClockBoundary(t *testing.T) {
	for _, floor := range []uint64{math.MaxUint64 - 1, math.MaxUint64} {
		st := store.NewMVCCStore()
		c := kvtest.NewLocalCoordinator(st, kvtest.LocalOptions{})
		c.Clock().Observe(floor)
		_, err := c.Dispatch(context.Background(), &kv.OperationGroup[kv.OP]{Elems: []*kv.Elem[kv.OP]{put("untouched")}})
		require.ErrorIs(t, err, kv.ErrTxnCommitTSRequired)
		require.Equal(t, floor, c.Clock().Current())
		require.Zero(t, st.LastCommitTS())
	}
}

func TestLocalCoordinatorAllowsLastNonexhaustingTimestamp(t *testing.T) {
	for _, req := range []*kv.OperationGroup[kv.OP]{
		{CommitTS: math.MaxUint64 - 1},
		{IsTxn: true, StartTS: math.MaxUint64 - 2},
	} {
		st := store.NewMVCCStore()
		c := kvtest.NewLocalCoordinator(st, kvtest.LocalOptions{})
		req.Elems = []*kv.Elem[kv.OP]{put("last")}
		response, err := c.Dispatch(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, uint64(math.MaxUint64-1), response.CommitTS)
		_, err = c.Dispatch(context.Background(), &kv.OperationGroup[kv.OP]{Elems: []*kv.Elem[kv.OP]{put("untouched")}})
		require.ErrorIs(t, err, kv.ErrTxnCommitTSRequired)
		require.Equal(t, response.CommitTS, c.Clock().Current())
		require.Equal(t, response.CommitTS, st.LastCommitTS())
	}
}

func TestLocalCoordinatorSharesInjectedClock(t *testing.T) {
	clock := kv.NewHLC()
	c := kvtest.NewLocalCoordinator(store.NewMVCCStore(), kvtest.LocalOptions{Clock: clock})
	require.Same(t, clock, c.Clock())
	result, err := c.Dispatch(context.Background(), &kv.OperationGroup[kv.OP]{Elems: []*kv.Elem[kv.OP]{put("key")}})
	require.NoError(t, err)
	require.GreaterOrEqual(t, clock.Current(), result.CommitTS, "the dispatch timestamp must come from the injected clock")
}
