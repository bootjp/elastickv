package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestCoordinateDispatch_RawPut(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	fsm := NewKvFSMWithHLC(st, NewHLC())
	r, stop := newSingleRaft(t, "dispatch-raw-put", fsm)
	t.Cleanup(stop)

	tm := NewTransactionWithProposer(r)
	c := NewCoordinatorWithEngine(tm, r)
	ctx := context.Background()

	resp, err := c.Dispatch(ctx, &OperationGroup[OP]{
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("k1"), Value: []byte("v1")},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Verify the value was written.
	val, err := st.GetAt(ctx, []byte("k1"), ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), val)
}

func TestCoordinateDispatch_RawDel(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	fsm := NewKvFSMWithHLC(st, NewHLC())
	r, stop := newSingleRaft(t, "dispatch-raw-del", fsm)
	t.Cleanup(stop)

	tm := NewTransactionWithProposer(r)
	c := NewCoordinatorWithEngine(tm, r)
	ctx := context.Background()

	// Write a value first.
	_, err := c.Dispatch(ctx, &OperationGroup[OP]{
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("k1"), Value: []byte("v1")},
		},
	})
	require.NoError(t, err)

	// Delete the value.
	_, err = c.Dispatch(ctx, &OperationGroup[OP]{
		Elems: []*Elem[OP]{
			{Op: Del, Key: []byte("k1")},
		},
	})
	require.NoError(t, err)

	// Verify the key is gone.
	_, err = st.GetAt(ctx, []byte("k1"), ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestCoordinateDispatch_TxnOnePhase(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	fsm := NewKvFSMWithHLC(st, NewHLC())
	r, stop := newSingleRaft(t, "dispatch-txn", fsm)
	t.Cleanup(stop)

	tm := NewTransactionWithProposer(r)
	c := NewCoordinatorWithEngine(tm, r)
	ctx := context.Background()

	startTS := c.clock.Next()
	resp, err := c.Dispatch(ctx, &OperationGroup[OP]{
		IsTxn:   true,
		StartTS: startTS,
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("a"), Value: []byte("1")},
			{Op: Put, Key: []byte("b"), Value: []byte("2")},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Both keys should be readable.
	v1, err := st.GetAt(ctx, []byte("a"), ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, []byte("1"), v1)

	v2, err := st.GetAt(ctx, []byte("b"), ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, []byte("2"), v2)
}

func TestCoordinateDispatch_NilRequest(t *testing.T) {
	t.Parallel()

	c := &Coordinate{
		clock: NewHLC(),
	}

	_, err := c.Dispatch(context.Background(), nil)
	require.ErrorIs(t, err, ErrInvalidRequest)
}

func TestCoordinateDispatch_EmptyElems(t *testing.T) {
	t.Parallel()

	c := &Coordinate{
		clock: NewHLC(),
	}

	_, err := c.Dispatch(context.Background(), &OperationGroup[OP]{})
	require.ErrorIs(t, err, ErrInvalidRequest)
}

func TestCoordinateDispatch_TxnAssignsStartTS(t *testing.T) {
	t.Parallel()

	tx := &stubTransactional{}
	st := store.NewMVCCStore()
	fsm := NewKvFSMWithHLC(st, NewHLC())
	r, stop := newSingleRaft(t, "dispatch-ts-assign", fsm)
	t.Cleanup(stop)

	c := &Coordinate{
		transactionManager: tx,
		engine:             r,
		clock:              NewHLC(),
	}

	// When StartTS is 0 for a txn, Dispatch should assign one.
	resp, err := c.Dispatch(context.Background(), &OperationGroup[OP]{
		IsTxn: true,
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("k"), Value: []byte("v")},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, 1, tx.commits)

	// The request should have a non-zero startTS.
	require.Len(t, tx.reqs, 1)
	require.Len(t, tx.reqs[0], 1)
	require.Greater(t, tx.reqs[0][0].Ts, uint64(0))
}

func TestCoordinateDispatchRaw_CallsTransactionManager(t *testing.T) {
	t.Parallel()

	tx := &stubTransactional{}
	st := store.NewMVCCStore()
	fsm := NewKvFSMWithHLC(st, NewHLC())
	r, stop := newSingleRaft(t, "dispatch-raw-tm", fsm)
	t.Cleanup(stop)

	c := &Coordinate{
		transactionManager: tx,
		engine:             r,
		clock:              NewHLC(),
	}

	resp, err := c.Dispatch(context.Background(), &OperationGroup[OP]{
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("k"), Value: []byte("v")},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, 1, tx.commits)
}
