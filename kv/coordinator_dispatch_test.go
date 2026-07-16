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

// TestToRawRequestLeavesTsForLeaderStamping is the regression for the
// codex P2 review on PR #867.
//
// buildRedirectRequests is the follower's forward path: when a client
// hits a non-leader, the follower constructs forwarded Requests and
// ships them to the leader's Internal.Forward, which is responsible
// for stamping ts (adapter.Internal.stampRawTimestamps). Issuing a ts
// on the follower violates the HLC-leader-only invariant (CLAUDE.md
// HLC contract) and — with the HLC-4 (iii) fence now wired up — can
// spuriously fail follower-routed raw writes when the follower's
// physicalCeiling is stale relative to the leader.
//
// Contract: toRawRequest must leave Ts == 0 so the leader stamps it.
func TestToRawRequestLeavesTsForLeaderStamping(t *testing.T) {
	t.Parallel()

	clock := NewHLC()
	// Simulate a stale follower: ceiling sits in the past so any
	// NextFenced() call on this clock would return ErrCeilingExpired.
	clock.SetPhysicalCeiling(1)

	c := &Coordinate{clock: clock}

	cases := []struct {
		name string
		req  *Elem[OP]
	}{
		{"put", &Elem[OP]{Op: Put, Key: []byte("k"), Value: []byte("v")}},
		{"del", &Elem[OP]{Op: Del, Key: []byte("k")}},
		{"del-prefix", &Elem[OP]{Op: DelPrefix, Key: []byte("p")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := c.toRawRequest(tc.req, 0)
			require.NotNil(t, r)
			require.Equal(t, uint64(0), r.Ts,
				"forwarded raw requests must arrive with Ts==0 so the leader's stampRawTimestamps assigns the canonical ts (HLC leader-only invariant + HLC-4 (iii) fence)")
		})
	}
}

func TestBuildRedirectRequests_PreservesRawObservedRouteVersion(t *testing.T) {
	t.Parallel()

	c := &Coordinate{}
	got, err := c.buildRedirectRequests(&OperationGroup[OP]{
		ObservedRouteVersion: 17,
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("k"), Value: []byte("v")},
		},
	})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, uint64(17), got[0].GetObservedRouteVersion())
}

// TestBuildRedirectRequestsSurvivesStaleFollowerCeiling exercises the
// follower's redirect path end-to-end with an expired ceiling: it
// confirms the follower hands off a Ts==0 request to the leader
// instead of failing locally on ErrCeilingExpired.
func TestBuildRedirectRequestsSurvivesStaleFollowerCeiling(t *testing.T) {
	t.Parallel()

	clock := NewHLC()
	clock.SetPhysicalCeiling(1) // wall_now >> 1, so NextFenced would fail

	c := &Coordinate{clock: clock}

	got, err := c.buildRedirectRequests(&OperationGroup[OP]{
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("k"), Value: []byte("v")},
		},
	})
	require.NoError(t, err,
		"buildRedirectRequests must succeed even when the follower's HLC ceiling is stale")
	require.Len(t, got, 1)
	require.Equal(t, uint64(0), got[0].Ts,
		"redirect-path raw requests must carry Ts==0 (leader stamps)")
}
