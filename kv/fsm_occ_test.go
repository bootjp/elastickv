package kv

import (
	"context"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestValidateConflictsDetectsStaleWrite(t *testing.T) {
	ctx := context.Background()
	st := store.NewMVCCStore()
	require.NoError(t, st.PutAt(ctx, []byte("k"), []byte("v1"), 50, 0))

	fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)

	muts := []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("v2")}}
	err := fsm.validateConflicts(ctx, muts, 40)
	require.ErrorIs(t, err, store.ErrWriteConflict)
}

func TestApplyReturnsErrorOnConflict(t *testing.T) {
	ctx := context.Background()
	st := store.NewMVCCStore()
	fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)

	// First write commits at ts=100.
	put := &pb.Request{
		IsTxn: false,
		Phase: pb.Phase_NONE,
		Ts:    100,
		Mutations: []*pb.Mutation{{
			Op:    pb.Op_PUT,
			Key:   []byte("k"),
			Value: []byte("v1"),
		}},
	}
	data, err := proto.Marshal(put)
	require.NoError(t, err)

	resp := fsm.Apply(data)
	require.Nil(t, resp)

	// Stale transaction attempts to prewrite with startTS=90.
	conflict := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_PREPARE,
		Ts:    90,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte("!txn|meta|"), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("k"), LockTTLms: 1000})},
			{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("v2")},
		},
	}
	data, err = proto.Marshal(conflict)
	require.NoError(t, err)

	resp = fsm.Apply(data)

	err, ok = resp.(error)
	require.True(t, ok)
	require.ErrorIs(t, err, store.ErrWriteConflict)

	// Ensure committed value remains.
	v, err := st.GetAt(ctx, []byte("k"), ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), v)
}

func TestOnePhaseTxnDetectsWriteConflict(t *testing.T) {
	ctx := context.Background()
	st := store.NewMVCCStore()
	require.NoError(t, st.PutAt(ctx, []byte("k"), []byte("v1"), 100, 0))

	fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)

	// One-phase txn with startTS < latest commit (100) should be rejected.
	req := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_NONE,
		Ts:    90,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("k"), CommitTS: 110})},
			{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("v2")},
		},
	}
	data, err := proto.Marshal(req)
	require.NoError(t, err)

	resp := fsm.Apply(data)
	err, ok = resp.(error)
	require.True(t, ok)
	require.ErrorIs(t, err, store.ErrWriteConflict)

	// Ensure the original value is unchanged.
	v, err := st.GetAt(ctx, []byte("k"), ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), v)
}

// TestOnePhaseTxnDetectsReadWriteConflictViaReadKeys verifies that the FSM
// rejects a one-phase transaction when a key in ReadKeys was committed after
// the transaction's startTS, closing the TOCTOU window.
func TestOnePhaseTxnDetectsReadWriteConflictViaReadKeys(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	// Concurrent write: "rk" committed at ts=100, after T_a's startTS=90.
	require.NoError(t, st.PutAt(ctx, []byte("rk"), []byte("concurrent"), 100, 0))

	fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)

	// T_a one-phase txn: startTS=90, writes "wk", reads "rk" (stale).
	req := &pb.Request{
		IsTxn:    true,
		Phase:    pb.Phase_NONE,
		Ts:       90,
		ReadKeys: [][]byte{[]byte("rk")},
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("wk"), CommitTS: 110})},
			{Op: pb.Op_PUT, Key: []byte("wk"), Value: []byte("val")},
		},
	}
	err := applyFSMRequest(t, fsm, req)
	require.ErrorIs(t, err, store.ErrWriteConflict)

	// The write must not have been applied.
	_, err = st.GetAt(ctx, []byte("wk"), ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

// TestOnePhaseTxnAllowsReadKeyWhenNoConflict verifies that a one-phase
// transaction succeeds when ReadKeys were not modified after startTS.
func TestOnePhaseTxnAllowsReadKeyWhenNoConflict(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	// "rk" committed at ts=80, before T_a's startTS=100 — not stale.
	require.NoError(t, st.PutAt(ctx, []byte("rk"), []byte("old"), 80, 0))

	fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)

	req := &pb.Request{
		IsTxn:    true,
		Phase:    pb.Phase_NONE,
		Ts:       100,
		ReadKeys: [][]byte{[]byte("rk")},
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("wk"), CommitTS: 120})},
			{Op: pb.Op_PUT, Key: []byte("wk"), Value: []byte("val")},
		},
	}
	require.NoError(t, applyFSMRequest(t, fsm, req))

	v, err := st.GetAt(ctx, []byte("wk"), ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, []byte("val"), v)
}

// TestOnePhaseTxnNilReadKeysSkipsReadWriteValidation verifies that when
// ReadKeys is nil the FSM skips read-write conflict checks, so a concurrent
// commit to an untracked key does not block the transaction.
func TestOnePhaseTxnNilReadKeysSkipsReadWriteValidation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	// "rk" committed at ts=200 — would trigger a conflict if tracked.
	require.NoError(t, st.PutAt(ctx, []byte("rk"), []byte("newer"), 200, 0))

	fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)

	// nil ReadKeys: read-write check is skipped entirely.
	req := &pb.Request{
		IsTxn:    true,
		Phase:    pb.Phase_NONE,
		Ts:       100,
		ReadKeys: nil,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("wk"), CommitTS: 120})},
			{Op: pb.Op_PUT, Key: []byte("wk"), Value: []byte("val")},
		},
	}
	require.NoError(t, applyFSMRequest(t, fsm, req))

	v, err := st.GetAt(ctx, []byte("wk"), ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, []byte("val"), v)
}

// TestOnePhaseTxnTOCTOUScenario simulates the exact TOCTOU window that the
// readKeys fix was designed to close: T_a reads a key at startTS=100, then
// T_b commits a newer version at ts=150 before T_a reaches the FSM. The FSM
// must reject T_a's one-phase commit because its read set is stale.
func TestOnePhaseTxnTOCTOUScenario(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	// Initial value seen by T_a.
	require.NoError(t, st.PutAt(ctx, []byte("balance"), []byte("100"), 50, 0))

	fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)

	// T_b commits a newer version while T_a is in flight.
	require.NoError(t, st.PutAt(ctx, []byte("balance"), []byte("50"), 150, 0))

	// T_a commits based on the stale value it read at startTS=100.
	req := &pb.Request{
		IsTxn:    true,
		Phase:    pb.Phase_NONE,
		Ts:       100,
		ReadKeys: [][]byte{[]byte("balance")},
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("result"), CommitTS: 200})},
			{Op: pb.Op_PUT, Key: []byte("result"), Value: []byte("derived-from-stale-balance")},
		},
	}
	err := applyFSMRequest(t, fsm, req)
	require.ErrorIs(t, err, store.ErrWriteConflict)

	// "result" must not have been written.
	_, err = st.GetAt(ctx, []byte("result"), ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

// TestPrepareRequestDetectsReadWriteConflictViaReadKeys verifies that the
// PREPARE phase also rejects transactions whose ReadKeys were committed after
// startTS, providing the same SSI guarantee as one-phase transactions.
func TestPrepareRequestDetectsReadWriteConflictViaReadKeys(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	// "rk" committed at ts=100, after T_a's startTS=90.
	require.NoError(t, st.PutAt(ctx, []byte("rk"), []byte("concurrent"), 100, 0))

	fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)

	req := &pb.Request{
		IsTxn:    true,
		Phase:    pb.Phase_PREPARE,
		Ts:       90,
		ReadKeys: [][]byte{[]byte("rk")},
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("wk"), LockTTLms: 1000})},
			{Op: pb.Op_PUT, Key: []byte("wk"), Value: []byte("val")},
		},
	}
	err := applyFSMRequest(t, fsm, req)
	require.ErrorIs(t, err, store.ErrWriteConflict)

	// The lock must not have been written.
	_, err = st.GetAt(ctx, txnLockKey([]byte("wk")), ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}
