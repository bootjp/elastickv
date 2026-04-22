package kv

import (
	"context"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

// prepareTxn is a test helper that applies a PREPARE request for a transaction
// that locks the given keys under the specified primaryKey and startTS.
func prepareTxn(t *testing.T, fsm *kvFSM, primaryKey []byte, startTS uint64, keys [][]byte, values [][]byte) {
	t.Helper()
	muts := make([]*pb.Mutation, 0, 1+len(keys))
	muts = append(muts, &pb.Mutation{
		Op:    pb.Op_PUT,
		Key:   []byte(txnMetaPrefix),
		Value: EncodeTxnMeta(TxnMeta{PrimaryKey: primaryKey, LockTTLms: defaultTxnLockTTLms}),
	})
	for i, key := range keys {
		mut := &pb.Mutation{Op: pb.Op_PUT, Key: key}
		if i < len(values) {
			mut.Value = values[i]
		}
		muts = append(muts, mut)
	}
	req := &pb.Request{
		IsTxn:     true,
		Phase:     pb.Phase_PREPARE,
		Ts:        startTS,
		Mutations: muts,
	}
	require.NoError(t, applyFSMRequest(t, fsm, req))
}

// abortTxn is a test helper that applies an ABORT request for a transaction.
func abortTxn(t *testing.T, fsm *kvFSM, primaryKey []byte, startTS, abortTS uint64, keys [][]byte) error {
	t.Helper()
	muts := make([]*pb.Mutation, 0, 1+len(keys))
	muts = append(muts, &pb.Mutation{
		Op:    pb.Op_PUT,
		Key:   []byte(txnMetaPrefix),
		Value: EncodeTxnMeta(TxnMeta{PrimaryKey: primaryKey, CommitTS: abortTS}),
	})
	for _, key := range keys {
		muts = append(muts, &pb.Mutation{Op: pb.Op_PUT, Key: key})
	}
	req := &pb.Request{
		IsTxn:     true,
		Phase:     pb.Phase_ABORT,
		Ts:        startTS,
		Mutations: muts,
	}
	return applyFSMRequest(t, fsm, req)
}

func TestFSMAbort_PrepareThenAbort(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)

	startTS := uint64(10)
	abortTS := uint64(20)
	primary := []byte("pk")
	key := []byte("k1")

	// Prepare: write lock + intent for primary and key.
	prepareTxn(t, fsm, primary, startTS, [][]byte{primary, key}, [][]byte{[]byte("pv"), []byte("v1")})

	// Verify locks and intents exist after prepare.
	_, err := st.GetAt(ctx, txnLockKey(primary), ^uint64(0))
	require.NoError(t, err, "primary lock should exist after prepare")
	_, err = st.GetAt(ctx, txnIntentKey(primary), ^uint64(0))
	require.NoError(t, err, "primary intent should exist after prepare")
	_, err = st.GetAt(ctx, txnLockKey(key), ^uint64(0))
	require.NoError(t, err, "key lock should exist after prepare")
	_, err = st.GetAt(ctx, txnIntentKey(key), ^uint64(0))
	require.NoError(t, err, "key intent should exist after prepare")

	// Abort.
	err = abortTxn(t, fsm, primary, startTS, abortTS, [][]byte{primary, key})
	require.NoError(t, err)

	// Locks should be cleaned up.
	_, err = st.GetAt(ctx, txnLockKey(primary), ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound, "primary lock should be deleted after abort")
	_, err = st.GetAt(ctx, txnLockKey(key), ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound, "key lock should be deleted after abort")

	// Intents should be cleaned up.
	_, err = st.GetAt(ctx, txnIntentKey(primary), ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound, "primary intent should be deleted after abort")
	_, err = st.GetAt(ctx, txnIntentKey(key), ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound, "key intent should be deleted after abort")

	// Rollback record should exist for the primary key.
	_, err = st.GetAt(ctx, txnRollbackKey(primary, startTS), ^uint64(0))
	require.NoError(t, err, "rollback record should exist after abort")

	// User data should NOT have been written.
	_, err = st.GetAt(ctx, primary, ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound, "primary user data should not exist after abort")
	_, err = st.GetAt(ctx, key, ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound, "key user data should not exist after abort")
}

func TestFSMAbort_RejectsAlreadyCommittedTxn(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)

	startTS := uint64(10)
	commitTS := uint64(20)
	abortTS := uint64(30)
	primary := []byte("pk")

	// Prepare.
	prepareTxn(t, fsm, primary, startTS, [][]byte{primary}, [][]byte{[]byte("v")})

	// Commit.
	commitReq := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_COMMIT,
		Ts:    startTS,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: primary, CommitTS: commitTS})},
			{Op: pb.Op_PUT, Key: primary},
		},
	}
	require.NoError(t, applyFSMRequest(t, fsm, commitReq))

	// Try to abort -- should fail because the transaction is already committed.
	err := abortTxn(t, fsm, primary, startTS, abortTS, [][]byte{primary})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrTxnAlreadyCommitted)
}

func TestFSMAbort_LockCleanup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)

	startTS := uint64(100)
	abortTS := uint64(200)
	primary := []byte("pkey")
	keys := [][]byte{primary, []byte("a"), []byte("b"), []byte("c")}
	values := [][]byte{[]byte("pv"), []byte("va"), []byte("vb"), []byte("vc")}

	prepareTxn(t, fsm, primary, startTS, keys, values)

	// Confirm all locks exist.
	for _, k := range keys {
		_, err := st.GetAt(ctx, txnLockKey(k), ^uint64(0))
		require.NoError(t, err, "lock for key %q should exist before abort", string(k))
	}

	// Abort.
	err := abortTxn(t, fsm, primary, startTS, abortTS, keys)
	require.NoError(t, err)

	// All locks should be deleted.
	for _, k := range keys {
		_, err := st.GetAt(ctx, txnLockKey(k), ^uint64(0))
		require.ErrorIs(t, err, store.ErrKeyNotFound, "lock for key %q should be deleted after abort", string(k))
	}
}

func TestFSMAbort_IntentCleanup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)

	startTS := uint64(100)
	abortTS := uint64(200)
	primary := []byte("pkey")
	keys := [][]byte{primary, []byte("x"), []byte("y")}
	values := [][]byte{[]byte("pv"), []byte("vx"), []byte("vy")}

	prepareTxn(t, fsm, primary, startTS, keys, values)

	// Confirm all intents exist.
	for _, k := range keys {
		_, err := st.GetAt(ctx, txnIntentKey(k), ^uint64(0))
		require.NoError(t, err, "intent for key %q should exist before abort", string(k))
	}

	// Abort.
	err := abortTxn(t, fsm, primary, startTS, abortTS, keys)
	require.NoError(t, err)

	// All intents should be deleted.
	for _, k := range keys {
		_, err := st.GetAt(ctx, txnIntentKey(k), ^uint64(0))
		require.ErrorIs(t, err, store.ErrKeyNotFound, "intent for key %q should be deleted after abort", string(k))
	}
}

func TestFSMAbort_RollbackRecord(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)

	startTS := uint64(50)
	abortTS := uint64(60)
	primary := []byte("rpk")
	key := []byte("rk")

	prepareTxn(t, fsm, primary, startTS, [][]byte{primary, key}, [][]byte{[]byte("pv"), []byte("v")})

	// Before abort, rollback record should not exist.
	_, err := st.GetAt(ctx, txnRollbackKey(primary, startTS), ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound, "rollback record should not exist before abort")

	// Abort.
	err = abortTxn(t, fsm, primary, startTS, abortTS, [][]byte{primary, key})
	require.NoError(t, err)

	// Rollback record should be created for the primary key.
	rbData, err := st.GetAt(ctx, txnRollbackKey(primary, startTS), ^uint64(0))
	require.NoError(t, err, "rollback record should exist after abort")
	require.Equal(t, encodeTxnRollbackRecord(), rbData, "rollback record content mismatch")
}

func TestFSMAbort_NonPrimaryOnlyDoesNotWriteRollback(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)

	startTS := uint64(50)
	abortTS := uint64(60)
	primary := []byte("pk")
	secondary := []byte("sk")

	// Prepare both keys.
	prepareTxn(t, fsm, primary, startTS, [][]byte{primary, secondary}, [][]byte{[]byte("pv"), []byte("sv")})

	// Abort only the secondary key (primary not included in this abort batch).
	err := abortTxn(t, fsm, primary, startTS, abortTS, [][]byte{secondary})
	require.NoError(t, err)

	// Secondary lock+intent cleaned up.
	_, err = st.GetAt(ctx, txnLockKey(secondary), ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound, "secondary lock should be deleted")
	_, err = st.GetAt(ctx, txnIntentKey(secondary), ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound, "secondary intent should be deleted")

	// No rollback record since we did not include the primary key in the abort.
	_, err = st.GetAt(ctx, txnRollbackKey(primary, startTS), ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound, "rollback record should not exist when primary is not aborted")
}

func TestFSMAbort_AbortTSMustBeGreaterThanStartTS(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)

	startTS := uint64(10)
	primary := []byte("pk")

	prepareTxn(t, fsm, primary, startTS, [][]byte{primary}, [][]byte{[]byte("v")})

	// Abort with abortTS == startTS should fail.
	err := abortTxn(t, fsm, primary, startTS, startTS, [][]byte{primary})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrTxnCommitTSRequired)

	// Abort with abortTS < startTS should also fail.
	err = abortTxn(t, fsm, primary, startTS, startTS-1, [][]byte{primary})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrTxnCommitTSRequired)
}

// TestFSMAbort_SecondAbortIsIdempotent pins the intended post-fix
// behaviour: once a (primaryKey, startTS) pair has a rollback marker,
// a subsequent abort against the same pair must return nil without
// touching the store. The previous behaviour (write-conflict on the
// rollback-marker Put) surfaced in prod as "secondary write failed"
// log spam whenever dualwrite replay or the lock resolver raced a
// completed abort.
func TestFSMAbort_SecondAbortIsIdempotent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)

	startTS := uint64(10)
	abortTS := uint64(20)
	primary := []byte("pk")
	key := []byte("k")

	prepareTxn(t, fsm, primary, startTS, [][]byte{primary, key}, [][]byte{[]byte("pv"), []byte("v")})

	// First abort succeeds.
	err := abortTxn(t, fsm, primary, startTS, abortTS, [][]byte{primary, key})
	require.NoError(t, err)

	// Verify cleanup happened.
	_, err = st.GetAt(ctx, txnLockKey(key), ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)

	// Rollback record exists.
	_, err = st.GetAt(ctx, txnRollbackKey(primary, startTS), ^uint64(0))
	require.NoError(t, err)

	// Same-abortTS retry: used to conflict; now must be a no-op.
	err = abortTxn(t, fsm, primary, startTS, abortTS, [][]byte{primary, key})
	require.NoError(t, err, "same-abortTS retry must be idempotent")

	// Later-abortTS retry (HLC-monotonic): also no-op. This is the
	// prod path where a second lock resolver arrives seconds after
	// the first one completed.
	err = abortTxn(t, fsm, primary, startTS, abortTS+100, [][]byte{primary, key})
	require.NoError(t, err, "later-abortTS retry must be idempotent")
}

func TestFSMAbort_MissingPrimaryKeyReturnsError(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)

	startTS := uint64(10)
	abortTS := uint64(20)

	// Abort with empty primary key in meta.
	req := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_ABORT,
		Ts:    startTS,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: nil, CommitTS: abortTS})},
			{Op: pb.Op_PUT, Key: []byte("k")},
		},
	}
	err := applyFSMRequest(t, fsm, req)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrTxnPrimaryKeyRequired)
}

func TestFSMAbort_EmptyMutationsReturnsError(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)

	startTS := uint64(10)
	abortTS := uint64(20)

	// Abort with only the meta mutation (no actual keys to abort).
	req := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_ABORT,
		Ts:    startTS,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("pk"), CommitTS: abortTS})},
		},
	}
	err := applyFSMRequest(t, fsm, req)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidRequest)
}
