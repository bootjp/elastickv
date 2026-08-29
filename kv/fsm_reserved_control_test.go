package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

// A RawKV mutation must not reach the migration and catalog control
// namespaces. Those are written only by the typed internal commands, and a user
// write that lands in one is later promoted as ordinary data.
func TestValidateRawMutationRejectsReservedControlKeys(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	t.Cleanup(func() { _ = st.Close() })
	f, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)

	for _, key := range [][]byte{
		[]byte("!dist|meta|version"),
		[]byte("!dist|route|0001"),
		[]byte("!dist|job|7"),
		[]byte("!dist|jobhist|7"),
		distribution.MigrationStagedDataKey(7, []byte("victim")),
		[]byte("!migstage|7|victim"),
		[]byte("!migwrite|7"),
		[]byte("!migfence|7"),
	} {
		err := f.validateRawMutationForApply(ctx, &pb.Mutation{Op: pb.Op_PUT, Key: key, Value: []byte("v")}, nil, 10)
		require.ErrorIs(t, err, ErrInvalidRequest, "key %q must be refused", key)
	}

	// Ordinary user keys are unaffected.
	require.NoError(t, f.validateRawMutationForApply(ctx,
		&pb.Mutation{Op: pb.Op_PUT, Key: []byte("user-key"), Value: []byte("v")}, nil, 10))
}

// DEL_PREFIX never reaches validateRawMutationsForApply, so it is gated in
// handleDelPrefix. A partial spelling of a control prefix is refused too,
// because the delete would sweep the namespace up.
func TestHandleDelPrefixRejectsReservedControlPrefixes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	t.Cleanup(func() { _ = st.Close() })
	f, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)

	for _, prefix := range [][]byte{
		[]byte("!dist|"),
		[]byte("!dist|route|"),
		[]byte("!dist"),
		distribution.MigrationStagedDataKey(7, []byte("user:")),
		[]byte("!migwrite|"),
		[]byte("!migfence"),
	} {
		require.ErrorIs(t, f.handleDelPrefix(ctx, prefix, 10), ErrInvalidRequest,
			"prefix %q must be refused", prefix)
	}

	// The whole-keyspace flush is a deliberate operation and stays allowed.
	require.NoError(t, f.handleDelPrefix(ctx, nil, 11))
	// So does an ordinary user prefix.
	require.NoError(t, f.handleDelPrefix(ctx, []byte("user:"), 12))
}

// Staged migration data is private to typed migration paths. A user-supplied
// RawKV request must not be able to forge a staged key that promotion later
// treats as migrated data.
func TestValidateRawMutationRejectsStagedDataKeys(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	t.Cleanup(func() { _ = st.Close() })
	f, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)

	staged := distribution.MigrationStagedDataKey(7, []byte("user-key"))
	require.ErrorIs(t, f.validateRawMutationForApply(ctx,
		&pb.Mutation{Op: pb.Op_PUT, Key: staged, Value: []byte("v")}, nil, 10), ErrInvalidRequest)
	require.ErrorIs(t, f.handleDelPrefix(ctx, distribution.MigrationStagedDataKey(7, []byte("user:")), 11), ErrInvalidRequest)
}

// The transactional paths reach the store through their own helpers, which did
// not carry the reserved-key check the raw path has. A TransactionalKV request
// could therefore write catalog or staged-migration state directly, and a
// forged !migstage|<job>| row is promoted as user data when the same group is
// the migration target.
func TestTxnMutationHelpersRejectReservedControlKeys(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	t.Cleanup(func() { _ = st.Close() })
	f, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)

	for _, key := range [][]byte{
		distribution.MigrationStagedDataKey(7, []byte("victim")),
		[]byte("!migwrite|7"),
		[]byte("!migfence|7"),
	} {
		muts := []*pb.Mutation{{Op: pb.Op_PUT, Key: key, Value: []byte("v")}}
		_, err := f.uniqueMutationsAboveFloor(muts, 10)
		require.ErrorIs(t, err, ErrInvalidRequest,
			"prepare/one-phase must refuse %q", key)
		_, err = f.uniqueTxnMutationsAboveFloor(muts, 10)
		require.ErrorIs(t, err, ErrInvalidRequest,
			"commit must refuse %q", key)
	}

	// Ordinary user keys, the transaction-internal keys the txn paths
	// legitimately write, and the catalog records the control plane commits
	// through the coordinator are unaffected. SplitRange is a transaction that
	// writes !dist|route| and !dist|meta|, so refusing those here would break
	// the control plane itself.
	for _, key := range [][]byte{
		[]byte("user-key"),
		txnLockKey([]byte("user-key")),
		txnIntentKey([]byte("user-key")),
		[]byte("!dist|route|0001"),
		[]byte("!dist|meta|version"),
	} {
		muts := []*pb.Mutation{{Op: pb.Op_PUT, Key: key, Value: []byte("v")}}
		_, err := f.uniqueMutationsAboveFloor(muts, 10)
		require.NoError(t, err, "key %q must be accepted", key)
		_, err = f.uniqueTxnMutationsAboveFloor(muts, 10)
		require.NoError(t, err, "key %q must be accepted", key)
	}
}
