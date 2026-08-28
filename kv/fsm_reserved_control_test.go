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

// ShardedCoordinator rewrites a user key into MigrationStagedDataKey while its
// route has staged visibility, so legitimate user writes do arrive under the
// staged prefix. Refusing those would break the migration this branch adds.
func TestValidateRawMutationAllowsStagedDataKeys(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	t.Cleanup(func() { _ = st.Close() })
	f, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)

	staged := distribution.MigrationStagedDataKey(7, []byte("user-key"))
	require.NoError(t, f.validateRawMutationForApply(ctx,
		&pb.Mutation{Op: pb.Op_PUT, Key: staged, Value: []byte("v")}, nil, 10))
	require.NoError(t, f.handleDelPrefix(ctx, distribution.MigrationStagedDataKey(7, []byte("user:")), 11))
}
