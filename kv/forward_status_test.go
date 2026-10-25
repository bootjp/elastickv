package kv

import (
	"bytes"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestForwardStatusClassificationIgnoresDisplayMessage(t *testing.T) {
	t.Parallel()

	key := []byte("binary\x00 desc = rpc error: code = Aborted")
	conflict := ForwardWriteConflictStatus(store.NewWriteConflictError(key))
	st, ok := status.FromError(conflict)
	require.True(t, ok)
	proto := st.Proto()
	proto.Message = "diagnostic changed to not leader"
	changed := errors.WithStack(status.FromProto(proto).Err())
	require.True(t, IsForwardWriteConflict(changed))
	require.False(t, isTransientLeaderError(changed))
	decoded, ok := ForwardWriteConflictKey(changed)
	require.True(t, ok)
	require.True(t, bytes.Equal(key, decoded))
	require.False(t, IsForwardWriteConflict(status.Error(codes.Aborted, conflict.Error())))
	emptyKey := ForwardWriteConflictStatus(store.NewWriteConflictError([]byte{}))
	emptyDecoded, ok := ForwardWriteConflictKey(emptyKey)
	require.True(t, ok)
	require.Empty(t, emptyDecoded)

	locked := ForwardTxnLockedStatus(NewTxnLockedErrorWithDetail(key, "owner: desc = changed"))
	lockedStatus, ok := status.FromError(locked)
	require.True(t, ok)
	lockedProto := lockedStatus.Proto()
	lockedProto.Message = "another message"
	changedLocked := status.FromProto(lockedProto).Err()
	require.True(t, IsForwardTxnLocked(changedLocked))
	lockedKey, detail, ok := ForwardTxnLockedDetails(changedLocked)
	require.True(t, ok)
	require.True(t, bytes.Equal(key, lockedKey))
	require.Equal(t, "owner: desc = changed", detail)

	beforeSubmit := ForwardNotLeaderStatus()
	leaderStatus, ok := status.FromError(beforeSubmit)
	require.True(t, ok)
	leaderProto := leaderStatus.Proto()
	leaderProto.Message = "renamed pre-submit rejection"
	require.True(t, IsForwardNotLeader(status.FromProto(leaderProto).Err()))
	require.False(t, IsForwardNotLeader(status.Error(codes.Unavailable, "not leader")))
}

func TestForwardStatusDoesNotAcceptUnmarkedErrors(t *testing.T) {
	t.Parallel()

	for _, err := range []error{
		status.Error(codes.Aborted, store.ErrWriteConflict.Error()),
		status.Error(codes.Unknown, store.NewWriteConflictError([]byte("key")).Error()),
		status.Error(codes.Unavailable, "not leader"),
	} {
		require.False(t, IsForwardWriteConflict(err))
		require.False(t, IsForwardTxnLocked(err))
		require.False(t, IsForwardNotLeader(err))
	}
}
