package kv

import (
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestForwardStatusClassificationIgnoresDisplayMessage(t *testing.T) {
	t.Parallel()

	conflict := ForwardWriteConflictStatus()
	st, ok := status.FromError(conflict)
	require.True(t, ok)
	proto := st.Proto()
	proto.Message = "diagnostic changed to not leader"
	changed := errors.WithStack(status.FromProto(proto).Err())
	require.True(t, IsForwardWriteConflict(changed))
	require.False(t, isTransientLeaderError(changed))
	require.False(t, IsForwardWriteConflict(status.Error(codes.Aborted, conflict.Error())))

	locked := ForwardTxnLockedStatus()
	lockedStatus, ok := status.FromError(locked)
	require.True(t, ok)
	lockedProto := lockedStatus.Proto()
	lockedProto.Message = "another message"
	changedLocked := status.FromProto(lockedProto).Err()
	require.True(t, IsForwardTxnLocked(changedLocked))

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

func TestForwardStatusContainsOnlyReason(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		err     error
		message string
	}{
		{ForwardWriteConflictStatus(), store.ErrWriteConflict.Error()},
		{ForwardTxnLockedStatus(), ErrTxnLocked.Error()},
	} {
		st, ok := status.FromError(errors.Unwrap(tc.err))
		require.True(t, ok)
		require.Equal(t, tc.message, st.Message())
		require.Len(t, st.Details(), 1)
		for _, detail := range st.Details() {
			info, ok := detail.(*errdetails.ErrorInfo)
			require.True(t, ok)
			require.Empty(t, info.GetMetadata())
		}
	}
}
