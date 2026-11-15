package kv

import (
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLegacyForwardStatusNeverProvesRejection(t *testing.T) {
	t.Parallel()
	for _, err := range []error{
		ForwardWriteConflictStatus(),
		ForwardTxnLockedStatus(),
		ForwardNotLeaderStatus(),
		ForwardComposed1Status(ErrComposed1Violation),
		status.Error(codes.Aborted, store.ErrWriteConflict.Error()),
		status.Error(codes.Unknown, store.NewWriteConflictError([]byte("key")).Error()),
		status.Error(codes.Unavailable, "not leader"),
	} {
		got := classifyForwardRPCError(errors.WithStack(err), []*pb.Request{{Phase: pb.Phase_NONE}})
		require.ErrorIs(t, got, ErrTransactionOutcomeUnknown)
		require.NotErrorIs(t, got, store.ErrWriteConflict)
		require.NotErrorIs(t, got, ErrTxnLocked)
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
