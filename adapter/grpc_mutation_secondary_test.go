package adapter

import (
	"io"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSecondaryMutationFailureStaysIndeterminate(t *testing.T) {
	for _, cause := range []error{store.ErrWriteConflict, kv.ErrLeaderProxyCircuitOpen} {
		mixed := errors.CombineErrors(cause, io.EOF)
		require.Equal(t, codes.Unknown, status.Code(projectPublicGRPCMutationError(mixed)), "a hidden lost reply must dominate the primary error")
	}
}

func TestJoinedTerminalCausesCannotProveNonadmission(t *testing.T) {
	for _, cause := range []error{kv.ErrRouteCutoverPending, store.ErrWriteConflict} {
		mixed := errors.Join(cause, kv.ErrLeaderProxyCircuitOpen)
		require.Equal(t, codes.Unknown, status.Code(projectPublicGRPCMutationError(mixed)), "a circuit error cannot erase another outcome")
	}
}
