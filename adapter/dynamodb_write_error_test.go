package adapter

import (
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNormalizeDynamoWriteDispatchError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"local conflict", store.NewWriteConflictError([]byte("k")), true},
		{"unknown conflict", status.Error(codes.Unknown, "key: k: write conflict"), true},
		{"wrapped aborted conflict", errors.WithStack(status.Error(codes.Aborted, "key: k: write conflict")), true},
		{"plain text", errors.New("key: k: write conflict"), false},
		{"missing key envelope", status.Error(codes.Unknown, "write conflict"), false},
		{"wrong code", status.Error(codes.Internal, "key: k: write conflict"), false},
		{"embedded sentinel", status.Error(codes.Unknown, "key: write conflict: invalid value"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			normalized := normalizeDynamoWriteDispatchError(tc.err)
			require.Equal(t, tc.want, errors.Is(normalized, store.ErrWriteConflict))
			switch {
			case tc.want:
				require.True(t, shouldPreserveTransactWriteAttempt(normalized))
			case tc.err != nil:
				require.Same(t, tc.err, normalized)
			default:
				require.NoError(t, normalized)
			}
		})
	}
}
