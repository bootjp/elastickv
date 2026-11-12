package splitmigration

import (
	"context"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMigrationReadinessGateMissingFailsClosed(t *testing.T) {
	t.Parallel()
	var missing ReadinessGate
	require.ErrorIs(t, missing.Check(t.Context()), ErrReadinessGateMissing)

	interceptor := NewCoverageUnaryGate(nil)
	_, err := interceptor(t.Context(), nil, &grpc.UnaryServerInfo{FullMethod: pb.Internal_ImportRangeVersions_FullMethodName},
		func(context.Context, any) (any, error) {
			t.Fatal("a server without a readiness gate must not run migration mutations")
			return nil, nil
		})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}
