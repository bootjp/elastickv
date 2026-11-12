package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMigrationReadinessGateMissingFailsClosed(t *testing.T) {
	t.Parallel()
	var missing migrationReadinessGate
	require.ErrorIs(t, missing.check(t.Context()), errMigrationReadinessGateMissing)

	interceptor := newMigrationCoverageUnaryGate(nil)
	_, err := interceptor(t.Context(), nil, &grpc.UnaryServerInfo{FullMethod: pb.Internal_ImportRangeVersions_FullMethodName},
		func(context.Context, any) (any, error) {
			t.Fatal("a server without a readiness gate must not run migration mutations")
			return nil, nil
		})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestValidateBootstrapStartupTokenRejectsServiceTokenReuse(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	admin := filepath.Join(dir, "admin")
	require.NoError(t, os.WriteFile(admin, []byte("admin-secret"), 0o600))
	nodeAuth := nodeAuthFlags{adminTokenFile: admin}

	require.ErrorContains(t, validateBootstrapStartupToken("admin-secret", nodeAuth.serviceTokenFiles()),
		"startup token must differ")
	require.NoError(t, validateBootstrapStartupToken("startup-secret", nodeAuth.serviceTokenFiles()))
	require.ErrorContains(t, validateBootstrapStartupToken("", nil), "dedicated startup token")
}

func TestNodeAuthServiceTokenFilesCoverEveryServiceToken(t *testing.T) {
	t.Parallel()
	f := nodeAuthFlags{adminTokenFile: "a", splitMigrationTokenFile: "m", s3BlobPeerTokenFile: "p"}
	require.Equal(t, []string{"a", "m", "p"}, f.serviceTokenFiles())
}
