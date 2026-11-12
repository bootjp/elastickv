package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

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
