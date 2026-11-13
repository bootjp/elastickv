package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/internal/nodeauth"
	"github.com/stretchr/testify/require"
)

func TestValidateBootstrapStartupTokenRejectsServiceTokenReuse(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	admin := filepath.Join(dir, "admin")
	require.NoError(t, os.WriteFile(admin, []byte("admin-secret"), 0o600))
	nodeAuth := nodeauth.Flags{AdminTokenFile: admin}

	require.ErrorContains(t, clusterbootstrap.ValidateBootstrapStartupToken("admin-secret", nodeAuth.ServiceTokenFiles()),
		"startup token must differ")
	require.NoError(t, clusterbootstrap.ValidateBootstrapStartupToken("startup-secret", nodeAuth.ServiceTokenFiles()))
	require.ErrorContains(t, clusterbootstrap.ValidateBootstrapStartupToken("", nil), "dedicated startup token")
}
