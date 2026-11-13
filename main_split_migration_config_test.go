package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/adapter"
	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/stretchr/testify/require"
)

func TestRunRejectsUnsafeMigrationAuthBeforeStartingServices(t *testing.T) {
	t.Cleanup(func() { require.NoError(t, internalutil.ConfigureGRPCTLS("", "", "")) })
	for _, env := range []string{adapter.MigrationImportOpcodeEnv, adapter.MigrationPromoteOpcodeEnv, adapter.MigrationCleanupOpcodeEnv} {
		t.Setenv(env, "false")
	}
	path := filepath.Join(t.TempDir(), "migration-token")
	require.NoError(t, os.WriteFile(path, []byte("migration-secret"), 0o600))
	flags := defaultServerFlags()
	flags.nodeAuth.SplitMigrationTokenFile = path
	require.ErrorContains(t, run(flags), "requires gRPC TLS")
	flags = defaultServerFlags()
	flags.nodeAuth.SplitMigrationPeerAuthEnforced = true
	require.ErrorContains(t, run(flags), "requires --splitMigrationTokenFile")
}
