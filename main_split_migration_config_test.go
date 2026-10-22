package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/adapter"
	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/stretchr/testify/require"
)

func TestSplitMigrationPeerSecurityConfiguration(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		token    string
		noAuth   bool
		enforced bool
		tls      bool
		wantErr  string
	}{
		{name: "ordinary plaintext cluster"},
		{name: "TLS send-only", token: "secret", tls: true},
		{name: "TLS enforced", token: "secret", tls: true, enforced: true},
		{name: "plaintext send-only", token: "secret", wantErr: "requires gRPC TLS"},
		{name: "plaintext enforced", token: "secret", enforced: true, wantErr: "requires gRPC TLS"},
		{name: "enforced without token", enforced: true, wantErr: "requires --splitMigrationTokenFile"},
		{name: "explicit isolated no-auth", enforced: true, noAuth: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := validateMigrationPeerSecurity(tc.token, tc.noAuth, tc.enforced, tc.tls)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRunRejectsUnsafeMigrationAuthBeforeStartingServices(t *testing.T) {
	paths := []*string{adminTokenFile, s3BlobPeerTokenFile, splitMigrationTokenFile, grpcTLSCAFile, grpcTLSCertFile, grpcTLSKeyFile}
	for _, path := range paths {
		old := *path
		*path = ""
		t.Cleanup(func() { *path = old })
	}
	oldNoAuth, oldEnforced := *adminInsecureNoAuth, *splitMigrationPeerAuthEnforced
	*adminInsecureNoAuth, *splitMigrationPeerAuthEnforced = false, false
	t.Cleanup(func() {
		*adminInsecureNoAuth, *splitMigrationPeerAuthEnforced = oldNoAuth, oldEnforced
		require.NoError(t, internalutil.ConfigureGRPCTLS("", "", ""))
	})
	for _, env := range []string{adapter.MigrationImportOpcodeEnv, adapter.MigrationPromoteOpcodeEnv, adapter.MigrationCleanupOpcodeEnv} {
		t.Setenv(env, "false")
	}
	path := filepath.Join(t.TempDir(), "migration-token")
	require.NoError(t, os.WriteFile(path, []byte("migration-secret"), 0o600))
	*splitMigrationTokenFile = path
	require.ErrorContains(t, run(), "requires gRPC TLS")
	*splitMigrationTokenFile, *splitMigrationPeerAuthEnforced = "", true
	require.ErrorContains(t, run(), "requires --splitMigrationTokenFile")
}
