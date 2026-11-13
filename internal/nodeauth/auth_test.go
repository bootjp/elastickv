package nodeauth

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/adapter"
	"github.com/stretchr/testify/require"
)

func TestLoadAdminTokenFileRejectsOversize(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "huge")
	if err := os.WriteFile(path, []byte(strings.Repeat("x", AdminTokenMaxBytes+1)), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadAdminTokenFile(path); err == nil || !strings.Contains(err.Error(), "exceeds maximum") {
		t.Fatalf("expected size-cap error, got %v", err)
	}
}

func TestNodeAuthServiceTokenFilesCoverEveryServiceToken(t *testing.T) {
	t.Parallel()
	f := Flags{AdminTokenFile: "a", SplitMigrationTokenFile: "m", S3BlobPeerTokenFile: "p"}
	require.Equal(t, []string{"a", "m", "p"}, f.ServiceTokenFiles())
}

func TestSplitMigrationOpcodesRequireExplicitAuthentication(t *testing.T) {
	for _, env := range []string{adapter.MigrationImportOpcodeEnv, adapter.MigrationPromoteOpcodeEnv, adapter.MigrationCleanupOpcodeEnv} {
		t.Setenv(env, "false")
	}
	for _, env := range []string{adapter.MigrationImportOpcodeEnv, adapter.MigrationPromoteOpcodeEnv, adapter.MigrationCleanupOpcodeEnv} {
		t.Run(env, func(t *testing.T) {
			t.Setenv(env, "true")
			_, err := LoadNodeGRPCAuth("", false, "", "")
			require.ErrorContains(t, err, "migration opcodes require")
			_, err = LoadNodeGRPCAuth("", true, "", "")
			require.NoError(t, err)
		})
	}
}

func TestSplitMigrationTokenMustBeDedicated(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	adminPath, peerPath := filepath.Join(dir, "admin"), filepath.Join(dir, "peer")
	require.NoError(t, os.WriteFile(adminPath, []byte("read-only-admin"), 0o600))
	require.NoError(t, os.WriteFile(peerPath, []byte("blob-peer"), 0o600))
	for _, token := range []string{"read-only-admin", "blob-peer", "migration-secret"} {
		path := filepath.Join(t.TempDir(), "migration")
		require.NoError(t, os.WriteFile(path, []byte(token), 0o600))
		cfg, err := LoadNodeGRPCAuth(adminPath, false, peerPath, path)
		if token != "migration-secret" {
			require.ErrorContains(t, err, "migration token must differ")
			continue
		}
		require.NoError(t, err)
		require.Equal(t, "migration-secret", cfg.MigrationToken)
		_, err = LoadNodeGRPCAuth("", true, "", path)
		require.ErrorContains(t, err, "mutually exclusive")
	}
	cfg, err := LoadNodeGRPCAuth(adminPath, false, peerPath, "")
	require.NoError(t, err)
	require.Empty(t, cfg.MigrationToken, "an Admin token must never implicitly configure migration authority")
}

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
			err := ValidateMigrationPeerSecurity(tc.token, tc.noAuth, tc.enforced, tc.tls)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSplitMigrationLocalReadinessGateRequiresMigrationOpcodes(t *testing.T) {
	nodeAuth := Flags{SplitMigrationPeerAuthEnforced: true}
	t.Setenv(adapter.MigrationImportOpcodeEnv, "")
	t.Setenv(adapter.MigrationPromoteOpcodeEnv, "1")
	t.Setenv(adapter.MigrationCleanupOpcodeEnv, "1")
	err := nodeAuth.SplitMigrationLocalReadinessGate(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, adapter.MigrationImportOpcodeEnv)

	t.Setenv(adapter.MigrationImportOpcodeEnv, "1")
	t.Setenv(adapter.MigrationPromoteOpcodeEnv, "")
	err = nodeAuth.SplitMigrationLocalReadinessGate(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, adapter.MigrationPromoteOpcodeEnv)

	t.Setenv(adapter.MigrationPromoteOpcodeEnv, "1")
	t.Setenv(adapter.MigrationCleanupOpcodeEnv, "")
	err = nodeAuth.SplitMigrationLocalReadinessGate(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, adapter.MigrationCleanupOpcodeEnv)

	t.Setenv(adapter.MigrationCleanupOpcodeEnv, "1")
	require.NoError(t, nodeAuth.SplitMigrationLocalReadinessGate(context.Background()))
}
