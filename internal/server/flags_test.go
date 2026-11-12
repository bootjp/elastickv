package server

import (
	"flag"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/internal/tsowiring"
	"github.com/stretchr/testify/require"
)

// defaultFlags returns an area's flag values as registered, without touching
// the process command line.
func defaultFlags[T any, P interface {
	*T
	register(*flag.FlagSet)
}]() T {
	var v T
	P(&v).register(flag.NewFlagSet("defaults", flag.ContinueOnError))
	return v
}

// defaultServerFlags returns every grouped flag at its registered default.
func defaultServerFlags() *Flags {
	return RegisterFlags(flag.NewFlagSet("defaults", flag.ContinueOnError))
}

func TestRegisterServerFlagsBindsCommandLineNames(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	f := RegisterFlags(fs)
	require.NoError(t, fs.Parse([]string{
		"-healthAddress", "127.0.0.1:0",
		"-filesystemMount", "/mnt/ekv",
		"-leaderBalance",
		"-tsoModeFile", "/etc/tso-mode",
		"-snapshotOffloadLocalDir", "/var/offload",
		"-snapshotOffloadGCEnabled",
		"-adminEnabled",
		"-keyvizFanoutNodes", "peer-a:8080",
		"-backupMaxActivePins", "7",
		"-autoSplit",
		"-adminTokenFile", "/etc/ekv/admin-token",
		"-splitMigrationPeerAuthEnforced",
		"-grpcTLSCertFile", "/etc/ekv/tls.crt",
	}))
	require.Equal(t, "127.0.0.1:0", f.healthAddress)
	require.Equal(t, "/mnt/ekv", f.filesystem.mount)
	require.True(t, f.leaderBalance.enabled)
	require.Equal(t, "/etc/tso-mode", f.tso.ModeFile)
	require.Equal(t, "/var/offload", f.snapshotOffload.LocalDir)
	require.True(t, f.snapshotOffload.Retention.Enabled)
	require.True(t, f.adminHTTP.enabled)
	require.Equal(t, "peer-a:8080", f.adminHTTP.keyvizFanoutNodes)
	require.Equal(t, 7, f.backup.maxActivePins)
	require.True(t, *f.autoSplit.Enabled)
	require.Equal(t, "/etc/ekv/admin-token", f.nodeAuth.AdminTokenFile)
	require.True(t, f.nodeAuth.SplitMigrationPeerAuthEnforced)
	require.Equal(t, "/etc/ekv/tls.crt", f.nodeAuth.GRPCTLSCertFile)

	defaults := defaultServerFlags()
	require.Equal(t, tsowiring.DefaultBatchSize, defaults.tso.BatchSize)
	require.Equal(t, tsowiring.DefaultReloadInterval, defaults.tso.ModeReloadInterval)
	require.Equal(t, uint(defaultFilesystemRootMode), defaults.filesystem.rootMode)
	require.Equal(t, keyvizFanoutDefaultTimeout, defaults.adminHTTP.keyvizFanoutTimeout)
	require.Equal(t, uint64(defaultBackupHeadroom), defaults.backup.snapshotHeadroomEntries)
	require.NoError(t, validateLiveBackupConfig(
		defaults.backup.defaultTTL, defaults.backup.maxTTL, defaults.backup.beginDeadline,
		defaults.backup.snapshotHeadroomEntries, defaults.backup.scanPageSize, defaults.backup.maxActivePins,
	), "registered backup defaults must pass start-up validation")
}

// TestServerFlagsNotBoundToCommandLine keeps the server package free of the
// process command line: the root binary registers the flags and hands them
// to Run, and only the subprocess half of TestShutdownProcess binds them in
// a test.
func TestServerFlagsNotBoundToCommandLine(t *testing.T) {
	t.Parallel()
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	fset := token.NewFileSet()
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		require.NoError(t, err)
		ast.Inspect(file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.SelectorExpr:
				if pkg, ok := n.X.(*ast.Ident); ok && pkg.Name == "flag" && n.Sel.Name == "CommandLine" {
					t.Errorf("%s: the server package must not use flag.CommandLine", fset.Position(n.Pos()))
				}
			case *ast.Ident:
				if n.Name == "cliFlags" {
					t.Errorf("%s: cliFlags belongs to the root binary", fset.Position(n.Pos()))
				}
			}
			return true
		})
	}
}
