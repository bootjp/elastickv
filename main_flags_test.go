package main

import (
	"flag"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"testing"

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
func defaultServerFlags() *serverFlags {
	return registerServerFlags(flag.NewFlagSet("defaults", flag.ContinueOnError))
}

func TestRegisterServerFlagsBindsCommandLineNames(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	f := registerServerFlags(fs)
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
	}))
	require.Equal(t, "127.0.0.1:0", f.healthAddress)
	require.Equal(t, "/mnt/ekv", f.filesystem.mount)
	require.True(t, f.leaderBalance.enabled)
	require.Equal(t, "/etc/tso-mode", f.tso.modeFile)
	require.Equal(t, "/var/offload", f.snapshotOffload.localDir)
	require.True(t, f.snapshotOffload.retention.enabled)
	require.True(t, f.adminHTTP.enabled)
	require.Equal(t, "peer-a:8080", f.adminHTTP.keyvizFanoutNodes)
	require.Equal(t, 7, f.backup.maxActivePins)
	require.True(t, *f.autoSplit.Enabled)

	defaults := defaultServerFlags()
	require.Equal(t, defaultTSOBatchSize, defaults.tso.batchSize)
	require.Equal(t, defaultTSOReload, defaults.tso.modeReloadInterval)
	require.Equal(t, uint(defaultFilesystemRootMode), defaults.filesystem.rootMode)
	require.Equal(t, keyvizFanoutDefaultTimeout, defaults.adminHTTP.keyvizFanoutTimeout)
	require.Equal(t, uint64(defaultBackupHeadroom), defaults.backup.snapshotHeadroomEntries)
	require.NoError(t, validateLiveBackupConfig(
		defaults.backup.defaultTTL, defaults.backup.maxTTL, defaults.backup.beginDeadline,
		defaults.backup.snapshotHeadroomEntries, defaults.backup.scanPageSize, defaults.backup.maxActivePins,
	), "registered backup defaults must pass start-up validation")
}

// TestCLIFlagsReadOnlyByMain keeps grouped flag values flowing down from
// run(): a helper that read cliFlags directly would bring back the hidden
// global inputs that serverFlags replaced.
func TestCLIFlagsReadOnlyByMain(t *testing.T) {
	allowed := map[string]bool{
		"main.go":       true,
		"main_flags.go": true,
		// The subprocess half of the shutdown test is the process entry
		// point: it parses the real command line and calls run().
		"main_shutdown_e2e_test.go": true,
	}
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	fset := token.NewFileSet()
	for _, name := range files {
		file, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		require.NoError(t, err)
		ast.Inspect(file, func(node ast.Node) bool {
			ident, ok := node.(*ast.Ident)
			if ok && ident.Name == "cliFlags" && !allowed[name] {
				t.Errorf("%s: cliFlags is read outside main(); pass the area's flags down instead", fset.Position(ident.Pos()))
			}
			return true
		})
	}
}
