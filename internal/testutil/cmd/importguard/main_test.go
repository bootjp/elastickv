package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func writeSource(t *testing.T, root, name, source string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(name))
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	require.NoError(t, os.WriteFile(path, []byte(source), 0o600))
}

func TestCheckTreeRejectsInactivePlatformAndTagImports(t *testing.T) {
	for _, name := range []string{"main_windows.go", "main_darwin.go", "main_linux.go", "tagged.go", "orphan/unused.go"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			writeSource(t, root, name, "//go:build ignored_platform_tag\n\npackage app\nimport _ \"testing\"\n")
			err := checkTree(root)
			require.ErrorContains(t, err, "production import of test support")
			require.ErrorContains(t, err, name)
		})
	}
}

func TestCheckTreeSkipsOnlyTestSourcesAndToolingDirectories(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{
		"main_test.go", "internal/testutil/helper.go", "kv/kvtest/helper.go", "store/storetest/helper.go",
		"internal/raftengine/raftenginetest/helper.go", "internal/encryption/encryptiontest/helper.go",
		"internal/filesystem/fstest/helper.go", "testdata/invalid.go", ".cache/invalid.go", "vendor/module/invalid.go",
	} {
		writeSource(t, root, name, "not valid Go")
	}
	writeSource(t, root, "kv/latest_commit_ts.go", "package kv\nimport _ \"fmt\"\n")
	require.NoError(t, checkTree(root))
	writeSource(t, root, "kv/latest_commit_ts.go", "package kv\nimport _ \"testing\"\n")
	require.ErrorContains(t, checkTree(root), "latest_commit_ts.go")
}

func TestCheckTreeRejectsUnreadableOrMalformedSource(t *testing.T) {
	root := t.TempDir()
	require.Error(t, checkTree(filepath.Join(root, "missing")))
	writeSource(t, root, "main.go", "package app\nimport (\n")
	require.ErrorContains(t, checkTree(root), "parse production imports")
}

func TestForbiddenImport(t *testing.T) {
	for _, path := range []string{
		"testing", "testing/quick", "github.com/stretchr/testify/require",
		"github.com/bootjp/elastickv/internal/testutil", "github.com/bootjp/elastickv/internal/testutil/grpctest",
		"github.com/bootjp/elastickv/internal/testutil/cmd/importguard",
		"github.com/bootjp/elastickv/kv/kvtest", "github.com/bootjp/elastickv/store/storetest",
		"github.com/bootjp/elastickv/internal/encryption/encryptiontest", "github.com/bootjp/elastickv/internal/raftengine/raftenginetest",
		"net/http/httptest", "example.com/newtest/nested", "github.com/cockroachdb/pebble/v2/internal/blobtest/nested",
	} {
		require.True(t, forbiddenImport(path), "%s", path)
	}
	for _, path := range []string{"fmt", "github.com/bootjp/elastickv/kv", "internal/synctest", "github.com/cockroachdb/pebble/v2/internal/blobtest"} {
		require.False(t, forbiddenImport(path), "%s", path)
	}
}
