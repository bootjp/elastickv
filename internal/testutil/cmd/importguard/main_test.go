package main

import (
	"os"
	"os/exec"
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
	root := t.TempDir()
	require.Error(t, checkTree(filepath.Join(root, "missing")))
	writeSource(t, root, "main.go", "package app\nimport (\n")
	require.ErrorContains(t, checkTree(root), "parse production imports")
}

func TestForbiddenImport(t *testing.T) {
	t.Parallel()
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

func TestCheckTreeInspectsProductionLoadTool(t *testing.T) {
	t.Parallel()
	for _, dir := range []string{"internal/loadtest", "cmd/loadtest"} {
		t.Run(dir, func(t *testing.T) {
			root := t.TempDir()
			writeSource(t, root, dir+"/main.go", "package loadtest\nimport _ \"testing\"\n")
			require.ErrorContains(t, checkTree(root), "production import of test support")
		})
	}
	root := t.TempDir()
	writeSource(t, root, "internal/loadtest/main.go", "package loadtest\nimport _ \"github.com/bootjp/elastickv/proto\"\n")
	writeSource(t, root, "cmd/loadtest/main.go", "package main\nimport _ \"github.com/bootjp/elastickv/internal/loadtest\"\n")
	require.NoError(t, checkTree(root))
}

func TestProductionLoadImportsRemainExact(t *testing.T) {
	t.Parallel()
	require.False(t, forbiddenImport(modulePath+"/internal/loadtest"))
	require.False(t, forbiddenImport(modulePath+"/cmd/loadtest"))
	require.True(t, forbiddenImport(modulePath+"/internal/loadtest/helpers/newtest"))
	require.True(t, forbiddenImport("example.com/loadtest"))
}

func TestShellProductionLoadGuard(t *testing.T) {
	t.Parallel()
	script, err := filepath.Abs(filepath.Join("..", "..", "..", "..", "scripts", "check-production-imports.sh"))
	require.NoError(t, err)
	for _, tc := range []struct {
		name, deps string
		allowed    bool
	}{
		{"production load tool", modulePath + "/internal/loadtest\n" + modulePath + "/cmd/loadtest", true},
		{"real test support", modulePath + "/kv/kvtest", false},
		{"nested support", modulePath + "/internal/loadtest/helpers/newtest", false},
		{"foreign package", "example.com/loadtest", false},
		{"testing package", "testing", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bin := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(bin, "go"), []byte("#!/bin/sh\nif [ \"$1\" = list ]; then echo \"$GUARD_TEST_DEPS\"; fi\n"), 0700))
			cmd := exec.Command("bash", script)
			cmd.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "GUARD_TEST_DEPS="+tc.deps)
			output, err := cmd.CombinedOutput()
			if tc.allowed {
				require.NoError(t, err, "%s", output)
			} else {
				require.Error(t, err)
				require.Contains(t, string(output), "Production dependency on test support")
			}
		})
	}
}
