package testutil_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProductionImportGuard(t *testing.T) {
	dir := t.TempDir()
	goStub := filepath.Join(dir, "go")
	require.NoError(t, os.WriteFile(goStub, []byte("#!/bin/sh\nprintf '%s\\n' \"$WP8_DEPS\"\nexit \"${WP8_EXIT:-0}\"\n"), 0o600))
	require.NoError(t, os.Chmod(goStub, 0o500))
	script, err := filepath.Abs(filepath.Join("..", "..", "scripts", "check-production-imports.sh"))
	require.NoError(t, err)
	for _, tc := range []struct {
		deps string
		fail bool
	}{
		{"fmt\nnet\ngithub.com/bootjp/elastickv/kv\ninternal/synctest\ngithub.com/cockroachdb/pebble/v2/internal/blobtest", false},
		{"testing", true},
		{"testing/quick", true},
		{"github.com/stretchr/testify/require", true},
		{"github.com/bootjp/elastickv/internal/testutil", true},
		{"github.com/bootjp/elastickv/internal/testutil/grpctest", true},
		{"github.com/bootjp/elastickv/internal/testutil/cmd/importguard", true},
		{"github.com/bootjp/elastickv/internal/raftengine/raftenginetest", true},
		{"github.com/bootjp/elastickv/internal/encryption/encryptiontest", true},
		{"github.com/bootjp/elastickv/store/storetest", true},
		{"github.com/bootjp/elastickv/kv/kvtest", true},
		{"github.com/bootjp/elastickv/internal/filesystem/fstest", true},
		{"net/http/httptest", true},
		{"example.com/newtest/nested", true},
		{"github.com/cockroachdb/pebble/v2/internal/blobtest/nested", true},
		{"github.com/bootjp/elastickv/kv/latest_commit_ts", false},
	} {
		t.Run(strings.ReplaceAll(tc.deps, "/", "_"), func(t *testing.T) {
			cmd := exec.CommandContext(context.Background(), "bash", script)
			cmd.Env = append(os.Environ(), "PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"), "WP8_DEPS="+tc.deps)
			output, err := cmd.CombinedOutput()
			if tc.fail {
				require.Error(t, err)
				require.Contains(t, string(output), tc.deps)
			} else {
				require.NoError(t, err, "%s", output)
			}
		})
	}
	t.Run("dependency enumeration error is fatal", func(t *testing.T) {
		cmd := exec.CommandContext(context.Background(), "bash", script)
		cmd.Env = append(os.Environ(), "PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"), "WP8_EXIT=23")
		output, err := cmd.CombinedOutput()
		require.Error(t, err, "%s", output)
		var exit *exec.ExitError
		require.ErrorAs(t, err, &exit)
		require.Equal(t, 23, exit.ExitCode())
	})
}
