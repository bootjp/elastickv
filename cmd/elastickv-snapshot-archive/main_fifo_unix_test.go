//go:build unix

package main

import (
	"io"
	"log/slog"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/bootjp/elastickv/internal/backup"
	"github.com/stretchr/testify/require"
)

func TestArchiveCLIRejectsFIFOArchiveInputBeforeOpen(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "dump.tar")
	require.NoError(t, syscall.Mkfifo(input, 0o600))

	code, err := run([]string{
		"unpack",
		"--input", input,
		"--output", filepath.Join(dir, "out"),
		"--compression", "none",
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.ErrorIs(t, err, backup.ErrArchiveNonRegular)
	require.Equal(t, exitDataErr, code)
}
