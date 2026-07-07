//go:build unix

package main

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/backup"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/stretchr/testify/require"
)

func TestRunPrepareRestoreRejectsFIFOInputBeforeOpen(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "encoded.fsm")
	require.NoError(t, syscall.Mkfifo(input, 0o600))
	writeEncodeInfoWithRecordedSHA(t, input, "cluster-a", "unused")

	code, err := run([]string{
		"--input", input,
		"--data-dir", filepath.Join(dir, "raft"),
		"--index", "5",
		"--peers", "n1=127.0.0.1:12001",
		"--target-cluster-id", "cluster-a",
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.ErrorIs(t, err, etcd.ErrExternalSnapshotRestoreInvalid)
	require.Equal(t, exitUserErr, code)
}

func TestRunPrepareRestoreRejectsFIFOEncodeInfoBeforeOpen(t *testing.T) {
	dir := t.TempDir()
	input := writeHeaderOnlyFSM(t, filepath.Join(dir, "encoded.fsm"))
	require.NoError(t, syscall.Mkfifo(backup.EncodeInfoSidecarPath(input), 0o600))

	code, err := run([]string{
		"--input", input,
		"--data-dir", filepath.Join(dir, "raft"),
		"--index", "5",
		"--peers", "n1=127.0.0.1:12001",
		"--target-cluster-id", "cluster-a",
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.ErrorIs(t, err, etcd.ErrExternalSnapshotRestoreInvalid)
	require.Equal(t, exitUserErr, code)
}

func TestRunPrepareRestoreRejectsSymlinkInputBeforeOpen(t *testing.T) {
	dir := t.TempDir()
	target := writeHeaderOnlyFSM(t, filepath.Join(dir, "target.fsm"))
	input := filepath.Join(dir, "encoded.fsm")
	require.NoError(t, os.Symlink(target, input))
	writeEncodeInfo(t, input, "cluster-a", true, true)

	code, err := run([]string{
		"--input", input,
		"--data-dir", filepath.Join(dir, "raft"),
		"--index", "5",
		"--peers", "n1=127.0.0.1:12001",
		"--target-cluster-id", "cluster-a",
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.ErrorIs(t, err, etcd.ErrExternalSnapshotRestoreInvalid)
	require.Equal(t, exitUserErr, code)
}

func writeEncodeInfoWithRecordedSHA(t *testing.T, input, clusterID, shaHex string) {
	t.Helper()
	info := backup.NewEncodeInfo(time.Unix(0, 0))
	info.EncoderVersion = "test"
	info.InputRoot = "dump"
	info.OutputFSMPath = input
	info.OutputFSMSHA256 = shaHex
	info.LastCommitTS = 1234
	info.ManifestLastCommitTS = 1234
	info.ManifestClusterID = clusterID
	info.AdaptersEnabled = []string{"redis"}
	info.SelfTest = backup.EncodeInfoSelfTest{Ran: true, Matched: true}

	f, err := os.Create(backup.EncodeInfoSidecarPath(input))
	require.NoError(t, err)
	require.NoError(t, backup.WriteEncodeInfo(f, info))
	require.NoError(t, f.Close())
}
