package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/backup"
	"github.com/stretchr/testify/require"
)

func TestRunPrepareRestoreSuccess(t *testing.T) {
	dir := t.TempDir()
	input := writeHeaderOnlyFSM(t, filepath.Join(dir, "encoded.fsm"))
	writeEncodeInfo(t, input, "cluster-a", true, true)
	dataDir := filepath.Join(dir, "raft")

	code, err := run([]string{
		"--input", input,
		"--data-dir", dataDir,
		"--index", "5",
		"--term", "2",
		"--peers", "n1=127.0.0.1:12001",
		"--target-cluster-id", "cluster-a",
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	require.Equal(t, exitSuccess, code)
	require.FileExists(t, filepath.Join(dataDir, "fsm-snap", "0000000000000005.fsm"))
	require.FileExists(t, filepath.Join(dataDir, "snap", "0000000000000002-0000000000000005.snap"))
	fsmBytes, err := os.ReadFile(filepath.Join(dataDir, "fsm-snap", "0000000000000005.fsm"))
	require.NoError(t, err)
	require.Equal(t, []byte("EKVTHLC1"), fsmBytes[:8])
	require.Equal(t, uint64(1), binary.BigEndian.Uint64(fsmBytes[8:16]))
}

func TestRunPrepareRestoreRejectsClusterMismatch(t *testing.T) {
	dir := t.TempDir()
	input := writeHeaderOnlyFSM(t, filepath.Join(dir, "encoded.fsm"))
	writeEncodeInfo(t, input, "cluster-a", true, true)

	code, err := run([]string{
		"--input", input,
		"--data-dir", filepath.Join(dir, "raft"),
		"--index", "5",
		"--peers", "n1=127.0.0.1:12001",
		"--target-cluster-id", "cluster-b",
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.ErrorIs(t, err, errRestoreClusterIDMismatch)
	require.Equal(t, exitDataErr, code)
}

func TestRunPrepareRestoreRequiresSelfTestByDefault(t *testing.T) {
	dir := t.TempDir()
	input := writeHeaderOnlyFSM(t, filepath.Join(dir, "encoded.fsm"))
	writeEncodeInfo(t, input, "cluster-a", false, false)

	code, err := run([]string{
		"--input", input,
		"--data-dir", filepath.Join(dir, "raft"),
		"--index", "5",
		"--peers", "n1=127.0.0.1:12001",
		"--target-cluster-id", "cluster-a",
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.ErrorIs(t, err, errRestoreSelfTestMissing)
	require.Equal(t, exitDataErr, code)
}

func TestRunPrepareRestoreRequiresSidecarSHA256(t *testing.T) {
	dir := t.TempDir()
	input := writeHeaderOnlyFSM(t, filepath.Join(dir, "encoded.fsm"))
	writeEncodeInfoWithSHA(t, input, "cluster-a", true, true, "")

	code, err := run([]string{
		"--input", input,
		"--data-dir", filepath.Join(dir, "raft"),
		"--index", "5",
		"--peers", "n1=127.0.0.1:12001",
		"--target-cluster-id", "cluster-a",
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.ErrorIs(t, err, errRestoreSHA256Missing)
	require.Equal(t, exitDataErr, code)
}

func TestHLCCeilingMsAfterLastCommitTS(t *testing.T) {
	require.Equal(t, uint64(0), hlcCeilingMsAfterLastCommitTS(0))
	require.Equal(t, uint64(124), hlcCeilingMsAfterLastCommitTS((123<<hlcLogicalBits)|42))
}

func writeHeaderOnlyFSM(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Create(path)
	require.NoError(t, err)
	_, err = f.WriteString(backup.PebbleSnapshotMagic)
	require.NoError(t, err)
	require.NoError(t, binary.Write(f, binary.LittleEndian, uint64(1234)))
	require.NoError(t, f.Close())
	return path
}

func writeEncodeInfo(t *testing.T, input, clusterID string, selfTestRan, selfTestMatched bool) {
	t.Helper()
	body, err := os.ReadFile(input)
	require.NoError(t, err)
	sum := sha256.Sum256(body)
	writeEncodeInfoWithSHA(t, input, clusterID, selfTestRan, selfTestMatched, hex.EncodeToString(sum[:]))
}

func writeEncodeInfoWithSHA(t *testing.T, input, clusterID string, selfTestRan, selfTestMatched bool, shaHex string) {
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
	info.SelfTest = backup.EncodeInfoSelfTest{Ran: selfTestRan, Matched: selfTestMatched}

	f, err := os.Create(backup.EncodeInfoSidecarPath(input))
	require.NoError(t, err)
	require.NoError(t, backup.WriteEncodeInfo(f, info))
	require.NoError(t, f.Close())
}
