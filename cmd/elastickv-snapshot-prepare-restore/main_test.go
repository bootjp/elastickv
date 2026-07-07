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
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
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

func TestRunPrepareRestoreFailureCases(t *testing.T) {
	testCases := []struct {
		name      string
		writeInfo func(t *testing.T, input string)
		extraArgs []string
		wantErr   error
	}{
		{
			name: "cluster mismatch",
			writeInfo: func(t *testing.T, input string) {
				t.Helper()
				writeEncodeInfo(t, input, "cluster-a", true, true)
			},
			extraArgs: []string{"--target-cluster-id", "cluster-b"},
			wantErr:   errRestoreClusterIDMismatch,
		},
		{
			name: "self test missing",
			writeInfo: func(t *testing.T, input string) {
				t.Helper()
				writeEncodeInfo(t, input, "cluster-a", false, false)
			},
			extraArgs: []string{"--target-cluster-id", "cluster-a"},
			wantErr:   errRestoreSelfTestMissing,
		},
		{
			name: "sidecar sha missing",
			writeInfo: func(t *testing.T, input string) {
				t.Helper()
				writeEncodeInfoWithSHA(t, input, "cluster-a", true, true, "")
			},
			extraArgs: []string{"--target-cluster-id", "cluster-a"},
			wantErr:   errRestoreSHA256Missing,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			input := writeHeaderOnlyFSM(t, filepath.Join(dir, "encoded.fsm"))
			tc.writeInfo(t, input)
			args := []string{
				"--input", input,
				"--data-dir", filepath.Join(dir, "raft"),
				"--index", "5",
				"--peers", "n1=127.0.0.1:12001",
			}
			args = append(args, tc.extraArgs...)

			code, err := run(args, slog.New(slog.NewTextHandler(io.Discard, nil)))
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, exitDataErr, code)
		})
	}
}

func TestRunPrepareRestoreAcceptedFlagCombinations(t *testing.T) {
	testCases := []struct {
		name            string
		clusterID       string
		selfTestRan     bool
		selfTestMatched bool
		extraArgs       []string
	}{
		{
			name:            "fresh cluster",
			clusterID:       "cluster-a",
			selfTestRan:     true,
			selfTestMatched: true,
			extraArgs:       []string{"--fresh-cluster"},
		},
		{
			name:            "missing cluster id allowed",
			selfTestRan:     true,
			selfTestMatched: true,
			extraArgs:       []string{"--allow-missing-cluster-id"},
		},
		{
			name:            "unverified self test allowed",
			clusterID:       "cluster-a",
			selfTestRan:     false,
			selfTestMatched: false,
			extraArgs:       []string{"--target-cluster-id", "cluster-a", "--allow-unverified-self-test=true"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			input := writeHeaderOnlyFSM(t, filepath.Join(dir, "encoded.fsm"))
			writeEncodeInfo(t, input, tc.clusterID, tc.selfTestRan, tc.selfTestMatched)
			dataDir := filepath.Join(dir, "raft")
			args := []string{
				"--input", input,
				"--data-dir", dataDir,
				"--index", "5",
				"--peers", "n1=127.0.0.1:12001",
			}
			args = append(args, tc.extraArgs...)

			code, err := run(args, slog.New(slog.NewTextHandler(io.Discard, nil)))
			require.NoError(t, err)
			require.Equal(t, exitSuccess, code)
			require.FileExists(t, filepath.Join(dataDir, "fsm-snap", "0000000000000005.fsm"))
		})
	}
}

func TestRunPrepareRestoreRejectsFreshClusterWithTargetClusterID(t *testing.T) {
	code, err := run([]string{
		"--input", "encoded.fsm",
		"--data-dir", filepath.Join(t.TempDir(), "raft"),
		"--index", "5",
		"--peers", "n1=127.0.0.1:12001",
		"--fresh-cluster",
		"--target-cluster-id", "cluster-a",
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.ErrorContains(t, err, "mutually exclusive")
	require.Equal(t, exitUserErr, code)
}

func TestParsePeersNormalizesDerivedNodeIDs(t *testing.T) {
	peers, err := parsePeers("n2=127.0.0.1:12002,n1=127.0.0.1:12001")
	require.NoError(t, err)
	require.Len(t, peers, 2)
	byID := make(map[string]etcd.Peer, len(peers))
	for _, peer := range peers {
		byID[peer.ID] = peer
	}
	require.Equal(t, etcd.DeriveNodeID("n1"), byID["n1"].NodeID)
	require.Equal(t, etcd.DeriveNodeID("n2"), byID["n2"].NodeID)
	require.Less(t, peers[0].NodeID, peers[1].NodeID)
}

func TestParsePeersRejectsDuplicateDerivedNodeIDs(t *testing.T) {
	_, err := parsePeers("n1=127.0.0.1:12001,n1=127.0.0.1:12002")
	require.ErrorContains(t, err, "duplicate peer node id")
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
