package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/internal/snapshotoffload"
	"github.com/stretchr/testify/require"
)

func TestSnapshotOffloadCLIPublishAndRestoreLocal(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	payload := []byte("EKVTHLC1cli-physical-snapshot-payload")
	sourceDataDir := seedCLISnapshot(t, root, payload, 60, 9)
	objectRoot := filepath.Join(root, "objects")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	var stdout bytes.Buffer
	code, err := run(ctx, []string{
		commandPublish,
		"--store", storeLocal,
		"--local-root", objectRoot,
		"--data-dir", sourceDataDir,
		"--prefix", "cluster-cli",
		"--group-id", "2",
		"--source-cluster", "cluster-cli",
		"--binary-version", "test-version",
	}, &stdout, logger)
	require.NoError(t, err)
	require.Equal(t, exitSuccess, code)

	manifest, err := snapshotoffload.DecodeManifest(stdout.Bytes())
	require.NoError(t, err)
	require.Equal(t, uint64(60), manifest.SnapshotIndex)
	require.Equal(t, int64(len(payload)), manifest.Payload.Bytes)

	restoreDataDir := filepath.Join(root, "restored")
	code, err = run(ctx, []string{
		commandRestore,
		"--store", storeLocal,
		"--local-root", objectRoot,
		"--manifest-key", manifest.ManifestKey,
		"--data-dir", restoreDataDir,
		"--peers", "n2=127.0.0.1:12002",
	}, io.Discard, logger)
	require.NoError(t, err)
	require.Equal(t, exitSuccess, code)

	export, ok, err := etcd.OpenPersistedSnapshotExport(restoreDataDir)
	require.NoError(t, err)
	require.True(t, ok)
	defer func() { require.NoError(t, export.Close()) }()
	require.Equal(t, []uint64{etcd.DeriveNodeID("n2")}, export.Metadata().ConfState.GetVoters())
}

func TestSnapshotOffloadCLIRequiresLocalRoot(t *testing.T) {
	code, err := run(context.Background(), []string{
		commandPublish,
		"--store", storeLocal,
		"--data-dir", "data",
		"--group-id", "1",
	}, io.Discard, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.ErrorContains(t, err, "--local-root is required")
	require.Equal(t, exitUserErr, code)
}

func TestSnapshotOffloadCLIRejectsPositionalArgs(t *testing.T) {
	_, err := parsePublishFlags([]string{
		"--store", storeLocal,
		"--local-root", "objects",
		"--data-dir", "data",
		"--group-id", "1",
		"extra",
	})
	require.ErrorContains(t, err, "unexpected positional argument")

	_, err = parseRestoreFlags([]string{
		"--store", storeLocal,
		"--local-root", "objects",
		"--manifest-key", "manifest.json",
		"--data-dir", "data",
		"--peers", "n1=127.0.0.1:12001",
		"extra",
	})
	require.ErrorContains(t, err, "unexpected positional argument")
}

func TestSnapshotOffloadCLIS3KMSRequiresAWSKMS(t *testing.T) {
	_, err := parsePublishFlags([]string{
		"--store", storeS3,
		"--s3-bucket", "bucket",
		"--s3-sse", s3SSEAWSKMS,
		"--s3-kms-key-id", "key-id",
		"--data-dir", "data",
		"--group-id", "1",
	})
	require.NoError(t, err)

	_, err = parsePublishFlags([]string{
		"--store", storeS3,
		"--s3-bucket", "bucket",
		"--s3-sse", "AES256",
		"--s3-kms-key-id", "key-id",
		"--data-dir", "data",
		"--group-id", "1",
	})
	require.ErrorContains(t, err, "--s3-kms-key-id requires --s3-sse=aws:kms")
}

func seedCLISnapshot(t *testing.T, root string, payload []byte, index uint64, term uint64) string {
	t.Helper()
	input := filepath.Join(root, "source.fsm")
	require.NoError(t, os.WriteFile(input, payload, 0o600))
	dataDir := filepath.Join(root, "source-raft")
	_, err := etcd.PreparePhysicalSnapshotRestore(etcd.PhysicalSnapshotRestoreOptions{
		InputFSMPath: input,
		DataDir:      dataDir,
		Index:        index,
		Term:         term,
		Peers: []etcd.Peer{
			{NodeID: 1, ID: "n1", Address: "127.0.0.1:12001"},
		},
	})
	require.NoError(t, err)
	return dataDir
}
