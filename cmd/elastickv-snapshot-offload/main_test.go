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
	"github.com/cockroachdb/errors"
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
		// The publish above used --group-id 2, so this is the group this
		// data dir is for.
		"--expect-group", "2",
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
	require.ErrorContains(t, err, "s3 KMS key id requires aws:kms encryption")
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

// TestClassifyErrorKeepsMissingSnapshotAsADataError pins the CLI exit
// contract across the ErrNoPersistedSnapshot split.
//
// Automation distinguishes "missing or invalid snapshot data" (2) from
// "bad invocation" (1). Giving the missing-local-snapshot case its own
// sentinel — so the scheduler could stop treating a vanished remote
// object as a routine skip — must not silently move it to exit 1.
func TestClassifyErrorKeepsMissingSnapshotAsADataError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want int
	}{
		{
			name: "data dir has no persisted snapshot",
			err:  errors.Wrap(snapshotoffload.ErrNoPersistedSnapshot, "publish"),
			want: exitDataErr,
		},
		{
			name: "object absent from the store",
			err:  errors.Wrap(snapshotoffload.ErrObjectNotFound, "publish"),
			want: exitDataErr,
		},
		{
			name: "integrity failure",
			err:  errors.Wrap(snapshotoffload.ErrIntegrity, "restore"),
			want: exitDataErr,
		},
		{
			name: "invalid invocation",
			err:  errors.Wrap(snapshotoffload.ErrInvalidOptions, "publish"),
			want: exitUserErr,
		},
		{
			// The snapshot is intact; the operator named the wrong
			// manifest for this data dir. Automation keys off the
			// difference, so this must not be reported as bad data.
			name: "wrong group's manifest",
			err:  errors.Wrap(snapshotoffload.ErrRestoreGroupMismatch, "restore"),
			want: exitUserErr,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, classifyError(tc.err))
		})
	}
}

// TestSnapshotOffloadCLIRestoreRefusesAnotherGroupsManifest is the regression
// test for a silent mis-restore.
//
// Nothing downstream of the restore records which group the data belongs to:
// the prepared artifacts carry index, term, peers and payload hash, and startup
// derives the group from the directory layout. So an operator repeating this
// command across groups and pasting the wrong manifest key produced a
// valid-looking directory that startup then loaded under a different group's
// routing identity, with no error at any point. --expect-group is the only
// place that mistake is detectable.
func TestSnapshotOffloadCLIRestoreRefusesAnotherGroupsManifest(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	root := t.TempDir()
	objectRoot := filepath.Join(root, "objects")
	sourceDataDir := seedCLISnapshot(t, root, []byte("EKVTHLC1group-2-payload"), 60, 9)

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
	require.Equal(t, uint64(2), manifest.GroupID)

	restoreDataDir := filepath.Join(root, "restored-as-group-1")
	code, err = run(ctx, []string{
		commandRestore,
		"--store", storeLocal,
		"--local-root", objectRoot,
		"--manifest-key", manifest.ManifestKey,
		"--data-dir", restoreDataDir,
		"--peers", "n1=127.0.0.1:12001",
		// The operator means group 1, but pasted group 2's manifest key.
		"--expect-group", "1",
	}, io.Discard, logger)
	require.Error(t, err)
	require.ErrorIs(t, err, snapshotoffload.ErrRestoreGroupMismatch)
	// exitUserErr, not exitDataErr: the snapshot data is fine, the
	// invocation named the wrong manifest. Automation distinguishes the two.
	require.Equal(t, exitUserErr, code)

	// Nothing may be left behind: the check runs before the download and
	// before the destination is created, so a mistaken key costs nothing.
	_, statErr := os.Stat(restoreDataDir)
	require.True(t, os.IsNotExist(statErr),
		"a refused restore must not create the destination")
}

// --expect-group is required, because defaulting it would silently accept
// whatever group the manifest happens to name -- which is the behaviour the
// flag exists to remove.
func TestSnapshotOffloadCLIRestoreRequiresAnExpectedGroup(t *testing.T) {
	t.Parallel()

	_, err := parseRestoreFlags([]string{
		"--store", storeLocal,
		"--local-root", "/tmp/objects",
		"--manifest-key", "k",
		"--data-dir", "/tmp/restored",
		"--peers", "n1=127.0.0.1:12001",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "--expect-group is required")
}

// Group 0 is the dedicated TSO group, so it has to be accepted as an explicit
// value rather than treated as "unset".
func TestSnapshotOffloadCLIRestoreAcceptsGroupZero(t *testing.T) {
	t.Parallel()

	cfg, err := parseRestoreFlags([]string{
		"--store", storeLocal,
		"--local-root", "/tmp/objects",
		"--manifest-key", "k",
		"--data-dir", "/tmp/restored",
		"--peers", "n1=127.0.0.1:12001",
		"--expect-group", "0",
	})
	require.NoError(t, err)
	require.Equal(t, uint64(0), cfg.expectGroupID)
}

func TestSnapshotOffloadCLIRestoreRejectsANonNumericGroup(t *testing.T) {
	t.Parallel()

	_, err := parseRestoreFlags([]string{
		"--store", storeLocal,
		"--local-root", "/tmp/objects",
		"--manifest-key", "k",
		"--data-dir", "/tmp/restored",
		"--peers", "n1=127.0.0.1:12001",
		"--expect-group", "one",
	})
	require.Error(t, err)
}
