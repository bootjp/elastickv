package etcd

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	etcdsnap "go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func TestPrepareExternalSnapshotRestoreSeedsRuntimeFiles(t *testing.T) {
	root := t.TempDir()
	input := filepath.Join(root, "encoded.fsm")
	payload := []byte("EKVPBBL1external snapshot payload")
	require.NoError(t, os.WriteFile(input, payload, 0o600))
	payloadSum := sha256.Sum256(payload)
	payloadSHA := hex.EncodeToString(payloadSum[:])

	dataDir := filepath.Join(root, "raft")
	res, err := PrepareExternalSnapshotRestore(ExternalSnapshotRestoreOptions{
		InputFSMPath:          input,
		DataDir:               dataDir,
		Index:                 42,
		Term:                  7,
		SnapshotCeilingMs:     123,
		ExpectedPayloadSHA256: payloadSHA,
		Peers: []Peer{
			{NodeID: 1, ID: "n1", Address: "127.0.0.1:12001"},
			{NodeID: 2, ID: "n2", Address: "127.0.0.1:12002"},
		},
	})
	require.NoError(t, err)
	require.Equal(t, dataDir, res.DataDir)
	require.Equal(t, uint64(42), parseSnapFileIndex(filepath.Base(res.SnapPath)))
	require.Equal(t, int64(len(payload)), res.PayloadBytes)
	require.Equal(t, payloadSHA, res.PayloadSHA256)
	require.NoError(t, verifyFSMSnapshotFile(res.FSMPath, res.CRC32C))

	fsmBytes, err := os.ReadFile(res.FSMPath)
	require.NoError(t, err)
	require.Equal(t, externalRestoreSnapshotMagic[:], fsmBytes[:externalRestoreSnapshotMagicLen])
	require.Equal(t, uint64(123), binary.BigEndian.Uint64(fsmBytes[externalRestoreSnapshotMagicLen:externalRestoreSnapshotHeaderLen]))
	require.Equal(t, payload, fsmBytes[externalRestoreSnapshotHeaderLen:len(fsmBytes)-fsmFooterSize])
	require.Equal(t, res.CRC32C, binary.BigEndian.Uint32(fsmBytes[len(fsmBytes)-fsmFooterSize:]))

	snap, err := etcdsnap.New(zap.NewNop(), filepath.Join(dataDir, snapDirName)).
		LoadNewestAvailable([]*walpb.Snapshot{{Index: proto.Uint64(42), Term: proto.Uint64(7)}})
	require.NoError(t, err)
	require.Equal(t, uint64(42), snap.GetMetadata().GetIndex())
	require.Equal(t, uint64(7), snap.GetMetadata().GetTerm())
	require.Equal(t, []uint64{1, 2}, snap.GetMetadata().GetConfState().GetVoters())
	tok, err := decodeSnapshotToken(snap.Data)
	require.NoError(t, err)
	require.Equal(t, uint64(42), tok.Index)
	require.Equal(t, res.CRC32C, tok.CRC32C)

	peers, ok, err := LoadPersistedPeers(dataDir)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []Peer{
		{NodeID: 1, ID: "n1", Address: "127.0.0.1:12001", Suffrage: SuffrageVoter},
		{NodeID: 2, ID: "n2", Address: "127.0.0.1:12002", Suffrage: SuffrageVoter},
	}, peers)
}

func TestPrepareExternalSnapshotRestoreFailures(t *testing.T) {
	testCases := []struct {
		name              string
		mutate            func(t *testing.T, dataDir string, opts *ExternalSnapshotRestoreOptions)
		wantErr           error
		wantDataDirAbsent bool
	}{
		{
			name: "existing destination",
			mutate: func(t *testing.T, dataDir string, _ *ExternalSnapshotRestoreOptions) {
				t.Helper()
				require.NoError(t, os.Mkdir(dataDir, 0o755))
			},
			wantErr: ErrExternalSnapshotRestoreExists,
		},
		{
			name: "existing temporary destination",
			mutate: func(t *testing.T, dataDir string, _ *ExternalSnapshotRestoreOptions) {
				t.Helper()
				require.NoError(t, os.Mkdir(dataDir+".restore-prep", 0o755))
			},
			wantErr: ErrExternalSnapshotRestoreExists,
		},
		{
			name: "zero peer node id",
			mutate: func(_ *testing.T, _ string, opts *ExternalSnapshotRestoreOptions) {
				opts.Peers = []Peer{{ID: "n1", Address: "127.0.0.1:12001"}}
			},
			wantErr: ErrExternalSnapshotRestoreInvalid,
		},
		{
			name: "duplicate peer node id",
			mutate: func(_ *testing.T, _ string, opts *ExternalSnapshotRestoreOptions) {
				opts.Peers = []Peer{
					{NodeID: 1, ID: "n1", Address: "127.0.0.1:12001"},
					{NodeID: 1, ID: "n2", Address: "127.0.0.1:12002"},
				}
			},
			wantErr: ErrExternalSnapshotRestoreInvalid,
		},
		{
			name: "duplicate explicit peer id",
			mutate: func(_ *testing.T, _ string, opts *ExternalSnapshotRestoreOptions) {
				opts.Peers = []Peer{
					{NodeID: 1, ID: "n1", Address: "127.0.0.1:12001"},
					{NodeID: 2, ID: "n1", Address: "127.0.0.1:12002"},
				}
			},
			wantErr: ErrExternalSnapshotRestoreInvalid,
		},
		{
			name: "duplicate normalized peer id",
			mutate: func(_ *testing.T, _ string, opts *ExternalSnapshotRestoreOptions) {
				opts.Peers = []Peer{
					{NodeID: 1, Address: "127.0.0.1:12001"},
					{NodeID: 2, Address: "127.0.0.1:12001"},
				}
			},
			wantErr: ErrExternalSnapshotRestoreInvalid,
		},
		{
			name: "invalid peer suffrage",
			mutate: func(_ *testing.T, _ string, opts *ExternalSnapshotRestoreOptions) {
				opts.Peers[0].Suffrage = "observer"
			},
			wantErr: ErrExternalSnapshotRestoreInvalid,
		},
		{
			name: "learner only membership",
			mutate: func(_ *testing.T, _ string, opts *ExternalSnapshotRestoreOptions) {
				opts.Peers[0].Suffrage = SuffrageLearner
			},
			wantErr: ErrExternalSnapshotRestoreInvalid,
		},
		{
			name: "copied payload sha mismatch",
			mutate: func(_ *testing.T, _ string, opts *ExternalSnapshotRestoreOptions) {
				opts.ExpectedPayloadSHA256 = "0000000000000000000000000000000000000000000000000000000000000000"
			},
			wantErr:           ErrExternalSnapshotRestoreSHA256,
			wantDataDirAbsent: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			input := filepath.Join(root, "encoded.fsm")
			require.NoError(t, os.WriteFile(input, []byte("payload"), 0o600))
			dataDir := filepath.Join(root, "raft")
			opts := ExternalSnapshotRestoreOptions{
				InputFSMPath: input,
				DataDir:      dataDir,
				Index:        1,
				Term:         1,
				Peers:        []Peer{{NodeID: 1, ID: "n1", Address: "127.0.0.1:12001"}},
			}
			tc.mutate(t, dataDir, &opts)

			_, err := PrepareExternalSnapshotRestore(opts)
			require.ErrorIs(t, err, tc.wantErr)
			if tc.wantDataDirAbsent {
				_, statErr := os.Stat(dataDir)
				require.True(t, os.IsNotExist(statErr))
			}
		})
	}
}

func TestPrepareExternalSnapshotRestoreCreatesTempRootAtomically(t *testing.T) {
	root := t.TempDir()
	dest := filepath.Join(root, "raft")
	gotDest, tempDir, err := prepareExternalSnapshotRestoreDest(dest)
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(tempDir) })
	require.Equal(t, dest, gotDest)
	require.DirExists(t, tempDir)

	_, _, err = prepareExternalSnapshotRestoreDest(dest)
	require.ErrorIs(t, err, ErrExternalSnapshotRestoreExists)
}

func TestFinalizeMigrationDirRefusesExistingDestination(t *testing.T) {
	root := t.TempDir()
	tempDir := filepath.Join(root, "temp")
	destDir := filepath.Join(root, "dest")
	require.NoError(t, os.Mkdir(tempDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(tempDir, "marker"), []byte("temp"), 0o600))
	require.NoError(t, os.Mkdir(destDir, 0o755))

	err := finalizeMigrationDir(tempDir, destDir)
	require.ErrorIs(t, err, errMigrationDestinationExists)
	require.DirExists(t, destDir)
	require.NoFileExists(t, filepath.Join(destDir, "marker"))
	_, statErr := os.Stat(tempDir)
	require.True(t, os.IsNotExist(statErr))
}
