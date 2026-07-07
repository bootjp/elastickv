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
		LoadNewestAvailable([]walpb.Snapshot{{Index: 42, Term: 7}})
	require.NoError(t, err)
	require.Equal(t, uint64(42), snap.Metadata.Index)
	require.Equal(t, uint64(7), snap.Metadata.Term)
	require.Equal(t, []uint64{1, 2}, snap.Metadata.ConfState.Voters)
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

func TestPrepareExternalSnapshotRestoreRefusesExistingDestination(t *testing.T) {
	root := t.TempDir()
	input := filepath.Join(root, "encoded.fsm")
	require.NoError(t, os.WriteFile(input, []byte("payload"), 0o600))
	dataDir := filepath.Join(root, "raft")
	require.NoError(t, os.Mkdir(dataDir, 0o755))

	_, err := PrepareExternalSnapshotRestore(ExternalSnapshotRestoreOptions{
		InputFSMPath: input,
		DataDir:      dataDir,
		Index:        1,
		Term:         1,
		Peers:        []Peer{{NodeID: 1, ID: "n1", Address: "127.0.0.1:12001"}},
	})
	require.ErrorIs(t, err, ErrExternalSnapshotRestoreExists)
}

func TestPrepareExternalSnapshotRestoreRequiresPeerNodeIDs(t *testing.T) {
	_, err := PrepareExternalSnapshotRestore(ExternalSnapshotRestoreOptions{
		InputFSMPath: "encoded.fsm",
		DataDir:      "raft",
		Index:        1,
		Term:         1,
		Peers:        []Peer{{ID: "n1", Address: "127.0.0.1:12001"}},
	})
	require.ErrorIs(t, err, ErrExternalSnapshotRestoreInvalid)
}

func TestPrepareExternalSnapshotRestoreRejectsCopiedPayloadSHA256Mismatch(t *testing.T) {
	root := t.TempDir()
	input := filepath.Join(root, "encoded.fsm")
	require.NoError(t, os.WriteFile(input, []byte("payload"), 0o600))
	dataDir := filepath.Join(root, "raft")

	_, err := PrepareExternalSnapshotRestore(ExternalSnapshotRestoreOptions{
		InputFSMPath:          input,
		DataDir:               dataDir,
		Index:                 1,
		Term:                  1,
		Peers:                 []Peer{{NodeID: 1, ID: "n1", Address: "127.0.0.1:12001"}},
		ExpectedPayloadSHA256: "0000000000000000000000000000000000000000000000000000000000000000",
	})
	require.ErrorIs(t, err, ErrExternalSnapshotRestoreSHA256)
	_, statErr := os.Stat(dataDir)
	require.True(t, os.IsNotExist(statErr))
}
