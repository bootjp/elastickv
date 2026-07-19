package etcd

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPreparePhysicalSnapshotRestoreAndExportOpaquePayload(t *testing.T) {
	root := t.TempDir()
	payload := append([]byte("EKVTHLC2opaque-header"), []byte("EKVSSTI1opaque-store-payload")...)
	input := filepath.Join(root, "physical.fsm")
	require.NoError(t, os.WriteFile(input, payload, 0o600))
	payloadSum := sha256.Sum256(payload)
	payloadSHA := hex.EncodeToString(payloadSum[:])
	dataDir := filepath.Join(root, "raft")

	result, err := PreparePhysicalSnapshotRestore(PhysicalSnapshotRestoreOptions{
		InputFSMPath:          input,
		DataDir:               dataDir,
		Index:                 42,
		Term:                  7,
		ExpectedPayloadSHA256: payloadSHA,
		Peers: []Peer{
			{NodeID: 1, ID: "n1", Address: "127.0.0.1:12001"},
			{NodeID: 2, ID: "n2", Address: "127.0.0.1:12002"},
		},
	})
	require.NoError(t, err)
	require.Equal(t, int64(len(payload)), result.PayloadBytes)
	require.Equal(t, payloadSHA, result.PayloadSHA256)

	raw, ok, err := OpenNewestFSMSnapshotPayload(dataDir)
	require.NoError(t, err)
	require.True(t, ok)
	gotRaw, err := os.ReadFile(result.FSMPath)
	require.NoError(t, err)
	require.Equal(t, payload, gotRaw[:len(gotRaw)-fsmFooterSize])
	streamed, err := readAllAndClose(raw)
	require.NoError(t, err)
	require.Equal(t, payload, streamed)

	export, ok, err := OpenPersistedSnapshotExport(dataDir)
	require.NoError(t, err)
	require.True(t, ok)
	metadata := export.Metadata()
	require.Equal(t, uint64(42), metadata.Index)
	require.Equal(t, uint64(7), metadata.Term)
	require.Equal(t, []uint64{1, 2}, metadata.ConfState.GetVoters())
	require.Equal(t, int64(len(payload)), metadata.PayloadBytes)
	require.Equal(t, result.CRC32C, metadata.CRC32C)
	metadata.ConfState.Voters[0] = 99
	require.Equal(t, []uint64{1, 2}, export.Metadata().ConfState.GetVoters())

	var out bytes.Buffer
	n, err := export.WriteTo(&out)
	require.NoError(t, err)
	require.Equal(t, int64(len(payload)), n)
	require.Equal(t, payload, out.Bytes())
	_, err = export.WriteTo(&bytes.Buffer{})
	require.ErrorIs(t, err, ErrPersistedSnapshotExportUsed)
	require.NoError(t, export.Close())
	require.NoError(t, export.Close())
}

func TestPersistedSnapshotExportDetectsPayloadCorruptionDuringStream(t *testing.T) {
	dataDir, result := preparePhysicalSnapshotExportFixture(t, []byte("EKVTHLC1payload-that-will-be-corrupted"))
	file, err := os.OpenFile(result.FSMPath, os.O_RDWR, 0)
	require.NoError(t, err)
	var first [1]byte
	_, err = file.ReadAt(first[:], 0)
	require.NoError(t, err)
	first[0] ^= 0xff
	_, err = file.WriteAt(first[:], 0)
	require.NoError(t, err)
	require.NoError(t, file.Close())

	export, ok, err := OpenPersistedSnapshotExport(dataDir)
	require.NoError(t, err)
	require.True(t, ok)
	defer export.Close()
	_, err = export.WriteTo(&bytes.Buffer{})
	require.ErrorIs(t, err, ErrFSMSnapshotFileCRC)
}

func TestPersistedSnapshotExportRejectsTokenFooterMismatchBeforeStream(t *testing.T) {
	dataDir, result := preparePhysicalSnapshotExportFixture(t, []byte("EKVTHLC1payload-with-footer-mismatch"))
	file, err := os.OpenFile(result.FSMPath, os.O_RDWR, 0)
	require.NoError(t, err)
	info, err := file.Stat()
	require.NoError(t, err)
	var last [1]byte
	_, err = file.ReadAt(last[:], info.Size()-1)
	require.NoError(t, err)
	last[0] ^= 0xff
	_, err = file.WriteAt(last[:], info.Size()-1)
	require.NoError(t, err)
	require.NoError(t, file.Close())

	_, ok, err := OpenPersistedSnapshotExport(dataDir)
	require.False(t, ok)
	require.ErrorIs(t, err, ErrFSMSnapshotTokenCRC)
}

func TestOpenPersistedSnapshotExportValidation(t *testing.T) {
	_, ok, err := OpenPersistedSnapshotExport("")
	require.False(t, ok)
	require.ErrorIs(t, err, ErrPersistedSnapshotExportInvalid)

	_, ok, err = OpenPersistedSnapshotExport(t.TempDir())
	require.NoError(t, err)
	require.False(t, ok)
}

func preparePhysicalSnapshotExportFixture(t *testing.T, payload []byte) (string, *ExternalSnapshotRestoreResult) {
	t.Helper()
	root := t.TempDir()
	input := filepath.Join(root, "physical.fsm")
	require.NoError(t, os.WriteFile(input, payload, 0o600))
	dataDir := filepath.Join(root, "raft")
	result, err := PreparePhysicalSnapshotRestore(PhysicalSnapshotRestoreOptions{
		InputFSMPath: input,
		DataDir:      dataDir,
		Index:        42,
		Term:         7,
		Peers:        []Peer{{NodeID: 1, ID: "n1", Address: "127.0.0.1:12001"}},
	})
	require.NoError(t, err)
	return dataDir, result
}

func readAllAndClose(r interface {
	Read([]byte) (int, error)
	Close() error
}) ([]byte, error) {
	defer r.Close()
	var out bytes.Buffer
	_, err := out.ReadFrom(r)
	return out.Bytes(), err
}
