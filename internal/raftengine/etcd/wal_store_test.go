package etcd

import (
	"testing"

	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

// --- restoreSnapshotState tests ---
//
// These tests cover the WAL startup path where the persisted raftpb.Snapshot is
// loaded from disk and the FSM is restored from its Data field. Two formats are
// supported: the 17-byte EKVT token (Phase 2) and the legacy full-payload
// (Phase 1 / HashiCorp migration).

func TestRestoreSnapshotStateEmptySnapshot(t *testing.T) {
	// An empty (zero-value) snapshot must be a no-op: FSM not touched.
	fsm := &dummyFSM{}
	err := restoreSnapshotState(fsm, raftpb.Snapshot{}, "")
	require.NoError(t, err)
	require.Nil(t, fsm.restored)
}

func TestRestoreSnapshotStateNilData(t *testing.T) {
	// Snapshot with non-zero metadata but empty Data must be a no-op.
	snap := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{Index: 5, Term: 1},
	}
	fsm := &dummyFSM{}
	err := restoreSnapshotState(fsm, snap, "")
	require.NoError(t, err)
	require.Nil(t, fsm.restored)
}

func TestRestoreSnapshotStateNilFSM(t *testing.T) {
	// Non-empty snapshot with a nil FSM must be a no-op (not a panic).
	snap := raftpb.Snapshot{
		Data:     []byte("some payload"),
		Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1},
	}
	err := restoreSnapshotState(nil, snap, "")
	require.NoError(t, err)
}

func TestRestoreSnapshotStateLegacyFormat(t *testing.T) {
	// Legacy format: raw FSM payload embedded directly in snapshot.Data.
	// This path is taken when the Data is not a 17-byte EKVT token.
	payload := []byte("legacy raw fsm state payload data here")
	snap := raftpb.Snapshot{
		Data:     payload,
		Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1},
	}

	fsm := &dummyFSM{}
	err := restoreSnapshotState(fsm, snap, "")
	require.NoError(t, err)
	require.Equal(t, payload, fsm.restored)
}

func TestRestoreSnapshotStateTokenFormat(t *testing.T) {
	// Token format: snapshot.Data is a 17-byte EKVT token referencing an .fsm file.
	// This is the Phase 2 path used after the disk-offload migration.
	dir := t.TempDir()
	payload := []byte("token format fsm state data for wal restore test")
	crc, _ := writeFSMFileForTest(t, dir, 7, payload)

	snap := raftpb.Snapshot{
		Data:     encodeSnapshotToken(7, crc),
		Metadata: raftpb.SnapshotMetadata{Index: 7, Term: 1},
	}

	fsm := &dummyFSM{}
	err := restoreSnapshotState(fsm, snap, dir)
	require.NoError(t, err)
	require.Equal(t, payload, fsm.restored)
}

func TestRestoreSnapshotStateTokenEmptyPayload(t *testing.T) {
	// Token pointing to an .fsm file with an empty payload (only CRC footer).
	// This is valid: crc32c("") == 0.
	dir := t.TempDir()
	crc, _ := writeFSMFileForTest(t, dir, 11, []byte{})

	snap := raftpb.Snapshot{
		Data:     encodeSnapshotToken(11, crc),
		Metadata: raftpb.SnapshotMetadata{Index: 11, Term: 1},
	}

	fsm := &dummyFSM{}
	err := restoreSnapshotState(fsm, snap, dir)
	require.NoError(t, err)
	require.Equal(t, []byte{}, fsm.restored)
}

func TestRestoreSnapshotStateTokenCRCMismatch(t *testing.T) {
	// Token CRC does not match the on-disk footer → ErrFSMSnapshotTokenCRC.
	// The FSM must NOT be modified (fail-fast before Restore).
	dir := t.TempDir()
	payload := []byte("payload for crc mismatch test here 123")
	crc, _ := writeFSMFileForTest(t, dir, 8, payload)
	wrongCRC := crc ^ 0xFFFFFFFF

	snap := raftpb.Snapshot{
		Data:     encodeSnapshotToken(8, wrongCRC),
		Metadata: raftpb.SnapshotMetadata{Index: 8, Term: 1},
	}

	fsm := &dummyFSM{}
	err := restoreSnapshotState(fsm, snap, dir)
	require.ErrorIs(t, err, ErrFSMSnapshotTokenCRC)
	require.Nil(t, fsm.restored, "FSM must not be restored after CRC mismatch")
}

func TestRestoreSnapshotStateTokenFileNotFound(t *testing.T) {
	// Token references an index for which no .fsm file exists on disk.
	// This can happen after a data-dir corruption or incomplete migration.
	dir := t.TempDir()
	token := encodeSnapshotToken(999, 0xDEADBEEF)

	snap := raftpb.Snapshot{
		Data:     token,
		Metadata: raftpb.SnapshotMetadata{Index: 999, Term: 1},
	}

	fsm := &dummyFSM{}
	err := restoreSnapshotState(fsm, snap, dir)
	require.ErrorIs(t, err, ErrFSMSnapshotNotFound)
	require.Nil(t, fsm.restored)
}
