package etcd

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sort"
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

// --- openAndReadWAL / WAL auto-repair tests ---
//
// These tests cover the OOM-SIGKILL → partial-trailing-record scenario:
// the kernel kills the process mid-WAL-write, leaving the last
// preallocated WAL segment with a torn trailing record. On restart,
// wal.ReadAll returns io.ErrUnexpectedEOF. openAndReadWAL should invoke
// wal.Repair to truncate the partial record and retry once. CRC
// mismatches (real corruption, not torn writes) must propagate.

// seedWAL bootstraps a fresh raft WAL with a few proposals, closes it
// cleanly, and returns the data dir.
func seedWAL(t *testing.T, proposals [][]byte) string {
	t.Helper()
	dir := t.TempDir()
	fsm := &testStateMachine{}
	engine, err := Open(context.Background(), OpenConfig{
		NodeID:       1,
		LocalID:      "n1",
		LocalAddress: "127.0.0.1:0",
		DataDir:      dir,
		Bootstrap:    true,
		StateMachine: fsm,
	})
	require.NoError(t, err)
	for _, p := range proposals {
		_, err := engine.Propose(context.Background(), p)
		require.NoError(t, err)
	}
	require.NoError(t, engine.Close())
	return dir
}

// truncateInsideLastRecord scans walPath for the end of written record
// data (the first 8-byte aligned block of zeros in the preallocated tail)
// and truncates a few bytes before that boundary so the truncation lands
// inside framing rather than in the zero padding.
func truncateInsideLastRecord(t *testing.T, walPath string) {
	t.Helper()
	data, err := os.ReadFile(walPath)
	require.NoError(t, err)
	end := len(data)
	// Walk backwards 8 bytes at a time, skipping zeros, until we hit a
	// block that isn't all zeros — that's where real record framing ends.
	zeros := make([]byte, 8)
	for end >= 8 && bytes.Equal(data[end-8:end], zeros) {
		end -= 8
	}
	require.Greater(t, end, 16, "WAL has no non-zero content; seedWAL likely didn't propose anything")
	// Lop off the final 5 bytes of real framing — enough to corrupt the
	// trailing record's length prefix or payload so wal.ReadAll surfaces
	// io.ErrUnexpectedEOF instead of a clean EOF.
	require.NoError(t, os.Truncate(walPath, int64(end-5)))
}

// lastWALFile returns the path of the lexicographically-last .wal in dir.
// etcd WAL filenames are seq-index padded, so lexicographic order matches
// sequence order.
func lastWALFile(t *testing.T, walDir string) string {
	t.Helper()
	entries, err := os.ReadDir(walDir)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if filepath.Ext(e.Name()) == walFileExt {
			names = append(names, e.Name())
		}
	}
	require.NotEmpty(t, names, "no .wal files in %s", walDir)
	sort.Strings(names)
	return filepath.Join(walDir, names[len(names)-1])
}

func TestLoadWalStateRepairsTruncatedTail(t *testing.T) {
	// Simulates the 2026-04-24 incident: OOM-SIGKILL mid-WAL-write left
	// the last segment with a torn trailing record. Before this fix the
	// process could not restart; now openAndReadWAL invokes wal.Repair
	// to truncate the partial record and continues.
	dir := seedWAL(t, [][]byte{[]byte("one"), []byte("two"), []byte("three")})

	// Chop bytes off the tail to simulate a mid-record SIGKILL. etcd
	// preallocates 64MiB per WAL with zero padding, so we must find the
	// actual end of written records and truncate *inside* framing;
	// truncating in the zero-padded region leaves valid records intact
	// and the decoder stops cleanly at the zero length header (no
	// ErrUnexpectedEOF → repair wouldn't trigger, test would pass for
	// the wrong reason).
	walPath := lastWALFile(t, filepath.Join(dir, walDirName))
	truncateInsideLastRecord(t, walPath)

	// Re-open: loadWalState → openAndReadWAL → repair → succeed.
	fsm := &testStateMachine{}
	engine, err := Open(context.Background(), OpenConfig{
		NodeID:       1,
		LocalID:      "n1",
		LocalAddress: "127.0.0.1:0",
		DataDir:      dir,
		StateMachine: fsm,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })

	// Entries committed before the torn write must survive.
	require.GreaterOrEqual(t, len(fsm.Applied()), 1,
		"repair should preserve entries committed before the torn write")
}

func TestLoadWalStateUnrepairableCRCMismatchReturnsError(t *testing.T) {
	// wal.Repair only fixes io.ErrUnexpectedEOF (torn trailing record).
	// A flipped byte inside a persisted record surfaces as a CRC
	// mismatch, which is genuine corruption — repair cannot help and
	// the error must propagate rather than silently masking it.
	dir := seedWAL(t, [][]byte{[]byte("one"), []byte("two"), []byte("three")})

	walPath := lastWALFile(t, filepath.Join(dir, walDirName))
	f, err := os.OpenFile(walPath, os.O_RDWR, 0)
	require.NoError(t, err)
	// Flip a byte ~200 bytes in — past the file header but inside a
	// real record. etcd WAL preallocates zeroes, so we need to land
	// in content not padding.
	var one [1]byte
	_, err = f.ReadAt(one[:], 200)
	require.NoError(t, err)
	one[0] ^= 0xff
	_, err = f.WriteAt(one[:], 200)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	fsm := &testStateMachine{}
	_, err = Open(context.Background(), OpenConfig{
		NodeID:       1,
		LocalID:      "n1",
		LocalAddress: "127.0.0.1:0",
		DataDir:      dir,
		StateMachine: fsm,
	})
	require.Error(t, err, "CRC mismatch is not repairable; error must surface")
}

func TestOpenAndReadWALSucceedsWithoutRepair(t *testing.T) {
	// Happy-path sanity check: a pristine WAL opens and ReadAll returns
	// the expected entries, no repair invoked.
	dir := seedWAL(t, [][]byte{[]byte("one"), []byte("two")})

	fsm := &testStateMachine{}
	engine, err := Open(context.Background(), OpenConfig{
		NodeID:       1,
		LocalID:      "n1",
		LocalAddress: "127.0.0.1:0",
		DataDir:      dir,
		StateMachine: fsm,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })

	require.Equal(t,
		[][]byte{[]byte("one"), []byte("two")},
		fsm.Applied(),
	)
}
