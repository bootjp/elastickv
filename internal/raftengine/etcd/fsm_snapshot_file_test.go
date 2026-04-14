package etcd

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// --- Token encode/decode tests ---

func TestTokenRoundTrip(t *testing.T) {
	cases := []struct {
		name   string
		index  uint64
		crc32c uint32
	}{
		{"zero", 0, 0},
		{"maxUint64", math.MaxUint64, math.MaxUint32},
		{"typical", 12345678, 0xDEADBEEF},
		{"indexOnly", 100, 0},
		{"crcOnly", 0, 42},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			encoded := encodeSnapshotToken(tc.index, tc.crc32c)
			require.Len(t, encoded, snapshotTokenSize)

			tok, err := decodeSnapshotToken(encoded)
			require.NoError(t, err)
			require.Equal(t, tc.index, tok.Index)
			require.Equal(t, tc.crc32c, tok.CRC32C)
		})
	}
}

func TestTokenMagicRejection(t *testing.T) {
	// isSnapshotToken returns false for non-EKVT prefixes.
	require.False(t, isSnapshotToken(nil))
	require.False(t, isSnapshotToken([]byte{}))
	require.False(t, isSnapshotToken([]byte{0, 1, 2, 3}))
	require.False(t, isSnapshotToken([]byte("EKVR"))) // different magic
	require.False(t, isSnapshotToken([]byte("EKVM")))
	require.False(t, isSnapshotToken([]byte("EKVW")))

	// Pebble magic prefix should not be recognised as a token.
	require.False(t, isSnapshotToken([]byte("EKVPBBL1")))

	// A valid EKVT token should be recognised.
	token := encodeSnapshotToken(1, 0)
	require.True(t, isSnapshotToken(token))

	// decodeSnapshotToken rejects wrong lengths (0–16).
	for l := 0; l < snapshotTokenSize; l++ {
		_, err := decodeSnapshotToken(make([]byte, l))
		require.ErrorIs(t, err, ErrFSMSnapshotTokenInvalid,
			"expected ErrFSMSnapshotTokenInvalid for length %d", l)
	}

	// decodeSnapshotToken rejects wrong magic.
	bad := encodeSnapshotToken(1, 0)
	bad[0] = 'X'
	_, err := decodeSnapshotToken(bad)
	require.ErrorIs(t, err, ErrFSMSnapshotTokenInvalid)
}

// --- CRC writer tests ---

func TestCRCWriterMatchesStdlib(t *testing.T) {
	data := []byte("hello world, this is a test payload")
	expected := crc32.Checksum(data, crc32cTable)

	// Full write.
	var buf bytes.Buffer
	w := newCRC32CWriter(&buf)
	_, err := w.Write(data)
	require.NoError(t, err)
	require.Equal(t, expected, w.Sum32())

	// Incremental writes must accumulate correctly.
	var buf2 bytes.Buffer
	w2 := newCRC32CWriter(&buf2)
	for _, b := range data {
		_, err := w2.Write([]byte{b})
		require.NoError(t, err)
	}
	require.Equal(t, expected, w2.Sum32())

	// Both should produce the same checksum as the stdlib.
	require.Equal(t, w.Sum32(), w2.Sum32())
}

// --- Helper to write a valid .fsm file for testing ---

func writeFSMFileForTest(t *testing.T, dir string, index uint64, payload []byte) (uint32, string) {
	t.Helper()
	path := fsmSnapPath(dir, index)
	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()

	h := crc32.New(crc32cTable)
	w := io.MultiWriter(f, h)
	_, err = w.Write(payload)
	require.NoError(t, err)

	checksum := h.Sum32()
	err = binary.Write(f, binary.BigEndian, checksum)
	require.NoError(t, err)
	return checksum, path
}

// dummyFSM is a state machine that records the bytes it restores.
type dummyFSM struct {
	restored []byte
}

func (d *dummyFSM) Apply(_ []byte) any { return nil }
func (d *dummyFSM) Restore(r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	d.restored = data
	return nil
}
func (d *dummyFSM) Snapshot() (Snapshot, error) {
	return &testSnapshot{data: d.restored}, nil
}

// --- openAndRestoreFSMSnapshot tests ---

func TestOpenAndRestoreFSMSnapshotGoodFile(t *testing.T) {
	dir := t.TempDir()
	payload := []byte("some FSM state data here")
	crc, _ := writeFSMFileForTest(t, dir, 42, payload)

	fsm := &dummyFSM{}
	err := openAndRestoreFSMSnapshot(fsm, fsmSnapPath(dir, 42), crc)
	require.NoError(t, err)
	require.Equal(t, payload, fsm.restored)
}

func TestOpenAndRestoreFSMSnapshotBadFooter(t *testing.T) {
	dir := t.TempDir()
	payload := []byte("good payload bytes here")
	_, path := writeFSMFileForTest(t, dir, 10, payload)

	// Corrupt the last byte of the footer.
	info, err := os.Stat(path)
	require.NoError(t, err)
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	require.NoError(t, err)
	_, err = f.Seek(info.Size()-1, io.SeekStart)
	require.NoError(t, err)
	_, err = f.Write([]byte{0xFF})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Read the now-corrupted footer to pass as tokenCRC so the pre-check passes.
	f2, err := os.Open(path)
	require.NoError(t, err)
	_, err = f2.Seek(info.Size()-4, io.SeekStart)
	require.NoError(t, err)
	var storedCRC uint32
	require.NoError(t, binary.Read(f2, binary.BigEndian, &storedCRC))
	require.NoError(t, f2.Close())

	fsm := &dummyFSM{}
	err = openAndRestoreFSMSnapshot(fsm, path, storedCRC)
	// The computed CRC won't match the footer bytes because the footer itself
	// was corrupted — ErrFSMSnapshotFileCRC expected.
	require.ErrorIs(t, err, ErrFSMSnapshotFileCRC)
}

func TestOpenAndRestoreFSMSnapshotTokenMismatch(t *testing.T) {
	dir := t.TempDir()
	payload := []byte("good payload")
	crc, _ := writeFSMFileForTest(t, dir, 7, payload)

	fsm := &dummyFSM{}
	wrongCRC := crc ^ 0xFFFFFFFF // flip all bits
	err := openAndRestoreFSMSnapshot(fsm, fsmSnapPath(dir, 7), wrongCRC)
	require.ErrorIs(t, err, ErrFSMSnapshotTokenCRC)
	// FSM should NOT have been restored.
	require.Nil(t, fsm.restored)
}

func TestOpenAndRestoreFSMSnapshotTooSmall(t *testing.T) {
	dir := t.TempDir()
	// Write a file with fewer than 5 bytes.
	path := fsmSnapPath(dir, 99)
	require.NoError(t, os.WriteFile(path, []byte{0x01, 0x02, 0x03, 0x04}, 0o600))

	fsm := &dummyFSM{}
	err := openAndRestoreFSMSnapshot(fsm, path, 0)
	require.ErrorIs(t, err, ErrFSMSnapshotTooSmall)
}

func TestStripFooterReaderBoundary(t *testing.T) {
	// Verify that io.LimitReader(file, payloadSize) exposes exactly the payload
	// bytes and that the CRC accumulator covers the full payload.
	dir := t.TempDir()
	payload := []byte("exact boundary test")
	crc, path := writeFSMFileForTest(t, dir, 1, payload)

	info, err := os.Stat(path)
	require.NoError(t, err)
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	payloadSize := info.Size() - 4
	h := crc32.New(crc32cTable)
	tee := io.TeeReader(io.LimitReader(f, payloadSize), h)
	got, err := io.ReadAll(tee)
	require.NoError(t, err)
	require.Equal(t, payload, got)
	require.Equal(t, crc, h.Sum32())

	// The next read from f must be the 4-byte footer.
	var footer uint32
	require.NoError(t, binary.Read(f, binary.BigEndian, &footer))
	require.Equal(t, crc, footer)
}

// --- Crash safety and startup cleanup tests ---

func TestCrashAfterTmpBeforeRename(t *testing.T) {
	snapDir := t.TempDir()
	fsmSnapDir := t.TempDir()

	// Simulate a .fsm.tmp orphan left by a crash.
	tmpPath := filepath.Join(fsmSnapDir, "crashtest.fsm.tmp")
	require.NoError(t, os.WriteFile(tmpPath, []byte("partial"), 0o600))

	// cleanupStaleFSMSnaps should remove the orphan; no .fsm file should exist.
	require.NoError(t, cleanupStaleFSMSnaps(snapDir, fsmSnapDir, false))

	// Orphan removed.
	_, err := os.Stat(tmpPath)
	require.True(t, os.IsNotExist(err))

	// No .fsm file was promoted.
	entries, err := os.ReadDir(fsmSnapDir)
	require.NoError(t, err)
	for _, e := range entries {
		require.NotEqual(t, ".fsm", filepath.Ext(e.Name()))
	}
}

func TestCleanupStaleFSMSnapsIndexBased(t *testing.T) {
	snapDir := t.TempDir()
	fsmSnapDir := t.TempDir()

	// Create a .fsm file with index 42 but no corresponding .snap token.
	payload := []byte("valid fsm data here 1234")
	crc, _ := writeFSMFileForTest(t, fsmSnapDir, 42, payload)
	require.NotZero(t, crc)

	// cleanupStaleFSMSnaps should remove it because there's no matching .snap.
	require.NoError(t, cleanupStaleFSMSnaps(snapDir, fsmSnapDir, false))

	_, err := os.Stat(fsmSnapPath(fsmSnapDir, 42))
	require.True(t, os.IsNotExist(err))
}

func TestSnapSavedOnlyAfterRename(t *testing.T) {
	dir := t.TempDir()

	// Create a snapshot that writes to a file; if WriteTo fails, no final .fsm
	// should be committed.
	errSnap := &failingSnapshot{err: io.ErrUnexpectedEOF}
	_, err := writeFSMSnapshotFile(errSnap, dir, 5)
	require.Error(t, err)

	// No .fsm file should exist.
	entries, err2 := os.ReadDir(dir)
	require.NoError(t, err2)
	for _, e := range entries {
		require.NotEqual(t, ".fsm", filepath.Ext(e.Name()),
			"unexpected .fsm file: %s", e.Name())
	}
}

// failingSnapshot is a Snapshot that always fails WriteTo.
type failingSnapshot struct {
	err error
}

func (e *failingSnapshot) WriteTo(_ io.Writer) (int64, error) { return 0, e.err }
func (e *failingSnapshot) Close() error                       { return nil }

func TestPurgeOldSnapshotFilesOrdering(t *testing.T) {
	snapDir := t.TempDir()
	fsmSnapDir := t.TempDir()

	// Create 5 snap+fsm pairs.
	for i := uint64(1); i <= 5; i++ {
		createSnapFile(t, snapDir, i*1000)
		payload := []byte("payload")
		writeFSMFileForTest(t, fsmSnapDir, i*1000, payload)
	}

	require.NoError(t, purgeOldSnapshotFiles(snapDir, fsmSnapDir))

	// After purge, only the newest 3 pairs should remain.
	snapEntries, err := os.ReadDir(snapDir)
	require.NoError(t, err)
	var snaps []string
	for _, e := range snapEntries {
		if filepath.Ext(e.Name()) == ".snap" {
			snaps = append(snaps, e.Name())
		}
	}
	require.Len(t, snaps, 3)

	fsmEntries, err := os.ReadDir(fsmSnapDir)
	require.NoError(t, err)
	var fsms []string
	for _, e := range fsmEntries {
		if filepath.Ext(e.Name()) == ".fsm" {
			fsms = append(fsms, e.Name())
		}
	}
	require.Len(t, fsms, 3)
}

// --- writeFSMSnapshotFile integration ---

func TestWriteFSMSnapshotFileRoundTrip(t *testing.T) {
	dir := t.TempDir()
	payload := []byte("round-trip FSM state data 12345678")
	snap := &testSnapshot{data: payload}

	crc, err := writeFSMSnapshotFile(snap, dir, 100)
	require.NoError(t, err)
	require.NotZero(t, crc)

	// The final .fsm file must exist.
	path := fsmSnapPath(dir, 100)
	info, statErr := os.Stat(path)
	require.NoError(t, statErr)
	require.Equal(t, int64(len(payload)+4), info.Size())

	// openAndRestoreFSMSnapshot must recover the original payload.
	fsm := &dummyFSM{}
	require.NoError(t, openAndRestoreFSMSnapshot(fsm, path, crc))
	require.Equal(t, payload, fsm.restored)
}

func TestFsmSnapPath(t *testing.T) {
	path := fsmSnapPath("/data/fsm-snap", 0x1234ABCD)
	require.Equal(t, "/data/fsm-snap/000000001234abcd.fsm", path)
}

func TestParseSnapFileIndex(t *testing.T) {
	require.Equal(t, uint64(0x60), parseSnapFileIndex("0000000000000001-0000000000000060.snap"))
	require.Equal(t, uint64(0), parseSnapFileIndex("badname.snap"))
	require.Equal(t, uint64(0), parseSnapFileIndex("nohyphen.snap"))
}
