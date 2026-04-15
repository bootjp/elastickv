package etcd

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

const (
	fsmSnapDirName       = "fsm-snap"
	snapshotTokenSize    = 17 // 4 (magic) + 1 (version) + 8 (index) + 4 (crc32c)
	snapshotTokenVersion = byte(0x01)

	// fsmFooterSize is the size of the CRC32C footer appended to each .fsm file.
	fsmFooterSize = 4

	// fsmMinFileSize is the minimum valid .fsm size: 0 bytes payload + 4 bytes CRC footer.
	// A state machine with empty state writes only the 4-byte CRC footer, which is valid.
	fsmMinFileSize = fsmFooterSize

	// snapshotTokenMagicLen is the number of magic bytes at the start of a token.
	snapshotTokenMagicLen = 4

	// fsmRestoreReadAhead is the bufio read-ahead size used when restoring .fsm files.
	fsmRestoreReadAhead = 1 << 20 // 1 MiB

	// fsmWriteBufSize is the bufio.Writer buffer size used when writing .fsm files.
	fsmWriteBufSize = 1 << 20 // 1 MiB

	// fsmMaxInMemPayload is the maximum payload size that readFSMSnapshotPayload
	// will materialise into memory. Larger snapshots must use the streaming path
	// (openFSMSnapshotPayloadReader) to avoid OOM. 1 GiB is chosen as a generous
	// upper bound; real FSM payloads for this workload are typically much smaller.
	fsmMaxInMemPayload = int64(1 << 30) // 1 GiB
)

var (
	snapshotTokenMagic = [snapshotTokenMagicLen]byte{'E', 'K', 'V', 'T'}
	crc32cTable        = crc32.MakeTable(crc32.Castagnoli)
)

var (
	// ErrFSMSnapshotFileCRC is returned when the on-disk CRC32C footer does not
	// match the computed checksum — the .fsm file is corrupt.
	ErrFSMSnapshotFileCRC = errors.New("fsm snapshot: file CRC32C mismatch (file corrupt)")

	// ErrFSMSnapshotTokenCRC is returned when the footer and the token CRC
	// disagree before restore — the metadata is suspect; do not auto-rewrite.
	ErrFSMSnapshotTokenCRC = errors.New("fsm snapshot: token CRC32C mismatch (metadata corrupt)")

	// ErrFSMSnapshotNotFound is returned when the expected .fsm file is absent.
	ErrFSMSnapshotNotFound = errors.New("fsm snapshot: file not found")

	// ErrFSMSnapshotTooSmall is returned when the file is shorter than the
	// minimum valid .fsm size (at least 1 byte payload + 4 bytes CRC footer).
	ErrFSMSnapshotTooSmall = errors.New("fsm snapshot: file too small to contain footer")

	// ErrFSMSnapshotTokenInvalid is returned when the token bytes cannot be
	// decoded (wrong length or magic prefix).
	ErrFSMSnapshotTokenInvalid = errors.New("fsm snapshot: token format invalid")

	// ErrFSMSnapshotTooLarge is returned when the payload exceeds fsmMaxInMemPayload.
	// Callers should switch to the streaming path (openFSMSnapshotPayloadReader).
	ErrFSMSnapshotTooLarge = errors.New("fsm snapshot: payload exceeds maximum in-memory size limit")
)

// snapshotToken holds the decoded fields of a 17-byte raftpb.Snapshot.Data token.
type snapshotToken struct {
	Index  uint64
	CRC32C uint32
}

// isSnapshotToken returns true if data is exactly snapshotTokenSize bytes and
// begins with the EKVT magic prefix. The length check prevents false positives
// from legacy FSM payloads that happen to start with the same magic bytes:
// a false positive in dispatchSnapshot would cause decodeSnapshotToken to fail
// and block the send instead of falling back to the legacy path.
func isSnapshotToken(data []byte) bool {
	if len(data) != snapshotTokenSize {
		return false
	}
	return [snapshotTokenMagicLen]byte(data[:snapshotTokenMagicLen]) == snapshotTokenMagic
}

// encodeSnapshotToken encodes index and crc32c into the 17-byte token format:
//
//	[magic:4][version:1][index:8][crc32c:4]
func encodeSnapshotToken(index uint64, crc32c uint32) []byte {
	token := make([]byte, snapshotTokenSize)
	copy(token[:snapshotTokenMagicLen], snapshotTokenMagic[:])
	token[snapshotTokenMagicLen] = snapshotTokenVersion
	binary.BigEndian.PutUint64(token[5:13], index)
	binary.BigEndian.PutUint32(token[13:17], crc32c)
	return token
}

// decodeSnapshotToken parses the 17-byte token. Returns ErrFSMSnapshotTokenInvalid
// if the length or magic is wrong.
func decodeSnapshotToken(data []byte) (snapshotToken, error) {
	if len(data) != snapshotTokenSize {
		return snapshotToken{}, errors.Wrapf(ErrFSMSnapshotTokenInvalid,
			"expected %d bytes, got %d", snapshotTokenSize, len(data))
	}
	if [snapshotTokenMagicLen]byte(data[:snapshotTokenMagicLen]) != snapshotTokenMagic {
		return snapshotToken{}, errors.Wrapf(ErrFSMSnapshotTokenInvalid,
			"invalid magic: %x", data[:snapshotTokenMagicLen])
	}
	if data[snapshotTokenMagicLen] != snapshotTokenVersion {
		return snapshotToken{}, errors.Wrapf(ErrFSMSnapshotTokenInvalid,
			"unknown version: %d", data[snapshotTokenMagicLen])
	}
	return snapshotToken{
		Index:  binary.BigEndian.Uint64(data[5:13]),
		CRC32C: binary.BigEndian.Uint32(data[13:17]),
	}, nil
}

// fsmSnapPath returns the canonical zero-padded hex path for a .fsm snapshot file.
func fsmSnapPath(fsmSnapDir string, index uint64) string {
	return filepath.Join(fsmSnapDir, fmt.Sprintf("%016x.fsm", index))
}

// parseSnapFileIndex extracts the applied index from an etcd snapshotter filename.
// Snap files are named "{term:016x}-{index:016x}.snap".
// Returns 0 on parse failure.
func parseSnapFileIndex(name string) uint64 {
	base := strings.TrimSuffix(name, ".snap")
	idx := strings.LastIndex(base, "-")
	if idx < 0 {
		return 0
	}
	index, err := strconv.ParseUint(base[idx+1:], 16, 64)
	if err != nil {
		return 0
	}
	return index
}

// crc32CWriter wraps an io.Writer and accumulates a CRC32C checksum over all
// bytes written through it.
type crc32CWriter struct {
	w io.Writer
	h hash.Hash32
}

func newCRC32CWriter(w io.Writer) *crc32CWriter {
	return &crc32CWriter{w: w, h: crc32.New(crc32cTable)}
}

func (c *crc32CWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.h.Write(p[:n])
	if err != nil {
		return n, errors.WithStack(err)
	}
	return n, nil
}

func (c *crc32CWriter) Sum32() uint32 { return c.h.Sum32() }

// writeFSMSnapshotFile writes the FSM snapshot to disk with a CRC32C footer.
//
// Write sequence (crash-safe):
//  1. os.CreateTemp → *.fsm.tmp
//  2. snapshot.WriteTo → stream FSM payload + accumulate CRC
//  3. binary.Write   → append 4-byte CRC footer
//  4. Sync           → flush to durable storage
//  5. os.Rename      → atomic commit (tmp → final)
//  6. syncDir        → persist directory entry
//
// Returns the CRC32C of the payload (excluding the footer) so the caller can
// embed it in the raftpb.Snapshot.Data token.
func writeFSMSnapshotFile(snapshot Snapshot, fsmSnapDir string, index uint64) (uint32, error) {
	if err := os.MkdirAll(fsmSnapDir, defaultDirPerm); err != nil {
		return 0, errors.WithStack(err)
	}

	tmpFile, err := os.CreateTemp(fsmSnapDir, "*.fsm.tmp")
	if err != nil {
		return 0, errors.WithStack(err)
	}
	tmpPath := tmpFile.Name()
	finalPath := fsmSnapPath(fsmSnapDir, index)

	committed := false
	defer func() {
		if !committed {
			_ = tmpFile.Close()
			_ = os.Remove(tmpPath)
		}
	}()

	checksum, err := writeFSMSnapshotPayload(snapshot, tmpFile)
	if err != nil {
		return 0, err
	}
	committed = true

	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return 0, errors.WithStack(err)
	}
	if err := syncDir(fsmSnapDir); err != nil {
		return 0, errors.WithStack(err)
	}

	return checksum, nil
}

// writeFSMSnapshotPayload streams the snapshot into f, appends the CRC32C
// footer, flushes the bufio buffer, and syncs+closes the file. On success the
// file handle is closed and the caller must not use it again.
func writeFSMSnapshotPayload(snapshot Snapshot, f *os.File) (uint32, error) {
	bw := bufio.NewWriterSize(f, fsmWriteBufSize)
	crcWriter := newCRC32CWriter(bw)
	if _, err := snapshot.WriteTo(crcWriter); err != nil {
		return 0, errors.WithStack(err)
	}

	checksum := crcWriter.Sum32()
	if err := binary.Write(bw, binary.BigEndian, checksum); err != nil {
		return 0, errors.WithStack(err)
	}
	if err := bw.Flush(); err != nil {
		return 0, errors.WithStack(err)
	}
	if err := f.Sync(); err != nil {
		return 0, errors.WithStack(err)
	}
	if err := f.Close(); err != nil {
		return 0, errors.WithStack(err)
	}
	return checksum, nil
}

// openAndRestoreFSMSnapshot opens the .fsm file at path, verifies the CRC, and
// restores the FSM state in a single streaming pass.
//
// Token fail-fast: the on-disk footer is compared to tokenCRC BEFORE fsm.Restore
// is called to avoid mutating the FSM with corrupt data.
//
// Returns typed errors (ErrFSMSnapshotFileCRC, ErrFSMSnapshotTokenCRC, etc.) to
// allow the caller to take the correct recovery action.
func openAndRestoreFSMSnapshot(fsm StateMachine, path string, tokenCRC uint32) error {
	info, err := os.Stat(path)
	if err != nil {
		return statFSMFileError(err)
	}
	if info.Size() < fsmMinFileSize {
		return errors.Wrapf(ErrFSMSnapshotTooSmall,
			"file too small: %d bytes (minimum %d)", info.Size(), fsmMinFileSize)
	}

	f, err := os.Open(path)
	if err != nil {
		return errors.WithStack(err)
	}
	defer f.Close()

	// Fail fast: read footer and compare to tokenCRC before calling fsm.Restore,
	// so a corrupt token never causes FSM state mutation.
	footer, err := readFSMFooter(f, info.Size())
	if err != nil {
		return err
	}
	if footer != tokenCRC {
		return errors.Wrapf(ErrFSMSnapshotTokenCRC,
			"path=%s footer=%08x token=%08x", path, footer, tokenCRC)
	}

	// Seek back to start for the combined restore + CRC verification pass.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return errors.WithStack(err)
	}

	computed, err := restoreAndComputeCRC(f, info.Size(), fsm)
	if err != nil {
		return err
	}
	if computed != footer {
		return errors.Wrapf(ErrFSMSnapshotFileCRC,
			"path=%s footer=%08x computed=%08x", path, footer, computed)
	}
	// footer == tokenCRC was verified above; computed == footer is now confirmed.
	return nil
}

// readFSMFooter seeks to the last fsmFooterSize bytes and reads the stored CRC.
func readFSMFooter(f *os.File, fileSize int64) (uint32, error) {
	if _, err := f.Seek(fileSize-fsmFooterSize, io.SeekStart); err != nil {
		return 0, errors.WithStack(err)
	}
	var footer uint32
	if err := binary.Read(f, binary.BigEndian, &footer); err != nil {
		return 0, errors.WithStack(err)
	}
	return footer, nil
}

// restoreAndComputeCRC streams the payload (all bytes except the footer) through
// a TeeReader that simultaneously feeds fsm.Restore and the CRC accumulator.
// Returns the computed CRC32C of the payload.
func restoreAndComputeCRC(f *os.File, fileSize int64, fsm StateMachine) (uint32, error) {
	payloadSize := fileSize - fsmFooterSize
	h := crc32.New(crc32cTable)
	tee := io.TeeReader(io.LimitReader(f, payloadSize), h)
	br := bufio.NewReaderSize(tee, fsmRestoreReadAhead)
	if err := fsm.Restore(br); err != nil {
		return 0, errors.WithStack(err)
	}
	// Drain bytes not consumed by fsm.Restore so the CRC covers the full payload.
	if _, err := io.Copy(io.Discard, br); err != nil {
		return 0, errors.WithStack(err)
	}
	return h.Sum32(), nil
}

// verifyFSMSnapshotFile performs a read-only CRC check without restoring the FSM.
// Used for startup orphan detection. Pass tokenCRC=0 to skip the token comparison.
func verifyFSMSnapshotFile(path string, tokenCRC uint32) error {
	info, err := os.Stat(path)
	if err != nil {
		return statFSMFileError(err)
	}
	if info.Size() < fsmMinFileSize {
		return errors.Wrapf(ErrFSMSnapshotTooSmall,
			"file too small: %d bytes (minimum %d)", info.Size(), fsmMinFileSize)
	}

	f, err := os.Open(path)
	if err != nil {
		return errors.WithStack(err)
	}
	defer f.Close()

	payloadSize := info.Size() - fsmFooterSize
	h := crc32.New(crc32cTable)
	if _, err := io.CopyN(h, f, payloadSize); err != nil {
		return errors.WithStack(err)
	}
	computed := h.Sum32()

	var footer uint32
	if err := binary.Read(f, binary.BigEndian, &footer); err != nil {
		return errors.WithStack(err)
	}
	if computed != footer {
		return errors.Wrapf(ErrFSMSnapshotFileCRC,
			"path=%s footer=%08x computed=%08x", path, footer, computed)
	}
	if tokenCRC != 0 && computed != tokenCRC {
		return errors.Wrapf(ErrFSMSnapshotTokenCRC,
			"path=%s footer=%08x token=%08x", path, footer, tokenCRC)
	}
	return nil
}

// limitedReadCloser pairs an io.Reader (typically a LimitReader over a file)
// with the underlying io.Closer so the file descriptor is released after streaming.
type limitedReadCloser struct {
	io.Reader
	io.Closer
}

// openFSMSnapshotPayloadReader opens the .fsm file at path and returns a
// ReadCloser over exactly the payload bytes (file size minus the 4-byte CRC
// footer). The caller must call Close() to release the file descriptor.
//
// This is the streaming counterpart to readFSMSnapshotPayload: no large buffer
// is allocated, so it is suitable for bridge-mode forwarding of GiB-scale
// snapshots.
func openFSMSnapshotPayloadReader(path string) (io.ReadCloser, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, statFSMFileError(err)
	}
	if info.Size() < fsmMinFileSize {
		return nil, errors.WithStack(ErrFSMSnapshotTooSmall)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	payloadSize := info.Size() - fsmFooterSize
	return &limitedReadCloser{
		Reader: io.LimitReader(f, payloadSize),
		Closer: f,
	}, nil
}

// openFSMSnapshotFile opens the .fsm file at path and returns the *os.File.
// Used by callers that need to control the lock scope separately from the stat
// (e.g. engine.openFSMPayloadLocked).
func openFSMSnapshotFile(path string) (*os.File, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, statFSMFileError(err)
	}
	return f, nil
}

// openFSMPayloadFromFD wraps an already-open .fsm file descriptor in a
// ReadCloser scoped to the payload bytes (file size minus the 4-byte CRC
// footer). The size is obtained via fstat on the open fd (no path lookup).
// On any error the file is closed before returning.
func openFSMPayloadFromFD(f *os.File) (io.ReadCloser, error) {
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, errors.WithStack(err)
	}
	if info.Size() < fsmMinFileSize {
		_ = f.Close()
		return nil, errors.WithStack(ErrFSMSnapshotTooSmall)
	}
	return &limitedReadCloser{
		Reader: io.LimitReader(f, info.Size()-fsmFooterSize),
		Closer: f,
	}, nil
}

// statFSMFileError converts an os.Stat error to the appropriate typed error.
func statFSMFileError(err error) error {
	if os.IsNotExist(err) {
		return errors.WithStack(ErrFSMSnapshotNotFound)
	}
	return errors.WithStack(err)
}

// readFSMSnapshotPayload reads the .fsm file payload (all bytes except the
// 4-byte CRC footer) into memory, verifying the CRC32C footer before returning.
// Used by the Phase 1 bridge mode in Dispatch to reconstruct the full FSM bytes
// for legacy receivers.
func readFSMSnapshotPayload(path string) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if info.Size() < fsmMinFileSize {
		return nil, errors.WithStack(ErrFSMSnapshotTooSmall)
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer f.Close()

	payloadSize := info.Size() - fsmFooterSize
	if payloadSize > fsmMaxInMemPayload {
		return nil, errors.Wrapf(ErrFSMSnapshotTooLarge,
			"payload %d bytes exceeds limit %d; use streaming path instead", payloadSize, fsmMaxInMemPayload)
	}
	data := make([]byte, payloadSize)
	h := crc32.New(crc32cTable)
	tr := io.TeeReader(io.LimitReader(f, payloadSize), h)
	if _, err := io.ReadFull(tr, data); err != nil {
		return nil, errors.WithStack(err)
	}

	var footer uint32
	if err := binary.Read(f, binary.BigEndian, &footer); err != nil {
		return nil, errors.WithStack(err)
	}
	if h.Sum32() != footer {
		return nil, errors.Wrapf(ErrFSMSnapshotFileCRC, "path=%s", path)
	}
	return data, nil
}

// cleanupStaleFSMSnaps removes orphaned .fsm.tmp files and .fsm files that
// have no matching live snap token. Also verifies CRC of retained files unless
// disableStartupCRCCheck is set.
//
// Steps:
//  1. Remove all *.fsm.tmp (crash orphans).
//  2. Enumerate live snap indexes from *.snap files in snapDir.
//  3. Remove any .fsm file whose index has no matching live snap.
//  4. For each remaining .fsm, call verifyFSMSnapshotFile; remove on ErrFSMSnapshotFileCRC.
func cleanupStaleFSMSnaps(snapDir, fsmSnapDir string, disableStartupCRCCheck bool) error {
	if err := os.MkdirAll(fsmSnapDir, defaultDirPerm); err != nil {
		return errors.WithStack(err)
	}
	if err := removeFSMTmpOrphans(fsmSnapDir); err != nil {
		return err
	}

	liveIndexes, err := collectLiveSnapIndexes(snapDir)
	if err != nil {
		return err
	}
	if liveIndexes == nil {
		// snapDir does not exist: cannot determine the authoritative live token
		// set. Skip FSM orphan removal to avoid deleting all .fsm files based
		// on incomplete information (e.g. misconfigured path or first-boot race).
		slog.Warn("snapshot directory not found; skipping FSM orphan cleanup", "snap_dir", snapDir)
		return nil
	}

	return removeStaleFSMFiles(fsmSnapDir, liveIndexes, disableStartupCRCCheck)
}

func removeFSMTmpOrphans(fsmSnapDir string) error {
	// Use os.ReadDir + strings.HasSuffix instead of filepath.Glob to avoid
	// misinterpretation of special characters (e.g. '[', ']') in fsmSnapDir
	// that glob treats as pattern syntax.
	entries, err := os.ReadDir(fsmSnapDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return errors.WithStack(err)
	}
	var combined error
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".fsm.tmp") {
			if removeErr := os.Remove(filepath.Join(fsmSnapDir, e.Name())); removeErr != nil && !os.IsNotExist(removeErr) {
				combined = errors.CombineErrors(combined, errors.WithStack(removeErr))
			}
		}
	}
	return errors.WithStack(combined)
}

func collectLiveSnapIndexes(snapDir string) (map[uint64]bool, error) {
	snapEntries, err := os.ReadDir(snapDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, errors.WithStack(err)
	}
	liveIndexes := make(map[uint64]bool, len(snapEntries))
	for _, e := range snapEntries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".snap" {
			if idx := parseSnapFileIndex(e.Name()); idx > 0 {
				liveIndexes[idx] = true
			}
		}
	}
	return liveIndexes, nil
}

func removeStaleFSMFiles(fsmSnapDir string, liveIndexes map[uint64]bool, disableStartupCRCCheck bool) error {
	fsmEntries, err := os.ReadDir(fsmSnapDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return errors.WithStack(err)
	}
	for _, e := range fsmEntries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".fsm" {
			continue
		}
		removeStaleFSMFile(fsmSnapDir, e.Name(), liveIndexes, disableStartupCRCCheck)
	}
	return nil
}

func removeStaleFSMFile(fsmSnapDir, name string, liveIndexes map[uint64]bool, disableStartupCRCCheck bool) {
	idx, err := strconv.ParseUint(strings.TrimSuffix(name, ".fsm"), 16, 64)
	if err != nil {
		return
	}
	fsmPath := filepath.Join(fsmSnapDir, name)
	if !liveIndexes[idx] {
		removeWithWarn(fsmPath, "orphan fsm snapshot")
		return
	}
	if disableStartupCRCCheck {
		return
	}
	if verifyErr := verifyFSMSnapshotFile(fsmPath, 0); verifyErr != nil && !errors.Is(verifyErr, ErrFSMSnapshotNotFound) {
		slog.Warn("removing invalid fsm snapshot", "path", fsmPath, "error", verifyErr)
		removeWithWarn(fsmPath, "invalid fsm snapshot")
	}
}

func removeWithWarn(path, label string) {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		slog.Warn("failed to remove "+label, "path", path, "error", err)
	}
}

// purgeOldSnapshotFiles removes old .snap and .fsm files in tandem, keeping the
// newest defaultMaxSnapFiles pairs. The .snap file is always deleted before its
// corresponding .fsm file so that no live token can reference a deleted .fsm.
//
// A crash between snap-remove and fsm-remove leaves a .fsm with no token:
// treated as an orphan on next startup by cleanupStaleFSMSnaps.
func purgeOldSnapshotFiles(snapDir, fsmSnapDir string) error {
	entries, err := os.ReadDir(snapDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return errors.WithStack(err)
	}

	snaps := collectSnapNames(entries)
	if len(snaps) <= defaultMaxSnapFiles {
		return nil
	}

	var combined error
	for _, name := range snaps[:len(snaps)-defaultMaxSnapFiles] {
		if err := purgeSnapPair(snapDir, fsmSnapDir, name); err != nil {
			combined = errors.CombineErrors(combined, err)
		}
	}

	combined = errors.CombineErrors(combined, syncDirIfExists(snapDir))
	combined = errors.CombineErrors(combined, syncDirIfExists(fsmSnapDir))
	return errors.WithStack(combined)
}

func collectSnapNames(entries []os.DirEntry) []string {
	var snaps []string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".snap" {
			snaps = append(snaps, e.Name())
		}
	}
	return snaps
}

func purgeSnapPair(snapDir, fsmSnapDir, snapName string) error {
	idx := parseSnapFileIndex(snapName)

	// Remove the .snap file first; skip .fsm removal if snap removal fails.
	snapPath := filepath.Join(snapDir, snapName)
	if removeErr := os.Remove(snapPath); removeErr != nil && !os.IsNotExist(removeErr) {
		return errors.WithStack(removeErr)
	}

	// Remove the corresponding .fsm file second.
	// Guard fsmSnapDir: when disk offloading is not configured (e.g. unit tests),
	// fsmSnapDir may be empty and fsmSnapPath would return a relative path.
	if idx > 0 && fsmSnapDir != "" {
		fsmPath := fsmSnapPath(fsmSnapDir, idx)
		if removeErr := os.Remove(fsmPath); removeErr != nil && !os.IsNotExist(removeErr) {
			return errors.WithStack(removeErr)
		}
	}
	return nil
}

func syncDirIfExists(dir string) error {
	if dir == "" {
		return nil
	}
	if err := syncDir(dir); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
