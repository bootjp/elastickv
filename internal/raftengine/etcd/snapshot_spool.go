package etcd

import (
	"encoding/binary"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

// defaultMaxSnapshotPayloadBytes is the receive-side cap on a single snapshot
// stream's spooled payload. Production hit a 1 GiB ceiling here that was
// silently rejecting real-world FSM transfers (1.35 GiB+), so the receiver
// returned errSnapshotPayloadTooLarge mid-stream, the gRPC stream broke,
// and etcd raft retried — indefinitely, since each retry hit the same wall.
// Followers stuck at stale applied indices, leader sustained ~100 MB/s
// outbound, host disks saturated for hours.
//
// 16 GiB is sized as ~12× the production-observed FSM size so the limit
// does not drift back into the runway as data grows. The cap still exists
// so a misbehaving / compromised peer cannot stream unbounded data into
// the spool dir; operators can raise it further via
// ELASTICKV_RAFT_MAX_SNAPSHOT_PAYLOAD_BYTES if a real FSM ever exceeds
// even this default.
const defaultMaxSnapshotPayloadBytes int64 = 16 << 30 // 16 GiB

const maxSnapshotPayloadBytesEnvVar = "ELASTICKV_RAFT_MAX_SNAPSHOT_PAYLOAD_BYTES"

const snapshotSpoolMinFreeBytesEnvVar = "ELASTICKV_RAFT_SNAPSHOT_SPOOL_MIN_FREE_BYTES"

// resolveMaxSnapshotPayloadBytes evaluates the env override once per spool
// creation. Snapshots are infrequent enough that one Getenv + ParseInt per
// spool is invisible in profiles, and resolving at construction means tests
// that flip the env via t.Setenv don't have to mutate process-wide globals.
func resolveMaxSnapshotPayloadBytes() int64 {
	v := strings.TrimSpace(os.Getenv(maxSnapshotPayloadBytesEnvVar))
	if v == "" {
		return defaultMaxSnapshotPayloadBytes
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		slog.Warn("invalid ELASTICKV_RAFT_MAX_SNAPSHOT_PAYLOAD_BYTES; using default",
			"value", v, "default_bytes", defaultMaxSnapshotPayloadBytes)
		return defaultMaxSnapshotPayloadBytes
	}
	return n
}

func resolveSnapshotSpoolMinFreeBytes(defaultBytes int64) int64 {
	v := strings.TrimSpace(os.Getenv(snapshotSpoolMinFreeBytesEnvVar))
	if v == "" {
		return defaultBytes
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		slog.Warn("invalid ELASTICKV_RAFT_SNAPSHOT_SPOOL_MIN_FREE_BYTES; using default",
			"value", v, "default_bytes", defaultBytes)
		return defaultBytes
	}
	return n
}

var errSnapshotPayloadTooLarge = errors.New("etcd raft snapshot payload exceeds limit")

var errSnapshotSpoolDiskHeadroom = errors.New("etcd raft snapshot spool insufficient disk headroom")

// snapshotSyncDir indirects fsync-on-directory through a package var so a
// fault-injection test can simulate "rename succeeded but the fsync that
// would persist the directory entry failed" — that's the partial-failure
// path FinalizeAsFSMFile's stepwise state-clearing was added to handle,
// and without an injection seam there's no portable way to reproduce it.
var snapshotSyncDir = syncDir

var snapshotSpoolAvailableBytes = snapshotSpoolAvailableBytesFS

const snapshotSpoolPattern = "elastickv-etcd-snapshot-*"

type snapshotSpool struct {
	file         *os.File
	path         string
	size         int64
	maxSize      int64
	minFreeBytes int64
}

func newSnapshotSpool(dir string) (*snapshotSpool, error) {
	return newSnapshotSpoolWithLimits(dir, resolveMaxSnapshotPayloadBytes(), 0)
}

func newReceiveSnapshotSpool(dir string) (*snapshotSpool, error) {
	maxSize := resolveMaxSnapshotPayloadBytes()
	// Keep one full max-size snapshot worth of headroom after receive-side
	// spooling. Applying a token snapshot restores the FSM from the completed
	// .fsm into a new local store directory, so a node can transiently need old
	// snapshot + incoming .fsm + restored store bytes.
	return newSnapshotSpoolWithLimits(dir, maxSize, resolveSnapshotSpoolMinFreeBytes(maxSize))
}

func newSnapshotSpoolWithLimits(dir string, maxSize, minFreeBytes int64) (*snapshotSpool, error) {
	file, err := os.CreateTemp(dir, snapshotSpoolPattern)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &snapshotSpool{
		file:         file,
		path:         file.Name(),
		maxSize:      maxSize,
		minFreeBytes: minFreeBytes,
	}, nil
}

func (s *snapshotSpool) Write(p []byte) (int, error) {
	// Subtraction-based comparison so the cap check stays correct even when
	// s.maxSize is set to a value near math.MaxInt64 via the env override:
	// `int64(len(p))+s.size > s.maxSize` would overflow into a negative number
	// at large maxSize and let the write through. `int64(len(p)) > s.maxSize-s.size`
	// stays in [0, maxSize] and rejects the same payloads correctly.
	if int64(len(p)) > s.maxSize-s.size {
		return 0, errors.Wrapf(errSnapshotPayloadTooLarge, "adding %d bytes to current %d would exceed limit %d", len(p), s.size, s.maxSize)
	}
	if err := s.checkDiskHeadroom(len(p)); err != nil {
		return 0, err
	}
	n, err := s.file.Write(p)
	s.size += int64(n)
	if err != nil {
		return n, errors.WithStack(err)
	}
	return n, nil
}

func (s *snapshotSpool) checkDiskHeadroom(writeBytes int) error {
	if writeBytes <= 0 || s.minFreeBytes <= 0 {
		return nil
	}
	available, err := snapshotSpoolAvailableBytes(filepath.Dir(s.path))
	if err != nil {
		return errors.WithStack(err)
	}
	if available < 0 {
		available = 0
	}
	if s.minFreeBytes > math.MaxInt64-int64(writeBytes) {
		return errors.Wrapf(errSnapshotSpoolDiskHeadroom,
			"write_bytes=%d min_free_bytes=%d available_bytes=%d",
			writeBytes, s.minFreeBytes, available)
	}
	required := s.minFreeBytes + int64(writeBytes)
	if available < required {
		return errors.Wrapf(errSnapshotSpoolDiskHeadroom,
			"write_bytes=%d min_free_bytes=%d available_bytes=%d",
			writeBytes, s.minFreeBytes, available)
	}
	return nil
}

func (s *snapshotSpool) Bytes() ([]byte, error) {
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return nil, errors.WithStack(err)
	}
	// Pre-allocate from the bytes we have already accepted past Write's
	// per-call cap check, instead of letting io.ReadAll grow the buffer
	// through several power-of-two doublings (a 1.35 GiB receive would
	// trigger ~30 reallocs and copy the running total each time). s.size
	// is the truth-of-record for what's on disk because Write only
	// increments it on successful os.File.Write returns.
	buf := make([]byte, s.size)
	if _, err := io.ReadFull(s.file, buf); err != nil {
		return nil, errors.WithStack(err)
	}
	return buf, nil
}

func (s *snapshotSpool) Reader() (io.Reader, error) {
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return nil, errors.WithStack(err)
	}
	return s.file, nil
}

// FinalizeAsFSMFile rewrites the spool file as a fully-formed .fsm snapshot
// (payload + 4-byte big-endian CRC32C footer) and atomically renames it to
// fsmSnapPath(fsmSnapDir, index). On success, ownership of the on-disk file
// transfers to fsmSnapDir — subsequent Close() becomes a no-op so the
// renamed file is NOT removed.
//
// The caller is responsible for having computed crc32c over the bytes
// already written to the spool (see crc32CWriter). This is the streaming
// receive-side counterpart of writeFSMSnapshotFile / writeFSMSnapshotPayload
// (sender-side in fsm_snapshot_file.go), and the on-disk format is identical
// so openAndRestoreFSMSnapshot can read either source uniformly.
//
// Memory safety: this is the path that lets receiveSnapshotStream avoid
// materializing a multi-GiB Snapshot.Data []byte. The spool file already
// holds the payload; we only append 4 bytes and rename. Heap stays flat
// regardless of FSM size.
//
// Filesystem requirement: the spool file MUST be on the same filesystem as
// fsmSnapDir, otherwise os.Rename returns syscall.EXDEV and the receive
// fails (the leader will retry indefinitely with no chance of success).
// receiveSnapshotStream guarantees this by creating the spool inside
// fsmSnapDir when the streaming-token path is wired; standard engine
// wiring (engine.go) keeps both spoolDir and fsmSnapDir under cfg.DataDir,
// so the constraint is satisfied even on the legacy fallback path.
func (s *snapshotSpool) FinalizeAsFSMFile(fsmSnapDir string, index uint64, crc32c uint32) error {
	if s == nil || s.file == nil {
		return errors.New("snapshot spool: finalize on closed/nil spool")
	}

	// State is cleared at each successful step so a deferred caller-side
	// spool.Close() never tries to operate on a resource we already
	// released. Two failure modes the original single-defer version would
	// have produced misleading errors for, that this stepwise approach
	// avoids:
	//   1. file.Close() succeeds, os.Rename() fails → s.file=nil but s.path
	//      still valid, so Close() correctly removes the half-written
	//      spool file at its original path (the rename never happened).
	//   2. os.Rename() succeeds, syncDir() fails → s.file=nil AND s.path=""
	//      so Close() is a no-op. Without the per-step clear, Close()
	//      would attempt os.Remove(s.path) and surface a misleading
	//      ErrNotExist that buries the real syncDir error.
	if err := s.checkDiskHeadroom(fsmFooterSize); err != nil {
		return err
	}
	if err := binary.Write(s.file, binary.BigEndian, crc32c); err != nil {
		return errors.WithStack(err)
	}
	if err := s.file.Sync(); err != nil {
		return errors.WithStack(err)
	}
	closeErr := s.file.Close()
	// File descriptor is released by os.File.Close before it returns, even
	// on error (the underlying fd is invalidated atomically before the
	// close syscall). Clearing s.file unconditionally avoids a double-Close
	// from the deferred caller-side Close that would log a confusing
	// os.ErrClosed warning on top of the real error.
	s.file = nil
	if closeErr != nil {
		return errors.WithStack(closeErr)
	}

	finalPath := fsmSnapPath(fsmSnapDir, index)
	if mkErr := os.MkdirAll(fsmSnapDir, defaultDirPerm); mkErr != nil {
		return errors.WithStack(mkErr)
	}
	if rnErr := os.Rename(s.path, finalPath); rnErr != nil {
		return errors.WithStack(rnErr)
	}
	s.path = "" // file no longer at this path; subsequent Close() must not try to remove it.

	if syErr := snapshotSyncDir(fsmSnapDir); syErr != nil {
		return errors.WithStack(syErr)
	}
	return nil
}

// cleanupStaleSnapshotSpools removes orphaned snapshot spool files left behind
// by a previous engine instance that crashed before Close could run.
func cleanupStaleSnapshotSpools(dir string) error {
	matches, err := filepath.Glob(filepath.Join(dir, snapshotSpoolPattern))
	if err != nil {
		return errors.WithStack(err)
	}
	var combined error
	for _, match := range matches {
		removeErr := os.Remove(match)
		if removeErr == nil || os.IsNotExist(removeErr) {
			continue
		}
		combined = errors.CombineErrors(combined, errors.WithStack(removeErr))
	}
	return errors.WithStack(combined)
}

func (s *snapshotSpool) Close() error {
	if s == nil {
		return nil
	}
	var err error
	if s.file != nil {
		err = errors.CombineErrors(err, errors.WithStack(s.file.Close()))
	}
	// Skip removal once FinalizeAsFSMFile has handed ownership of the file
	// off to fsmSnapDir (it nils path so we don't try to delete a renamed
	// .fsm file that's now serving as the durable snapshot).
	if s.path != "" {
		err = errors.CombineErrors(err, errors.WithStack(os.Remove(s.path)))
	}
	return errors.WithStack(err)
}
