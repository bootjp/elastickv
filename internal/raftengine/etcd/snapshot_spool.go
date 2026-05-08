package etcd

import (
	"io"
	"log/slog"
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

var errSnapshotPayloadTooLarge = errors.New("etcd raft snapshot payload exceeds limit")

const snapshotSpoolPattern = "elastickv-etcd-snapshot-*"

type snapshotSpool struct {
	file    *os.File
	path    string
	size    int64
	maxSize int64
}

func newSnapshotSpool(dir string) (*snapshotSpool, error) {
	file, err := os.CreateTemp(dir, snapshotSpoolPattern)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &snapshotSpool{
		file:    file,
		path:    file.Name(),
		maxSize: resolveMaxSnapshotPayloadBytes(),
	}, nil
}

func (s *snapshotSpool) Write(p []byte) (int, error) {
	if int64(len(p))+s.size > s.maxSize {
		return 0, errors.Wrapf(errSnapshotPayloadTooLarge, "%d > %d", int64(len(p))+s.size, s.maxSize)
	}
	n, err := s.file.Write(p)
	s.size += int64(n)
	if err != nil {
		return n, errors.WithStack(err)
	}
	return n, nil
}

func (s *snapshotSpool) Bytes() ([]byte, error) {
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return nil, errors.WithStack(err)
	}
	// Read incrementally instead of sizing a buffer from s.size so malformed
	// inputs stay bounded by s.maxSize and file-backed I/O.
	data, err := io.ReadAll(s.file)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return data, nil
}

func (s *snapshotSpool) Reader() (io.Reader, error) {
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return nil, errors.WithStack(err)
	}
	return s.file, nil
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
	if s.path != "" {
		err = errors.CombineErrors(err, errors.WithStack(os.Remove(s.path)))
	}
	return errors.WithStack(err)
}
