package etcd

import (
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/client/pkg/v3/fileutil"
)

// defaultMaxWALFiles is the number of .wal segment files to retain after a
// successful snapshot. etcd's wal package allocates segments of 64 MiB each;
// keeping the most recent N segments bounds WAL disk usage at ~N * 64 MiB.
//
// Rationale for keeping more than 1:
//   - The tail segment (currently being written) must always remain.
//   - A slow follower that hasn't applied the latest snapshot can still catch
//     up via log replication from a previous segment, avoiding an expensive
//     full snapshot transfer.
//   - etcd's own defaults keep max=5 segments via fileutil.PurgeFile.
const defaultMaxWALFiles = 5

// maxWALFilesEnvVar lets operators tune the WAL retention cap without a
// rebuild. Value must parse as an integer >= 1; invalid or unset values
// fall back to defaultMaxWALFiles.
const maxWALFilesEnvVar = "ELASTICKV_RAFT_MAX_WAL_FILES"

// walLockMode is the file mode passed to fileutil.TryLockFile when probing
// whether a .wal segment is still held by the active writer. 0o600 matches
// etcd's own PurgeFile behaviour (owner read/write only): the file already
// exists so the mode is only consulted on create, but we pin it here to
// avoid an mnd lint exception at the call site.
const walLockMode = 0o600

// maxWALFilesFromEnv returns the effective WAL retention cap. Operators can
// override via ELASTICKV_RAFT_MAX_WAL_FILES; invalid values are ignored and
// the default is used instead (retention is safety-critical, so we prefer a
// conservative fallback over failing open).
func maxWALFilesFromEnv() int {
	v := strings.TrimSpace(os.Getenv(maxWALFilesEnvVar))
	if v == "" {
		return defaultMaxWALFiles
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		slog.Warn("invalid ELASTICKV_RAFT_MAX_WAL_FILES; using default",
			"value", v, "default", defaultMaxWALFiles)
		return defaultMaxWALFiles
	}
	return n
}

// purgeOldWALFiles removes .wal segment files beyond the keep most recent.
//
// Safety:
//   - Segments still held under a flock by the active WAL writer (tail and
//     any segments whose lock the wal package has NOT yet released via
//     ReleaseLockTo) will refuse the TryLockFile acquire and are skipped.
//   - Filenames are sorted lexicographically; .wal names are
//     %016x-%016x.wal (seq-index), so lex order == chronological order.
//   - We never delete more than (N - keep) files, so when segments are held
//     the active tail is naturally preserved.
//
// This function must be called only AFTER persist.Release(snap) has run so
// that the wal package has closed the flocks on segments strictly older
// than the snapshot. Calling it earlier is a no-op (TryLockFile will fail
// on every segment) rather than a safety violation.
func purgeOldWALFiles(walDir string, keep int) error {
	if keep < 1 {
		keep = 1
	}
	entries, err := os.ReadDir(walDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return errors.WithStack(err)
	}

	names := collectWALNames(entries)
	if len(names) <= keep {
		return nil
	}
	sort.Strings(names)

	victims := names[:len(names)-keep]
	var combined error
	for _, name := range victims {
		full := filepath.Join(walDir, name)
		// Try-lock so we never delete a segment the wal package still owns.
		// If the lock fails (segment is active), skip it and continue: the
		// next snapshot's purge pass will pick it up once Release advances.
		l, lockErr := fileutil.TryLockFile(full, os.O_WRONLY, walLockMode)
		if lockErr != nil {
			slog.Debug("skipping locked wal segment", "path", full, "error", lockErr)
			continue
		}
		if rmErr := os.Remove(full); rmErr != nil && !os.IsNotExist(rmErr) {
			combined = errors.CombineErrors(combined, errors.WithStack(rmErr))
			_ = l.Close()
			continue
		}
		if closeErr := l.Close(); closeErr != nil {
			combined = errors.CombineErrors(combined, errors.WithStack(closeErr))
		}
	}
	combined = errors.CombineErrors(combined, syncDirIfExists(walDir))
	return errors.WithStack(combined)
}

// collectWALNames returns the subset of entries that look like WAL segment
// files. We filter by extension rather than regex-matching the full hex
// format so that we remain forward-compatible with any future wal naming
// tweaks — the worst case is that an unrecognised .wal file is treated as
// an older segment, which is exactly what we want.
func collectWALNames(entries []os.DirEntry) []string {
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if filepath.Ext(e.Name()) != ".wal" {
			continue
		}
		names = append(names, e.Name())
	}
	return names
}
