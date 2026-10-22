package backup

import (
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

// source.go decodes operator-supplied .fsm paths into the
// MANIFEST.json fields the cmd/ wrapper needs to stamp at write
// time. The decoder package owns the parsing so a future change to
// the snapshot filename convention surfaces in one place.

// SnapshotIndexFromPath extracts the integer applied-index encoded
// in a `.fsm` filename. The live writer (fsmSnapPath in
// internal/raftengine/etcd/fsm_snapshot_file.go) names every snapshot
// `%016x.fsm`: the FSM's applied_index at the moment the snapshot was
// taken, as 16 zero-padded lowercase hex digits. Phase 0a's
// MANIFEST.json carries that value as `snapshot_index` so a
// restore-time operator knows how stale the dump is relative to the
// cluster's last activity.
//
// The numeric portion is everything between the final path
// separator (or string start) and the `.fsm` suffix. A stem of
// exactly 16 hex digits is the live writer's name and parses as hex
// (`0000000000000040.fsm` is index 64); any other stem parses as
// decimal, so a hand-rolled snapshot named `42.fsm` decodes as 42.
//
// Returns ErrSnapshotIndexUnparseable when path lacks the .fsm
// suffix or when the basename is not a parseable uint64.
func SnapshotIndexFromPath(path string) (uint64, error) {
	base := filepath.Base(path)
	const fsmSuffix = ".fsm"
	if !strings.HasSuffix(base, fsmSuffix) {
		return 0, errors.Wrapf(ErrSnapshotIndexUnparseable,
			"path %q: missing %q suffix", path, fsmSuffix)
	}
	stem := strings.TrimSuffix(base, fsmSuffix)
	if stem == "" {
		return 0, errors.Wrapf(ErrSnapshotIndexUnparseable,
			"path %q: empty stem before %q", path, fsmSuffix)
	}
	numBase := decimalBase
	if isLiveWriterFSMStem(stem) {
		numBase = hexBase
	}
	idx, err := strconv.ParseUint(stem, numBase, 64)
	if err != nil {
		return 0, errors.Wrapf(ErrSnapshotIndexUnparseable,
			"path %q: stem %q is not a uint64", path, stem)
	}
	return idx, nil
}

// liveWriterFSMStemLen is the width of the live writer's `%016x` name;
// decimalBase and hexBase are the strconv bases for the two stem shapes.
const (
	liveWriterFSMStemLen = 16
	decimalBase          = 10
	hexBase              = 16
)

// isLiveWriterFSMStem reports whether stem has the live writer's shape:
// exactly 16 lowercase hex digits.
func isLiveWriterFSMStem(stem string) bool {
	if len(stem) != liveWriterFSMStemLen {
		return false
	}
	for _, c := range stem {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// ErrSnapshotIndexUnparseable is returned by SnapshotIndexFromPath
// when the input filename does not match the live writer's
// `<uint64>.fsm` convention. The Phase 0a CLI surfaces this as a
// soft warning (the dump still completes; MANIFEST.snapshot_index
// is left zero) rather than a hard failure — operator-supplied
// .fsm files copied off-cluster sometimes carry generic names.
var ErrSnapshotIndexUnparseable = errors.New("backup: snapshot path does not match <index>.fsm convention")
