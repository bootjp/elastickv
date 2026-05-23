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
// in a `.fsm` filename. The live writer (internal/raftengine/etcd/
// engine.go) names every snapshot `<index>.fsm` where <index> is
// the FSM's applied_index at the moment the snapshot was taken.
// Phase 0a's MANIFEST.json carries that value as `snapshot_index`
// so a restore-time operator knows how stale the dump is relative
// to the cluster's last activity.
//
// The numeric portion is everything between the final path
// separator (or string start) and the `.fsm` suffix. Lengths and
// zero-padding are not enforced — the live writer happens to pad
// to 16 digits, but a hand-rolled snapshot named `42.fsm` decodes
// correctly too.
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
	idx, err := strconv.ParseUint(stem, 10, 64)
	if err != nil {
		return 0, errors.Wrapf(ErrSnapshotIndexUnparseable,
			"path %q: stem %q is not a uint64", path, stem)
	}
	return idx, nil
}

// ErrSnapshotIndexUnparseable is returned by SnapshotIndexFromPath
// when the input filename does not match the live writer's
// `<uint64>.fsm` convention. The Phase 0a CLI surfaces this as a
// soft warning (the dump still completes; MANIFEST.snapshot_index
// is left zero) rather than a hard failure — operator-supplied
// .fsm files copied off-cluster sometimes carry generic names.
var ErrSnapshotIndexUnparseable = errors.New("backup: snapshot path does not match <index>.fsm convention")
