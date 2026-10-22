package backup

import (
	"bytes"
	"encoding/binary"

	cockroachdberr "github.com/cockroachdb/errors"
)

// ErrSnapshotUnsorted is returned when a snapshot's entries are not in
// the store's key order, which the newest-version filter depends on.
var ErrSnapshotUnsorted = cockroachdberr.New("backup: snapshot entries are not in key order")

// latestVersionFilter keeps the newest version of each user key and
// hides the older ones, which is what a logical dump wants from a live
// store's snapshot: the store streams every physical MVCC version
// (writePebbleSnapshotEntries, and the SST ingest export likewise), while
// an encoder-produced snapshot has one version per key.
//
// The store encodes a version as userKey || BE(^commitTS) and orders keys
// bytewise, so a key's versions arrive newest first. They need not be
// adjacent: another user key that extends this one (userKey || x) can
// sort between two of its versions. Every encoded key that sorts between
// two versions of userKey starts with userKey, so the filter keeps a key
// "open" while the current encoded key still starts with it, and an entry
// whose user key is already open is an older version. The open set holds
// only user keys that are prefixes of the current encoded key, so it stays
// as small as the deepest such nesting.
type latestVersionFilter struct {
	open     [][]byte
	enc      []byte
	previous []byte
}

// keep reports whether e is the newest version of its user key. It fails
// with ErrSnapshotUnsorted when e does not sort after the previous entry.
func (f *latestVersionFilter) keep(e SnapshotEntry) (bool, error) {
	f.enc = append(f.enc[:0], e.UserKey...)
	f.enc = binary.BigEndian.AppendUint64(f.enc, ^e.CommitTS)
	if f.previous != nil && bytes.Compare(f.enc, f.previous) <= 0 {
		return false, cockroachdberr.Wrapf(ErrSnapshotUnsorted, "key %q at commit_ts %d", e.UserKey, e.CommitTS)
	}
	f.previous = append(f.previous[:0], f.enc...)

	newest := true
	open := f.open[:0]
	for _, k := range f.open {
		if !bytes.HasPrefix(f.enc, k) {
			continue // every version of k is behind us
		}
		if bytes.Equal(k, e.UserKey) {
			newest = false
		}
		open = append(open, k)
	}
	f.open = open
	if newest {
		f.open = append(f.open, bytes.Clone(e.UserKey))
	}
	return newest, nil
}
