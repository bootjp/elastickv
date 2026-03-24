package store

import (
	"encoding/binary"
	"io"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

// pebbleSnapshotMagic is the 8-byte header that identifies a native Pebble
// snapshot stream (format: magic + lastCommitTS + key-value entries).
//
// Format history / compatibility:
//
//	This magic prefix was introduced when the pebbleSnapshotMagic format was
//	first defined. Snapshots written by an older Pebble-backed store that pre-
//	dates this header (i.e. starting directly with lastCommitTS) do not contain
//	the magic bytes and cannot be restored via the native Pebble path; they
//	will fall through to the legacy-gob restore path and surface as a gob
//	decode error.
var pebbleSnapshotMagic = [8]byte{'E', 'K', 'V', 'P', 'B', 'B', 'L', '1'}

type pebbleSnapshot struct {
	snapshot     *pebble.Snapshot
	lastCommitTS uint64
	once         sync.Once
	err          error
}

func newPebbleSnapshot(snapshot *pebble.Snapshot, lastCommitTS uint64) Snapshot {
	return &pebbleSnapshot{
		snapshot:     snapshot,
		lastCommitTS: lastCommitTS,
	}
}

func readPebbleSnapshotLastCommitTS(snapshot *pebble.Snapshot) (uint64, error) {
	return readPebbleUint64(snapshot, metaLastCommitTSBytes)
}

func (s *pebbleSnapshot) WriteTo(w io.Writer) (int64, error) {
	if s == nil || s.snapshot == nil {
		return 0, errors.New("snapshot is not available")
	}
	cw := &countingWriter{w: w}
	if _, err := cw.Write(pebbleSnapshotMagic[:]); err != nil {
		return cw.n, errors.WithStack(err)
	}
	if err := binary.Write(cw, binary.LittleEndian, s.lastCommitTS); err != nil {
		return cw.n, errors.WithStack(err)
	}
	if err := writePebbleSnapshotEntries(s.snapshot, cw); err != nil {
		return cw.n, err
	}
	return cw.n, nil
}

func (s *pebbleSnapshot) Close() error {
	if s == nil {
		return nil
	}
	s.once.Do(func() {
		if s.snapshot != nil {
			s.err = errors.WithStack(s.snapshot.Close())
			s.snapshot = nil
		}
	})
	return s.err
}

func writePebbleSnapshotEntries(snap *pebble.Snapshot, w io.Writer) error {
	iter, err := snap.NewIter(nil)
	if err != nil {
		return errors.WithStack(err)
	}

	for iter.First(); iter.Valid(); iter.Next() {
		k := iter.Key()
		v := iter.Value()

		if err := binary.Write(w, binary.LittleEndian, uint64(len(k))); err != nil {
			_ = iter.Close()
			return errors.WithStack(err)
		}
		if _, err := w.Write(k); err != nil {
			_ = iter.Close()
			return errors.WithStack(err)
		}
		if err := binary.Write(w, binary.LittleEndian, uint64(len(v))); err != nil {
			_ = iter.Close()
			return errors.WithStack(err)
		}
		if _, err := w.Write(v); err != nil {
			_ = iter.Close()
			return errors.WithStack(err)
		}
	}
	if err := iter.Error(); err != nil {
		_ = iter.Close()
		return errors.WithStack(err)
	}
	return errors.WithStack(iter.Close())
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.n += int64(n)
	return n, errors.WithStack(err)
}
