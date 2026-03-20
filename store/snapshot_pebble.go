package store

import (
	"encoding/binary"
	"io"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

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
	val, closer, err := snapshot.Get(metaLastCommitTSBytes)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, nil
		}
		return 0, errors.WithStack(err)
	}
	defer closer.Close()
	if len(val) < timestampSize {
		return 0, nil
	}
	return binary.LittleEndian.Uint64(val), nil
}

func (s *pebbleSnapshot) WriteTo(w io.Writer) (int64, error) {
	if s == nil || s.snapshot == nil {
		return 0, errors.New("snapshot is not available")
	}
	cw := &countingWriter{w: w}
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
