package store

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"hash/crc32"
	"io"

	"github.com/cockroachdb/errors"
)

// writeLegacyGobSnapshot writes the old gob-encoded snapshot format used only
// in migration tests. The format is: gob(mvccSnapshot) + CRC32(LE).
func (s *mvccStore) writeLegacyGobSnapshot(w io.Writer) error {
	s.mtx.RLock()
	snap := mvccSnapshot{
		LastCommitTS:  s.lastCommitTS,
		MinRetainedTS: s.minRetainedTS,
	}
	iter := s.tree.Iterator()
	for iter.Next() {
		key, _ := iter.Key().([]byte)
		versions, _ := iter.Value().([]VersionedValue)
		snap.Entries = append(snap.Entries, mvccSnapshotEntry{
			Key:      bytes.Clone(key),
			Versions: append([]VersionedValue(nil), versions...),
		})
	}
	s.mtx.RUnlock()

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&snap); err != nil {
		return errors.WithStack(err)
	}
	payload := buf.Bytes()
	checksum := crc32.ChecksumIEEE(payload)
	if _, err := w.Write(payload); err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(binary.Write(w, binary.LittleEndian, checksum))
}
