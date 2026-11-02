package store

import (
	"encoding/binary"
	"io"
	"sort"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
)

func writeRaftControlSnapshot(w io.Writer, records map[string][]byte, index uint64) error {
	if len(records) > maxSnapshotVersionCount {
		return errors.WithStack(ErrSnapshotVersionCountTooLarge)
	}
	if err := binary.Write(w, binary.LittleEndian, index); err != nil {
		return errors.WithStack(err)
	}
	if err := binary.Write(w, binary.LittleEndian, uint64(len(records))); err != nil {
		return errors.WithStack(err)
	}
	keys := make([]string, 0, len(records))
	for key := range records {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if _, err := raftControlKey([]byte(key)); err != nil {
			return err
		}
		if err := writeMVCCSnapshotBytes(w, []byte(key)); err != nil {
			return err
		}
		if err := writeMVCCSnapshotBytes(w, records[key]); err != nil {
			return err
		}
	}
	return nil
}

func readRaftControlSnapshot(r io.Reader, version uint32) (map[string][]byte, uint64, error) {
	records := make(map[string][]byte)
	if version < mvccSnapshotVersionV8 {
		return records, 0, nil
	}
	var index, count uint64
	if err := binary.Read(r, binary.LittleEndian, &index); err != nil {
		return nil, 0, errors.WithStack(err)
	}
	if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
		return nil, 0, errors.WithStack(err)
	}
	if count > maxSnapshotVersionCount {
		return nil, 0, errors.WithStack(ErrSnapshotVersionCountTooLarge)
	}
	previous := ""
	for range count {
		key, err := readRaftControlSnapshotField(r, maxSnapshotKeySize-len(RaftControlMetadataPrefix))
		if err != nil {
			return nil, 0, err
		}
		if len(key) == 0 || string(key) <= previous {
			return nil, 0, errors.New("store: noncanonical raft control snapshot keys")
		}
		value, err := readRaftControlSnapshotField(r, maxSnapshotValueSize)
		if err != nil {
			return nil, 0, err
		}
		previous = string(key)
		records[previous] = value
	}
	return records, index, nil
}

func readRaftControlSnapshotField(r io.Reader, limit int) ([]byte, error) {
	if limit < 0 {
		return nil, errors.New("store: negative raft control snapshot field limit")
	}
	var size uint64
	if err := binary.Read(r, binary.LittleEndian, &size); err != nil {
		return nil, errors.WithStack(err)
	}
	if size > uint64(limit) {
		return nil, errors.New("store: raft control snapshot field exceeds limit")
	}
	value := make([]byte, int(size))
	_, err := io.ReadFull(r, value)
	return value, errors.WithStack(err)
}

func (s *mvccStore) writeSnapshotControlTail(w io.Writer, version uint32) error {
	if version >= mvccSnapshotVersionV7 {
		data, err := encodeMigrationOwnershipPartitions(s.migrationPartitions)
		if err != nil {
			return err
		}
		if err := writeMVCCSnapshotBytes(w, data); err != nil {
			return err
		}
	}
	if version >= mvccSnapshotVersionV8 {
		return writeRaftControlSnapshot(w, s.raftControl, s.controlAppliedIndex)
	}
	return nil
}

func stageSnapshotRaftControl(batch *pebble.Batch, records map[string][]byte, index uint64) error {
	for key, value := range records {
		physical, err := raftControlKey([]byte(key))
		if err != nil {
			return err
		}
		if err := batch.Set(physical, encodeValue(value, false, 0, encStateCleartext), nil); err != nil {
			return errors.WithStack(err)
		}
	}
	if index != 0 {
		return setPebbleUint64InBatch(batch, metaAppliedIndexBytes, index)
	}
	return nil
}
