package raftstore

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/hashicorp/raft"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	logPrefix         byte = 'l'
	stablePrefix      byte = 's'
	pebbleDirPerm          = 0o755
	pebbleUint64Bytes      = 8
	pebbleLogKeyBytes      = 1 + pebbleUint64Bytes
)

var _ raft.LogStore = (*PebbleStore)(nil)
var _ raft.StableStore = (*PebbleStore)(nil)
var _ io.Closer = (*PebbleStore)(nil)

type PebbleStore struct {
	db *pebble.DB
}

func NewPebbleStore(dir string) (*PebbleStore, error) {
	if err := os.MkdirAll(dir, pebbleDirPerm); err != nil {
		return nil, errors.WithStack(err)
	}

	db, err := pebble.Open(dir, pebbleOptions())
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &PebbleStore{db: db}, nil
}

// Pinned so existing v1-era DBs are ratcheted above pebble v2's
// FormatMinSupported (FormatFlushableIngest) before the v2 upgrade lands.
func pebbleOptions() *pebble.Options {
	opts := &pebble.Options{
		FormatMajorVersion: pebble.FormatVirtualSSTables,
	}
	opts.EnsureDefaults()
	return opts
}

func (s *PebbleStore) FirstIndex() (uint64, error) {
	iter, err := s.db.NewIter(logIterOptions())
	if err != nil {
		return 0, errors.WithStack(err)
	}
	defer func() { _ = iter.Close() }()

	if !iter.First() {
		if err := iter.Error(); err != nil {
			return 0, errors.WithStack(err)
		}
		return 0, nil
	}
	return decodeLogIndex(iter.Key()), nil
}

func (s *PebbleStore) LastIndex() (uint64, error) {
	iter, err := s.db.NewIter(logIterOptions())
	if err != nil {
		return 0, errors.WithStack(err)
	}
	defer func() { _ = iter.Close() }()

	if !iter.Last() {
		if err := iter.Error(); err != nil {
			return 0, errors.WithStack(err)
		}
		return 0, nil
	}
	return decodeLogIndex(iter.Key()), nil
}

func (s *PebbleStore) GetLog(index uint64, out *raft.Log) error {
	value, closer, err := s.db.Get(logKey(index))
	if errors.Is(err, pebble.ErrNotFound) {
		return raft.ErrLogNotFound
	}
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() { _ = closer.Close() }()

	return errors.WithStack(msgpack.Unmarshal(value, out))
}

func (s *PebbleStore) StoreLog(log *raft.Log) error {
	return errors.WithStack(s.StoreLogs([]*raft.Log{log}))
}

func (s *PebbleStore) StoreLogs(logs []*raft.Log) error {
	if len(logs) == 0 {
		return nil
	}

	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()

	for _, logEntry := range logs {
		if logEntry == nil {
			continue
		}
		payload, err := msgpack.Marshal(logEntry)
		if err != nil {
			return errors.WithStack(err)
		}
		if err := batch.Set(logKey(logEntry.Index), payload, pebble.NoSync); err != nil {
			return errors.WithStack(err)
		}
	}

	return errors.WithStack(batch.Commit(pebble.Sync))
}

func (s *PebbleStore) DeleteRange(min, max uint64) error {
	if max < min {
		return nil
	}

	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()

	end := []byte{logPrefix + 1}
	if max < ^uint64(0) {
		end = logKey(max + 1)
	}
	if err := batch.DeleteRange(logKey(min), end, pebble.NoSync); err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(batch.Commit(pebble.Sync))
}

func (s *PebbleStore) Set(key []byte, value []byte) error {
	return errors.WithStack(s.db.Set(stableKey(key), value, pebble.Sync))
}

func (s *PebbleStore) Get(key []byte) ([]byte, error) {
	value, closer, err := s.db.Get(stableKey(key))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer func() { _ = closer.Close() }()

	return append([]byte(nil), value...), nil
}

func (s *PebbleStore) SetUint64(key []byte, value uint64) error {
	buf := make([]byte, pebbleUint64Bytes)
	binary.BigEndian.PutUint64(buf, value)
	return errors.WithStack(s.Set(key, buf))
}

func (s *PebbleStore) GetUint64(key []byte) (uint64, error) {
	value, err := s.Get(key)
	if err != nil || len(value) == 0 {
		return 0, err
	}
	if len(value) != pebbleUint64Bytes {
		return 0, errors.WithStack(errors.Newf("invalid uint64 value length: %d", len(value)))
	}
	return binary.BigEndian.Uint64(value), nil
}

func (s *PebbleStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return errors.WithStack(s.db.Close())
}

func logKey(index uint64) []byte {
	key := make([]byte, pebbleLogKeyBytes)
	key[0] = logPrefix
	binary.BigEndian.PutUint64(key[1:], index)
	return key
}

func stableKey(key []byte) []byte {
	out := make([]byte, 1+len(key))
	out[0] = stablePrefix
	copy(out[1:], key)
	return out
}

func decodeLogIndex(key []byte) uint64 {
	if len(key) != pebbleLogKeyBytes || key[0] != logPrefix {
		return 0
	}
	return binary.BigEndian.Uint64(key[1:])
}

func logIterOptions() *pebble.IterOptions {
	return &pebble.IterOptions{
		LowerBound: []byte{logPrefix},
		UpperBound: []byte{logPrefix + 1},
	}
}
