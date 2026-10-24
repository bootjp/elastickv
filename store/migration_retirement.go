package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"sort"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
)

var migrationRetiredMetaKeyBytes = []byte("!migstage|retired")

// ErrMigrationRetired is a replicated per-job refusal, not a storage failure.
var ErrMigrationRetired = errors.New("migration job is retired")

func IsMigrationRetirementMetadataKey(key []byte) bool {
	return bytes.Equal(key, migrationRetiredMetaKeyBytes)
}

func encodeMigrationRetiredJobs(jobs map[uint64]struct{}) []byte {
	ids := make([]uint64, 0, len(jobs))
	for id := range jobs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := binary.AppendUvarint([]byte{migrationMetadataVersion}, lenAsUint64(len(ids)))
	for _, id := range ids {
		out = binary.BigEndian.AppendUint64(out, id)
	}
	return out
}

func decodeMigrationRetiredJobs(data []byte) (map[uint64]struct{}, bool) {
	if len(data) == 0 || data[0] != migrationMetadataVersion {
		return nil, false
	}
	count, size := binary.Uvarint(data[1:])
	if size <= 0 {
		return nil, false
	}
	rest := data[1+size:]
	if count != lenAsUint64(len(rest))/migrationUint64Bytes || len(rest)%migrationUint64Bytes != 0 {
		return nil, false
	}
	jobs := make(map[uint64]struct{}, count)
	for len(rest) != 0 {
		id := binary.BigEndian.Uint64(rest[:migrationUint64Bytes])
		if _, duplicate := jobs[id]; id == 0 || duplicate {
			return nil, false
		}
		jobs[id] = struct{}{}
		rest = rest[migrationUint64Bytes:]
	}
	return jobs, true
}

func (s *pebbleStore) readMigrationRetiredJobs() (map[uint64]struct{}, error) {
	value, closer, err := s.db.Get(migrationRetiredMetaKeyBytes)
	if errors.Is(err, pebble.ErrNotFound) {
		return make(map[uint64]struct{}), nil
	}
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer func() { _ = closer.Close() }()
	jobs, valid := decodeMigrationRetiredJobs(value)
	if !valid {
		return nil, errors.New("corrupt migration retirement witnesses")
	}
	return jobs, nil
}

func (s *pebbleStore) checkMigrationNotRetiredLocked(jobID uint64) error {
	jobs, err := s.readMigrationRetiredJobs()
	if err != nil {
		return err
	}
	if _, retired := jobs[jobID]; retired {
		return errors.WithStack(ErrMigrationRetired)
	}
	return nil
}

func (s *mvccStore) MigrationRetired(_ context.Context, jobID uint64) (bool, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	_, retired := s.migrationRetired[jobID]
	return retired, nil
}

func (s *pebbleStore) MigrationRetired(_ context.Context, jobID uint64) (bool, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	jobs, err := s.readMigrationRetiredJobs()
	_, retired := jobs[jobID]
	return retired, err
}
