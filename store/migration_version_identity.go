package store

import (
	"bytes"
	"math"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
)

// ErrMigrationVersionConflict reports an attempt to assign different contents
// to an existing MVCC version. Expired versions and tombstones retain their
// identity, so this check does not use the public visibility rules.
var ErrMigrationVersionConflict = errors.New("migration MVCC version has different contents")

type migrationVersionLookup func([]byte, uint64) (VersionedValue, bool, error)

type migrationVersionID struct {
	key string
	ts  uint64
}

func immutableMigrationWrites(
	versions []MVCCVersion,
	logicalKey func([]byte) []byte,
	lookup migrationVersionLookup,
) ([]MVCCVersion, error) {
	seen := make(map[migrationVersionID]MVCCVersion, len(versions))
	writes := make([]MVCCVersion, 0, len(versions))
	for _, version := range versions {
		key := version.Key
		if logicalKey != nil {
			key = logicalKey(key)
		}
		id := migrationVersionID{key: string(key), ts: version.CommitTS}
		if previous, ok := seen[id]; ok {
			if !sameMigrationVersion(version, previous.Value, previous.ExpireAt, previous.Tombstone) {
				return nil, migrationVersionConflict(version)
			}
			if bytes.Equal(previous.Key, version.Key) {
				continue
			}
		}
		seen[id] = version
		if !bytes.Equal(key, version.Key) {
			if _, err := checkMigrationVersion(lookup, key, version); err != nil {
				return nil, err
			}
		}
		present, err := checkMigrationVersion(lookup, version.Key, version)
		if err != nil {
			return nil, err
		}
		if !present {
			writes = append(writes, version)
		}
	}
	return writes, nil
}

func checkMigrationVersion(lookup migrationVersionLookup, key []byte, version MVCCVersion) (bool, error) {
	existing, present, err := lookup(key, version.CommitTS)
	if err != nil || !present {
		return false, err
	}
	if !sameMigrationVersion(version, existing.Value, existing.ExpireAt, existing.Tombstone) {
		return false, migrationVersionConflict(version)
	}
	return true, nil
}

func sameMigrationVersion(version MVCCVersion, value []byte, expireAt uint64, tombstone bool) bool {
	return version.Tombstone == tombstone && version.ExpireAt == expireAt && bytes.Equal(version.Value, value)
}

func migrationVersionConflict(version MVCCVersion) error {
	return errors.Wrapf(ErrMigrationVersionConflict, "key %q, commit timestamp %d", version.Key, version.CommitTS)
}

func (s *mvccStore) lookupMigrationVersion(key []byte, ts uint64) (VersionedValue, bool, error) {
	existing, _ := s.entryTreeGet(key)
	versions, _ := existing.([]VersionedValue)
	i := findVersionIndex(versions, ts)
	if i < 0 {
		return VersionedValue{}, false, nil
	}
	return versions[i], true, nil
}

// Local compaction can prune different historical versions on different
// voters. Only heads survive on every replica and can satisfy exact CAS.
// Raft conflict verdicts must therefore use heads (plus the batch itself).
// Direct store calls may enforce identity on all retained versions.
func (s *mvccStore) lookupMigrationHead(key []byte, ts uint64) (VersionedValue, bool, error) {
	head, ok := s.latestVersionLocked(key)
	return head, ok && head.TS == ts, nil
}

func (s *pebbleStore) lookupMigrationHead(key []byte, ts uint64) (VersionedValue, bool, error) {
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: encodeKey(key, math.MaxUint64),
		UpperBound: keyUpperBound(key),
	})
	if err != nil {
		return VersionedValue{}, false, errors.WithStack(err)
	}
	defer iter.Close()
	if !iter.First() || !skipPebbleMetadataForward(iter) {
		return VersionedValue{}, false, errors.WithStack(iter.Error())
	}
	physical := iter.Key()
	userKey, latest := decodeKeyView(physical)
	if latest != ts || !bytes.Equal(userKey, key) {
		return VersionedValue{}, false, nil
	}
	data, err := iter.ValueAndErr()
	if err != nil {
		return VersionedValue{}, false, errors.WithStack(err)
	}
	record, err := s.decodeMigrationVersion(physical, ts, data)
	return record, err == nil, err
}

func (s *pebbleStore) lookupMigrationVersion(key []byte, ts uint64) (VersionedValue, bool, error) {
	physical, err := encodePebbleUserVersionKey(key, ts)
	if err != nil {
		return VersionedValue{}, false, err
	}
	data, closer, err := s.db.Get(physical)
	if errors.Is(err, pebble.ErrNotFound) {
		return VersionedValue{}, false, nil
	}
	if err != nil {
		return VersionedValue{}, false, errors.WithStack(err)
	}
	record, err := s.decodeMigrationVersion(physical, ts, data)
	err = errors.Join(err, errors.WithStack(closer.Close()))
	return record, err == nil, err
}

func (s *pebbleStore) decodeMigrationVersion(physical []byte, ts uint64, data []byte) (VersionedValue, error) {
	value, err := decodeValueView(data)
	if err != nil {
		return VersionedValue{}, err
	}
	plain, err := s.decryptForKey(physical, value, value.Value)
	if err != nil {
		return VersionedValue{}, err
	}
	return VersionedValue{Value: bytes.Clone(plain), TS: ts, ExpireAt: value.ExpireAt, Tombstone: value.Tombstone}, nil
}
