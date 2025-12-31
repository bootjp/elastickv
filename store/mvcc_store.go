package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/emirpasic/gods/maps/treemap"
)

// VersionedValue represents a single committed version in MVCC storage.
type VersionedValue struct {
	TS        uint64
	Value     []byte
	Tombstone bool
	ExpireAt  uint64 // HLC timestamp; 0 means no TTL
}

const (
	checksumSize = 4
)

func byteSliceComparator(a, b interface{}) int {
	ab, okA := a.([]byte)
	bb, okB := b.([]byte)
	switch {
	case okA && okB:
		return bytes.Compare(ab, bb)
	case okA:
		return 1
	case okB:
		return -1
	default:
		return 0
	}
}

func withinBoundsKey(k, start, end []byte) bool {
	if start != nil && bytes.Compare(k, start) < 0 {
		return false
	}
	if end != nil && bytes.Compare(k, end) > 0 {
		return false
	}
	return true
}

// mvccStore is an in-memory MVCC implementation backed by a treemap for
// deterministic iteration order and range scans.
type mvccStore struct {
	tree         *treemap.Map // key []byte -> []VersionedValue
	mtx          sync.RWMutex
	log          *slog.Logger
	lastCommitTS uint64
}

// LastCommitTS exposes the latest commit timestamp for read snapshot selection.
// It is intentionally not part of the public MVCCStore interface.
func (s *mvccStore) LastCommitTS() uint64 {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.lastCommitTS
}

// NewMVCCStore creates a new MVCC-enabled in-memory store.
func NewMVCCStore() MVCCStore {
	return &mvccStore{
		tree: treemap.NewWith(byteSliceComparator),
		log: slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelWarn,
		})),
	}
}

var _ MVCCStore = (*mvccStore)(nil)

// ---- helpers guarded by caller locks ----

func latestVisible(vs []VersionedValue, ts uint64) (VersionedValue, bool) {
	for i := len(vs) - 1; i >= 0; i-- {
		if vs[i].TS <= ts {
			if vs[i].ExpireAt != 0 && vs[i].ExpireAt <= ts {
				// Treat expired value as a tombstone for visibility.
				return VersionedValue{}, false
			}
			return vs[i], true
		}
	}
	return VersionedValue{}, false
}

func visibleValue(versions []VersionedValue, ts uint64) ([]byte, bool) {
	ver, ok := latestVisible(versions, ts)
	if !ok || ver.Tombstone {
		return nil, false
	}
	return ver.Value, true
}

func (s *mvccStore) putVersionLocked(key, value []byte, commitTS, expireAt uint64) {
	existing, _ := s.tree.Get(key)
	var versions []VersionedValue
	if existing != nil {
		versions, _ = existing.([]VersionedValue)
	}
	versions = append(versions, VersionedValue{
		TS:        commitTS,
		Value:     bytes.Clone(value),
		Tombstone: false,
		ExpireAt:  expireAt,
	})
	s.tree.Put(bytes.Clone(key), versions)
}

func (s *mvccStore) deleteVersionLocked(key []byte, commitTS uint64) {
	existing, _ := s.tree.Get(key)
	var versions []VersionedValue
	if existing != nil {
		versions, _ = existing.([]VersionedValue)
	}
	versions = append(versions, VersionedValue{
		TS:        commitTS,
		Value:     nil,
		Tombstone: true,
		ExpireAt:  0,
	})
	s.tree.Put(bytes.Clone(key), versions)
}

func (s *mvccStore) PutAt(ctx context.Context, key []byte, value []byte, commitTS uint64, expireAt uint64) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	commitTS = s.alignCommitTS(commitTS)
	s.putVersionLocked(key, value, commitTS, expireAt)
	s.log.InfoContext(ctx, "put_at",
		slog.String("key", string(key)),
		slog.String("value", string(value)),
		slog.Uint64("commit_ts", commitTS),
	)
	return nil
}

func (s *mvccStore) DeleteAt(ctx context.Context, key []byte, commitTS uint64) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	commitTS = s.alignCommitTS(commitTS)
	s.deleteVersionLocked(key, commitTS)
	s.log.InfoContext(ctx, "delete_at",
		slog.String("key", string(key)),
		slog.Uint64("commit_ts", commitTS),
	)
	return nil
}

func (s *mvccStore) readTS() uint64 {
	return s.lastCommitTS
}

func (s *mvccStore) alignCommitTS(commitTS uint64) uint64 {
	ts := commitTS
	if ts <= s.lastCommitTS {
		ts = s.lastCommitTS + 1
	}
	s.lastCommitTS = ts
	return ts
}

func (s *mvccStore) latestVersionLocked(key []byte) (VersionedValue, bool) {
	v, ok := s.tree.Get(key)
	if !ok {
		return VersionedValue{}, false
	}
	vs, _ := v.([]VersionedValue)
	if len(vs) == 0 {
		return VersionedValue{}, false
	}
	return vs[len(vs)-1], true
}

// ---- MVCCStore methods ----

func (s *mvccStore) GetAt(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	v, ok := s.tree.Get(key)
	if !ok {
		return nil, ErrKeyNotFound
	}
	versions, _ := v.([]VersionedValue)
	ver, ok := latestVisible(versions, ts)
	if !ok || ver.Tombstone {
		return nil, ErrKeyNotFound
	}
	return bytes.Clone(ver.Value), nil
}

func (s *mvccStore) ExistsAt(_ context.Context, key []byte, ts uint64) (bool, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	v, ok := s.tree.Get(key)
	if !ok {
		return false, nil
	}
	versions, _ := v.([]VersionedValue)
	if len(versions) == 0 {
		return false, nil
	}
	ver, ok := latestVisible(versions, ts)
	if !ok || ver.Tombstone {
		return false, nil
	}
	return true, nil
}

func (s *mvccStore) ScanAt(_ context.Context, start []byte, end []byte, limit int, ts uint64) ([]*KVPair, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	if limit <= 0 {
		return []*KVPair{}, nil
	}

	capHint := limit
	if size := s.tree.Size(); size < capHint {
		capHint = size
	}
	if capHint < 0 {
		capHint = 0
	}

	result := make([]*KVPair, 0, capHint)
	s.tree.Each(func(key interface{}, value interface{}) {
		if len(result) >= limit {
			return
		}
		k, ok := key.([]byte)
		if !ok || !withinBoundsKey(k, start, end) {
			return
		}

		versions, _ := value.([]VersionedValue)
		val, ok := visibleValue(versions, ts)
		if !ok {
			return
		}

		result = append(result, &KVPair{
			Key:   bytes.Clone(k),
			Value: bytes.Clone(val),
		})
	})

	return result, nil
}

func (s *mvccStore) LatestCommitTS(_ context.Context, key []byte) (uint64, bool, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	ver, ok := s.latestVersionLocked(key)
	if !ok {
		return 0, false, nil
	}
	return ver.TS, true, nil
}

func (s *mvccStore) ApplyMutations(ctx context.Context, mutations []*KVPairMutation, startTS, commitTS uint64) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	for _, mut := range mutations {
		if latestVer, ok := s.latestVersionLocked(mut.Key); ok && latestVer.TS > startTS {
			return errors.Wrapf(ErrWriteConflict, "key: %s", string(mut.Key))
		}
	}

	commitTS = s.alignCommitTS(commitTS)

	for _, mut := range mutations {
		switch mut.Op {
		case OpTypePut:
			s.putVersionLocked(mut.Key, mut.Value, commitTS, mut.ExpireAt)
		case OpTypeDelete:
			s.deleteVersionLocked(mut.Key, commitTS)
		default:
			return errors.WithStack(ErrUnknownOp)
		}
		s.log.InfoContext(ctx, "apply mutation",
			slog.String("key", string(mut.Key)),
			slog.Uint64("commit_ts", commitTS),
			slog.Bool("delete", mut.Op == OpTypeDelete),
		)
	}

	return nil
}

// ---- Store / ScanStore methods ----

func (s *mvccStore) PutWithTTLAt(ctx context.Context, key []byte, value []byte, commitTS uint64, expireAt uint64) error {
	return s.PutAt(ctx, key, value, commitTS, expireAt)
}

func (s *mvccStore) ExpireAt(ctx context.Context, key []byte, expireAt uint64, commitTS uint64) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	v, ok := s.tree.Get(key)
	if !ok {
		return ErrKeyNotFound
	}
	versions, _ := v.([]VersionedValue)
	if len(versions) == 0 {
		return ErrKeyNotFound
	}
	ver := versions[len(versions)-1]
	if ver.Tombstone || (ver.ExpireAt != 0 && ver.ExpireAt <= commitTS) {
		return ErrKeyNotFound
	}

	s.putVersionLocked(key, ver.Value, s.alignCommitTS(commitTS), expireAt)
	s.log.InfoContext(ctx, "expire",
		slog.String("key", string(key)),
		slog.Uint64("expire_at", expireAt),
	)
	return nil
}

func (s *mvccStore) Scan(_ context.Context, start []byte, end []byte, limit int) ([]*KVPair, error) {
	return s.ScanAt(context.Background(), start, end, limit, s.readTS())
}

func (s *mvccStore) Snapshot() (io.ReadWriter, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	state := make([]mvccSnapshotEntry, 0, s.tree.Size())
	s.tree.Each(func(key interface{}, value interface{}) {
		k, ok := key.([]byte)
		if !ok {
			return
		}
		versions, ok := value.([]VersionedValue)
		if !ok {
			return
		}
		state = append(state, mvccSnapshotEntry{
			Key:      bytes.Clone(k),
			Versions: append([]VersionedValue(nil), versions...),
		})
	})

	buf := &bytes.Buffer{}
	if err := gob.NewEncoder(buf).Encode(state); err != nil {
		return nil, errors.WithStack(err)
	}

	sum := crc32.ChecksumIEEE(buf.Bytes())
	if err := binary.Write(buf, binary.LittleEndian, sum); err != nil {
		return nil, errors.WithStack(err)
	}

	return buf, nil
}

func (s *mvccStore) Restore(r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return errors.WithStack(err)
	}
	if len(data) < checksumSize {
		return errors.WithStack(ErrInvalidChecksum)
	}
	payload := data[:len(data)-checksumSize]
	expected := binary.LittleEndian.Uint32(data[len(data)-checksumSize:])
	if crc32.ChecksumIEEE(payload) != expected {
		return errors.WithStack(ErrInvalidChecksum)
	}

	var state []mvccSnapshotEntry
	if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&state); err != nil {
		return errors.WithStack(err)
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.tree.Clear()
	for _, entry := range state {
		versions := append([]VersionedValue(nil), entry.Versions...)
		s.tree.Put(bytes.Clone(entry.Key), versions)
		if len(versions) > 0 {
			last := versions[len(versions)-1].TS
			if last > s.lastCommitTS {
				s.lastCommitTS = last
			}
		}
	}

	return nil
}

func (s *mvccStore) Close() error {
	return nil
}

// mvccSnapshotEntry is used solely for gob snapshot serialization.
type mvccSnapshotEntry struct {
	Key      []byte
	Versions []VersionedValue
}
