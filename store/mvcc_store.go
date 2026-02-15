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
	"sort"
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

// mvccSnapshot is used solely for gob snapshot serialization.
type mvccSnapshot struct {
	LastCommitTS uint64
	Entries      []mvccSnapshotEntry
}

// mvccSnapshotEntry is used solely for gob snapshot serialization.
type mvccSnapshotEntry struct {
	Key      []byte
	Versions []VersionedValue
}

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

// MVCCStoreOption configures the MVCCStore.
type MVCCStoreOption func(*mvccStore)

// WithLogger sets a custom logger for the store.
func WithLogger(l *slog.Logger) MVCCStoreOption {
	return func(s *mvccStore) {
		s.log = l
	}
}

// NewMVCCStore creates a new MVCC-enabled in-memory store.
func NewMVCCStore(opts ...MVCCStoreOption) MVCCStore {
	s := &mvccStore{
		tree: treemap.NewWith(byteSliceComparator),
		log: slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelWarn,
		})),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
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
	versions = insertVersionSorted(versions, VersionedValue{
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
	versions = insertVersionSorted(versions, VersionedValue{
		TS:        commitTS,
		Value:     nil,
		Tombstone: true,
		ExpireAt:  0,
	})
	s.tree.Put(bytes.Clone(key), versions)
}

func insertVersionSorted(versions []VersionedValue, vv VersionedValue) []VersionedValue {
	// Keep versions sorted by TS ascending so lookups can assume max TS is last.
	i := sort.Search(len(versions), func(i int) bool { return versions[i].TS >= vv.TS })
	if i < len(versions) && versions[i].TS == vv.TS {
		// Idempotence: overwrite same timestamp.
		versions[i] = vv
		return versions
	}
	versions = append(versions, VersionedValue{})
	copy(versions[i+1:], versions[i:])
	versions[i] = vv
	return versions
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
	if commitTS > s.lastCommitTS {
		s.lastCommitTS = commitTS
	}
	return commitTS
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

	entries := make([]mvccSnapshotEntry, 0, s.tree.Size())
	s.tree.Each(func(key interface{}, value interface{}) {
		k, ok := key.([]byte)
		if !ok {
			return
		}
		versions, ok := value.([]VersionedValue)
		if !ok {
			return
		}
		entries = append(entries, mvccSnapshotEntry{
			Key:      bytes.Clone(k),
			Versions: append([]VersionedValue(nil), versions...),
		})
	})

	snapshot := mvccSnapshot{
		LastCommitTS: s.lastCommitTS,
		Entries:      entries,
	}

	buf := &bytes.Buffer{}
	if err := gob.NewEncoder(buf).Encode(snapshot); err != nil {
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

	var snapshot mvccSnapshot
	if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&snapshot); err != nil {
		return errors.WithStack(err)
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.tree.Clear()
	s.lastCommitTS = snapshot.LastCommitTS
	for _, entry := range snapshot.Entries {
		versions := append([]VersionedValue(nil), entry.Versions...)
		s.tree.Put(bytes.Clone(entry.Key), versions)
	}

	return nil
}

func compactVersions(versions []VersionedValue, minTS uint64) ([]VersionedValue, bool) {
	if len(versions) == 0 {
		return versions, false
	}

	// Find the latest version that is <= minTS
	keepIdx := -1
	for i := len(versions) - 1; i >= 0; i-- {
		if versions[i].TS <= minTS {
			keepIdx = i
			break
		}
	}

	// If all versions are newer than minTS, keep everything
	if keepIdx == -1 {
		return versions, false
	}

	// If the oldest version is the one to keep, we can't remove anything before it
	if keepIdx == 0 {
		return versions, false
	}

	// We keep versions starting from keepIdx
	// The version at keepIdx represents the state at minTS.
	newVersions := make([]VersionedValue, len(versions)-keepIdx)
	copy(newVersions, versions[keepIdx:])
	return newVersions, true
}

func (s *mvccStore) Compact(ctx context.Context, minTS uint64) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	// Estimate size to avoid frequent allocations, though exact count is unknown
	updates := make(map[string][]VersionedValue)

	it := s.tree.Iterator()
	for it.Next() {
		versions, ok := it.Value().([]VersionedValue)
		if !ok {
			continue
		}

		newVersions, changed := compactVersions(versions, minTS)
		if changed {
			// tree keys are []byte, need string for map key
			keyBytes, ok := it.Key().([]byte)
			if !ok {
				continue
			}
			updates[string(keyBytes)] = newVersions
		}
	}

	for k, v := range updates {
		s.tree.Put([]byte(k), v)
	}

	s.log.InfoContext(ctx, "compact",
		slog.Uint64("min_ts", minTS),
		slog.Int("updated_keys", len(updates)),
	)
	return nil
}

func (s *mvccStore) Close() error {
	return nil
}
