package store

import (
	"bufio"
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
	checksumSize            = 4
	mvccSnapshotVersion     = uint32(1)
	maxSnapshotKeySize      = 1 << 20        // 1 MiB per key
	maxSnapshotVersionCount = 1 << 20        // 1M versions per key
	maxSnapshotValueSize    = 256 << 20      // 256 MiB per value; prevents OOM from malformed snapshots
)

var mvccSnapshotMagic = [8]byte{'E', 'K', 'V', 'M', 'V', 'C', 'C', '2'}

// mvccSnapshot is retained for backward compatibility with older gob-backed
// snapshots that were materialized fully in memory.
type mvccSnapshot struct {
	LastCommitTS  uint64
	MinRetainedTS uint64
	Entries       []mvccSnapshotEntry
}

// mvccSnapshotEntry is used solely for gob snapshot serialization.
type mvccSnapshotEntry struct {
	Key      []byte
	Versions []VersionedValue
}

func byteSliceComparator(a, b any) int {
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
	tree          *treemap.Map // key []byte -> []VersionedValue
	mtx           sync.RWMutex
	log           *slog.Logger
	lastCommitTS  uint64
	minRetainedTS uint64
}

// LastCommitTS exposes the latest commit timestamp for read snapshot selection.
// It is intentionally not part of the public MVCCStore interface.
func (s *mvccStore) LastCommitTS() uint64 {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.lastCommitTS
}

func (s *mvccStore) MinRetainedTS() uint64 {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.minRetainedTS
}

func (s *mvccStore) SetMinRetainedTS(ts uint64) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if ts > s.minRetainedTS {
		s.minRetainedTS = ts
	}
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
var _ RetentionController = (*mvccStore)(nil)

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
	if n := len(versions); n == 0 || versions[n-1].TS < vv.TS {
		return append(versions, vv)
	}
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
	if err := validateValueSize(value); err != nil {
		return err
	}
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
	s.mtx.RLock()
	defer s.mtx.RUnlock()
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

func readTSCompacted(ts, minRetainedTS uint64) bool {
	return minRetainedTS != 0 && ts != 0 && ts != ^uint64(0) && ts < minRetainedTS
}

// ---- MVCCStore methods ----

func (s *mvccStore) GetAt(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	if readTSCompacted(ts, s.minRetainedTS) {
		return nil, ErrReadTSCompacted
	}

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
	if readTSCompacted(ts, s.minRetainedTS) {
		return false, ErrReadTSCompacted
	}

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
	if readTSCompacted(ts, s.minRetainedTS) {
		return nil, ErrReadTSCompacted
	}

	if limit <= 0 {
		return []*KVPair{}, nil
	}

	capHint := boundedScanResultCapacity(limit)
	if size := s.tree.Size(); size < capHint {
		capHint = size
	}
	if capHint < 0 {
		capHint = 0
	}

	result := make([]*KVPair, 0, capHint)
	s.tree.Each(func(key any, value any) {
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

func (s *mvccStore) ReverseScanAt(_ context.Context, start []byte, end []byte, limit int, ts uint64) ([]*KVPair, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	if readTSCompacted(ts, s.minRetainedTS) {
		return nil, ErrReadTSCompacted
	}

	if limit <= 0 {
		return []*KVPair{}, nil
	}

	capHint := boundedScanResultCapacity(limit)
	if size := s.tree.Size(); size < capHint {
		capHint = size
	}
	if capHint < 0 {
		capHint = 0
	}

	result := make([]*KVPair, 0, capHint)
	it := s.tree.Iterator()
	if !seekReverseIteratorStart(s.tree, &it, end) {
		return result, nil
	}
	collectReverseVisibleKVs(&it, start, limit, ts, &result)

	return result, nil
}

func seekReverseIteratorStart(tree *treemap.Map, it *treemap.Iterator, end []byte) bool {
	if end == nil {
		return it.Last()
	}
	floorKey, _ := tree.Floor(end)
	if floorKey == nil {
		return false
	}
	target, ok := floorKey.([]byte)
	if !ok {
		return false
	}
	it.End()
	if bytes.Compare(target, end) < 0 {
		return it.PrevTo(func(key any, value any) bool {
			k, keyOK := key.([]byte)
			return keyOK && bytes.Equal(k, target)
		})
	}
	return it.PrevTo(func(key any, value any) bool {
		k, keyOK := key.([]byte)
		return keyOK && bytes.Compare(k, end) < 0
	})
}

func collectReverseVisibleKVs(
	it *treemap.Iterator,
	start []byte,
	limit int,
	ts uint64,
	result *[]*KVPair,
) {
	for ok := true; ok && len(*result) < limit; ok = it.Prev() {
		k, keyOK := it.Key().([]byte)
		if !keyOK {
			continue
		}
		if start != nil && bytes.Compare(k, start) < 0 {
			return
		}
		appendVisibleReverseKV(it, k, ts, result)
	}
}

func appendVisibleReverseKV(it *treemap.Iterator, key []byte, ts uint64, result *[]*KVPair) {
	versions, _ := it.Value().([]VersionedValue)
	val, visible := visibleValue(versions, ts)
	if !visible {
		return
	}
	*result = append(*result, &KVPair{
		Key:   bytes.Clone(key),
		Value: bytes.Clone(val),
	})
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
			return NewWriteConflictError(mut.Key)
		}
	}

	commitTS = s.alignCommitTS(commitTS)

	for _, mut := range mutations {
		switch mut.Op {
		case OpTypePut:
			if err := validateValueSize(mut.Value); err != nil {
				return err
			}
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

func (s *mvccStore) Snapshot() (Snapshot, error) {
	f, err := os.CreateTemp("", "elastickv-mvcc-snapshot-*")
	if err != nil {
		return nil, errors.WithStack(err)
	}
	path := f.Name()
	if err := s.writeSnapshotFile(f); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return nil, err
	}
	return newFileSnapshot(f, path), nil
}

func (s *mvccStore) Restore(r io.Reader) error {
	br := bufio.NewReader(r)
	if streaming, err := isStreamingMVCCSnapshot(br); err != nil {
		return errors.WithStack(err)
	} else if streaming {
		return s.restoreStreamingSnapshot(br)
	}
	return s.restoreLegacySnapshot(br)
}

func isStreamingMVCCSnapshot(r *bufio.Reader) (bool, error) {
	header, err := r.Peek(len(mvccSnapshotMagic))
	if err != nil {
		if errors.Is(err, io.EOF) {
			return false, nil
		}
		return false, errors.WithStack(err)
	}
	return bytes.Equal(header, mvccSnapshotMagic[:]), nil
}

func (s *mvccStore) restoreLegacySnapshot(r io.Reader) error {
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
	s.minRetainedTS = snapshot.MinRetainedTS
	for _, entry := range snapshot.Entries {
		versions := append([]VersionedValue(nil), entry.Versions...)
		s.tree.Put(bytes.Clone(entry.Key), versions)
	}

	return nil
}

func (s *mvccStore) writeSnapshotFile(f *os.File) error {
	checksumOffset, err := writeMVCCSnapshotHeader(f)
	if err != nil {
		return err
	}

	sum, err := s.writeSnapshotBody(f)
	if err != nil {
		return err
	}

	return finalizeMVCCSnapshotFile(f, checksumOffset, sum)
}

func writeMVCCSnapshotHeader(f *os.File) (int64, error) {
	if _, err := f.Write(mvccSnapshotMagic[:]); err != nil {
		return 0, errors.WithStack(err)
	}
	if err := binary.Write(f, binary.LittleEndian, mvccSnapshotVersion); err != nil {
		return 0, errors.WithStack(err)
	}
	checksumOffset, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	if err := binary.Write(f, binary.LittleEndian, uint32(0)); err != nil {
		return 0, errors.WithStack(err)
	}
	return checksumOffset, nil
}

func (s *mvccStore) writeSnapshotBody(f *os.File) (uint32, error) {
	hash := crc32.NewIEEE()
	bw := bufio.NewWriter(f)
	w := io.MultiWriter(bw, hash)

	s.mtx.RLock()
	defer s.mtx.RUnlock()

	if err := binary.Write(w, binary.LittleEndian, s.lastCommitTS); err != nil {
		return 0, errors.WithStack(err)
	}
	if err := binary.Write(w, binary.LittleEndian, s.minRetainedTS); err != nil {
		return 0, errors.WithStack(err)
	}
	iter := s.tree.Iterator()
	for iter.Next() {
		key, ok := iter.Key().([]byte)
		if !ok {
			continue
		}
		versions, ok := iter.Value().([]VersionedValue)
		if !ok {
			continue
		}
		if err := writeMVCCSnapshotEntry(w, key, versions); err != nil {
			return 0, err
		}
	}
	if err := bw.Flush(); err != nil {
		return 0, errors.WithStack(err)
	}
	return hash.Sum32(), nil
}

func finalizeMVCCSnapshotFile(f *os.File, checksumOffset int64, sum uint32) error {
	if _, err := f.Seek(checksumOffset, io.SeekStart); err != nil {
		return errors.WithStack(err)
	}
	if err := binary.Write(f, binary.LittleEndian, sum); err != nil {
		return errors.WithStack(err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func writeMVCCSnapshotEntry(w io.Writer, key []byte, versions []VersionedValue) error {
	if err := binary.Write(w, binary.LittleEndian, uint64(len(key))); err != nil {
		return errors.WithStack(err)
	}
	if _, err := w.Write(key); err != nil {
		return errors.WithStack(err)
	}
	if err := binary.Write(w, binary.LittleEndian, uint64(len(versions))); err != nil {
		return errors.WithStack(err)
	}
	for _, version := range versions {
		if err := writeMVCCSnapshotVersion(w, version); err != nil {
			return err
		}
	}
	return nil
}

func writeMVCCSnapshotVersion(w io.Writer, version VersionedValue) error {
	if err := binary.Write(w, binary.LittleEndian, version.TS); err != nil {
		return errors.WithStack(err)
	}
	if _, err := w.Write([]byte{mvccSnapshotTombstoneByte(version.Tombstone)}); err != nil {
		return errors.WithStack(err)
	}
	if err := binary.Write(w, binary.LittleEndian, version.ExpireAt); err != nil {
		return errors.WithStack(err)
	}
	if err := binary.Write(w, binary.LittleEndian, uint64(len(version.Value))); err != nil {
		return errors.WithStack(err)
	}
	if _, err := w.Write(version.Value); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func mvccSnapshotTombstoneByte(tombstone bool) byte {
	if tombstone {
		return 1
	}
	return 0
}

func (s *mvccStore) restoreStreamingSnapshot(r io.Reader) error {
	expected, err := readMVCCSnapshotHeader(r)
	if err != nil {
		return err
	}

	tree, lastCommitTS, minRetainedTS, actual, err := restoreStreamingMVCCSnapshotBody(r)
	if err != nil {
		return err
	}
	if actual != expected {
		return errors.WithStack(ErrInvalidChecksum)
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.tree = tree
	s.lastCommitTS = lastCommitTS
	s.minRetainedTS = minRetainedTS
	return nil
}

func readMVCCSnapshotHeader(r io.Reader) (uint32, error) {
	var magic [8]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return 0, errors.WithStack(err)
	}
	if magic != mvccSnapshotMagic {
		return 0, errors.WithStack(ErrInvalidChecksum)
	}

	var version uint32
	if err := binary.Read(r, binary.LittleEndian, &version); err != nil {
		return 0, errors.WithStack(err)
	}
	if version != mvccSnapshotVersion {
		return 0, errors.WithStack(errors.Newf("unsupported mvcc snapshot version %d", version))
	}

	var expected uint32
	if err := binary.Read(r, binary.LittleEndian, &expected); err != nil {
		return 0, errors.WithStack(err)
	}
	return expected, nil
}

func restoreStreamingMVCCSnapshotBody(r io.Reader) (*treemap.Map, uint64, uint64, uint32, error) {
	hash := crc32.NewIEEE()
	body := io.TeeReader(r, hash)

	lastCommitTS, minRetainedTS, err := readMVCCSnapshotMetadata(body)
	if err != nil {
		return nil, 0, 0, 0, err
	}

	tree, err := readMVCCSnapshotTree(body)
	if err != nil {
		return nil, 0, 0, 0, err
	}

	return tree, lastCommitTS, minRetainedTS, hash.Sum32(), nil
}

func readMVCCSnapshotMetadata(r io.Reader) (uint64, uint64, error) {
	var lastCommitTS uint64
	if err := binary.Read(r, binary.LittleEndian, &lastCommitTS); err != nil {
		return 0, 0, errors.WithStack(err)
	}
	var minRetainedTS uint64
	if err := binary.Read(r, binary.LittleEndian, &minRetainedTS); err != nil {
		return 0, 0, errors.WithStack(err)
	}
	return lastCommitTS, minRetainedTS, nil
}

func readMVCCSnapshotTree(r io.Reader) (*treemap.Map, error) {
	tree := treemap.NewWith(byteSliceComparator)
	for {
		key, versions, eof, err := readMVCCSnapshotEntry(r)
		if err != nil {
			return nil, err
		}
		if eof {
			return tree, nil
		}
		tree.Put(key, versions)
	}
}

func readMVCCSnapshotEntry(r io.Reader) ([]byte, []VersionedValue, bool, error) {
	var keyLen uint64
	if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil, true, nil
		}
		return nil, nil, false, errors.WithStack(err)
	}
	if keyLen > maxSnapshotKeySize {
		return nil, nil, false, errors.Wrapf(ErrSnapshotKeyTooLarge, "%d > %d", keyLen, maxSnapshotKeySize)
	}

	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r, key); err != nil {
		return nil, nil, false, errors.WithStack(err)
	}

	var versionCount uint64
	if err := binary.Read(r, binary.LittleEndian, &versionCount); err != nil {
		return nil, nil, false, errors.WithStack(err)
	}
	if versionCount > maxSnapshotVersionCount {
		return nil, nil, false, errors.Wrapf(ErrSnapshotVersionCountTooLarge, "%d > %d", versionCount, maxSnapshotVersionCount)
	}
	versions := make([]VersionedValue, 0, versionCount)
	for i := uint64(0); i < versionCount; i++ {
		version, err := readMVCCSnapshotVersion(r)
		if err != nil {
			return nil, nil, false, err
		}
		versions = append(versions, version)
	}
	return key, versions, false, nil
}

func readMVCCSnapshotVersion(r io.Reader) (VersionedValue, error) {
	var ts uint64
	if err := binary.Read(r, binary.LittleEndian, &ts); err != nil {
		return VersionedValue{}, errors.WithStack(err)
	}

	var tombstone [1]byte
	if _, err := io.ReadFull(r, tombstone[:]); err != nil {
		return VersionedValue{}, errors.WithStack(err)
	}

	var expireAt uint64
	if err := binary.Read(r, binary.LittleEndian, &expireAt); err != nil {
		return VersionedValue{}, errors.WithStack(err)
	}

	var valueLen uint64
	if err := binary.Read(r, binary.LittleEndian, &valueLen); err != nil {
		return VersionedValue{}, errors.WithStack(err)
	}
	if valueLen > maxSnapshotValueSize {
		return VersionedValue{}, errors.Wrapf(ErrValueTooLarge, "%d > %d", valueLen, maxSnapshotValueSize)
	}
	value := make([]byte, valueLen)
	if _, err := io.ReadFull(r, value); err != nil {
		return VersionedValue{}, errors.WithStack(err)
	}

	return VersionedValue{
		TS:        ts,
		Value:     value,
		Tombstone: tombstone[0] != 0,
		ExpireAt:  expireAt,
	}, nil
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
	if minTS > s.minRetainedTS {
		s.minRetainedTS = minTS
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
