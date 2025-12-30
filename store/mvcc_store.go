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
	"time"

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
	hlcLogicalBits = 16
	msPerSecond    = 1000
)

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
	clock        HybridClock
}

// NewMVCCStore creates a new MVCC-enabled in-memory store.
func NewMVCCStore() MVCCStore {
	return NewMVCCStoreWithClock(defaultHLC{})
}

// NewMVCCStoreWithClock allows injecting a hybrid clock (for tests or cluster-wide clocks).
func NewMVCCStoreWithClock(clock HybridClock) MVCCStore {
	return &mvccStore{
		tree: treemap.NewWith(byteSliceComparator),
		log: slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelWarn,
		})),
		clock: clock,
	}
}

type defaultHLC struct{}

func nonNegativeMillis() uint64 {
	nowMs := time.Now().UnixMilli()
	if nowMs < 0 {
		return 0
	}
	return uint64(nowMs)
}

func (defaultHLC) Now() uint64 {
	return nonNegativeMillis() << hlcLogicalBits
}

var _ MVCCStore = (*mvccStore)(nil)
var _ ScanStore = (*mvccStore)(nil)
var _ Store = (*mvccStore)(nil)

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

func visibleTxnValue(tv mvccTxnValue, now uint64) ([]byte, bool) {
	if tv.tombstone {
		return nil, false
	}
	if tv.expireAt != 0 && tv.expireAt <= now {
		return nil, false
	}
	return tv.value, true
}

func cloneKVPair(key, val []byte) *KVPair {
	return &KVPair{
		Key:   bytes.Clone(key),
		Value: bytes.Clone(val),
	}
}

type iterEntry struct {
	key      []byte
	ok       bool
	versions []VersionedValue
	stageVal mvccTxnValue
}

func nextBaseEntry(it *treemap.Iterator, start, end []byte) iterEntry {
	for it.Next() {
		k, ok := it.Key().([]byte)
		if !ok {
			continue
		}
		if !withinBoundsKey(k, start, end) {
			if end != nil && bytes.Compare(k, end) > 0 {
				return iterEntry{}
			}
			continue
		}
		versions, _ := it.Value().([]VersionedValue)
		return iterEntry{key: k, ok: true, versions: versions}
	}
	return iterEntry{}
}

func nextStageEntry(it *treemap.Iterator, start, end []byte) iterEntry {
	for it.Next() {
		k, ok := it.Key().([]byte)
		if !ok {
			continue
		}
		if !withinBoundsKey(k, start, end) {
			if end != nil && bytes.Compare(k, end) > 0 {
				return iterEntry{}
			}
			continue
		}
		val, _ := it.Value().(mvccTxnValue)
		return iterEntry{key: k, ok: true, stageVal: val}
	}
	return iterEntry{}
}

func (s *mvccStore) nextCommitTSLocked() uint64 {
	return s.alignCommitTS(s.clock.Now())
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

func (s *mvccStore) ttlExpireAt(ttl int64) uint64 {
	now := s.readTS()
	if ttl <= 0 {
		return now
	}
	// ttl is seconds; convert to milliseconds then shift to HLC layout.
	deltaMs := uint64(ttl) * msPerSecond
	return now + (deltaMs << hlcLogicalBits)
}

func (s *mvccStore) readTS() uint64 {
	now := s.clock.Now()
	if now < s.lastCommitTS {
		return s.lastCommitTS
	}
	return now
}

func (s *mvccStore) alignCommitTS(commitTS uint64) uint64 {
	ts := commitTS
	read := s.readTS()
	if ts < read {
		ts = read
	}
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

func (s *mvccStore) Get(_ context.Context, key []byte) ([]byte, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	now := s.readTS()

	v, ok := s.tree.Get(key)
	if !ok {
		return nil, ErrKeyNotFound
	}
	versions, _ := v.([]VersionedValue)
	val, ok := visibleValue(versions, now)
	if !ok {
		return nil, ErrKeyNotFound
	}
	return bytes.Clone(val), nil
}

func (s *mvccStore) Put(ctx context.Context, key []byte, value []byte) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.putVersionLocked(key, value, s.nextCommitTSLocked(), 0)
	s.log.InfoContext(ctx, "put",
		slog.String("key", string(key)),
		slog.String("value", string(value)),
	)
	return nil
}

func (s *mvccStore) PutWithTTL(ctx context.Context, key []byte, value []byte, ttl int64) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	exp := s.ttlExpireAt(ttl)
	s.putVersionLocked(key, value, s.nextCommitTSLocked(), exp)
	s.log.InfoContext(ctx, "put_ttl",
		slog.String("key", string(key)),
		slog.String("value", string(value)),
		slog.Int64("ttl_sec", ttl),
	)
	return nil
}

func (s *mvccStore) Delete(ctx context.Context, key []byte) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.deleteVersionLocked(key, s.nextCommitTSLocked())
	s.log.InfoContext(ctx, "delete",
		slog.String("key", string(key)),
	)
	return nil
}

func (s *mvccStore) Exists(_ context.Context, key []byte) (bool, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	now := s.readTS()

	v, ok := s.tree.Get(key)
	if !ok {
		return false, nil
	}
	versions, _ := v.([]VersionedValue)
	if len(versions) == 0 {
		return false, nil
	}
	ver, ok := latestVisible(versions, now)
	if !ok {
		return false, nil
	}
	return !ver.Tombstone, nil
}

func (s *mvccStore) Expire(ctx context.Context, key []byte, ttl int64) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	now := s.clock.Now()
	v, ok := s.tree.Get(key)
	if !ok {
		return ErrKeyNotFound
	}
	versions, _ := v.([]VersionedValue)
	if len(versions) == 0 {
		return ErrKeyNotFound
	}
	ver := versions[len(versions)-1]
	if ver.Tombstone || (ver.ExpireAt != 0 && ver.ExpireAt <= now) {
		return ErrKeyNotFound
	}

	exp := s.ttlExpireAt(ttl)
	s.putVersionLocked(key, ver.Value, s.nextCommitTSLocked(), exp)
	s.log.InfoContext(ctx, "expire",
		slog.String("key", string(key)),
		slog.Int64("ttl_sec", ttl),
	)
	return nil
}

func (s *mvccStore) Scan(_ context.Context, start []byte, end []byte, limit int) ([]*KVPair, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	if limit <= 0 {
		return []*KVPair{}, nil
	}

	now := s.readTS()

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
		val, ok := visibleValue(versions, now)
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

func (s *mvccStore) Txn(ctx context.Context, f func(ctx context.Context, txn Txn) error) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	txn := &mvccTxn{
		stage: treemap.NewWith(byteSliceComparator),
		ops:   []mvccOp{},
		s:     s,
	}

	if err := f(ctx, txn); err != nil {
		return errors.WithStack(err)
	}

	commitTS := s.nextCommitTSLocked()
	for _, op := range txn.ops {
		switch op.opType {
		case OpTypePut:
			s.putVersionLocked(op.key, op.value, commitTS, op.expireAt)
		case OpTypeDelete:
			s.deleteVersionLocked(op.key, commitTS)
		default:
			return errors.WithStack(ErrUnknownOp)
		}
	}

	return nil
}

func (s *mvccStore) TxnWithTTL(ctx context.Context, f func(ctx context.Context, txn TTLTxn) error) error {
	return s.Txn(ctx, func(ctx context.Context, txn Txn) error {
		tt, ok := txn.(*mvccTxn)
		if !ok {
			return ErrNotSupported
		}
		return f(ctx, tt)
	})
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

// ---- transactional staging ----

type mvccOp struct {
	opType   OpType
	key      []byte
	value    []byte
	expireAt uint64
}

type mvccTxn struct {
	stage *treemap.Map // key []byte -> mvccTxnValue
	ops   []mvccOp
	s     *mvccStore
}

type mvccTxnValue struct {
	value     []byte
	tombstone bool
	expireAt  uint64
}

func (t *mvccTxn) Get(_ context.Context, key []byte) ([]byte, error) {
	if v, ok := t.stage.Get(key); ok {
		tv, _ := v.(mvccTxnValue)
		if tv.tombstone {
			return nil, ErrKeyNotFound
		}
		if tv.expireAt != 0 && tv.expireAt <= t.s.clock.Now() {
			return nil, ErrKeyNotFound
		}
		return bytes.Clone(tv.value), nil
	}

	return t.s.Get(context.Background(), key)
}

func (t *mvccTxn) Put(_ context.Context, key []byte, value []byte) error {
	t.stage.Put(key, mvccTxnValue{value: bytes.Clone(value)})
	t.ops = append(t.ops, mvccOp{opType: OpTypePut, key: bytes.Clone(key), value: bytes.Clone(value)})
	return nil
}

func (t *mvccTxn) Delete(_ context.Context, key []byte) error {
	t.stage.Put(key, mvccTxnValue{tombstone: true})
	t.ops = append(t.ops, mvccOp{opType: OpTypeDelete, key: bytes.Clone(key)})
	return nil
}

func (t *mvccTxn) Exists(_ context.Context, key []byte) (bool, error) {
	if v, ok := t.stage.Get(key); ok {
		tv, _ := v.(mvccTxnValue)
		if tv.expireAt != 0 && tv.expireAt <= t.s.clock.Now() {
			return false, nil
		}
		return !tv.tombstone, nil
	}
	return t.s.Exists(context.Background(), key)
}

func (t *mvccTxn) Expire(_ context.Context, key []byte, ttl int64) error {
	exp := t.s.ttlExpireAt(ttl)

	if v, ok := t.stage.Get(key); ok {
		tv, _ := v.(mvccTxnValue)
		if tv.tombstone {
			return ErrKeyNotFound
		}
		tv.expireAt = exp
		t.stage.Put(key, tv)
		t.ops = append(t.ops, mvccOp{opType: OpTypePut, key: bytes.Clone(key), value: bytes.Clone(tv.value), expireAt: exp})
		return nil
	}

	val, err := t.s.Get(context.Background(), key)
	if err != nil {
		return err
	}
	t.stage.Put(key, mvccTxnValue{value: bytes.Clone(val), expireAt: exp})
	t.ops = append(t.ops, mvccOp{opType: OpTypePut, key: bytes.Clone(key), value: bytes.Clone(val), expireAt: exp})
	return nil
}

func (t *mvccTxn) PutWithTTL(_ context.Context, key []byte, value []byte, ttl int64) error {
	exp := t.s.ttlExpireAt(ttl)
	t.stage.Put(key, mvccTxnValue{value: bytes.Clone(value), expireAt: exp})
	t.ops = append(t.ops, mvccOp{opType: OpTypePut, key: bytes.Clone(key), value: bytes.Clone(value), expireAt: exp})
	return nil
}

func (t *mvccTxn) Scan(_ context.Context, start []byte, end []byte, limit int) ([]*KVPair, error) {
	if limit <= 0 {
		return []*KVPair{}, nil
	}

	totalSize := t.s.tree.Size() + t.stage.Size()
	capHint := limit
	if totalSize < capHint {
		capHint = totalSize
	}
	if capHint < 0 {
		capHint = 0
	}

	result := make([]*KVPair, 0, capHint)
	now := t.s.clock.Now()

	baseIt := t.s.tree.Iterator()
	baseIt.Begin()
	stageIt := t.stage.Iterator()
	stageIt.Begin()

	result = mergeTxnEntries(result, limit, start, end, now, &baseIt, &stageIt)

	return result, nil
}

func mergeTxnEntries(result []*KVPair, limit int, start []byte, end []byte, now uint64, baseIt, stageIt *treemap.Iterator) []*KVPair {
	baseNext := nextBaseEntry(baseIt, start, end)
	stageNext := nextStageEntry(stageIt, start, end)

	for len(result) < limit && (baseNext.ok || stageNext.ok) {
		useStage := chooseStage(baseNext, stageNext)

		if useStage {
			k := stageNext.key
			if val, visible := visibleTxnValue(stageNext.stageVal, now); visible {
				result = append(result, cloneKVPair(k, val))
			}
			if baseNext.ok && bytes.Equal(baseNext.key, k) {
				baseNext = nextBaseEntry(baseIt, start, end)
			}
			stageNext = nextStageEntry(stageIt, start, end)
			continue
		}

		if val, ok := visibleValue(baseNext.versions, now); ok {
			result = append(result, cloneKVPair(baseNext.key, val))
		}
		baseNext = nextBaseEntry(baseIt, start, end)
	}

	return result
}

func chooseStage(baseNext, stageNext iterEntry) bool {
	if !baseNext.ok {
		return stageNext.ok
	}
	if !stageNext.ok {
		return false
	}
	return bytes.Compare(stageNext.key, baseNext.key) <= 0
}

// mvccSnapshotEntry is used solely for gob snapshot serialization.
type mvccSnapshotEntry struct {
	Key      []byte
	Versions []VersionedValue
}
