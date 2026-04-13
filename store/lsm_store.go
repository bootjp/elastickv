package store

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"hash"
	"hash/crc32"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

const (
	timestampSize            = 8
	valueHeaderSize          = 9 // 1 byte tombstone + 8 bytes expireAt
	snapshotBatchCountLimit  = 1000
	snapshotBatchByteLimit   = 8 << 20 // 8 MiB; balances restore write amplification vs peak memory usage
	dirPerms                 = 0755
	metaLastCommitTS         = "_meta_last_commit_ts"
	metaMinRetainedTS        = "_meta_min_retained_ts"
	metaPendingMinRetainedTS = "_meta_pending_min_retained_ts"
	spoolBufSize             = 32 * 1024 // buffer size for streaming I/O during restore

	// maxPebbleEncodedKeySize is the limit for encoded Pebble on-disk keys,
	// which are the user key concatenated with the 8-byte inverted timestamp.
	// Using maxSnapshotKeySize+timestampSize (instead of just maxSnapshotKeySize)
	// avoids rejecting keys that are valid at the user-key level but slightly
	// exceed maxSnapshotKeySize once the timestamp suffix is appended.
	maxPebbleEncodedKeySize = maxSnapshotKeySize + timestampSize
)

var metaLastCommitTSBytes = []byte(metaLastCommitTS)
var metaMinRetainedTSBytes = []byte(metaMinRetainedTS)
var metaPendingMinRetainedTSBytes = []byte(metaPendingMinRetainedTS)

// pebbleStore implements MVCCStore using CockroachDB's Pebble LSM tree.
//
// Lock ordering (must always be acquired in this order to avoid deadlocks):
//
//  1. maintenanceMu – serialises Compact/Restore/Close.
//  2. dbMu          – guards the s.db pointer; held as a write-lock while the
//     DB is being swapped (Restore/Close), and as a read-lock by every
//     operation that accesses s.db.
//  3. mtx           – guards the in-memory metadata fields
//     (lastCommitTS, minRetainedTS, pendingMinRetainedTS).
type pebbleStore struct {
	db                   *pebble.DB
	dbMu                 sync.RWMutex // guards s.db pointer – see lock ordering above
	log                  *slog.Logger
	lastCommitTS         uint64
	minRetainedTS        uint64
	pendingMinRetainedTS uint64
	mtx                  sync.RWMutex
	applyMu              sync.Mutex // serializes ApplyMutations: conflict check → commit
	maintenanceMu        sync.Mutex
	dir                  string
}

// Ensure pebbleStore implements MVCCStore and RetentionController.
var _ MVCCStore = (*pebbleStore)(nil)
var _ RetentionController = (*pebbleStore)(nil)

// PebbleStoreOption configures the PebbleStore.
type PebbleStoreOption func(*pebbleStore)

// WithPebbleLogger sets a custom logger.
func WithPebbleLogger(l *slog.Logger) PebbleStoreOption {
	return func(s *pebbleStore) {
		s.log = l
	}
}

// defaultPebbleOptions returns the standard Pebble options used throughout
// the store (including restores) to ensure consistent behaviour between a
// freshly opened and a restored/swapped-in database.
func defaultPebbleOptions() *pebble.Options {
	opts := &pebble.Options{FS: vfs.Default}
	// Enable automatic compactions and apply all other Pebble defaults.
	opts.EnsureDefaults()
	return opts
}

// NewPebbleStore creates a new Pebble-backed MVCC store.
func NewPebbleStore(dir string, opts ...PebbleStoreOption) (MVCCStore, error) {
	s := &pebbleStore{
		dir: dir,
		log: slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelWarn,
		})),
	}
	for _, opt := range opts {
		opt(s)
	}

	db, err := pebble.Open(dir, defaultPebbleOptions())
	if err != nil {
		return nil, errors.WithStack(err)
	}
	s.db = db

	// Initialize lastCommitTS by scanning specifically or persisting it separately.
	// For simplicity, we scan on startup to find the max TS.
	// In a production system, this should be stored in a separate meta key.
	maxTS, err := s.findMaxCommitTS()
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	minRetainedTS, err := s.findMinRetainedTS()
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	pendingMinRetainedTS, err := s.findPendingMinRetainedTS()
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	s.lastCommitTS = maxTS
	s.minRetainedTS = minRetainedTS
	s.pendingMinRetainedTS = pendingMinRetainedTS

	return s, nil
}

// Key encoding: UserKey + Separator(0x00) + Timestamp(BigEndian, BitInverted)
// We use 0x00 as separator. UserKey must not contain 0x00 ideally, or we need escaping.
// For this implementation, we assume keys can contain anything, so we might need a safer encoding scheme.
// A common approach is: LengthPrefixedKey + Timestamp.
// Let's use: UserKey + 0x00 + ^Timestamp (8 bytes).
// Note: If UserKey contains 0x00, it might confuse logic if we scan blindly.
// Better: Encode key length or ensure a terminator.
// Since the instruction is "implement LSM Store", we'll stick to a simple encoding:
// Key ++ TS. But we need to separate Key from TS during iteration.
// We will use a fixed-length suffix for TS (8 bytes).
// Key = [UserKeyBytes] [8-byte Inverted Timestamp]

func encodeKey(key []byte, ts uint64) []byte {
	k := make([]byte, encodedKeyLen(key))
	fillEncodedKey(k, key, ts)
	return k
}

func encodedKeyLen(key []byte) int {
	return len(key) + timestampSize
}

func fillEncodedKey(dst []byte, key []byte, ts uint64) {
	copy(dst, key)
	// Invert TS for descending order (newer first)
	binary.BigEndian.PutUint64(dst[len(key):], ^ts)
}

// keyUpperBound returns the smallest key that is strictly greater than all
// encoded keys with the given userKey prefix (i.e. the next lexicographic
// prefix after key). Returns nil when the key consists entirely of 0xFF bytes
// (no finite upper bound exists). This is used as the UpperBound in Pebble
// IterOptions to tightly confine iteration to a single user key.
func keyUpperBound(key []byte) []byte {
	upper := make([]byte, len(key))
	copy(upper, key)
	for i := len(upper) - 1; i >= 0; i-- {
		upper[i]++
		if upper[i] != 0 {
			return upper[:i+1]
		}
	}
	return nil // key is all 0xFF; no finite upper bound
}

func decodeKey(k []byte) ([]byte, uint64) {
	if len(k) < timestampSize {
		return nil, 0
	}
	keyLen := len(k) - timestampSize
	key := make([]byte, keyLen)
	copy(key, k[:keyLen])
	invTs := binary.BigEndian.Uint64(k[keyLen:])
	return key, ^invTs
}

// decodeKeyView returns the user-key portion of k as a slice into k (no
// allocation) together with the decoded timestamp. Returns nil, 0 if k is too
// short to hold a timestamp suffix.
func decodeKeyView(k []byte) ([]byte, uint64) {
	if len(k) < timestampSize {
		return nil, 0
	}
	keyLen := len(k) - timestampSize
	invTs := binary.BigEndian.Uint64(k[keyLen:])
	return k[:keyLen], ^invTs
}

// Value encoding: We use gob to encode VersionedValue structure minus the key/ts which are in the key.
type storedValue struct {
	Value     []byte
	Tombstone bool
	ExpireAt  uint64
}

func encodeValue(val []byte, tombstone bool, expireAt uint64) []byte {
	// Format: [Tombstone(1)] [ExpireAt(8)] [Value(...)]
	buf := make([]byte, encodedValueLen(len(val)))
	fillEncodedValue(buf, val, tombstone, expireAt)
	return buf
}

func encodedValueLen(valueLen int) int {
	return valueHeaderSize + valueLen
}

func fillEncodedValue(dst []byte, val []byte, tombstone bool, expireAt uint64) {
	if tombstone {
		dst[0] = 1
	} else {
		dst[0] = 0
	}
	binary.LittleEndian.PutUint64(dst[1:], expireAt)
	copy(dst[valueHeaderSize:], val)
}

func decodeValue(data []byte) (storedValue, error) {
	if len(data) < valueHeaderSize {
		return storedValue{}, errors.New("invalid value length")
	}
	tombstone := data[0] != 0
	expireAt := binary.LittleEndian.Uint64(data[1:])
	val := make([]byte, len(data)-valueHeaderSize)
	copy(val, data[valueHeaderSize:])

	return storedValue{
		Value:     val,
		Tombstone: tombstone,
		ExpireAt:  expireAt,
	}, nil
}

type pebbleUint64Getter interface {
	Get(key []byte) ([]byte, io.Closer, error)
}

func readPebbleUint64(src pebbleUint64Getter, key []byte) (uint64, error) {
	val, closer, err := src.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, nil
		}
		return 0, errors.WithStack(err)
	}
	defer func() { _ = closer.Close() }()
	if len(val) < timestampSize {
		return 0, nil
	}
	return binary.LittleEndian.Uint64(val), nil
}

func writePebbleUint64(db *pebble.DB, key []byte, value uint64, opts *pebble.WriteOptions) error {
	var buf [timestampSize]byte
	binary.LittleEndian.PutUint64(buf[:], value)
	return errors.WithStack(db.Set(key, buf[:], opts))
}

// writeTempDBMetadata writes lastCommitTS and minRetainedTS atomically in a
// single synced batch so that both values are either fully durable or fully
// absent after a crash.  This is critical for restore paths that swap a
// temporary Pebble directory into place: losing lastCommitTS could allow
// future commits to reuse timestamps, violating monotonic ordering.
func writeTempDBMetadata(db *pebble.DB, lastCommitTS, minRetainedTS uint64) error {
	batch := db.NewBatch()
	defer func() { _ = batch.Close() }()

	var buf [timestampSize]byte
	binary.LittleEndian.PutUint64(buf[:], lastCommitTS)
	if err := batch.Set(metaLastCommitTSBytes, buf[:], nil); err != nil {
		return errors.WithStack(err)
	}
	binary.LittleEndian.PutUint64(buf[:], minRetainedTS)
	if err := batch.Set(metaMinRetainedTSBytes, buf[:], nil); err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(batch.Commit(pebble.Sync))
}

func isPebbleMetaKey(rawKey []byte) bool {
	return bytes.Equal(rawKey, metaLastCommitTSBytes) ||
		bytes.Equal(rawKey, metaMinRetainedTSBytes) ||
		bytes.Equal(rawKey, metaPendingMinRetainedTSBytes)
}

func (s *pebbleStore) findMaxCommitTS() (uint64, error) {
	return readPebbleUint64(s.db, metaLastCommitTSBytes)
}

func (s *pebbleStore) findMinRetainedTS() (uint64, error) {
	return readPebbleUint64(s.db, metaMinRetainedTSBytes)
}

func (s *pebbleStore) findPendingMinRetainedTS() (uint64, error) {
	return readPebbleUint64(s.db, metaPendingMinRetainedTSBytes)
}

func (s *pebbleStore) saveLastCommitTS(ts uint64) error {
	return writePebbleUint64(s.db, metaLastCommitTSBytes, ts, pebble.NoSync)
}

func (s *pebbleStore) saveMinRetainedTS(ts uint64) error {
	return writePebbleUint64(s.db, metaMinRetainedTSBytes, ts, pebble.NoSync)
}

func (s *pebbleStore) savePendingMinRetainedTS(ts uint64, opts *pebble.WriteOptions) error {
	return writePebbleUint64(s.db, metaPendingMinRetainedTSBytes, ts, opts)
}

func (s *pebbleStore) LastCommitTS() uint64 {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.lastCommitTS
}

func (s *pebbleStore) MinRetainedTS() uint64 {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.minRetainedTS
}

func (s *pebbleStore) effectiveMinRetainedTSLocked() uint64 {
	if s.pendingMinRetainedTS > s.minRetainedTS {
		return s.pendingMinRetainedTS
	}
	return s.minRetainedTS
}

func (s *pebbleStore) effectiveMinRetainedTS() uint64 {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.effectiveMinRetainedTSLocked()
}

func (s *pebbleStore) updateLastCommitTS(ts uint64) {
	if ts > s.lastCommitTS {
		s.lastCommitTS = ts
	}
}

func (s *pebbleStore) updateAndPersistLastCommitTS(ts uint64) {
	if ts > s.lastCommitTS {
		s.lastCommitTS = ts
		// Best effort persist
		if err := s.saveLastCommitTS(ts); err != nil {
			s.log.Error("failed to persist last commit timestamp", slog.Any("error", err))
		}
	}
}

func (s *pebbleStore) setMinRetainedTSLocked(ts uint64) error {
	if ts <= s.minRetainedTS {
		if s.pendingMinRetainedTS != 0 && s.pendingMinRetainedTS <= ts {
			return s.clearPendingMinRetainedTSLocked(pebble.NoSync)
		}
		return nil
	}
	if err := s.saveMinRetainedTS(ts); err != nil {
		return err
	}
	s.minRetainedTS = ts
	if s.pendingMinRetainedTS != 0 && s.pendingMinRetainedTS <= ts {
		return s.clearPendingMinRetainedTSLocked(pebble.NoSync)
	}
	return nil
}

func (s *pebbleStore) SetMinRetainedTS(ts uint64) {
	// Acquire dbMu.RLock() before mtx.Lock() to respect the lock ordering
	// (dbMu before mtx); the underlying setMinRetainedTSLocked writes to s.db.
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if err := s.setMinRetainedTSLocked(ts); err != nil {
		s.log.Error("failed to persist min retained timestamp", slog.Any("error", err))
	}
}

func (s *pebbleStore) setPendingMinRetainedTSLocked(ts uint64, opts *pebble.WriteOptions) error {
	if ts <= s.pendingMinRetainedTS {
		return nil
	}
	if err := s.savePendingMinRetainedTS(ts, opts); err != nil {
		return err
	}
	s.pendingMinRetainedTS = ts
	return nil
}

func (s *pebbleStore) clearPendingMinRetainedTSLocked(opts *pebble.WriteOptions) error {
	if s.pendingMinRetainedTS == 0 {
		return nil
	}
	if err := errors.WithStack(s.db.Delete(metaPendingMinRetainedTSBytes, opts)); err != nil {
		return err
	}
	s.pendingMinRetainedTS = 0
	return nil
}

func setPebbleUint64InBatch(batch *pebble.Batch, key []byte, value uint64) error {
	var buf [timestampSize]byte
	binary.LittleEndian.PutUint64(buf[:], value)
	return errors.WithStack(batch.Set(key, buf[:], pebble.NoSync))
}

func (s *pebbleStore) commitMinRetainedTSLocked(ts uint64, opts *pebble.WriteOptions) error {
	if ts <= s.minRetainedTS && s.pendingMinRetainedTS == 0 {
		return nil
	}

	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()

	if ts > s.minRetainedTS {
		if err := setPebbleUint64InBatch(batch, metaMinRetainedTSBytes, ts); err != nil {
			return err
		}
	}
	if s.pendingMinRetainedTS != 0 {
		if err := batch.Delete(metaPendingMinRetainedTSBytes, pebble.NoSync); err != nil {
			return errors.WithStack(err)
		}
	}
	if err := errors.WithStack(batch.Commit(opts)); err != nil {
		return err
	}
	if ts > s.minRetainedTS {
		s.minRetainedTS = ts
	}
	s.pendingMinRetainedTS = 0
	return nil
}

func (s *pebbleStore) alignCommitTS(commitTS uint64) uint64 {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.updateAndPersistLastCommitTS(commitTS)
	return commitTS
}

// MVCC Implementation

// getAt is the internal implementation of GetAt.  It must be called while the
// caller holds at least s.dbMu.RLock().
func (s *pebbleStore) getAt(_ context.Context, key []byte, ts uint64) ([]byte, error) {
	if readTSCompacted(ts, s.effectiveMinRetainedTS()) {
		return nil, ErrReadTSCompacted
	}

	seekKey := encodeKey(key, ts)
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: seekKey,
		UpperBound: keyUpperBound(key),
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer iter.Close()

	if iter.SeekGE(seekKey) {
		k := iter.Key()
		userKey, _ := decodeKeyView(k)

		if !bytes.Equal(userKey, key) {
			// Moved to next user key
			return nil, ErrKeyNotFound
		}

		// Found a version. Check if valid.
		valBytes := iter.Value()
		sv, err := decodeValue(valBytes)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		if sv.Tombstone {
			return nil, ErrKeyNotFound
		}
		if sv.ExpireAt != 0 && sv.ExpireAt <= ts {
			return nil, ErrKeyNotFound
		}

		return sv.Value, nil
	}

	return nil, ErrKeyNotFound
}

func (s *pebbleStore) GetAt(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.getAt(ctx, key, ts)
}

func (s *pebbleStore) ExistsAt(ctx context.Context, key []byte, ts uint64) (bool, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

	// Use internal getAt to avoid a recursive dbMu.RLock() acquisition.
	val, err := s.getAt(ctx, key, ts)
	if errors.Is(err, ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return val != nil, nil
}

func (s *pebbleStore) processFoundValue(iter *pebble.Iterator, userKey []byte, ts uint64) (*KVPair, error) {
	valBytes := iter.Value()
	sv, err := decodeValue(valBytes)
	if err != nil {
		return nil, err
	}

	if !sv.Tombstone && (sv.ExpireAt == 0 || sv.ExpireAt > ts) {
		return &KVPair{
			Key:   userKey,
			Value: sv.Value,
		}, nil
	}
	return nil, nil
}

func (s *pebbleStore) seekToVisibleVersion(iter *pebble.Iterator, userKey []byte, currentVersion, ts uint64) bool {
	if currentVersion <= ts {
		return true
	}
	seekKey := encodeKey(userKey, ts)
	if !iter.SeekGE(seekKey) {
		return false
	}
	k := iter.Key()
	currentUserKey, _ := decodeKeyView(k)
	return bytes.Equal(currentUserKey, userKey)
}

func (s *pebbleStore) skipToNextUserKey(iter *pebble.Iterator, userKey []byte) bool {
	if !iter.SeekGE(encodeKey(userKey, 0)) {
		return false
	}
	k := iter.Key()
	u, _ := decodeKeyView(k)
	if bytes.Equal(u, userKey) {
		return iter.Next()
	}
	return true
}

func pastScanEnd(userKey, end []byte) bool {
	return end != nil && bytes.Compare(userKey, end) >= 0
}

func nextScannableUserKey(iter *pebble.Iterator) ([]byte, uint64, bool) {
	for iter.Valid() {
		rawKey := iter.Key()
		if isPebbleMetaKey(rawKey) {
			if !iter.Next() {
				return nil, 0, false
			}
			continue
		}
		userKey, version := decodeKey(rawKey)
		if userKey == nil {
			if !iter.Next() {
				return nil, 0, false
			}
			continue
		}
		return userKey, version, true
	}
	return nil, 0, false
}

func prevScannableUserKey(iter *pebble.Iterator) ([]byte, bool) {
	for iter.Valid() {
		rawKey := iter.Key()
		if isPebbleMetaKey(rawKey) {
			if !iter.Prev() {
				return nil, false
			}
			continue
		}
		userKey, _ := decodeKey(rawKey)
		if userKey == nil {
			if !iter.Prev() {
				return nil, false
			}
			continue
		}
		return userKey, true
	}
	return nil, false
}

func (s *pebbleStore) collectScanResults(iter *pebble.Iterator, start, end []byte, limit int, ts uint64) ([]*KVPair, error) {
	result := make([]*KVPair, 0, boundedScanResultCapacity(limit))

	for iter.SeekGE(encodeKey(start, math.MaxUint64)); iter.Valid() && len(result) < limit; {
		userKey, version, ok := nextScannableUserKey(iter)
		if !ok {
			break
		}

		if pastScanEnd(userKey, end) {
			break
		}

		if !s.seekToVisibleVersion(iter, userKey, version, ts) {
			continue
		}

		kv, err := s.processFoundValue(iter, userKey, ts)
		if err != nil {
			return nil, err
		}
		if kv != nil {
			result = append(result, kv)
		}

		if !s.skipToNextUserKey(iter, userKey) {
			break
		}
	}

	return result, nil
}

func (s *pebbleStore) ScanAt(ctx context.Context, start []byte, end []byte, limit int, ts uint64) ([]*KVPair, error) {
	if limit <= 0 {
		return []*KVPair{}, nil
	}

	// Acquire dbMu.RLock before reading effectiveMinRetainedTS (which takes
	// mtx.RLock) to preserve the global lock ordering: dbMu before mtx.
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

	if readTSCompacted(ts, s.effectiveMinRetainedTS()) {
		return nil, ErrReadTSCompacted
	}

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: encodeKey(start, math.MaxUint64),
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer iter.Close()

	return s.collectScanResults(iter, start, end, limit, ts)
}

func (s *pebbleStore) collectReverseScanResults(
	iter *pebble.Iterator,
	start []byte,
	end []byte,
	limit int,
	ts uint64,
) ([]*KVPair, error) {
	result := make([]*KVPair, 0, boundedScanResultCapacity(limit))

	valid := seekReverseScanStart(iter, end)
	for valid && len(result) < limit {
		kv, nextValid, done, err := s.nextReverseScanKV(iter, start, ts)
		if err != nil {
			return nil, err
		}
		valid = nextValid
		if done {
			break
		}
		if kv != nil {
			result = append(result, kv)
		}
	}

	return result, nil
}

func seekReverseScanStart(iter *pebble.Iterator, end []byte) bool {
	if end != nil {
		return iter.SeekLT(encodeKey(end, math.MaxUint64))
	}
	return iter.Last()
}

func (s *pebbleStore) nextReverseScanKV(
	iter *pebble.Iterator,
	start []byte,
	ts uint64,
) (*KVPair, bool, bool, error) {
	userKey, ok := prevScannableUserKey(iter)
	if !ok {
		return nil, false, true, nil
	}
	if start != nil && bytes.Compare(userKey, start) < 0 {
		return nil, false, true, nil
	}
	if !iter.SeekGE(encodeKey(userKey, ts)) {
		return nil, false, true, nil
	}
	currentUserKey, _ := decodeKeyView(iter.Key())
	if !bytes.Equal(currentUserKey, userKey) {
		nextValid := iter.SeekLT(encodeKey(userKey, math.MaxUint64))
		return nil, nextValid, false, nil
	}
	kv, err := s.processFoundValue(iter, userKey, ts)
	if err != nil {
		return nil, false, false, err
	}
	nextValid := iter.SeekLT(encodeKey(userKey, math.MaxUint64))
	return kv, nextValid, false, nil
}

func (s *pebbleStore) ReverseScanAt(ctx context.Context, start []byte, end []byte, limit int, ts uint64) ([]*KVPair, error) {
	if limit <= 0 {
		return []*KVPair{}, nil
	}

	// Acquire dbMu.RLock before reading effectiveMinRetainedTS (which takes
	// mtx.RLock) to preserve the global lock ordering: dbMu before mtx.
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

	if readTSCompacted(ts, s.effectiveMinRetainedTS()) {
		return nil, ErrReadTSCompacted
	}

	opts := &pebble.IterOptions{
		LowerBound: encodeKey(start, math.MaxUint64),
	}
	if end != nil {
		opts.UpperBound = encodeKey(end, math.MaxUint64)
	}
	iter, err := s.db.NewIter(opts)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer iter.Close()

	return s.collectReverseScanResults(iter, start, end, limit, ts)
}

func (s *pebbleStore) PutAt(ctx context.Context, key []byte, value []byte, commitTS uint64, expireAt uint64) error {
	if err := validateValueSize(value); err != nil {
		return err
	}
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	commitTS = s.alignCommitTS(commitTS)

	k := encodeKey(key, commitTS)
	v := encodeValue(value, false, expireAt)

	if err := s.db.Set(k, v, pebble.NoSync); err != nil { //nolint:wrapcheck
		return errors.WithStack(err)
	}
	s.log.InfoContext(ctx, "put_at", slog.String("key", string(key)), slog.Uint64("ts", commitTS))
	return nil
}

func (s *pebbleStore) DeleteAt(ctx context.Context, key []byte, commitTS uint64) error {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	commitTS = s.alignCommitTS(commitTS)

	k := encodeKey(key, commitTS)
	v := encodeValue(nil, true, 0)

	if err := s.db.Set(k, v, pebble.NoSync); err != nil {
		return errors.WithStack(err)
	}
	s.log.InfoContext(ctx, "delete_at", slog.String("key", string(key)), slog.Uint64("ts", commitTS))
	return nil
}

func (s *pebbleStore) PutWithTTLAt(ctx context.Context, key []byte, value []byte, commitTS uint64, expireAt uint64) error {
	return s.PutAt(ctx, key, value, commitTS, expireAt)
}

func (s *pebbleStore) ExpireAt(ctx context.Context, key []byte, expireAt uint64, commitTS uint64) error {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

	// Must read latest value first to preserve it; use internal getAt to
	// avoid a recursive dbMu.RLock() while we already hold it.
	val, err := s.getAt(ctx, key, commitTS)
	if err != nil {
		return err
	}

	commitTS = s.alignCommitTS(commitTS)
	k := encodeKey(key, commitTS)
	v := encodeValue(val, false, expireAt)
	if err := s.db.Set(k, v, pebble.NoSync); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// latestCommitTS is the internal implementation of LatestCommitTS.  It must be
// called while the caller holds at least s.dbMu.RLock().
func (s *pebbleStore) latestCommitTS(_ context.Context, key []byte) (uint64, bool, error) {
	// Peek latest version (SeekGE key + ^MaxUint64)
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: encodeKey(key, math.MaxUint64),
		UpperBound: keyUpperBound(key),
	})
	if err != nil {
		return 0, false, errors.WithStack(err)
	}
	defer iter.Close()

	if iter.First() {
		k := iter.Key()
		userKey, version := decodeKeyView(k)
		if bytes.Equal(userKey, key) {
			return version, true, nil
		}
	}
	return 0, false, nil
}

func (s *pebbleStore) LatestCommitTS(ctx context.Context, key []byte) (uint64, bool, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.latestCommitTS(ctx, key)
}

func (s *pebbleStore) checkConflicts(ctx context.Context, mutations []*KVPairMutation, startTS uint64) error {
	for _, mut := range mutations {
		// Use the internal latestCommitTS to avoid re-acquiring dbMu (the caller
		// – ApplyMutations – already holds dbMu.RLock()).
		ts, exists, err := s.latestCommitTS(ctx, mut.Key)
		if err != nil {
			return err
		}
		if exists && ts > startTS {
			return NewWriteConflictError(mut.Key)
		}
	}
	return nil
}

func (s *pebbleStore) checkReadConflicts(ctx context.Context, readKeys [][]byte, startTS uint64) error {
	for _, key := range readKeys {
		ts, exists, err := s.latestCommitTS(ctx, key)
		if err != nil {
			return err
		}
		if exists && ts > startTS {
			return NewWriteConflictError(key)
		}
	}
	return nil
}

func (s *pebbleStore) applyMutationsBatch(b *pebble.Batch, mutations []*KVPairMutation, commitTS uint64) error {
	for _, mut := range mutations {
		k := encodeKey(mut.Key, commitTS)
		var v []byte

		switch mut.Op {
		case OpTypePut:
			if err := validateValueSize(mut.Value); err != nil {
				return err
			}
			v = encodeValue(mut.Value, false, mut.ExpireAt)
		case OpTypeDelete:
			v = encodeValue(nil, true, 0)
		default:
			return ErrUnknownOp
		}
		if err := b.Set(k, v, nil); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (s *pebbleStore) ApplyMutations(ctx context.Context, mutations []*KVPairMutation, readKeys [][]byte, startTS, commitTS uint64) error {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

	// Serialize conflict check → batch commit so concurrent ApplyMutations
	// cannot both pass checkConflicts and then both commit.
	s.applyMu.Lock()
	defer s.applyMu.Unlock()

	b := s.db.NewBatch()
	defer b.Close()

	if err := s.checkConflicts(ctx, mutations, startTS); err != nil {
		return err
	}
	if err := s.checkReadConflicts(ctx, readKeys, startTS); err != nil {
		return err
	}

	if err := s.applyMutationsBatch(b, mutations, commitTS); err != nil {
		return err
	}

	// Hold mtx across read → batch-set → commit → in-memory update so that a
	// concurrent alignCommitTS (PutAt/DeleteAt/ExpireAt) cannot advance+persist
	// metaLastCommitTS between our read and batch commit, which would let this
	// batch overwrite the meta key with a smaller value.
	s.mtx.Lock()
	newLastTS := s.lastCommitTS
	if commitTS > newLastTS {
		newLastTS = commitTS
	}
	if err := setPebbleUint64InBatch(b, metaLastCommitTSBytes, newLastTS); err != nil {
		s.mtx.Unlock()
		return err
	}
	if err := b.Commit(pebble.Sync); err != nil {
		s.mtx.Unlock()
		return errors.WithStack(err)
	}
	s.updateLastCommitTS(newLastTS)
	s.mtx.Unlock()

	return nil
}

// DeletePrefixAt atomically deletes all visible keys matching prefix by writing
// tombstone versions at commitTS. An empty prefix deletes all keys. Keys
// matching excludePrefix are preserved. Uses an iterator-based approach that
// avoids loading values into caller memory.
func (s *pebbleStore) DeletePrefixAt(ctx context.Context, prefix []byte, excludePrefix []byte, commitTS uint64) error {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

	s.applyMu.Lock()
	defer s.applyMu.Unlock()

	var lowerBound []byte
	if len(prefix) > 0 {
		lowerBound = encodeKey(prefix, math.MaxUint64)
	}

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
	})
	if err != nil {
		return errors.WithStack(err)
	}
	defer iter.Close()

	batch := s.db.NewBatch()
	defer batch.Close()

	if err := s.scanDeletePrefix(iter, batch, prefix, excludePrefix, commitTS); err != nil {
		return err
	}

	// Persist lastCommitTS update atomically with the tombstones.
	s.mtx.Lock()
	defer s.mtx.Unlock()

	newLastTS := s.lastCommitTS
	if commitTS > newLastTS {
		newLastTS = commitTS
	}
	if err := setPebbleUint64InBatch(batch, metaLastCommitTSBytes, newLastTS); err != nil {
		return err
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return errors.WithStack(err)
	}
	s.updateLastCommitTS(newLastTS)

	return nil
}

func (s *pebbleStore) scanDeletePrefix(iter *pebble.Iterator, batch *pebble.Batch, prefix, excludePrefix []byte, commitTS uint64) error {
	tombstoneVal := encodeValue(nil, true, 0)

	for iter.SeekGE(encodeKey(prefix, math.MaxUint64)); iter.Valid(); {
		userKey, version, ok := nextScannableUserKey(iter)
		if !ok {
			break
		}

		action := s.classifyDeletePrefixKey(userKey, prefix, excludePrefix)
		if action == deletePrefixStop {
			break
		}
		if action == deletePrefixSkip {
			if !s.skipToNextUserKey(iter, userKey) {
				break
			}
			continue
		}

		needsTombstone, err := s.isVisibleLiveKey(iter, userKey, version, commitTS)
		if err != nil {
			return err
		}
		if needsTombstone {
			if err := batch.Set(encodeKey(userKey, commitTS), tombstoneVal, nil); err != nil {
				return errors.WithStack(err)
			}
		}
		if !s.skipToNextUserKey(iter, userKey) {
			break
		}
	}
	return nil
}

type deletePrefixAction int

const (
	deletePrefixProcess deletePrefixAction = iota
	deletePrefixSkip
	deletePrefixStop
)

func (s *pebbleStore) classifyDeletePrefixKey(userKey, prefix, excludePrefix []byte) deletePrefixAction {
	if len(prefix) > 0 && !bytes.HasPrefix(userKey, prefix) {
		return deletePrefixStop
	}
	if len(excludePrefix) > 0 && bytes.HasPrefix(userKey, excludePrefix) {
		return deletePrefixSkip
	}
	return deletePrefixProcess
}

// isVisibleLiveKey checks whether the key has a visible, non-tombstone,
// non-expired version at commitTS. It advances the iterator as a side effect.
func (s *pebbleStore) isVisibleLiveKey(iter *pebble.Iterator, userKey []byte, version, commitTS uint64) (bool, error) {
	if !s.seekToVisibleVersion(iter, userKey, version, commitTS) {
		return false, nil
	}
	sv, err := decodeValue(iter.Value())
	if err != nil {
		return false, errors.WithStack(err)
	}
	if sv.Tombstone || (sv.ExpireAt != 0 && sv.ExpireAt <= commitTS) {
		return false, nil
	}
	return true, nil
}

type pebbleCompactionStats struct {
	updatedKeys      int
	deletedVersions  int
	committedDeletes bool
}

func ensureContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func (s *pebbleStore) beginCompaction(minTS uint64) (bool, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if minTS <= s.effectiveMinRetainedTSLocked() {
		return false, nil
	}
	if err := s.setPendingMinRetainedTSLocked(minTS, pebble.Sync); err != nil {
		return false, err
	}
	return true, nil
}

func (s *pebbleStore) cleanupPendingCompaction(committedDeletes *bool) {
	if committedDeletes != nil && *committedDeletes {
		return
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()
	if err := s.clearPendingMinRetainedTSLocked(pebble.Sync); err != nil {
		s.log.Error("failed to clear pending min retained timestamp", slog.Any("error", err))
	}
}

func (s *pebbleStore) commitCompactionMinRetainedTS(minTS uint64) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return s.commitMinRetainedTSLocked(minTS, pebble.Sync)
}

func shouldDeleteCompactionVersion(rawKey []byte, minTS uint64, currentUserKey *[]byte, keptVisibleAtMinTS *bool, changedCurrentKey *bool) bool {
	// decodeKeyView returns a slice into rawKey – no allocation per key.
	// The slice is only valid until rawKey is overwritten by the iterator, so
	// we copy into currentUserKey using append (reusing the existing backing
	// array) only when the user key actually changes, reducing allocations to
	// O(unique-keys) instead of O(total-versions).
	userKey, version := decodeKeyView(rawKey)
	if userKey == nil {
		return false
	}
	if !bytes.Equal(*currentUserKey, userKey) {
		*currentUserKey = append((*currentUserKey)[:0], userKey...)
		*keptVisibleAtMinTS = false
		*changedCurrentKey = false
	}
	if version > minTS {
		return false
	}
	if !*keptVisibleAtMinTS {
		*keptVisibleAtMinTS = true
		return false
	}
	return true
}

func addCompactionDelete(batch *pebble.Batch, rawKey []byte, stats *pebbleCompactionStats, changedCurrentKey *bool) error {
	if err := batch.Delete(append([]byte(nil), rawKey...), pebble.NoSync); err != nil {
		return errors.WithStack(err)
	}
	if !*changedCurrentKey {
		stats.updatedKeys++
		*changedCurrentKey = true
	}
	stats.deletedVersions++
	return nil
}

func (s *pebbleStore) flushCompactionBatch(batch **pebble.Batch, stats *pebbleCompactionStats, opts *pebble.WriteOptions) error {
	batchHasDeletes := batch != nil && *batch != nil && !(*batch).Empty()
	if err := flushSnapshotBatch(s.db, batch, opts); err != nil {
		return err
	}
	if batchHasDeletes {
		stats.committedDeletes = true
	}
	return nil
}

func (s *pebbleStore) scanCompactionDeletes(ctx context.Context, minTS uint64, iter *pebble.Iterator, batch **pebble.Batch, stats *pebbleCompactionStats) error {
	var currentUserKey []byte
	keptVisibleAtMinTS := false
	changedCurrentKey := false

	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return errors.WithStack(err)
		}

		rawKey := iter.Key()
		if isPebbleMetaKey(rawKey) {
			continue
		}
		if !shouldDeleteCompactionVersion(rawKey, minTS, &currentUserKey, &keptVisibleAtMinTS, &changedCurrentKey) {
			continue
		}
		if err := addCompactionDelete(*batch, rawKey, stats, &changedCurrentKey); err != nil {
			return err
		}
		if snapshotBatchShouldFlush(*batch) {
			if err := s.flushCompactionBatch(batch, stats, pebble.NoSync); err != nil {
				return err
			}
		}
	}
	return errors.WithStack(iter.Error())
}

func commitCompactionDeletes(batch **pebble.Batch, stats *pebbleCompactionStats) error {
	if batch == nil || *batch == nil {
		return nil
	}
	if (*batch).Empty() {
		if err := (*batch).Close(); err != nil {
			return errors.WithStack(err)
		}
		*batch = nil
		return nil
	}
	if err := commitSnapshotBatch(*batch, pebble.Sync); err != nil {
		return err
	}
	stats.committedDeletes = true
	*batch = nil
	return nil
}

func (s *pebbleStore) runCompactionGC(ctx context.Context, minTS uint64) (pebbleCompactionStats, error) {
	snap := s.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	iter, err := snap.NewIter(nil)
	if err != nil {
		return pebbleCompactionStats{}, errors.WithStack(err)
	}
	defer func() { _ = iter.Close() }()

	batch := s.db.NewBatch()
	defer func() {
		if batch != nil {
			_ = batch.Close()
		}
	}()

	stats := pebbleCompactionStats{}
	if err := s.scanCompactionDeletes(ctx, minTS, iter, &batch, &stats); err != nil {
		return pebbleCompactionStats{}, errors.WithStack(err)
	}
	if err := commitCompactionDeletes(&batch, &stats); err != nil {
		return pebbleCompactionStats{}, err
	}
	return stats, nil
}

func (s *pebbleStore) Compact(ctx context.Context, minTS uint64) error {
	ctx = ensureContext(ctx)

	// Acquire locks in canonical order (maintenanceMu → dbMu → mtx) before
	// reading the effective watermark.  Taking maintenanceMu and dbMu.RLock
	// first prevents a deadlock with Restore, which takes maintenanceMu →
	// dbMu.Lock → mtx.Lock: if Compact read effectiveMinRetainedTS (mtx.RLock)
	// before acquiring maintenanceMu, Go's sync.RWMutex could block the
	// RLock when Restore has queued a pending Lock(), causing both goroutines
	// to wait on each other indefinitely.
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()

	// Hold dbMu.RLock() for the entire compaction so that concurrent Restore
	// cannot swap out s.db while the GC scan and metadata commits are in
	// progress.  Lock ordering: dbMu before mtx.
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

	if minTS <= s.effectiveMinRetainedTS() {
		// Fast path: the requested watermark is already covered. However, a
		// pending watermark may have survived a previous crash (GC ran and
		// deleted versions but minRetainedTS was never committed). Finalize it
		// now so reads < pending are unblocked and the pending key is not leaked.
		s.mtx.RLock()
		pending, committed := s.pendingMinRetainedTS, s.minRetainedTS
		s.mtx.RUnlock()
		if pending <= committed {
			return nil
		}
		return s.commitCompactionMinRetainedTS(pending)
	}

	shouldRun, err := s.beginCompaction(minTS)
	if err != nil {
		return err
	}
	if !shouldRun {
		return nil
	}

	committedDeletes := false
	defer s.cleanupPendingCompaction(&committedDeletes)

	stats, err := s.runCompactionGC(ctx, minTS)
	if err != nil {
		return err
	}
	committedDeletes = stats.committedDeletes

	if err := s.commitCompactionMinRetainedTS(minTS); err != nil {
		return err
	}
	committedDeletes = false

	s.log.InfoContext(ctx, "compact",
		slog.Uint64("min_ts", minTS),
		slog.Int("updated_keys", stats.updatedKeys),
		slog.Int("deleted_versions", stats.deletedVersions),
	)
	return nil
}

func (s *pebbleStore) Snapshot() (Snapshot, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	snap := s.db.NewSnapshot()
	lastCommitTS, err := readPebbleSnapshotLastCommitTS(snap)
	if err != nil {
		snap.Close()
		return nil, err
	}
	return newPebbleSnapshot(snap, lastCommitTS), nil
}

// readRestoreEntry reads one entry's key-length, key bytes, and value-length
// from r. The key bytes are stored in *keyBuf (grown as needed to avoid per-entry
// allocations). Returns (kLen, vLen, eof=true, nil) on clean EOF at the key-length field.
func readRestoreEntry(r io.Reader, keyBuf *[]byte) (kLen, vLen int, eof bool, err error) {
	kLen, err = readRestoreFieldLen(r, "snapshot key", maxPebbleEncodedKeySize)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return 0, 0, true, nil
		}
		return 0, 0, false, err
	}
	if cap(*keyBuf) < kLen {
		*keyBuf = make([]byte, kLen)
	}
	if _, err = io.ReadFull(r, (*keyBuf)[:kLen]); err != nil {
		return 0, 0, false, errors.WithStack(err)
	}
	vLen, err = readRestoreFieldLen(r, "snapshot value", maxSnapshotValueSize+valueHeaderSize)
	if err != nil {
		return 0, 0, false, err
	}
	return kLen, vLen, false, nil
}

func readRestoreFieldLen(r io.Reader, field string, maxSize int) (int, error) {
	var raw uint64
	if err := binary.Read(r, binary.LittleEndian, &raw); err != nil {
		return 0, errors.WithStack(err)
	}
	return restoreFieldLenInt(raw, field, maxSize)
}

func restoreFieldLenInt(raw uint64, field string, maxSize int) (int, error) {
	if raw > uint64(math.MaxInt) || int(raw) > maxSize {
		switch field {
		case "snapshot key":
			return 0, errors.WithStack(errors.Wrapf(ErrSnapshotKeyTooLarge, "length %d > %d", raw, maxSize))
		case "snapshot value":
			return 0, errors.WithStack(errors.Wrapf(ErrValueTooLarge, "length %d > %d", raw, maxSize))
		default:
			return 0, errors.WithStack(errors.Newf("%s length %d > %d", field, raw, maxSize))
		}
	}
	return int(raw), nil
}

func snapshotBatchShouldFlush(batch *pebble.Batch) bool {
	return batch.Count() >= snapshotBatchCountLimit || batch.Len() >= snapshotBatchByteLimit
}

func commitSnapshotBatch(batch *pebble.Batch, opts *pebble.WriteOptions) error {
	if batch == nil {
		return nil
	}
	defer func() {
		_ = batch.Close()
	}()
	if batch.Empty() && (opts == nil || !opts.Sync) {
		return nil
	}
	return errors.WithStack(batch.Commit(opts))
}

func flushSnapshotBatch(db *pebble.DB, batch **pebble.Batch, opts *pebble.WriteOptions) error {
	if err := commitSnapshotBatch(*batch, opts); err != nil {
		return err
	}
	*batch = db.NewBatch()
	return nil
}

func setEncodedVersionInBatch(batch *pebble.Batch, key []byte, version VersionedValue) error {
	deferred := batch.SetDeferred(encodedKeyLen(key), encodedValueLen(len(version.Value)))
	fillEncodedKey(deferred.Key, key, version.TS)
	fillEncodedValue(deferred.Value, version.Value, version.Tombstone, version.ExpireAt)
	return errors.WithStack(deferred.Finish())
}

// writeRestoreEntry writes one key-value entry from the stream into batch,
// reading the value bytes directly into the deferred write buffer. keyBuf must
// already contain kLen key bytes.
func writeRestoreEntry(r io.Reader, batch *pebble.Batch, keyBuf []byte, kLen, vLen int) error {
	deferred := batch.SetDeferred(kLen, vLen)
	copy(deferred.Key, keyBuf[:kLen])
	if _, err := io.ReadFull(r, deferred.Value); err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(deferred.Finish())
}

// restoreBatchLoopInto reads raw Pebble key-value entries from r and writes
// them into db using batched commits. It is used for both the direct and the
// temp-dir atomic native Pebble restore paths.
func restoreBatchLoopInto(r io.Reader, db *pebble.DB) error {
	batch := db.NewBatch()
	var keyBuf []byte // reused across entries to reduce per-entry allocations

	for {
		kLen, vLen, eof, err := readRestoreEntry(r, &keyBuf)
		if err != nil {
			_ = batch.Close()
			return err
		}
		if eof {
			break
		}

		// Flush before adding when the batch is non-empty and the anticipated
		// entry size would push the batch over the byte limit.
		if !batch.Empty() && batch.Len()+kLen+vLen >= snapshotBatchByteLimit {
			if err := flushSnapshotBatch(db, &batch, pebble.NoSync); err != nil {
				return err
			}
		}

		if err := writeRestoreEntry(r, batch, keyBuf, kLen, vLen); err != nil {
			_ = batch.Close()
			return err
		}

		if snapshotBatchShouldFlush(batch) {
			if err := flushSnapshotBatch(db, &batch, pebble.NoSync); err != nil {
				return err
			}
		}
	}
	return commitSnapshotBatch(batch, pebble.Sync)
}

func (s *pebbleStore) Restore(r io.Reader) error {
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()

	// Acquire dbMu exclusively before mtx to respect the global lock ordering
	// (dbMu before mtx).  This prevents any concurrent DB read/write from
	// observing a partially swapped or closed database handle.
	s.dbMu.Lock()
	defer s.dbMu.Unlock()

	s.mtx.Lock()
	defer s.mtx.Unlock()

	br := bufio.NewReader(r)
	header, err := br.Peek(len(pebbleSnapshotMagic))
	if err != nil {
		return s.handleRestorePeekError(err, header)
	}

	switch {
	case bytes.Equal(header, pebbleSnapshotMagic[:]):
		// Native Pebble format: restorePebbleNativeAtomic performs a temp-dir
		// swap so the existing DB is preserved if the restore fails midway.
		return s.restorePebbleNativeAtomic(br)
	case bytes.Equal(header, mvccSnapshotMagic[:]):
		// Streaming MVCC format: restoreFromStreamingMVCC performs an atomic
		// temp-dir swap, so the existing DB is preserved until checksum
		// verification succeeds.
		return s.restoreFromStreamingMVCC(br)
	default:
		// Legacy gob format: restoreFromLegacyGob performs an atomic
		// temp-dir swap similarly.
		// NOTE: Streams without a recognised magic header are assumed to be
		// in the legacy gob format. Snapshots produced by older Pebble
		// implementations that did not include a magic prefix are therefore
		// not supported by this dispatcher and will typically surface as a
		// gob decode error rather than being restored successfully.
		return s.restoreFromLegacyGob(br)
	}
}

// handleRestorePeekError processes the error from the initial Peek in Restore.
// It distinguishes a truly empty snapshot (no bytes at all) from a truncated one.
func (s *pebbleStore) handleRestorePeekError(err error, header []byte) error {
	if !errors.Is(err, io.EOF) {
		return errors.WithStack(err)
	}
	if len(header) != 0 {
		// Partial header means a truncated/corrupt snapshot.
		return errors.New("truncated snapshot: partial magic header")
	}
	// Truly empty snapshot: clear DB and reset metadata.
	if err := s.reopenFreshDB(); err != nil {
		return err
	}
	s.lastCommitTS = 0
	s.minRetainedTS = 0
	s.pendingMinRetainedTS = 0
	if setErr := writePebbleUint64(s.db, metaLastCommitTSBytes, 0, pebble.NoSync); setErr != nil {
		return errors.WithStack(setErr)
	}
	if setErr := writePebbleUint64(s.db, metaMinRetainedTSBytes, 0, pebble.Sync); setErr != nil {
		return errors.WithStack(setErr)
	}
	return nil
}

func (s *pebbleStore) reopenFreshDB() error {
	if s.db != nil {
		if err := s.db.Close(); err != nil {
			return errors.WithStack(err)
		}
	}
	if err := os.RemoveAll(s.dir); err != nil {
		return errors.WithStack(err)
	}
	if err := os.MkdirAll(s.dir, dirPerms); err != nil {
		return errors.WithStack(err)
	}
	db, err := pebble.Open(s.dir, defaultPebbleOptions())
	if err != nil {
		return errors.WithStack(err)
	}
	s.db = db
	return nil
}

// restorePebbleNative restores from the current Pebble snapshot format
// (magic "EKVPBBL1" + lastCommitTS + raw key-value entries) into s.db directly.
//
// Format history / compatibility:
//   - The pebbleSnapshotMagic header ("EKVPBBL1") was introduced along with
//     this snapshot format. Older Pebble snapshots that pre-date this header
//     (i.e. those that started directly with lastCommitTS) are not handled
//     here. A stream dispatched to this function MUST start with the magic;
//     otherwise "invalid pebble snapshot magic header" is returned.
func (s *pebbleStore) restorePebbleNative(r io.Reader) error {
	var magic [8]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return errors.WithStack(err)
	}
	if !bytes.Equal(magic[:], pebbleSnapshotMagic[:]) {
		return errors.New("invalid pebble snapshot magic header")
	}

	var ts uint64
	if err := binary.Read(r, binary.LittleEndian, &ts); err != nil {
		return errors.WithStack(err)
	}
	if err := s.saveLastCommitTS(ts); err != nil {
		return err
	}
	if err := restoreBatchLoopInto(r, s.db); err != nil {
		return err
	}
	minRetainedTS, err := s.findMinRetainedTS()
	if err != nil {
		return err
	}
	pendingMinRetainedTS, err := s.findPendingMinRetainedTS()
	if err != nil {
		return err
	}
	s.lastCommitTS = ts
	s.minRetainedTS = minRetainedTS
	s.pendingMinRetainedTS = pendingMinRetainedTS
	return nil
}

// restorePebbleNativeAtomic atomically restores a native Pebble snapshot.
// It writes all entries into a temporary sibling directory and only swaps the
// temp directory into s.dir after a full successful read, preserving the
// existing DB on any failure.
func (s *pebbleStore) restorePebbleNativeAtomic(r io.Reader) error {
	magic, ts, err := readPebbleNativeHeader(r)
	if err != nil {
		return err
	}
	if !bytes.Equal(magic[:], pebbleSnapshotMagic[:]) {
		return errors.New("invalid pebble snapshot magic header")
	}

	tmpDir, err := makeSiblingTempDir(s.dir, "pebble-native")
	if err != nil {
		return err
	}

	if err := writeNativeSnapshotToTempDir(r, tmpDir, ts); err != nil {
		_ = os.RemoveAll(tmpDir)
		return err
	}

	if err := s.swapInTempDB(tmpDir); err != nil {
		return err
	}
	return nil
}

// readPebbleNativeHeader reads the 8-byte magic and 8-byte lastCommitTS from r.
func readPebbleNativeHeader(r io.Reader) ([8]byte, uint64, error) {
	var magic [8]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return magic, 0, errors.WithStack(err)
	}
	var ts uint64
	if err := binary.Read(r, binary.LittleEndian, &ts); err != nil {
		return magic, 0, errors.WithStack(err)
	}
	return magic, ts, nil
}

// makeSiblingTempDir creates a temp directory next to dir (same parent) with
// the given tag in its name. MkdirTemp leaves an empty directory; we remove
// it so that pebble.Open can create its own directory structure at that path.
func makeSiblingTempDir(dir, tag string) (string, error) {
	clean := filepath.Clean(dir)
	tmpDir, err := os.MkdirTemp(filepath.Dir(clean), filepath.Base(clean)+"-"+tag+"-*")
	if err != nil {
		return "", errors.WithStack(err)
	}
	if err := os.Remove(tmpDir); err != nil {
		return "", errors.WithStack(err)
	}
	return tmpDir, nil
}

// writeNativeSnapshotToTempDir writes batch-loop entries from r into a fresh
// Pebble database at tmpDir, persists ts as the lastCommitTS meta-key, then
// closes the database.
func writeNativeSnapshotToTempDir(r io.Reader, tmpDir string, ts uint64) error {
	tmpDB, err := pebble.Open(tmpDir, defaultPebbleOptions())
	if err != nil {
		return errors.WithStack(err)
	}

	if err := restoreBatchLoopInto(r, tmpDB); err != nil {
		_ = tmpDB.Close()
		return err
	}

	if err := writePebbleUint64(tmpDB, metaLastCommitTSBytes, ts, pebble.Sync); err != nil {
		_ = tmpDB.Close()
		return errors.WithStack(err)
	}

	return errors.WithStack(tmpDB.Close())
}

// restoreFromStreamingMVCC restores from the in-memory MVCCStore streaming
// snapshot format (magic "EKVMVCC2") by converting each MVCC entry into
// Pebble's key encoding (userKey + inverted TS) and value encoding.
//
// Entries are written to a temporary Pebble directory and only swapped into
// place after the CRC32 checksum is verified, preserving the existing store
// on failure.
func readStreamingMVCCRestoreHeader(r io.Reader) (io.Reader, hash.Hash32, uint32, uint64, uint64, error) {
	expectedChecksum, err := readMVCCSnapshotHeader(r)
	if err != nil {
		return nil, nil, 0, 0, 0, err
	}

	hash := crc32.NewIEEE()
	body := io.TeeReader(r, hash)
	lastCommitTS, minRetainedTS, err := readMVCCSnapshotMetadata(body)
	if err != nil {
		return nil, nil, 0, 0, 0, err
	}
	return body, hash, expectedChecksum, lastCommitTS, minRetainedTS, nil
}

func writeStreamingMVCCRestoreTempDB(dir string, body io.Reader, hash hash.Hash32, expectedChecksum uint32, lastCommitTS uint64, minRetainedTS uint64) (string, error) {
	tmpDir := filepath.Clean(dir) + ".restore-tmp"
	if err := os.RemoveAll(tmpDir); err != nil {
		return "", errors.WithStack(err)
	}
	tmpDB, err := pebble.Open(tmpDir, defaultPebbleOptions())
	if err != nil {
		return "", errors.WithStack(err)
	}
	cleanupTmp := func() {
		_ = tmpDB.Close()
		_ = os.RemoveAll(tmpDir)
	}

	if err := writeMVCCEntriesToDB(body, tmpDB); err != nil {
		cleanupTmp()
		return "", err
	}
	if hash.Sum32() != expectedChecksum {
		cleanupTmp()
		return "", errors.WithStack(ErrInvalidChecksum)
	}
	if err := writeTempDBMetadata(tmpDB, lastCommitTS, minRetainedTS); err != nil {
		cleanupTmp()
		return "", err
	}
	if err := tmpDB.Close(); err != nil {
		_ = os.RemoveAll(tmpDir)
		return "", errors.WithStack(err)
	}
	return tmpDir, nil
}

func (s *pebbleStore) restoreFromStreamingMVCC(r io.Reader) error {
	body, hash, expectedChecksum, lastCommitTS, minRetainedTS, err := readStreamingMVCCRestoreHeader(r)
	if err != nil {
		return err
	}

	tmpDir, err := writeStreamingMVCCRestoreTempDB(s.dir, body, hash, expectedChecksum, lastCommitTS, minRetainedTS)
	if err != nil {
		return err
	}
	return s.swapInTempDB(tmpDir)
}

// writeMVCCEntriesToDB reads MVCC snapshot entries from body and writes them
// into the given Pebble database using batched commits.
func writeMVCCEntriesToDB(body io.Reader, db *pebble.DB) error {
	batch := db.NewBatch()
	for {
		key, versions, eof, err := readMVCCSnapshotEntry(body)
		if err != nil {
			_ = batch.Close()
			return err
		}
		if eof {
			break
		}
		for _, v := range versions {
			if err := setEncodedVersionInBatch(batch, key, v); err != nil {
				_ = batch.Close()
				return err
			}
			if snapshotBatchShouldFlush(batch) {
				if err := flushSnapshotBatch(db, &batch, pebble.NoSync); err != nil {
					return err
				}
			}
		}
	}
	return commitSnapshotBatch(batch, pebble.Sync)
}

// swapInTempDB closes the current DB, removes its directory, and renames tmpDir
// to s.dir, then reopens the DB. The caller is responsible for closing tmpDB
// before calling this.
func (s *pebbleStore) swapInTempDB(tmpDir string) error {
	if err := s.db.Close(); err != nil {
		_ = os.RemoveAll(tmpDir)
		return errors.WithStack(err)
	}
	if err := os.RemoveAll(s.dir); err != nil {
		_ = os.RemoveAll(tmpDir)
		return errors.WithStack(err)
	}
	if err := os.Rename(tmpDir, s.dir); err != nil {
		_ = os.RemoveAll(tmpDir)
		return errors.WithStack(err)
	}
	newDB, err := pebble.Open(s.dir, defaultPebbleOptions())
	if err != nil {
		return errors.WithStack(err)
	}
	s.db = newDB
	lastCommitTS, err := s.findMaxCommitTS()
	if err != nil {
		_ = newDB.Close()
		s.db = nil
		return err
	}
	minRetainedTS, err := s.findMinRetainedTS()
	if err != nil {
		_ = newDB.Close()
		s.db = nil
		return err
	}
	pendingMinRetainedTS, err := s.findPendingMinRetainedTS()
	if err != nil {
		_ = newDB.Close()
		s.db = nil
		return err
	}
	s.lastCommitTS = lastCommitTS
	s.minRetainedTS = minRetainedTS
	s.pendingMinRetainedTS = pendingMinRetainedTS
	return nil
}

// restoreLegacyGobToTempDB writes the decoded gob snapshot into a temporary
// Pebble directory sibling to s.dir and atomically swaps it into place.
func (s *pebbleStore) restoreLegacyGobToTempDB(entries []mvccSnapshotEntry, lastCommitTS uint64, minRetainedTS uint64) error {
	tmpDir := filepath.Clean(s.dir) + ".legacy-tmp"
	if err := os.RemoveAll(tmpDir); err != nil {
		return errors.WithStack(err)
	}
	tmpDB, err := pebble.Open(tmpDir, defaultPebbleOptions())
	if err != nil {
		return errors.WithStack(err)
	}
	if err := writeGobEntriesToDB(entries, tmpDB); err != nil {
		_ = tmpDB.Close()
		_ = os.RemoveAll(tmpDir)
		return err
	}

	if err := writeTempDBMetadata(tmpDB, lastCommitTS, minRetainedTS); err != nil {
		_ = tmpDB.Close()
		_ = os.RemoveAll(tmpDir)
		return err
	}

	if err := tmpDB.Close(); err != nil {
		_ = os.RemoveAll(tmpDir)
		return errors.WithStack(err)
	}
	return s.swapInTempDB(tmpDir)
}

// restoreFromLegacyGob restores from the legacy gob-encoded MVCCStore
// snapshot format (gob payload + CRC32 trailer).
//
// The CRC32 is computed in a single pass while spooling the gob payload to a
// temporary file co-located with s.dir. The CRC32 trailer is NOT written to
// the spool file — only the pure gob payload is stored, so the decoder can
// read the spool file directly without needing a LimitReader. Entries are
// written into a temporary Pebble directory and only swapped into place after
// decoding succeeds, preserving the existing store on failure.
func (s *pebbleStore) restoreFromLegacyGob(r io.Reader) error {
	snapshot, err := spoolAndDecodeGobSnapshot(r, s.dir)
	if err != nil {
		return err
	}
	if err := s.restoreLegacyGobToTempDB(snapshot.Entries, snapshot.LastCommitTS, snapshot.MinRetainedTS); err != nil {
		return err
	}
	return nil
}

// spoolAndDecodeGobSnapshot streams r into a spool file co-located with dir,
// verifies the CRC32 trailer, then gob-decodes the payload and returns the
// snapshot. The spool file is always cleaned up before returning.
func spoolAndDecodeGobSnapshot(r io.Reader, dir string) (mvccSnapshot, error) {
	tmpFile, err := os.CreateTemp(dir, "ekv-legacy-gob-*.tmp")
	if err != nil {
		return mvccSnapshot{}, errors.WithStack(err)
	}
	tmpPath := tmpFile.Name()
	closeTmp := func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
	}

	if err := spoolGobPayload(r, tmpFile); err != nil {
		closeTmp()
		return mvccSnapshot{}, err
	}

	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		closeTmp()
		return mvccSnapshot{}, errors.WithStack(err)
	}
	var snapshot mvccSnapshot
	if err := gob.NewDecoder(tmpFile).Decode(&snapshot); err != nil {
		closeTmp()
		return mvccSnapshot{}, errors.WithStack(err)
	}
	// Close and remove the spool file before swapInTempDB removes dir.
	closeTmp()
	return snapshot, nil
}

// spoolGobPayload streams r into dst, stripping the final checksumSize-byte
// CRC32 trailer. The payload bytes are written to dst and hashed; the trailer
// is verified but not written. Returns ErrInvalidChecksum on mismatch or a
// truncated stream.
func spoolGobPayload(r io.Reader, dst io.Writer) error {
	hasher := crc32.NewIEEE()
	buf := make([]byte, spoolBufSize)
	var tail []byte
	for {
		n, readErr := r.Read(buf)
		if n > 0 {
			tail = append(tail, buf[:n]...)
			if len(tail) > checksumSize {
				toProcessLen := len(tail) - checksumSize
				toProcess := tail[:toProcessLen]
				if _, err := dst.Write(toProcess); err != nil {
					return errors.WithStack(err)
				}
				_, _ = hasher.Write(toProcess)
				tail = append([]byte(nil), tail[toProcessLen:]...)
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return errors.WithStack(readErr)
		}
	}
	if len(tail) != checksumSize {
		return errors.WithStack(ErrInvalidChecksum)
	}
	if hasher.Sum32() != binary.LittleEndian.Uint32(tail) {
		return errors.WithStack(ErrInvalidChecksum)
	}
	return nil
}

// writeGobEntriesToDB writes the decoded gob snapshot entries into db using
// batched commits. NOTE: this function mutates entries in-place by clearing
// each version's Value and then nilling the entry's Key and Versions slice
// after encoding to reduce peak memory usage. Callers must not reuse entries
// after this call.
func writeGobEntriesToDB(entries []mvccSnapshotEntry, db *pebble.DB) error {
	batch := db.NewBatch()
	for i := range entries {
		entry := &entries[i]
		for j := range entry.Versions {
			version := entry.Versions[j]
			if err := setEncodedVersionInBatch(batch, entry.Key, version); err != nil {
				_ = batch.Close()
				return err
			}
			entry.Versions[j].Value = nil
			if snapshotBatchShouldFlush(batch) {
				if err := flushSnapshotBatch(db, &batch, pebble.NoSync); err != nil {
					return err
				}
			}
		}
		entry.Key = nil
		entry.Versions = nil
	}
	return commitSnapshotBatch(batch, pebble.Sync)
}

func (s *pebbleStore) Close() error {
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	// Acquire dbMu exclusively so that all in-flight DB operations finish
	// before we close the database.  Lock ordering: dbMu before mtx.
	s.dbMu.Lock()
	defer s.dbMu.Unlock()
	return errors.WithStack(s.db.Close())
}
