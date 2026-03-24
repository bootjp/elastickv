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
	"math"
	"os"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

const (
	timestampSize     = 8
	valueHeaderSize   = 9 // 1 byte tombstone + 8 bytes expireAt
	snapshotBatchSize = 1000
	dirPerms          = 0755
	metaLastCommitTS  = "_meta_last_commit_ts"
)

var metaLastCommitTSBytes = []byte(metaLastCommitTS)

// pebbleStore implements MVCCStore using CockroachDB's Pebble LSM tree.
type pebbleStore struct {
	db            *pebble.DB
	log           *slog.Logger
	lastCommitTS  uint64
	minRetainedTS uint64
	mtx           sync.RWMutex
	dir           string
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

	pebbleOpts := &pebble.Options{
		FS: vfs.Default,
	}
	// Enable automatic compactions
	pebbleOpts.EnsureDefaults()

	db, err := pebble.Open(dir, pebbleOpts)
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
	s.lastCommitTS = maxTS

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
	k := make([]byte, len(key)+timestampSize)
	copy(k, key)
	// Invert TS for descending order (newer first)
	binary.BigEndian.PutUint64(k[len(key):], ^ts)
	return k
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

// Value encoding: We use gob to encode VersionedValue structure minus the key/ts which are in the key.
type storedValue struct {
	Value     []byte
	Tombstone bool
	ExpireAt  uint64
}

func encodeValue(val []byte, tombstone bool, expireAt uint64) []byte {
	// Format: [Tombstone(1)] [ExpireAt(8)] [Value(...)]
	buf := make([]byte, valueHeaderSize+len(val))
	if tombstone {
		buf[0] = 1
	}
	binary.LittleEndian.PutUint64(buf[1:], expireAt)
	copy(buf[valueHeaderSize:], val)
	return buf
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

func (s *pebbleStore) findMaxCommitTS() (uint64, error) {
	// This is expensive on large DBs. Ideally, persist LastCommitTS in a special key.
	// iterating the whole DB is not feasible for large datasets.
	// For this task, we will persist LastCommitTS in a special meta key "_meta_last_commit_ts"
	// whenever we update it (or periodically).
	// For now, let's look for that key.
	val, closer, err := s.db.Get(metaLastCommitTSBytes)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, nil
		}
		return 0, errors.WithStack(err)
	}
	defer closer.Close()
	if len(val) < timestampSize {
		return 0, nil
	}
	return binary.LittleEndian.Uint64(val), nil
}

func (s *pebbleStore) saveLastCommitTSToBatch(b *pebble.Batch, ts uint64) error {
	buf := make([]byte, timestampSize)
	binary.LittleEndian.PutUint64(buf, ts)
	return errors.WithStack(b.Set(metaLastCommitTSBytes, buf, nil))
}

func (s *pebbleStore) saveLastCommitTS(ts uint64) error {
	buf := make([]byte, timestampSize)
	binary.LittleEndian.PutUint64(buf, ts)
	return errors.WithStack(s.db.Set(metaLastCommitTSBytes, buf, pebble.Sync))
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

func (s *pebbleStore) SetMinRetainedTS(ts uint64) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if ts > s.minRetainedTS {
		s.minRetainedTS = ts
	}
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

func (s *pebbleStore) alignCommitTS(commitTS uint64) uint64 {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.updateAndPersistLastCommitTS(commitTS)
	return commitTS
}

// MVCC Implementation

func (s *pebbleStore) GetAt(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	lower := encodeKey(key, ts)
	upper := encodeKey(key, 0)
	// Append a byte to upper bound to make it exclusive and cover all timestamps
	upper = append(upper, 0)
	opts := &pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	}
	iter, err := s.db.NewIter(opts)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer iter.Close()

	seekKey := encodeKey(key, ts)

	if iter.SeekGE(seekKey) {
		k := iter.Key()
		userKey, _ := decodeKey(k)

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

func (s *pebbleStore) ExistsAt(ctx context.Context, key []byte, ts uint64) (bool, error) {
	val, err := s.GetAt(ctx, key, ts)
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
	currentUserKey, _ := decodeKey(k)
	return bytes.Equal(currentUserKey, userKey)
}

func (s *pebbleStore) skipToNextUserKey(iter *pebble.Iterator, userKey []byte) bool {
	if !iter.SeekGE(encodeKey(userKey, 0)) {
		return false
	}
	k := iter.Key()
	u, _ := decodeKey(k)
	if bytes.Equal(u, userKey) {
		return iter.Next()
	}
	return true
}

func pastScanEnd(userKey, end []byte) bool {
	return end != nil && bytes.Compare(userKey, end) > 0
}

func nextScannableUserKey(iter *pebble.Iterator) ([]byte, uint64, bool) {
	for iter.Valid() {
		rawKey := iter.Key()
		if bytes.Equal(rawKey, metaLastCommitTSBytes) {
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
		if bytes.Equal(rawKey, metaLastCommitTSBytes) {
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
	currentUserKey, _ := decodeKey(iter.Key())
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
	// Must read latest value first to preserve it
	val, err := s.GetAt(ctx, key, commitTS)
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

func (s *pebbleStore) LatestCommitTS(ctx context.Context, key []byte) (uint64, bool, error) {
	lower := encodeKey(key, math.MaxUint64)
	upper := encodeKey(key, 0)
	// Append a byte to upper bound to make it exclusive and cover all timestamps
	upper = append(upper, 0)
	opts := &pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	}
	iter, err := s.db.NewIter(opts)
	if err != nil {
		return 0, false, errors.WithStack(err)
	}
	defer iter.Close()

	if iter.First() {
		k := iter.Key()
		userKey, version := decodeKey(k)
		if bytes.Equal(userKey, key) {
			return version, true, nil
		}
	}
	return 0, false, nil
}

func (s *pebbleStore) checkConflicts(ctx context.Context, mutations []*KVPairMutation, startTS uint64) error {
	for _, mut := range mutations {
		ts, exists, err := s.LatestCommitTS(ctx, mut.Key)
		if err != nil {
			return err
		}
		if exists && ts > startTS {
			return NewWriteConflictError(mut.Key)
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

func (s *pebbleStore) ApplyMutations(ctx context.Context, mutations []*KVPairMutation, startTS, commitTS uint64) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	b := s.db.NewBatch()
	defer b.Close()

	if err := s.checkConflicts(ctx, mutations, startTS); err != nil {
		return err
	}

	s.updateLastCommitTS(commitTS)
	if err := s.saveLastCommitTSToBatch(b, s.lastCommitTS); err != nil {
		return err
	}

	if err := s.applyMutationsBatch(b, mutations, commitTS); err != nil {
		return err
	}

	return errors.WithStack(b.Commit(pebble.Sync))
}

const compactBatchLimit = 1000

func (s *pebbleStore) Compact(ctx context.Context, minTS uint64) error {
	// MVCC GC: for each user key, keep the newest version <= minTS and remove
	// all older versions. We iterate in encoded key order; consecutive entries
	// sharing the same user key belong to the same version chain.
	iter, err := s.db.NewIter(nil)
	if err != nil {
		return errors.WithStack(err)
	}
	defer iter.Close()

	batch := s.db.NewBatch()
	batchCount := 0
	deletedTotal := 0

	var prevUserKey []byte
	keepSeen := false // true once we've seen the version to keep for the current user key

	for iter.First(); iter.Valid(); iter.Next() {
		if ctx.Err() != nil {
			batch.Close()
			return ctx.Err()
		}

		raw := iter.Key()
		userKey, ts := decodeKey(raw)
		if userKey == nil {
			continue
		}

		// Skip meta keys.
		if bytes.Equal(raw, metaLastCommitTSBytes) {
			continue
		}

		// Skip transaction internal keys — their lifecycle is managed by
		// lock resolution, not MVCC compaction.
		if bytes.HasPrefix(userKey, TxnInternalKeyPrefix) {
			continue
		}

		// Detect user-key boundary.
		if !bytes.Equal(userKey, prevUserKey) {
			prevUserKey = append(prevUserKey[:0], userKey...)
			keepSeen = false
		}

		if ts > minTS {
			// Version newer than compaction watermark — always keep.
			continue
		}

		if !keepSeen {
			// First version <= minTS for this key — keep it as the snapshot anchor.
			keepSeen = true
			continue
		}

		// Older than the kept version — safe to delete.
		encodedKey := make([]byte, len(raw))
		copy(encodedKey, raw)
		if err := batch.Delete(encodedKey, nil); err != nil {
			batch.Close()
			return errors.WithStack(err)
		}
		batchCount++
		deletedTotal++

		if batchCount >= compactBatchLimit {
			if err := batch.Commit(pebble.NoSync); err != nil {
				return errors.WithStack(err)
			}
			batch = s.db.NewBatch()
			batchCount = 0
		}
	}

	if batchCount > 0 {
		if err := batch.Commit(pebble.NoSync); err != nil {
			return errors.WithStack(err)
		}
	} else {
		batch.Close()
	}

	s.log.InfoContext(ctx, "compact",
		slog.Uint64("min_ts", minTS),
		slog.Int("deleted_versions", deletedTotal),
	)
	return nil
}

func (s *pebbleStore) Snapshot() (Snapshot, error) {
	snap := s.db.NewSnapshot()
	lastCommitTS, err := readPebbleSnapshotLastCommitTS(snap)
	if err != nil {
		snap.Close()
		return nil, err
	}
	return newPebbleSnapshot(snap, lastCommitTS), nil
}

func (s *pebbleStore) restoreOneEntry(r io.Reader, batch *pebble.Batch) (bool, error) {
	var kLen uint64
	if err := binary.Read(r, binary.LittleEndian, &kLen); err != nil {
		if errors.Is(err, io.EOF) {
			return true, nil
		}
		return false, errors.WithStack(err)
	}
	key := make([]byte, kLen)
	if _, err := io.ReadFull(r, key); err != nil {
		return false, errors.WithStack(err)
	}

	var vLen uint64
	if err := binary.Read(r, binary.LittleEndian, &vLen); err != nil {
		return false, errors.WithStack(err)
	}
	val := make([]byte, vLen)
	if _, err := io.ReadFull(r, val); err != nil {
		return false, errors.WithStack(err)
	}

	if err := batch.Set(key, val, nil); err != nil {
		return false, errors.WithStack(err)
	}
	return false, nil
}

func (s *pebbleStore) restoreBatchLoop(r io.Reader) error {
	batch := s.db.NewBatch()
	batchCnt := 0

	for {
		eof, err := s.restoreOneEntry(r, batch)
		if err != nil {
			return err
		}
		if eof {
			break
		}

		batchCnt++
		if batchCnt > snapshotBatchSize {
			if err := batch.Commit(pebble.NoSync); err != nil {
				return errors.WithStack(err)
			}
			batch = s.db.NewBatch()
			batchCnt = 0
		}
	}
	return errors.WithStack(batch.Commit(pebble.Sync))
}

func (s *pebbleStore) Restore(r io.Reader) error {
	if err := s.reopenFreshDB(); err != nil {
		return err
	}

	br := bufio.NewReader(r)
	header, err := br.Peek(len(pebbleSnapshotMagic))
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil // empty snapshot
		}
		return errors.WithStack(err)
	}

	switch {
	case bytes.Equal(header, pebbleSnapshotMagic[:]):
		return s.restorePebbleNative(br)
	case bytes.Equal(header, mvccSnapshotMagic[:]):
		return s.restoreFromStreamingMVCC(br)
	default:
		return s.restoreFromLegacyGob(br)
	}
}

func (s *pebbleStore) reopenFreshDB() error {
	if s.db != nil {
		_ = s.db.Close()
	}
	if err := os.RemoveAll(s.dir); err != nil {
		return errors.WithStack(err)
	}
	if err := os.MkdirAll(s.dir, dirPerms); err != nil {
		return errors.WithStack(err)
	}
	db, err := pebble.Open(s.dir, &pebble.Options{FS: vfs.Default})
	if err != nil {
		return errors.WithStack(err)
	}
	s.db = db
	return nil
}

// restorePebbleNative restores from the current Pebble snapshot format
// (magic "EKVPBBL1" + lastCommitTS + raw key-value entries).
func (s *pebbleStore) restorePebbleNative(r io.Reader) error {
	var magic [8]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return errors.WithStack(err)
	}

	var ts uint64
	if err := binary.Read(r, binary.LittleEndian, &ts); err != nil {
		return errors.WithStack(err)
	}
	s.lastCommitTS = ts
	if err := s.saveLastCommitTS(ts); err != nil {
		return err
	}
	return s.restoreBatchLoop(r)
}

// restoreFromStreamingMVCC restores from the in-memory MVCCStore streaming
// snapshot format (magic "EKVMVCC2") by converting each MVCC entry into
// Pebble's key encoding (userKey + inverted TS) and value encoding.
func (s *pebbleStore) restoreFromStreamingMVCC(r io.Reader) error {
	expectedChecksum, err := readMVCCSnapshotHeader(r)
	if err != nil {
		return err
	}

	hash := crc32.NewIEEE()
	body := io.TeeReader(r, hash)

	lastCommitTS, _, err := readMVCCSnapshotMetadata(body)
	if err != nil {
		return err
	}
	s.lastCommitTS = lastCommitTS
	if err := s.saveLastCommitTS(lastCommitTS); err != nil {
		return err
	}

	batch := s.db.NewBatch()
	batchCnt := 0
	for {
		key, versions, eof, err := readMVCCSnapshotEntry(body)
		if err != nil {
			return err
		}
		if eof {
			break
		}
		for _, v := range versions {
			pKey := encodeKey(key, v.TS)
			pVal := encodeValue(v.Value, v.Tombstone, v.ExpireAt)
			if err := batch.Set(pKey, pVal, nil); err != nil {
				return errors.WithStack(err)
			}
			batchCnt++
			if batchCnt >= snapshotBatchSize {
				if err := batch.Commit(pebble.NoSync); err != nil {
					return errors.WithStack(err)
				}
				batch = s.db.NewBatch()
				batchCnt = 0
			}
		}
	}

	if hash.Sum32() != expectedChecksum {
		return errors.WithStack(ErrInvalidChecksum)
	}
	return errors.WithStack(batch.Commit(pebble.Sync))
}

// restoreFromLegacyGob restores from the legacy gob-encoded MVCCStore
// snapshot format (gob payload + CRC32 trailer).
func (s *pebbleStore) restoreFromLegacyGob(r io.Reader) error {
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

	s.lastCommitTS = snapshot.LastCommitTS
	if err := s.saveLastCommitTS(snapshot.LastCommitTS); err != nil {
		return err
	}

	batch := s.db.NewBatch()
	batchCnt := 0
	for _, entry := range snapshot.Entries {
		for _, v := range entry.Versions {
			pKey := encodeKey(entry.Key, v.TS)
			pVal := encodeValue(v.Value, v.Tombstone, v.ExpireAt)
			if err := batch.Set(pKey, pVal, nil); err != nil {
				return errors.WithStack(err)
			}
			batchCnt++
			if batchCnt >= snapshotBatchSize {
				if err := batch.Commit(pebble.NoSync); err != nil {
					return errors.WithStack(err)
				}
				batch = s.db.NewBatch()
				batchCnt = 0
			}
		}
	}
	return errors.WithStack(batch.Commit(pebble.Sync))
}

func (s *pebbleStore) Close() error {
	return errors.WithStack(s.db.Close())
}
