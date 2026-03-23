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
	"path/filepath"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

const (
	timestampSize           = 8
	valueHeaderSize         = 9 // 1 byte tombstone + 8 bytes expireAt
	snapshotBatchCountLimit = 1000
	snapshotBatchByteLimit  = 8 << 20 // 8 MiB; balances restore write amplification vs peak memory usage
	dirPerms                = 0755
	metaLastCommitTS        = "_meta_last_commit_ts"
	spoolBufSize            = 32 * 1024 // buffer size for streaming I/O during restore
)

var metaLastCommitTSBytes = []byte(metaLastCommitTS)

// pebbleStore implements MVCCStore using CockroachDB's Pebble LSM tree.
type pebbleStore struct {
	db           *pebble.DB
	log          *slog.Logger
	lastCommitTS uint64
	mtx          sync.RWMutex
	dir          string
}

// Ensure pebbleStore implements MVCCStore
var _ MVCCStore = (*pebbleStore)(nil)

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

func (s *pebbleStore) saveLastCommitTS(ts uint64) error {
	buf := make([]byte, timestampSize)
	binary.LittleEndian.PutUint64(buf, ts)
	return errors.WithStack(s.db.Set(metaLastCommitTSBytes, buf, pebble.NoSync))
}

func (s *pebbleStore) LastCommitTS() uint64 {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.lastCommitTS
}

func (s *pebbleStore) updateLastCommitTS(ts uint64) {
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
	s.updateLastCommitTS(commitTS)
	return commitTS
}

// MVCC Implementation

func (s *pebbleStore) GetAt(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	iter, err := s.db.NewIter(nil)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer iter.Close()

	// Seek to Key + ^ts (which effectively means Key with version <= ts)
	// Because we use inverted timestamp, larger TS (smaller inverted) comes first.
	// We want the largest TS that is <= requested ts.
	// So we construct a key with requested ts.
	seekKey := encodeKey(key, ts)

	// SeekGE will find the first key >= seekKey.
	// Since keys are [UserKey][InvTS], and InvTS decreases as TS increases.
	// We want TS <= requested_ts.
	// Example: Request TS=10. InvTS=^10 (Large).
	// Stored: TS=12 (Inv=Small), TS=10 (Inv=Large), TS=8 (Inv=Larger).
	// SeekGE(Key + ^10) will skip TS=12 (Key + Small) because Key+Small < Key+Large.
	// It will land on TS=10 or TS=8.
	// Wait, standard byte comparison:
	// Key is same.
	// ^12 < ^10 < ^8.
	// We want largest TS <= 10.
	// If we SeekGE(Key + ^10), we might find Key + ^10 (TS=10) or Key + ^8 (TS=8).
	// These are valid candidates.
	// If we hit Key + ^12, that is smaller than seekKey (since ^12 < ^10), so SeekGE wouldn't find it if we started before it.
	// But we want to filter out TS > 10 (i.e. ^TS < ^10).
	// So SeekGE(Key + ^10) is correct. It skips anything with ^TS < ^10 (meaning TS > 10).

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

	if err := s.db.Set(k, v, pebble.Sync); err != nil { //nolint:wrapcheck
		return errors.WithStack(err)
	}
	s.log.InfoContext(ctx, "put_at", slog.String("key", string(key)), slog.Uint64("ts", commitTS))
	return nil
}

func (s *pebbleStore) DeleteAt(ctx context.Context, key []byte, commitTS uint64) error {
	commitTS = s.alignCommitTS(commitTS)

	k := encodeKey(key, commitTS)
	v := encodeValue(nil, true, 0)

	if err := s.db.Set(k, v, pebble.Sync); err != nil {
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
	if err := s.db.Set(k, v, pebble.Sync); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (s *pebbleStore) LatestCommitTS(ctx context.Context, key []byte) (uint64, bool, error) {
	// Peek latest version (SeekGE key + ^MaxUint64)
	iter, err := s.db.NewIter(nil)
	if err != nil {
		return 0, false, errors.WithStack(err)
	}
	defer iter.Close()

	seekKey := encodeKey(key, math.MaxUint64)
	if iter.SeekGE(seekKey) {
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
	// Write Batch
	b := s.db.NewBatch()
	defer b.Close()

	if err := s.checkConflicts(ctx, mutations, startTS); err != nil {
		return err
	}

	commitTS = s.alignCommitTS(commitTS)

	if err := s.applyMutationsBatch(b, mutations, commitTS); err != nil {
		return err
	}

	return errors.WithStack(b.Commit(pebble.Sync))
}

func (s *pebbleStore) Compact(ctx context.Context, minTS uint64) error {
	// TODO: Implement MVCC garbage collection.
	// We should iterate through all keys and remove versions older than minTS,
	// keeping at least one version <= minTS for snapshot consistency.
	// This is a heavy operation and should be done in the background or using
	// a Pebble CompactionFilter if possible.
	// For now, we simply log the request.
	s.log.Info("Compact requested", slog.Uint64("minTS", minTS))
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

func restoreOneEntry(r io.Reader, batch *pebble.Batch) (bool, error) {
	kLen, err := readRestoreFieldLen(r, "snapshot key")
	if err != nil {
		if errors.Is(err, io.EOF) {
			return true, nil
		}
		return false, err
	}
	key := make([]byte, kLen)
	if _, err := io.ReadFull(r, key); err != nil {
		return false, errors.WithStack(err)
	}
	vLenInt, err := readRestoreFieldLen(r, "snapshot value")
	if err != nil {
		return false, err
	}

	deferred := batch.SetDeferred(kLen, vLenInt)
	copy(deferred.Key, key)
	if _, err := io.ReadFull(r, deferred.Value); err != nil {
		return false, errors.WithStack(err)
	}
	if err := deferred.Finish(); err != nil {
		return false, errors.WithStack(err)
	}
	return false, nil
}

func readRestoreFieldLen(r io.Reader, field string) (int, error) {
	var raw uint64
	if err := binary.Read(r, binary.LittleEndian, &raw); err != nil {
		return 0, errors.WithStack(err)
	}
	return restoreFieldLenInt(raw, field)
}

func restoreFieldLenInt(raw uint64, field string) (int, error) {
	if raw > uint64(math.MaxInt) {
		return 0, errors.WithStack(errors.Newf("%s length %d exceeds supported size", field, raw))
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

// restoreBatchLoopInto reads raw Pebble key-value entries from r and writes
// them into db using batched commits. It is used for both the direct and the
// temp-dir atomic native Pebble restore paths.
func restoreBatchLoopInto(r io.Reader, db *pebble.DB) error {
	batch := db.NewBatch()

	for {
		eof, err := restoreOneEntry(r, batch)
		if err != nil {
			_ = batch.Close()
			return err
		}
		if eof {
			break
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
	var tsBuf [timestampSize]byte
	if setErr := s.db.Set(metaLastCommitTSBytes, tsBuf[:], pebble.Sync); setErr != nil {
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
	s.lastCommitTS = ts
	if err := s.saveLastCommitTS(ts); err != nil {
		return err
	}
	return restoreBatchLoopInto(r, s.db)
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
	s.lastCommitTS = ts
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

	var tsBuf [timestampSize]byte
	binary.LittleEndian.PutUint64(tsBuf[:], ts)
	if err := tmpDB.Set(metaLastCommitTSBytes, tsBuf[:], pebble.Sync); err != nil {
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

	// Write to a temporary Pebble directory so that the real store is only
	// updated after checksum verification succeeds.
	tmpDir := filepath.Clean(s.dir) + ".restore-tmp"
	if err := os.RemoveAll(tmpDir); err != nil {
		return errors.WithStack(err)
	}
	tmpDB, err := pebble.Open(tmpDir, defaultPebbleOptions())
	if err != nil {
		return errors.WithStack(err)
	}
	cleanupTmp := func() {
		_ = tmpDB.Close()
		_ = os.RemoveAll(tmpDir)
	}

	if err := writeMVCCEntriesToDB(body, tmpDB); err != nil {
		cleanupTmp()
		return err
	}

	// Verify checksum before committing the restore.
	if hash.Sum32() != expectedChecksum {
		cleanupTmp()
		return errors.WithStack(ErrInvalidChecksum)
	}

	// Persist lastCommitTS in the temp DB.
	var tsBuf [timestampSize]byte
	binary.LittleEndian.PutUint64(tsBuf[:], lastCommitTS)
	if err := tmpDB.Set(metaLastCommitTSBytes, tsBuf[:], pebble.Sync); err != nil {
		cleanupTmp()
		return errors.WithStack(err)
	}

	// Close the temp DB and atomically swap it into place.
	if err := tmpDB.Close(); err != nil {
		_ = os.RemoveAll(tmpDir)
		return errors.WithStack(err)
	}
	if err := s.swapInTempDB(tmpDir); err != nil {
		return err
	}
	s.lastCommitTS = lastCommitTS
	return nil
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
	return nil
}

// restoreLegacyGobToTempDB writes the decoded gob snapshot into a temporary
// Pebble directory sibling to s.dir and atomically swaps it into place.
func (s *pebbleStore) restoreLegacyGobToTempDB(entries []mvccSnapshotEntry, lastCommitTS uint64) error {
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

	var tsBuf [timestampSize]byte
	binary.LittleEndian.PutUint64(tsBuf[:], lastCommitTS)
	if err := tmpDB.Set(metaLastCommitTSBytes, tsBuf[:], pebble.Sync); err != nil {
		_ = tmpDB.Close()
		_ = os.RemoveAll(tmpDir)
		return errors.WithStack(err)
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
	if err := s.restoreLegacyGobToTempDB(snapshot.Entries, snapshot.LastCommitTS); err != nil {
		return err
	}
	s.lastCommitTS = snapshot.LastCommitTS
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
	return errors.WithStack(s.db.Close())
}
