package store

import (
	"bytes"
	"context"
	"encoding/binary"
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

func (s *pebbleStore) collectScanResults(iter *pebble.Iterator, start, end []byte, limit int, ts uint64) ([]*KVPair, error) {
	result := make([]*KVPair, 0, limit)

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
			return errors.Wrapf(ErrWriteConflict, "key: %s", string(mut.Key))
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

func (s *pebbleStore) Snapshot() (io.ReadWriter, error) {
	// Pebble Snapshots are point-in-time views.
	// But `Snapshot()` interface here implies "serialize state to io.ReadWriter" for Raft snapshotting.
	// We need to dump the *current visible state* or *all state*.
	// Usually Raft snapshots include everything needed to restore the FSM.
	// So we should dump ALL keys in the DB.

	snap := s.db.NewSnapshot()
	defer snap.Close()

	iter, err := snap.NewIter(nil)

	if err != nil {

		return nil, errors.WithStack(err)

	}

	defer iter.Close()

	buf := &bytes.Buffer{}

	// Format: [LastCommitTS] [KeyLen] [Key] [ValLen] [Val] ...

	// We need to persist s.lastCommitTS too.

	if err := binary.Write(buf, binary.LittleEndian, s.LastCommitTS()); err != nil {

		return nil, errors.WithStack(err)

	}

	if err := s.writeSnapshotEntries(snap, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func (s *pebbleStore) writeSnapshotEntries(snap *pebble.Snapshot, w io.Writer) error {
	iter, err := snap.NewIter(nil)
	if err != nil {
		return errors.WithStack(err)
	}
	defer iter.Close()

	// Iterate all raw keys
	for iter.First(); iter.Valid(); iter.Next() {
		// We store Raw Key and Raw Value
		k := iter.Key()
		v := iter.Value()

		// Let's just write Key, Value length-prefixed.
		kLen := uint64(len(k))
		vLen := uint64(len(v))

		if err := binary.Write(w, binary.LittleEndian, kLen); err != nil {
			return errors.WithStack(err)
		}
		if _, err := w.Write(k); err != nil {
			return errors.WithStack(err)
		}
		if err := binary.Write(w, binary.LittleEndian, vLen); err != nil {
			return errors.WithStack(err)
		}
		if _, err := w.Write(v); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
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
	// Close current DB and reopen fresh?
	// Or DeleteAll and apply?
	// Pebble doesn't support "DeleteAll" natively fast.
	// Better to Close, Remove Dir, Open fresh.

	if s.db != nil {
		_ = s.db.Close()
	}

	// Clear directory
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

	// Read LastCommitTS
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

func (s *pebbleStore) Close() error {
	return errors.WithStack(s.db.Close())
}
