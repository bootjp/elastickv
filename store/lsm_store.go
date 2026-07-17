package store

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"hash"
	"hash/crc32"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

const (
	timestampSize   = 8
	valueHeaderSize = 9 // 1 byte flags + 8 bytes expireAt
	// First-byte (flags) layout per design §4.1:
	//   bit 0      tombstone
	//   bits 1-2   encryption_state (0b00 cleartext, 0b01 encrypted, 0b10/0b11 reserved)
	//   bits 3-7   reserved (must be zero)
	encStateMask                    byte = 0b0000_0110
	encStateShift                        = 1
	tombstoneMask                   byte = 0b0000_0001
	encStateCleartext               byte = 0b00
	encStateEncrypted               byte = 0b01
	encStateReservedMask            byte = 0b1111_1000 // bits 3-7 must stay zero
	snapshotBatchCountLimit              = 1000
	snapshotBatchByteLimit               = 8 << 20 // 8 MiB; balances restore write amplification vs peak memory usage
	compactionDeleteBatchCountLimit      = 128
	compactionDeleteBatchByteLimit       = 1 << 20
	dirPerms                             = 0755
	metaLastCommitTS                     = "_meta_last_commit_ts"
	metaMinRetainedTS                    = "_meta_min_retained_ts"
	metaPendingMinRetainedTS             = "_meta_pending_min_retained_ts"
	// metaAppliedIndex is the durable raft-applied index meta key.
	// Bundled atomically with each raft-Apply pebble.Batch (see
	// applyMutationsRaftAt / deletePrefixAtRaftAt) and pinned to
	// snap.Metadata.Index by SetDurableAppliedIndex at every snapshot
	// persist site. See
	// docs/design/2026_06_02_implemented_idempotent_snapshot_restore.md §3.
	metaAppliedIndex = "_meta_applied_index"
	spoolBufSize     = 32 * 1024 // buffer size for streaming I/O during restore

	// maxPebbleEncodedKeySize is the limit for encoded Pebble on-disk keys,
	// which are the user key concatenated with the 8-byte inverted timestamp.
	// Using maxSnapshotKeySize+timestampSize (instead of just maxSnapshotKeySize)
	// avoids rejecting keys that are valid at the user-key level but slightly
	// exceed maxSnapshotKeySize once the timestamp suffix is appended.
	maxPebbleEncodedKeySize = maxSnapshotKeySize + timestampSize

	// defaultPebbleCacheBytes is the default Pebble block-cache capacity per
	// store. Pebble's built-in EnsureDefaults() supplies only 8 MiB, which
	// is far too small for our workloads: production observed a block-cache
	// hit rate of 0.003% (1.8B misses vs 58k hits) because the working set
	// evicted faster than it filled. 256 MiB is a conservative baseline per
	// shard; operators can override via ELASTICKV_PEBBLE_CACHE_MB.
	defaultPebbleCacheBytes int64 = 256 << 20

	// pebbleCacheMBEnv is the env var operators use to override the per-store
	// Pebble block-cache capacity. Units are MiB, integer only. Malformed or
	// out-of-range values fall back to the default.
	pebbleCacheMBEnv = "ELASTICKV_PEBBLE_CACHE_MB"

	// pebbleCacheMBMin / pebbleCacheMBMax define the accepted range for the
	// env override. Values below the 8 MiB floor or above the 64 GiB ceiling
	// are rejected and fall back to the default; the ceiling guards against
	// typos ("65536" instead of "6553").
	pebbleCacheMBMin = 8
	pebbleCacheMBMax = 65536

	// mebibyteShift converts MiB to bytes via x << mebibyteShift. Named to
	// avoid a magic-number lint violation on the shift amount.
	mebibyteShift = 20

	// fsmSyncModeEnv selects the Pebble WriteOptions used on the
	// raft-apply FSM commit path (ApplyMutationsRaft / DeletePrefixAtRaft).
	// Direct (non-raft) callers of ApplyMutations / DeletePrefixAt are
	// unaffected by this knob and always use pebble.Sync. Values:
	//
	//   "sync"    (default) — b.Commit(pebble.Sync); every committed raft
	//                         entry triggers an fsync on the Pebble WAL.
	//                         Strongest local durability; slowest.
	//   "nosync"            — b.Commit(pebble.NoSync); the Pebble WAL
	//                         still records the write, but is not fsynced.
	//                         Durability still holds because the raft WAL
	//                         (etcd/raft) fsyncs the committed entry
	//                         upstream, and on restart the raft log is
	//                         replayed from the last FSM-snapshot index;
	//                         any apply that did not reach Pebble's
	//                         fsync'd region is re-applied.
	//
	// The default is "sync" so production behaviour is unchanged without
	// an explicit opt-in. See docs/fsm_sync_mode.md (or the PR body) for
	// the full durability argument.
	fsmSyncModeEnv = "ELASTICKV_FSM_SYNC_MODE"

	// fsmSyncModeSync / fsmSyncModeNoSync are the accepted values for
	// fsmSyncModeEnv. Any other value falls back to the default.
	fsmSyncModeSync   = "sync"
	fsmSyncModeNoSync = "nosync"

	// conflictCheckSmallLinearAdvanceLimit bounds the number of physical Pebble
	// keys we scan with Next before falling back to SeekGE during small batched
	// OCC checks.
	conflictCheckSmallLinearAdvanceLimit = 64

	// conflictCheckLargeLinearAdvanceLimit stays intentionally small: large
	// replay batches often touch sparse key ranges, where broad Next scans burn
	// CPU before the server starts listening. Fallback seeks are bounded to the
	// target key range with SeekGEWithLimit.
	conflictCheckLargeLinearAdvanceLimit = 16
	conflictCheckLargeBatchThreshold     = 1024
)

// pebbleCacheBytes is the effective per-store Pebble block-cache capacity,
// resolved once at process start. Exposed as a package variable so tests can
// swap it via setPebbleCacheBytesForTest; production code treats it as
// read-only after init().
//
// TODO(perf/pebble): introduce a process-wide shared cache plumbed through
// NewPebbleStore so all shards on a node share one LRU eviction pool rather
// than each carrying an independent 256 MiB budget. That requires changing
// NewPebbleStore's signature and is deferred to a follow-up PR.
var pebbleCacheBytes = defaultPebbleCacheBytes

func init() {
	pebbleCacheBytes = resolvePebbleCacheBytes(os.Getenv(pebbleCacheMBEnv))
}

// resolveFSMApplyWriteOpts parses an ELASTICKV_FSM_SYNC_MODE value and
// returns both the *pebble.WriteOptions used on the FSM commit path and
// the canonical label name. Case is normalised. Empty, malformed, or
// unrecognised values fall back to the default ("sync").
//
// Exported via package-internal calls only; tests use it directly.
func resolveFSMApplyWriteOpts(envVal string) (*pebble.WriteOptions, string) {
	switch strings.ToLower(strings.TrimSpace(envVal)) {
	case fsmSyncModeNoSync:
		return pebble.NoSync, fsmSyncModeNoSync
	case "", fsmSyncModeSync:
		return pebble.Sync, fsmSyncModeSync
	default:
		return pebble.Sync, fsmSyncModeSync
	}
}

// resolvePebbleCacheBytes parses an ELASTICKV_PEBBLE_CACHE_MB value and
// returns the resolved cache size in bytes. Empty, malformed, or
// out-of-range values are rejected and fall back to the default rather
// than being clamped to the nearest bound. "0" is below the 8 MiB floor
// and therefore also falls back to the default.
func resolvePebbleCacheBytes(envVal string) int64 {
	if envVal == "" {
		return defaultPebbleCacheBytes
	}
	mb, err := strconv.Atoi(envVal)
	if err != nil {
		return defaultPebbleCacheBytes
	}
	if mb < pebbleCacheMBMin || mb > pebbleCacheMBMax {
		return defaultPebbleCacheBytes
	}
	return int64(mb) << mebibyteShift
}

var metaLastCommitTSBytes = []byte(metaLastCommitTS)
var metaMinRetainedTSBytes = []byte(metaMinRetainedTS)
var metaPendingMinRetainedTSBytes = []byte(metaPendingMinRetainedTS)
var metaAppliedIndexBytes = []byte(metaAppliedIndex)

// pebbleStore implements MVCCStore using CockroachDB's Pebble LSM tree.
//
// Lock ordering (must always be acquired in this order to avoid deadlocks):
//
//  1. maintenanceMu – serialises Compact/Restore/Close.
//  2. dbMu          – guards the s.db pointer; held as a write-lock while the
//     DB is being swapped (Restore/Close), and as a read-lock by every
//     operation that accesses s.db.
//  3. applyMu       – serialises raft-apply conflict-check → batch-commit so
//     concurrent ApplyMutationsRaft/At cannot both pass checkConflicts and
//     then both commit. Also held by deletePrefixAtWithOpts and by
//     SetDurableAppliedIndex's read-modify-write monotonic guard
//     (PR #915 round-3) so the snapshot-persist checkpoint cannot rewind
//     metaAppliedIndex below a concurrent per-Apply value.
//  4. mtx           – guards the in-memory metadata fields
//     (lastCommitTS, minRetainedTS, pendingMinRetainedTS).
type pebbleStore struct {
	db                   *pebble.DB
	cache                *pebble.Cache // owns one ref; Unref on Close / reopen.
	dbMu                 sync.RWMutex  // guards s.db pointer – see lock ordering above
	log                  *slog.Logger
	lastCommitTS         uint64
	minRetainedTS        uint64
	pendingMinRetainedTS uint64
	mtx                  sync.RWMutex
	applyMu              sync.Mutex // serializes ApplyMutations: conflict check → commit
	maintenanceMu        sync.Mutex
	dir                  string
	// writeConflicts tracks per-(kind, key_prefix) OCC conflict counts
	// detected inside ApplyMutations. Polled by the monitoring
	// WriteConflictCollector; not part of the authoritative OCC path.
	writeConflicts *writeConflictCounter
	// fsmApplyWriteOpts is the Pebble WriteOptions value applied on the
	// raft-apply FSM commit path (ApplyMutationsRaft / DeletePrefixAtRaft).
	// Resolved once from ELASTICKV_FSM_SYNC_MODE in NewPebbleStore and
	// then treated as read-only for the store's lifetime. The default is
	// pebble.Sync; operators may opt into pebble.NoSync when the raft
	// WAL's durability is considered sufficient. Direct (non-raft)
	// callers of ApplyMutations / DeletePrefixAt use pebble.Sync
	// unconditionally and are never affected by this field.
	fsmApplyWriteOpts *pebble.WriteOptions
	// fsmApplySyncModeLabel is the human-readable label corresponding
	// to fsmApplyWriteOpts ("sync" or "nosync"). Kept alongside the
	// write-options pointer so monitoring (elastickv_fsm_apply_sync_mode)
	// and log lines stay in sync with the resolved mode.
	fsmApplySyncModeLabel string
	// cipher / nonceFactory / activeStorageKeyID drive the §4.1
	// storage envelope. nil cipher = cleartext-only legacy behaviour;
	// see WithEncryption. Once wired, cipher and nonceFactory MUST
	// outlive the store (the keystore behind cipher is itself
	// copy-on-write so rotation does not break this invariant).
	cipher             *encryption.Cipher
	nonceFactory       NonceFactory
	activeStorageKeyID ActiveStorageKeyID
	// storageEnvelopeActive is the Stage 6D-5 cutover gate (design
	// doc §6.2). When wired, every write path that would otherwise
	// emit a §4.1 envelope first consults this closure and falls
	// back to cleartext when it returns false. A nil closure
	// preserves the pre-6D-5 behaviour where activeStorageKeyID
	// alone decides the encrypt/cleartext split — the legacy test
	// fixtures depend on that posture and the 6D-6 production wiring
	// in main.go is what flips the gate on.
	storageEnvelopeActive StorageEnvelopeActive
	// storageRegistered is the Stage 7a-2 §4.1 registration gate. When
	// wired, the DIRECT write path (PutAt / ExpireAt / ApplyMutations)
	// refuses to emit an encrypted envelope — returning
	// ErrWriterNotRegistered — until this process load's writer
	// registration has committed for the active storage DEK. The
	// FSM-apply path (ApplyMutationsRaft) never consults it: replicated
	// apply must stay deterministic and may legitimately run before this
	// node's own registration entry commits (design §1). A nil closure
	// preserves the pre-7a-2 posture (no direct-path gating); the
	// production main.go wiring threads cache.Registered in.
	storageRegistered StorageRegistered
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

// defaultPebbleOptionsWithCache returns the standard Pebble options used
// throughout the store (including restores) to ensure consistent behaviour
// between a freshly opened and a restored/swapped-in database, along with
// the owned *pebble.Cache handle.
//
// FormatMajorVersion is pinned to ratchet v1-era DBs above pebble v2's
// FormatMinSupported (FormatFlushableIngest) before the v2 upgrade lands.
//
// The returned options carry a freshly-allocated block cache sized from
// pebbleCacheBytes. pebble.NewCache hands back a refcounted Cache with
// ref=1; pebble.Open adds one reference, so the caller MUST Unref the
// returned cache after the DB is closed (or after pebble.Open fails) to
// fully release the memory. Callers that only need *pebble.Options should
// still take the cache handle and defer its Unref to avoid leaking a
// 256 MiB (default) allocation per call.
func defaultPebbleOptionsWithCache() (*pebble.Options, *pebble.Cache) {
	cache := pebble.NewCache(pebbleCacheBytes)
	opts := &pebble.Options{
		FS:                 vfs.Default,
		FormatMajorVersion: pebble.FormatVirtualSSTables,
		Cache:              cache,
	}
	// Enable automatic compactions and apply all other Pebble defaults.
	// EnsureDefaults leaves Cache alone because we already set it.
	opts.EnsureDefaults()
	return opts, cache
}

// NewPebbleStore creates a new Pebble-backed MVCC store.
func NewPebbleStore(dir string, opts ...PebbleStoreOption) (MVCCStore, error) {
	fsmOpts, fsmLabel := resolveFSMApplyWriteOpts(os.Getenv(fsmSyncModeEnv))
	s := &pebbleStore{
		dir: dir,
		log: slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelWarn,
		})),
		writeConflicts:        newWriteConflictCounter(),
		fsmApplyWriteOpts:     fsmOpts,
		fsmApplySyncModeLabel: fsmLabel,
	}
	for _, opt := range opts {
		opt(s)
	}

	pebbleOpts, cache := defaultPebbleOptionsWithCache()
	db, err := pebble.Open(dir, pebbleOpts)
	if err != nil {
		cache.Unref()
		return nil, errors.WithStack(err)
	}
	s.db = db
	s.cache = cache

	// cleanupOnInitFail closes the just-opened DB and releases our creator
	// reference on the cache so that a partially corrupt store (where the
	// metadata scans below fail) does not leak the ~256 MiB cache
	// allocation. Also nil's out the store pointers so a zero-valued
	// pebbleStore does not linger with stale refs.
	cleanupOnInitFail := func() {
		_ = db.Close()
		cache.Unref()
		s.db = nil
		s.cache = nil
	}

	// Initialize lastCommitTS from the persisted meta key, then validate it
	// against data versions. Older stores and crash windows can have missing or
	// stale metadata while MVCC versions remain present; using the lower meta
	// watermark for OCC would skip conflict scans incorrectly.
	maxTS, err := s.findMaxCommitTS()
	if err != nil {
		cleanupOnInitFail()
		return nil, err
	}
	minRetainedTS, err := s.findMinRetainedTS()
	if err != nil {
		cleanupOnInitFail()
		return nil, err
	}
	pendingMinRetainedTS, err := s.findPendingMinRetainedTS()
	if err != nil {
		cleanupOnInitFail()
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

func appendEncodedKey(dst []byte, key []byte, ts uint64) []byte {
	needed := encodedKeyLen(key)
	if cap(dst) < needed {
		dst = make([]byte, needed)
	} else {
		dst = dst[:needed]
	}
	fillEncodedKey(dst, key, ts)
	return dst
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

func appendKeyUpperBound(dst []byte, key []byte) []byte {
	if cap(dst) < len(key) {
		dst = make([]byte, len(key))
	} else {
		dst = dst[:len(key)]
	}
	copy(dst, key)
	for i := len(dst) - 1; i >= 0; i-- {
		dst[i]++
		if dst[i] != 0 {
			return dst[:i+1]
		}
	}
	return nil
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

// Value encoding: fixed binary header
//
//	byte 0: bit 0 tombstone | bits 1-2 encryption_state | bits 3-7 reserved
//	bytes 1-8: ExpireAt (LittleEndian uint64)
//	bytes 9..: body — either raw plaintext (encState=0b00) or the §4.1
//	           authenticated envelope bytes (encState=0b01). Reserved
//	           encryption_state values (0b10, 0b11) are rejected at decode
//	           per design §7.1.
//
// The Pebble key (`encodeKey(user_key, commit_ts)`) is signed into the
// envelope's AAD so a cut-and-paste / version-substitution attack
// rejects on Decrypt; see §4.1 case 2/3.
type storedValue struct {
	Value     []byte
	Tombstone bool
	EncState  byte // 0b00 cleartext, 0b01 encrypted; reserved values rejected at decode
	ExpireAt  uint64
}

// ErrEncryptedValueReservedState indicates decodeValue saw an
// encryption_state value (0b10 or 0b11) that the current build does
// not know how to interpret. Per design §7.1, this is a fail-closed
// trip-wire so an old binary cannot silently treat a future-version
// encrypted entry as cleartext bytes.
var ErrEncryptedValueReservedState = errors.New("store: value header carries reserved encryption_state; binary too old to read this entry")

func encodeValue(val []byte, tombstone bool, expireAt uint64, encState byte) []byte {
	// Format: [flags(1)] [ExpireAt(8)] [Body(...)]
	buf := make([]byte, encodedValueLen(len(val)))
	fillEncodedValue(buf, val, tombstone, expireAt, encState)
	return buf
}

func encodedValueLen(valueLen int) int {
	return valueHeaderSize + valueLen
}

func fillEncodedValue(dst []byte, val []byte, tombstone bool, expireAt uint64, encState byte) {
	writeValueHeaderBytes(dst, tombstone, expireAt, encState)
	copy(dst[valueHeaderSize:], val)
}

// writeValueHeaderBytes writes only the 9-byte value-header (flags +
// expireAt) into dst[0:valueHeaderSize]. Extracted from fillEncodedValue
// so the encryption path (encryption_glue.go) can reproduce the
// header bytes for AAD without having a body slice in hand: the AAD
// must bind tombstone, encryption_state, and expireAt so a disk
// attacker cannot flip those fields to force a silent
// ErrKeyNotFound / expired read on an encrypted record.
func writeValueHeaderBytes(dst []byte, tombstone bool, expireAt uint64, encState byte) {
	var flags byte
	if tombstone {
		flags |= tombstoneMask
	}
	flags |= (encState << encStateShift) & encStateMask
	dst[0] = flags
	binary.LittleEndian.PutUint64(dst[1:], expireAt)
}

func decodeValue(data []byte) (storedValue, error) {
	if len(data) < valueHeaderSize {
		return storedValue{}, errors.New("invalid value length")
	}
	flags := data[0]
	if flags&encStateReservedMask != 0 {
		return storedValue{}, errors.Wrapf(ErrEncryptedValueReservedState,
			"value header byte = %#08b", flags)
	}
	encState := (flags & encStateMask) >> encStateShift
	if encState != encStateCleartext && encState != encStateEncrypted {
		return storedValue{}, errors.Wrapf(ErrEncryptedValueReservedState,
			"encryption_state=%#x is reserved", encState)
	}
	tombstone := (flags & tombstoneMask) != 0
	expireAt := binary.LittleEndian.Uint64(data[1:])
	val := make([]byte, len(data)-valueHeaderSize)
	copy(val, data[valueHeaderSize:])

	return storedValue{
		Value:     val,
		Tombstone: tombstone,
		EncState:  encState,
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
		bytes.Equal(rawKey, metaPendingMinRetainedTSBytes) ||
		bytes.Equal(rawKey, metaAppliedIndexBytes)
}

func (s *pebbleStore) findMaxCommitTS() (uint64, error) {
	metaTS, err := readPebbleUint64(s.db, metaLastCommitTSBytes)
	if err != nil {
		return 0, err
	}
	dataTS, err := s.findMaxDataCommitTS()
	if err != nil {
		return 0, err
	}
	if dataTS <= metaTS {
		return metaTS, nil
	}
	if err := writePebbleUint64(s.db, metaLastCommitTSBytes, dataTS, pebble.Sync); err != nil {
		return 0, err
	}
	return dataTS, nil
}

func (s *pebbleStore) findMaxDataCommitTS() (uint64, error) {
	iter, err := s.db.NewIter(&pebble.IterOptions{})
	if err != nil {
		return 0, errors.WithStack(err)
	}
	defer iter.Close()

	var maxTS uint64
	for iter.First(); iter.Valid(); iter.Next() {
		rawKey := iter.Key()
		if isPebbleMetaKey(rawKey) {
			continue
		}
		userKey, ts := decodeKeyView(rawKey)
		if userKey == nil {
			continue
		}
		if ts > maxTS {
			maxTS = ts
		}
	}
	if err := iter.Error(); err != nil {
		return 0, errors.WithStack(err)
	}
	return maxTS, nil
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

// LastAppliedIndex implements raftengine.AppliedIndexReader. Returns
// the largest Raft entry index whose Apply produced a durable
// mutation on this store (via applyMutationsRaftAt /
// deletePrefixAtRaftAt — same WriteBatch as the data — or via
// SetDurableAppliedIndex at a snapshot persist).
//
// (0, false, nil) means the meta key is absent — either a pre-upgrade
// fsm.db that has not yet bumped through a raft-Apply, or a freshly
// restored store. Callers MUST treat this as "missing" and fall back
// to the full restore path; see
// docs/design/2026_06_02_implemented_idempotent_snapshot_restore.md §4 fallback
// policy. Any other error propagates.
//
// dbMu.RLock matches the rest of the read path
// (lsm_store.go:153 / :675); without it a concurrent swapInTempDB
// could replace s.db between db.Get and the closer.Close()/value
// access, racing the snapshot install path.
func (s *pebbleStore) LastAppliedIndex() (uint64, bool, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	val, closer, err := s.db.Get(metaAppliedIndexBytes)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, false, nil
		}
		return 0, false, errors.WithStack(err)
	}
	defer func() { _ = closer.Close() }()
	if len(val) < timestampSize {
		// Truncated meta key — treat as missing for strictly-additive
		// safety. The caller will fall back to full restore, which
		// produces a fresh well-formed value via the next Apply or
		// snapshot persist.
		return 0, false, nil
	}
	return binary.LittleEndian.Uint64(val), true, nil
}

// SetDurableAppliedIndex implements raftengine.AppliedIndexWriter.
// Used at snapshot persist time to pin metaAppliedIndex to
// snap.Metadata.Index BEFORE persist.SaveSnap, so a successful
// snapshot persist always implies LastAppliedIndex >=
// snap.Metadata.Index — closing the HLC-lease-only / encryption-only
// fallback (see design doc §6).
//
// The write is single-key and goes through a fresh pebble.Batch with
// pebble.Sync UNCONDITIONALLY — independent of
// ELASTICKV_FSM_SYNC_MODE. The reason is durability boundary: WAL
// compaction following SaveSnap discards every log entry at or
// before snap.Metadata.Index, so there is no source to replay the
// meta key bump from. If we honoured nosync mode here, a crash
// between Pebble's deferred flush and SaveSnap's internal fsync
// would leave snapshot pointer at X but metaAppliedIndex at Y < X
// forever. The +1 fsync per snapshot persist (rare; default
// SnapshotCount=10000) is negligible vs that risk.
//
// Monotonicity (round-2 P2 fix for PR #915): when persistLocalSnapshot
// runs in a background worker, raft apply can continue and the per-
// entry ApplyMutationsRaftAt path can advance metaAppliedIndex past
// `idx` before this method runs. An unconditional write would rewind
// the meta key — defeating the soak/verification invariant and
// causing the future skip gate to fall back unnecessarily. The
// applyMu lock serialises with applyMutationsWithOpts /
// deletePrefixAtWithOpts (both hold it across their batch commit),
// and the read-modify-write keeps the meta key strictly monotonic.
//
// Lock order: dbMu.RLock before applyMu.Lock matches the existing
// discipline in applyMutationsWithOpts (lsm_store.go around :1438 /
// :1444). No deadlock.
func (s *pebbleStore) SetDurableAppliedIndex(idx uint64) error {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	s.applyMu.Lock()
	defer s.applyMu.Unlock()

	existing, present, err := s.readAppliedIndexLocked()
	if err != nil {
		return err
	}
	if present && existing >= idx {
		// Concurrent apply already advanced the meta key past `idx`;
		// no work needed. The skip-invariant still holds because the
		// snapshot's claim (LastAppliedIndex >= snap.Metadata.Index)
		// is satisfied by existing >= idx >= snap.Metadata.Index.
		return nil
	}

	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()
	if err := setPebbleUint64InBatch(batch, metaAppliedIndexBytes, idx); err != nil {
		return err
	}
	return errors.WithStack(batch.Commit(pebble.Sync))
}

// readAppliedIndexLocked decodes the metaAppliedIndex key. Caller
// MUST hold s.dbMu.RLock (so s.db is stable) AND s.applyMu.Lock (so
// the value reflects a consistent snapshot vs concurrent batch
// commits in applyMutationsWithOpts). The body is shared with the
// unlocked LastAppliedIndex() reader; same (0, false, nil) semantics
// for absent / truncated meta keys.
func (s *pebbleStore) readAppliedIndexLocked() (uint64, bool, error) {
	val, closer, err := s.db.Get(metaAppliedIndexBytes)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, false, nil
		}
		return 0, false, errors.WithStack(err)
	}
	defer func() { _ = closer.Close() }()
	if len(val) < timestampSize {
		return 0, false, nil
	}
	return binary.LittleEndian.Uint64(val), true, nil
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

// FSMApplySyncModeLabel returns the resolved FSM sync-mode label
// ("sync" or "nosync") for this store. Consumed by monitoring to
// surface the current durability posture as a gauge with a mode label.
// The value is fixed for the store's lifetime (resolved once from
// ELASTICKV_FSM_SYNC_MODE in NewPebbleStore) so no locking is needed.
func (s *pebbleStore) FSMApplySyncModeLabel() string {
	return s.fsmApplySyncModeLabel
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

	if !iter.SeekGE(seekKey) {
		return nil, ErrKeyNotFound
	}
	return s.readVisibleVersion(iter, key, ts)
}

// readVisibleVersion examines the iterator's current entry and
// returns the live plaintext value at ts, or ErrKeyNotFound if the
// entry is a different user key, a tombstone, or expired.
//
// For encrypted entries the decrypt step runs BEFORE the
// tombstone/expireAt visibility checks. The AAD passed to Decrypt
// includes the on-disk value-header (tombstone bit + encryption_state
// + expireAt), so a disk attacker cannot flip those fields to force
// a silent ErrKeyNotFound/expired branch — any tamper either fails
// GCM (returns ErrEncryptedReadIntegrity) or matches the original
// values, in which case the visibility checks below are operating
// on authenticated bytes.
func (s *pebbleStore) readVisibleVersion(iter *pebble.Iterator, key []byte, ts uint64) ([]byte, error) {
	k := iter.Key()
	userKey, _ := decodeKeyView(k)
	if !bytes.Equal(userKey, key) {
		return nil, ErrKeyNotFound
	}
	sv, err := decodeValue(iter.Value())
	if err != nil {
		return nil, errors.WithStack(err)
	}
	plain, err := s.decryptForKey(k, sv, sv.Value)
	if err != nil {
		return nil, err
	}
	if sv.Tombstone {
		return nil, ErrKeyNotFound
	}
	if sv.ExpireAt != 0 && sv.ExpireAt <= ts {
		return nil, ErrKeyNotFound
	}
	return plain, nil
}

func (s *pebbleStore) GetAt(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.getAt(ctx, key, ts)
}

func (s *pebbleStore) GetAtBatch(ctx context.Context, keys [][]byte, ts uint64) (map[string][]byte, error) {
	if len(keys) == 0 {
		return map[string][]byte{}, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.WithStack(err)
	}

	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, errors.WithStack(err)
	}
	if readTSCompacted(ts, s.effectiveMinRetainedTS()) {
		return nil, ErrReadTSCompacted
	}

	sortedKeys := uniqueSortedKeys(keys)
	iter, err := s.db.NewIter(&pebble.IterOptions{})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer iter.Close()

	return s.getAtBatchWithIter(ctx, iter, sortedKeys, ts)
}

func (s *pebbleStore) getAtBatchWithIter(ctx context.Context, iter *pebble.Iterator, sortedKeys [][]byte, ts uint64) (map[string][]byte, error) {
	values := make(map[string][]byte, len(sortedKeys))
	var seekKey []byte
	var upperBound []byte
	for _, key := range sortedKeys {
		if err := ctx.Err(); err != nil {
			return nil, errors.WithStack(err)
		}
		seekKey = appendEncodedKey(seekKey, key, ts)
		upperBound = appendKeyUpperBound(upperBound, key)
		if iter.SeekGEWithLimit(seekKey, upperBound) != pebble.IterValid {
			if err := iter.Error(); err != nil {
				return nil, errors.WithStack(err)
			}
			continue
		}
		value, err := s.readVisibleVersion(iter, key, ts)
		if err != nil {
			if errors.Is(err, ErrKeyNotFound) {
				continue
			}
			return nil, err
		}
		values[string(key)] = bytes.Clone(value)
	}
	if err := iter.Error(); err != nil {
		return nil, errors.WithStack(err)
	}
	return values, nil
}

func uniqueSortedKeys(keys [][]byte) [][]byte {
	sortedKeys := make([][]byte, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		keyString := string(key)
		if _, ok := seen[keyString]; ok {
			continue
		}
		seen[keyString] = struct{}{}
		sortedKeys = append(sortedKeys, key)
	}
	sort.Slice(sortedKeys, func(i, j int) bool {
		return bytes.Compare(sortedKeys[i], sortedKeys[j]) < 0
	})
	return sortedKeys
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

// CommittedVersionAt reports whether a version stamped EXACTLY commitTS
// exists for key. It is a single point lookup on the MVCC-encoded key
// (userKey + inverted commitTS), not a <=ts scan: only the exact version
// matters for the one-phase idempotency probe. A tombstone version counts
// as present — the previous attempt landed even if it committed a delete —
// so the raw key existence is the answer and the value is not decoded.
//
// Unlike GetAt/ExistsAt, this probe does NOT enforce the retention watermark
// (codex P1 round-11): branching FSM apply on the per-replica minRetainedTS
// is non-deterministic across raft replicas (compaction is driven by local
// wall clock, not by the replicated log), and a fail-closed retention check
// here would produce split-brain — some replicas surface ErrReadTSCompacted
// and skip the apply while others see the version and no-op. Returning the
// raw pebble.Get answer makes the probe deterministic for the option-2
// dedup use case, where each per-element key has at most one MVCC version
// so physical pebble compaction does not remove it. The invariant the
// caller depends on is: retention window > max adapter retry latency, so a
// live retry's PrevCommitTS never falls below pebble's compacted floor on
// any replica. The earlier round-10 retention guard was reverted with this
// rationale; see design doc §race-freedom.
func (s *pebbleStore) CommittedVersionAt(_ context.Context, key []byte, commitTS uint64) (bool, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.committedVersionAtLocked(key, commitTS)
}

func (s *pebbleStore) committedVersionAtLocked(key []byte, commitTS uint64) (bool, error) {
	_, closer, err := s.db.Get(encodeKey(key, commitTS))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		return false, errors.WithStack(err)
	}
	if err := closer.Close(); err != nil {
		return false, errors.WithStack(err)
	}
	return true, nil
}

func (s *pebbleStore) processFoundValue(iter *pebble.Iterator, userKey []byte, ts uint64) (*KVPair, error) {
	valBytes := iter.Value()
	sv, err := decodeValue(valBytes)
	if err != nil {
		return nil, err
	}

	// Decrypt before the tombstone/expireAt visibility checks so the
	// per-value AAD authenticates the header bits we are about to
	// branch on. See readVisibleVersion for the matching rationale:
	// a flipped tombstone or lowered expireAt would otherwise force
	// a silent skip on an encrypted entry.
	plain, err := s.decryptForKey(iter.Key(), sv, sv.Value)
	if err != nil {
		return nil, err
	}
	if !sv.Tombstone && (sv.ExpireAt == 0 || sv.ExpireAt > ts) {
		return &KVPair{
			Key:   userKey,
			Value: plain,
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

func nextScannableUserKeyContext(ctx context.Context, iter *pebble.Iterator) ([]byte, uint64, bool, error) {
	for iter.Valid() {
		if err := ctx.Err(); err != nil {
			return nil, 0, false, errors.WithStack(err)
		}
		rawKey := iter.Key()
		if isPebbleMetaKey(rawKey) {
			if !iter.Next() {
				return nil, 0, false, nil
			}
			continue
		}
		userKey, version := decodeKey(rawKey)
		if userKey == nil {
			if !iter.Next() {
				return nil, 0, false, nil
			}
			continue
		}
		return userKey, version, true, nil
	}
	return nil, 0, false, nil
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

func (s *pebbleStore) collectScanResults(ctx context.Context, iter *pebble.Iterator, start, end []byte, limit int, ts uint64) ([]*KVPair, error) {
	result, _, err := s.collectScanResultsWithPhysicalLimit(ctx, iter, start, end, limit, 0, ts)
	return result, err
}

func (s *pebbleStore) collectScanResultsWithPhysicalLimit(ctx context.Context, iter *pebble.Iterator, start, end []byte, limit, physicalLimit int, ts uint64) ([]*KVPair, bool, error) {
	result := make([]*KVPair, 0, boundedScanResultCapacity(limit))
	visited := 0

	for iter.SeekGE(encodeKey(start, math.MaxUint64)); iter.Valid() && len(result) < limit; {
		userKey, version, ok, err := nextScannableUserKeyContext(ctx, iter)
		if err != nil {
			return nil, false, err
		}
		if forwardScanDone(userKey, end, ok) {
			break
		}
		if physicalLimit > 0 && visited >= physicalLimit {
			return result, true, nil
		}
		visited++

		kv, advance, err := s.collectForwardScanKV(iter, userKey, version, ts)
		if err != nil {
			return nil, false, err
		}
		if kv != nil {
			result = append(result, kv)
		}
		if !advance {
			break
		}
	}

	return result, false, nil
}

func forwardScanDone(userKey, end []byte, ok bool) bool {
	return !ok || pastScanEnd(userKey, end)
}

func (s *pebbleStore) collectForwardScanKV(iter *pebble.Iterator, userKey []byte, version uint64, ts uint64) (*KVPair, bool, error) {
	if !s.seekToVisibleVersion(iter, userKey, version, ts) {
		return nil, true, nil
	}
	kv, err := s.processFoundValue(iter, userKey, ts)
	if err != nil {
		return nil, false, err
	}
	return kv, s.skipToNextUserKey(iter, userKey), nil
}

func (s *pebbleStore) ScanAt(ctx context.Context, start []byte, end []byte, limit int, ts uint64) ([]*KVPair, error) {
	if limit <= 0 {
		return []*KVPair{}, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.WithStack(err)
	}

	// Acquire dbMu.RLock before reading effectiveMinRetainedTS (which takes
	// mtx.RLock) to preserve the global lock ordering: dbMu before mtx.
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, errors.WithStack(err)
	}

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

	return s.collectScanResults(ctx, iter, start, end, limit, ts)
}

func (s *pebbleStore) ScanAtPhysicalLimit(ctx context.Context, start []byte, end []byte, visibleLimit, physicalLimit int, ts uint64) ([]*KVPair, bool, error) {
	if visibleLimit <= 0 || physicalLimit <= 0 {
		return []*KVPair{}, false, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, false, errors.WithStack(err)
	}

	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, false, errors.WithStack(err)
	}
	if readTSCompacted(ts, s.effectiveMinRetainedTS()) {
		return nil, false, ErrReadTSCompacted
	}

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: encodeKey(start, math.MaxUint64),
	})
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	defer iter.Close()

	return s.collectScanResultsWithPhysicalLimit(ctx, iter, start, end, visibleLimit, physicalLimit, ts)
}

func (s *pebbleStore) collectReverseScanResults(
	iter *pebble.Iterator,
	start []byte,
	end []byte,
	limit int,
	ts uint64,
) ([]*KVPair, error) {
	result, _, err := s.collectReverseScanResultsWithPhysicalLimit(iter, start, end, limit, 0, ts)
	return result, err
}

func (s *pebbleStore) collectReverseScanResultsWithPhysicalLimit(
	iter *pebble.Iterator,
	start []byte,
	end []byte,
	limit int,
	physicalLimit int,
	ts uint64,
) ([]*KVPair, bool, error) {
	result := make([]*KVPair, 0, boundedScanResultCapacity(limit))

	valid := seekReverseScanStart(iter, end)
	visited := 0
	for valid && len(result) < limit {
		if physicalLimit > 0 && visited >= physicalLimit {
			return result, true, nil
		}
		kv, nextValid, done, err := s.nextReverseScanKV(iter, start, ts)
		if err != nil {
			return nil, false, err
		}
		valid = nextValid
		if done {
			break
		}
		visited++
		if kv != nil {
			result = append(result, kv)
		}
	}

	return result, false, nil
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

func (s *pebbleStore) ReverseScanAtPhysicalLimit(ctx context.Context, start []byte, end []byte, visibleLimit, physicalLimit int, ts uint64) ([]*KVPair, bool, error) {
	if visibleLimit <= 0 || physicalLimit <= 0 {
		return []*KVPair{}, false, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, false, errors.WithStack(err)
	}

	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, false, errors.WithStack(err)
	}
	if readTSCompacted(ts, s.effectiveMinRetainedTS()) {
		return nil, false, ErrReadTSCompacted
	}

	opts := &pebble.IterOptions{
		LowerBound: encodeKey(start, math.MaxUint64),
	}
	if end != nil {
		opts.UpperBound = encodeKey(end, math.MaxUint64)
	}
	iter, err := s.db.NewIter(opts)
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	defer iter.Close()

	return s.collectReverseScanResultsWithPhysicalLimit(iter, start, end, visibleLimit, physicalLimit, ts)
}

func (s *pebbleStore) PutAt(ctx context.Context, key []byte, value []byte, commitTS uint64, expireAt uint64) error {
	if err := validateValueSize(value); err != nil {
		return err
	}
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	commitTS = s.alignCommitTS(commitTS)

	k := encodeKey(key, commitTS)
	// gateRegistration=true: PutAt is a direct (non-raft) write path.
	body, encState, err := s.encryptForKey(k, value, expireAt, true)
	if err != nil {
		return err
	}
	v := encodeValue(body, false, expireAt, encState)

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
	v := encodeValue(nil, true, 0, encStateCleartext)

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
	// gateRegistration=true: ExpireAt is a direct (non-raft) write path
	// that calls encryptForKey directly (it does not delegate to PutAt).
	body, encState, err := s.encryptForKey(k, val, expireAt, true)
	if err != nil {
		return err
	}
	v := encodeValue(body, false, expireAt, encState)
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

type conflictCheckKey struct {
	key   []byte
	index int
}

func sortConflictCheckKeys(keys []conflictCheckKey) {
	sort.Slice(keys, func(i, j int) bool {
		if c := bytes.Compare(keys[i].key, keys[j].key); c != 0 {
			return c < 0
		}
		return keys[i].index < keys[j].index
	})
}

func sortedMutationConflictKeys(mutations []*KVPairMutation) []conflictCheckKey {
	keys := make([]conflictCheckKey, len(mutations))
	for i, mut := range mutations {
		keys[i] = conflictCheckKey{key: mut.Key, index: i}
	}
	sortConflictCheckKeys(keys)
	return keys
}

func sortedReadConflictKeys(readKeys [][]byte) []conflictCheckKey {
	keys := make([]conflictCheckKey, len(readKeys))
	for i, key := range readKeys {
		keys[i] = conflictCheckKey{key: key, index: i}
	}
	sortConflictCheckKeys(keys)
	return keys
}

func conflictTSFromCurrentIter(iter *pebble.Iterator, key []byte, limit int) (uint64, bool, bool, error) {
	for steps := 0; steps < limit && iter.Valid(); steps++ {
		userKey, version := decodeKeyView(iter.Key())
		switch c := bytes.Compare(userKey, key); {
		case c < 0:
			iter.Next()
			continue
		case c == 0:
			return version, true, true, nil
		default:
			return 0, false, true, nil
		}
	}
	if err := iter.Error(); err != nil {
		return 0, false, true, errors.WithStack(err)
	}
	if !iter.Valid() {
		return 0, false, true, nil
	}
	return 0, false, false, nil
}

func conflictCheckLinearAdvanceLimitFor(keyCount int) int {
	if keyCount >= conflictCheckLargeBatchThreshold {
		return conflictCheckLargeLinearAdvanceLimit
	}
	return conflictCheckSmallLinearAdvanceLimit
}

func (s *pebbleStore) latestCommitTSWithIter(ctx context.Context, iter *pebble.Iterator, key []byte, seekKey, upperBound *[]byte, preferNext bool, linearAdvanceLimit int) (uint64, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, false, errors.WithStack(err)
	}
	if preferNext {
		ts, exists, decided, err := conflictTSFromCurrentIter(iter, key, linearAdvanceLimit)
		if err != nil || decided {
			return ts, exists, err
		}
	}
	*seekKey = appendEncodedKey(*seekKey, key, math.MaxUint64)
	*upperBound = appendKeyUpperBound(*upperBound, key)
	if iter.SeekGEWithLimit(*seekKey, *upperBound) == pebble.IterValid {
		userKey, version := decodeKeyView(iter.Key())
		if bytes.Equal(userKey, key) {
			return version, true, nil
		}
	}
	if err := iter.Error(); err != nil {
		return 0, false, errors.WithStack(err)
	}
	return 0, false, nil
}

func nextConflictCheckKeyGroup(keys []conflictCheckKey, start int) ([]byte, int, int) {
	current := keys[start].key
	minOriginalIndex := keys[start].index
	next := start + 1
	for next < len(keys) && bytes.Equal(keys[next].key, current) {
		if keys[next].index < minOriginalIndex {
			minOriginalIndex = keys[next].index
		}
		next++
	}
	return current, minOriginalIndex, next
}

func (s *pebbleStore) firstConflictKey(ctx context.Context, keys []conflictCheckKey, startTS uint64) ([]byte, bool, error) {
	if len(keys) == 0 {
		return nil, false, nil
	}
	iter, err := s.db.NewIter(&pebble.IterOptions{})
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	defer iter.Close()

	firstConflictIndex := len(keys)
	var firstConflictKey []byte
	var seekKey []byte
	var upperBound []byte
	preferNext := false
	linearAdvanceLimit := conflictCheckLinearAdvanceLimitFor(len(keys))
	for i := 0; i < len(keys); {
		current, minOriginalIndex, next := nextConflictCheckKeyGroup(keys, i)
		ts, exists, err := s.latestCommitTSWithIter(ctx, iter, current, &seekKey, &upperBound, preferNext, linearAdvanceLimit)
		if err != nil {
			return nil, false, err
		}
		preferNext = iter.Valid()
		if exists && ts > startTS && minOriginalIndex < firstConflictIndex {
			firstConflictIndex = minOriginalIndex
			firstConflictKey = current
		}
		i = next
	}
	if firstConflictKey == nil {
		return nil, false, nil
	}
	return firstConflictKey, true, nil
}

func (s *pebbleStore) checkConflicts(ctx context.Context, mutations []*KVPairMutation, startTS uint64) error {
	conflictKey, found, err := s.firstConflictKey(ctx, sortedMutationConflictKeys(mutations), startTS)
	if err != nil {
		return err
	}
	if found {
		// Record the first conflicting key's bucket only — a single
		// failing ApplyMutations corresponds to one OCC conflict
		// regardless of how many subsequent mutations would also
		// collide, which matches the Prometheus rate semantics.
		s.writeConflicts.record(WriteConflictKindWrite, classifyWriteConflictKey(conflictKey))
		return NewWriteConflictError(conflictKey)
	}
	return nil
}

func (s *pebbleStore) checkReadConflicts(ctx context.Context, readKeys [][]byte, startTS uint64) error {
	conflictKey, found, err := s.firstConflictKey(ctx, sortedReadConflictKeys(readKeys), startTS)
	if err != nil {
		return err
	}
	if found {
		s.writeConflicts.record(WriteConflictKindRead, classifyWriteConflictKey(conflictKey))
		return NewWriteConflictError(conflictKey)
	}
	return nil
}

// WriteConflictCountsByPrefix returns a snapshot of the OCC conflict
// counts keyed by "<kind>|<key_prefix>" (see EncodeWriteConflictLabel).
// Counts are monotonic for the lifetime of this *pebbleStore; a store
// reopen starts them back at zero, which the monitoring collector
// absorbs by rebasing its delta baseline.
func (s *pebbleStore) WriteConflictCountsByPrefix() map[string]uint64 {
	return s.writeConflicts.snapshot()
}

// WriteConflictCount is the aggregate across every (kind, key_prefix)
// bucket. Primarily useful for tests and for quick single-number
// sanity checks; Prometheus exports the per-bucket view via
// WriteConflictCountsByPrefix.
func (s *pebbleStore) WriteConflictCount() uint64 {
	return s.writeConflicts.total()
}

func (s *pebbleStore) applyMutationsBatch(b *pebble.Batch, mutations []*KVPairMutation, commitTS uint64, gateRegistration bool) error {
	for _, mut := range mutations {
		k := encodeKey(mut.Key, commitTS)
		var v []byte

		switch mut.Op {
		case OpTypePut:
			if err := validateValueSize(mut.Value); err != nil {
				return err
			}
			body, encState, encErr := s.encryptForKey(k, mut.Value, mut.ExpireAt, gateRegistration)
			if encErr != nil {
				return encErr
			}
			v = encodeValue(body, false, mut.ExpireAt, encState)
		case OpTypeDelete:
			v = encodeValue(nil, true, 0, encStateCleartext)
		default:
			return ErrUnknownOp
		}
		if err := b.Set(k, v, nil); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

// directApplyWriteOpts returns the Pebble WriteOptions used by the
// direct (non-raft) commit path. Always pebble.Sync: direct callers do
// not have raft-log replay as a durability backstop, so they must not
// be affected by ELASTICKV_FSM_SYNC_MODE=nosync.
//
// Exposed so tests can assert the non-raft path is always Sync even
// when the FSM-apply path has been reconfigured to NoSync.
func (s *pebbleStore) directApplyWriteOpts() *pebble.WriteOptions {
	return pebble.Sync
}

// raftApplyWriteOpts returns the Pebble WriteOptions used by the
// raft-apply commit path, as configured by ELASTICKV_FSM_SYNC_MODE.
func (s *pebbleStore) raftApplyWriteOpts() *pebble.WriteOptions {
	return s.fsmApplyWriteOpts
}

// ApplyMutations is the direct (non-raft) commit path. It unconditionally
// uses pebble.Sync so that callers without raft-log replay as a durability
// backstop (catalog bootstrap, admin snapshots, migrations, tests) are never
// affected by ELASTICKV_FSM_SYNC_MODE=nosync.
func (s *pebbleStore) ApplyMutations(ctx context.Context, mutations []*KVPairMutation, readKeys [][]byte, startTS, commitTS uint64) error {
	// gateRegistration=true: the direct path is self-originated (catalog
	// bootstrap Save, admin snapshot, migration). Stage 7a-2 refuses to
	// emit an encrypted envelope here before this load's writer
	// registration commits.
	// appliedIndex=0: direct path has no raft index; the leaf treats 0 as
	// "do not write metaAppliedIndex" so the meta key stays unchanged.
	return s.applyMutationsWithOpts(ctx, mutations, readKeys, startTS, commitTS, s.directApplyWriteOpts(), true, 0)
}

// ApplyMutationsRaft is the raft-apply commit path. Durability is governed
// by ELASTICKV_FSM_SYNC_MODE (s.fsmApplyWriteOpts, default pebble.Sync):
// operators may opt into pebble.NoSync when the raft WAL's fsync is
// considered the authoritative durability boundary. On crash, raft-log
// replay from the last FSM snapshot re-applies any entries lost from
// Pebble's un-fsynced WAL tail.
//
// Must only be called from inside the FSM apply loop. All other call sites
// must use ApplyMutations so a nosync opt-in cannot silently drop
// acknowledged writes that have no raft backstop.
//
// Callers that have a raft entry index in hand (the kvFSM data-Apply
// path via the raftengine.ApplyIndexAware seam) SHOULD prefer
// ApplyMutationsRaftAt so the metaAppliedIndex meta key is bundled
// atomically with the data mutation — see PR #910 / B2.
func (s *pebbleStore) ApplyMutationsRaft(ctx context.Context, mutations []*KVPairMutation, readKeys [][]byte, startTS, commitTS uint64) error {
	// gateRegistration=false: the FSM-apply path replays committed Raft
	// entries and must stay deterministic. It may legitimately encrypt
	// before this node's own registration entry commits (design §1);
	// fail-closing here would halt the apply loop and could deadlock a
	// node whose storage entry is ordered before its registration entry.
	// appliedIndex=0: callers that have not yet been wired to the
	// raftengine.ApplyIndexAware seam (test fakes, legacy FSM impls)
	// land here; their LastAppliedIndex() will stay behind the snapshot
	// pointer and the skip optimisation will fall back to full restore
	// for them. Preferred path is ApplyMutationsRaftAt.
	return s.applyMutationsWithOpts(ctx, mutations, readKeys, startTS, commitTS, s.raftApplyWriteOpts(), false, 0)
}

// ApplyMutationsRaftAt is ApplyMutationsRaft with the raft entry
// index threaded through so the leaf can bundle metaAppliedIndex in
// the same pebble.Batch as the data mutation. See PR #910 design §2.
//
// appliedIndex==0 is treated as "no index" — the leaf will not write
// metaAppliedIndex, preserving the ApplyMutationsRaft semantics.
// Production callers (kvFSM.applyXxx with f.pendingApplyIdx) SHOULD
// pass the entry.Index value the engine delivered via SetApplyIndex.
func (s *pebbleStore) ApplyMutationsRaftAt(ctx context.Context, mutations []*KVPairMutation, readKeys [][]byte, startTS, commitTS, appliedIndex uint64) error {
	return s.applyMutationsWithOpts(ctx, mutations, readKeys, startTS, commitTS, s.raftApplyWriteOpts(), false, appliedIndex)
}

func (s *pebbleStore) raftApplyAlreadyLandedLocked(mutations []*KVPairMutation, commitTS uint64) (bool, error) {
	if len(mutations) == 0 {
		return false, nil
	}
	for _, mut := range mutations {
		if mut == nil || len(mut.Key) == 0 {
			continue
		}
		return s.committedVersionAtLocked(mut.Key, commitTS)
	}
	return false, nil
}

func (s *pebbleStore) shouldWriteAppliedIndexLocked(appliedIndex uint64) (bool, error) {
	if appliedIndex == 0 {
		return false, nil
	}
	existing, present, err := s.readAppliedIndexLocked()
	if err != nil {
		return false, err
	}
	return !present || existing < appliedIndex, nil
}

func (s *pebbleStore) writeRaftApplyMetaLocked(newLastTS uint64, writeAppliedIndex bool, appliedIndex uint64, writeOpts *pebble.WriteOptions) error {
	b := s.db.NewBatch()
	defer func() { _ = b.Close() }()
	if err := setPebbleUint64InBatch(b, metaLastCommitTSBytes, newLastTS); err != nil {
		return err
	}
	if writeAppliedIndex {
		if err := setPebbleUint64InBatch(b, metaAppliedIndexBytes, appliedIndex); err != nil {
			return err
		}
	}
	return errors.WithStack(b.Commit(raftApplyMetaWriteOpts(writeAppliedIndex, writeOpts)))
}

func raftApplyMetaWriteOpts(writeAppliedIndex bool, writeOpts *pebble.WriteOptions) *pebble.WriteOptions {
	if writeAppliedIndex {
		return pebble.Sync
	}
	return writeOpts
}

func (s *pebbleStore) markRaftApplyAlreadyLandedLocked(commitTS, appliedIndex uint64, writeOpts *pebble.WriteOptions) error {
	writeAppliedIndex, err := s.shouldWriteAppliedIndexLocked(appliedIndex)
	if err != nil {
		return err
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	newLastTS := s.lastCommitTS
	if commitTS > newLastTS {
		newLastTS = commitTS
	}
	if !writeAppliedIndex && newLastTS == s.lastCommitTS {
		return nil
	}
	if err := s.writeRaftApplyMetaLocked(newLastTS, writeAppliedIndex, appliedIndex, writeOpts); err != nil {
		return err
	}
	s.updateLastCommitTS(newLastTS)
	return nil
}

func (s *pebbleStore) staleRaftApplyFastPathLocked(mutations []*KVPairMutation, commitTS, appliedIndex uint64, writeOpts *pebble.WriteOptions) (bool, error) {
	if appliedIndex == 0 {
		return false, nil
	}
	landed, err := s.raftApplyAlreadyLandedLocked(mutations, commitTS)
	if err != nil || !landed {
		return false, err
	}
	return true, s.markRaftApplyAlreadyLandedLocked(commitTS, appliedIndex, writeOpts)
}

func (s *pebbleStore) hasCommitsAfter(startTS uint64) bool {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.lastCommitTS > startTS
}

func (s *pebbleStore) checkApplyConflicts(ctx context.Context, mutations []*KVPairMutation, readKeys [][]byte, startTS uint64) error {
	if !s.hasCommitsAfter(startTS) {
		return nil
	}
	if err := s.checkConflicts(ctx, mutations, startTS); err != nil {
		return err
	}
	return s.checkReadConflicts(ctx, readKeys, startTS)
}

func (s *pebbleStore) applyMutationsWithOpts(ctx context.Context, mutations []*KVPairMutation, readKeys [][]byte, startTS, commitTS uint64, writeOpts *pebble.WriteOptions, gateRegistration bool, appliedIndex uint64) error {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

	// Serialize conflict check → batch commit so concurrent ApplyMutations
	// cannot both pass checkConflicts and then both commit.
	s.applyMu.Lock()
	defer s.applyMu.Unlock()

	fastPathDone, err := s.staleRaftApplyFastPathLocked(mutations, commitTS, appliedIndex, writeOpts)
	if err != nil {
		return err
	}
	if fastPathDone {
		return nil
	}

	b := s.db.NewBatch()
	defer b.Close()

	// Keep OCC at the store boundary: callers pass the read snapshot via
	// startTS/readKeys, and leader-issued commit timestamps alone do not prove
	// the read set is still current.
	if err := s.checkApplyConflicts(ctx, mutations, readKeys, startTS); err != nil {
		return err
	}

	if err := s.applyMutationsBatch(b, mutations, commitTS, gateRegistration); err != nil {
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
	// Bundle metaAppliedIndex in the same batch as the data + commitTS
	// meta key so a crash either commits all three atomically or none.
	// appliedIndex==0 is the legacy / non-raft callers (ApplyMutations
	// or ApplyMutationsRaft); they leave the key unchanged.
	if appliedIndex > 0 {
		if err := setPebbleUint64InBatch(b, metaAppliedIndexBytes, appliedIndex); err != nil {
			s.mtx.Unlock()
			return err
		}
	}
	if err := b.Commit(writeOpts); err != nil {
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
//
// Like ApplyMutations, this direct-commit entry point unconditionally uses
// pebble.Sync so non-raft callers are never affected by
// ELASTICKV_FSM_SYNC_MODE=nosync. Raft-apply callers must use
// DeletePrefixAtRaft instead.
func (s *pebbleStore) DeletePrefixAt(ctx context.Context, prefix []byte, excludePrefix []byte, commitTS uint64) error {
	return s.deletePrefixAtWithOpts(ctx, prefix, excludePrefix, commitTS, s.directApplyWriteOpts(), 0)
}

// DeletePrefixAtRaft is the raft-apply variant of DeletePrefixAt. Durability
// is governed by s.fsmApplyWriteOpts (ELASTICKV_FSM_SYNC_MODE). See
// ApplyMutationsRaft for the full durability argument.
//
// Callers that have a raft entry index in hand SHOULD prefer
// DeletePrefixAtRaftAt to bundle metaAppliedIndex atomically — see
// PR #910 design §2 "why both leaves".
func (s *pebbleStore) DeletePrefixAtRaft(ctx context.Context, prefix []byte, excludePrefix []byte, commitTS uint64) error {
	return s.deletePrefixAtWithOpts(ctx, prefix, excludePrefix, commitTS, s.raftApplyWriteOpts(), 0)
}

// DeletePrefixAtRaftAt is DeletePrefixAtRaft with the raft entry
// index threaded through. handleDelPrefix builds an independent
// pebble.Batch separate from applyMutationsWithOpts, so the meta
// key bundle must happen here too — otherwise DEL_PREFIX entries
// would land without bumping metaAppliedIndex and silently leave
// LastAppliedIndex behind the true applied count for any workload
// that uses DEL_PREFIX. PR #910 design §2.
func (s *pebbleStore) DeletePrefixAtRaftAt(ctx context.Context, prefix []byte, excludePrefix []byte, commitTS, appliedIndex uint64) error {
	return s.deletePrefixAtWithOpts(ctx, prefix, excludePrefix, commitTS, s.raftApplyWriteOpts(), appliedIndex)
}

func (s *pebbleStore) deletePrefixAtWithOpts(_ context.Context, prefix []byte, excludePrefix []byte, commitTS uint64, writeOpts *pebble.WriteOptions, appliedIndex uint64) error {
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
	// Bundle metaAppliedIndex atomically with the tombstones + commitTS
	// — same rationale as applyMutationsWithOpts. appliedIndex==0 means
	// the legacy / non-raft caller path (DeletePrefixAt or
	// DeletePrefixAtRaft); leave the meta key unchanged.
	if appliedIndex > 0 {
		if err := setPebbleUint64InBatch(batch, metaAppliedIndexBytes, appliedIndex); err != nil {
			return err
		}
	}
	if err := batch.Commit(writeOpts); err != nil {
		return errors.WithStack(err)
	}
	s.updateLastCommitTS(newLastTS)

	return nil
}

func (s *pebbleStore) scanDeletePrefix(iter *pebble.Iterator, batch *pebble.Batch, prefix, excludePrefix []byte, commitTS uint64) error {
	tombstoneVal := encodeValue(nil, true, 0, encStateCleartext)

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
//
// Sole caller is scanDeletePrefix, which uses the bool to decide
// whether DeletePrefixAt needs to write a fresh tombstone for the
// observed live key. The read path's value-header tamper guard
// (rounds 3–5 of PR #742) is therefore reproduced here: for encrypted
// entries we run cipher.Decrypt over (header bytes ‖ pebble key) AAD
// before branching on the unauthenticated tombstone / expireAt fields.
// Without this, a disk attacker who flips the tombstone bit on an
// encrypted entry would cause DeletePrefixAt to skip writing the
// deletion tombstone — the key survives the prefix delete silently
// (a write-side integrity bypass, not just a transient wrong return).
func (s *pebbleStore) isVisibleLiveKey(iter *pebble.Iterator, userKey []byte, version, commitTS uint64) (bool, error) {
	if !s.seekToVisibleVersion(iter, userKey, version, commitTS) {
		return false, nil
	}
	sv, err := decodeValue(iter.Value())
	if err != nil {
		return false, errors.WithStack(err)
	}
	// decryptForKey authenticates the value-header bytes when the
	// entry is encrypted (cleartext entries no-op except for the
	// rebadge guard). We discard the plaintext — we only need the
	// authentication side-effect; tombstone / expireAt visibility
	// is then decided on now-trusted bytes.
	if _, err := s.decryptForKey(iter.Key(), sv, sv.Value); err != nil {
		return false, err
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

func (s *pebbleStore) cleanupPendingCompaction(retainPending *bool) {
	if retainPending != nil && *retainPending {
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

func (s *pebbleStore) compactionWatermarks() (pending, committed uint64) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.pendingMinRetainedTS, s.minRetainedTS
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
		if compactionDeleteBatchShouldFlush(*batch) {
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
		return stats, errors.WithStack(err)
	}
	if err := commitCompactionDeletes(&batch, &stats); err != nil {
		return stats, err
	}
	return stats, nil
}

func (s *pebbleStore) runCompactionPhase(ctx context.Context, targetMinTS uint64, resumingPending bool) (pebbleCompactionStats, error) {
	retainPending := resumingPending
	stats, err := s.runCompactionGC(ctx, targetMinTS)
	retainPending = retainPending || stats.committedDeletes
	if err != nil {
		s.cleanupPendingCompaction(&retainPending)
		return stats, err
	}

	if err := s.commitCompactionMinRetainedTS(targetMinTS); err != nil {
		s.cleanupPendingCompaction(&retainPending)
		return stats, err
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

	for {
		pending, committed := s.compactionWatermarks()
		resumingPending := pending > committed
		targetMinTS := minTS
		if resumingPending {
			// A pending watermark means an earlier compaction may have already
			// committed delete batches. Keep reads fail-closed, but do not finalize
			// the watermark until an idempotent GC pass reaches EOF.
			targetMinTS = pending
		} else if minTS <= committed {
			return nil
		}

		if !resumingPending {
			shouldRun, err := s.beginCompaction(targetMinTS)
			if err != nil {
				return err
			}
			if !shouldRun {
				return nil
			}
		}

		stats, err := s.runCompactionPhase(ctx, targetMinTS, resumingPending)
		if err != nil {
			return err
		}

		s.log.InfoContext(ctx, "compact",
			slog.Uint64("min_ts", targetMinTS),
			slog.Int("updated_keys", stats.updatedKeys),
			slog.Int("deleted_versions", stats.deletedVersions),
		)
		if targetMinTS >= minTS {
			return nil
		}
	}
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
	// Native Pebble snapshots ship raw on-disk bytes, which for an
	// encrypted row is value-header(9B) + envelope-overhead(34B) +
	// ciphertext. The cap must accommodate envelope overhead so a
	// plaintext written at maxSnapshotValueSize round-trips through
	// snapshot restore — without it, validateValueSize accepts the
	// plaintext but restore rejects the encrypted body with
	// ErrValueTooLarge.
	vLen, err = readRestoreFieldLen(r, "snapshot value",
		maxSnapshotValueSize+valueHeaderSize+encryption.EnvelopeOverhead)
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

func compactionDeleteBatchShouldFlush(batch *pebble.Batch) bool {
	return batch.Count() >= compactionDeleteBatchCountLimit || batch.Len() >= compactionDeleteBatchByteLimit
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
	// MVCC snapshot format v2 does not carry encryption_state — Stage 8 of
	// the encryption rollout (per docs/design/2026_04_29_proposed...) bumps
	// the format to v3 to round-trip encrypted entries through this path.
	// Until then, restored versions are written as cleartext and any node
	// snapshotting/restoring an encrypted dataset must use the native
	// Pebble snapshot path (snapshot_pebble.go), which ships raw bytes
	// and thus preserves the on-disk envelope verbatim.
	fillEncodedValue(deferred.Value, version.Value, version.Tombstone, version.ExpireAt, encStateCleartext)
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
		return errors.WithStack(errors.Newf("unrecognized snapshot format: unknown magic header"))
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
		if s.cache != nil {
			s.cache.Unref()
			s.cache = nil
		}
	}
	if err := os.RemoveAll(s.dir); err != nil {
		return errors.WithStack(err)
	}
	if err := os.MkdirAll(s.dir, dirPerms); err != nil {
		return errors.WithStack(err)
	}
	opts, cache := defaultPebbleOptionsWithCache()
	db, err := pebble.Open(s.dir, opts)
	if err != nil {
		cache.Unref()
		return errors.WithStack(err)
	}
	s.db = db
	s.cache = cache
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
	opts, cache := defaultPebbleOptionsWithCache()
	tmpDB, err := pebble.Open(tmpDir, opts)
	if err != nil {
		cache.Unref()
		return errors.WithStack(err)
	}
	// The tmpDB is discarded once swapInTempDB renames its directory over
	// s.dir (which reopens with a fresh cache), so release our creator ref
	// as soon as we're done writing to avoid leaking per-snapshot caches.
	defer cache.Unref()

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
	opts, cache := defaultPebbleOptionsWithCache()
	tmpDB, err := pebble.Open(tmpDir, opts)
	if err != nil {
		cache.Unref()
		return "", errors.WithStack(err)
	}
	// As in writeNativeSnapshotToTempDir, the tmpDB is discarded once the
	// directory is renamed, so drop our creator ref when this helper
	// returns. swapInTempDB reopens with a fresh cache.
	defer cache.Unref()
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
	if s.cache != nil {
		s.cache.Unref()
		s.cache = nil
	}
	if err := os.RemoveAll(s.dir); err != nil {
		_ = os.RemoveAll(tmpDir)
		return errors.WithStack(err)
	}
	if err := os.Rename(tmpDir, s.dir); err != nil {
		_ = os.RemoveAll(tmpDir)
		return errors.WithStack(err)
	}
	opts, cache := defaultPebbleOptionsWithCache()
	newDB, err := pebble.Open(s.dir, opts)
	if err != nil {
		cache.Unref()
		return errors.WithStack(err)
	}
	s.db = newDB
	s.cache = cache
	// cleanupOnSwapFail mirrors the cleanup in NewPebbleStore: release the
	// creator ref on the freshly-opened cache so a failed metadata read
	// does not pin a ~256 MiB cache on the store across restore retries.
	cleanupOnSwapFail := func() {
		_ = newDB.Close()
		cache.Unref()
		s.db = nil
		s.cache = nil
	}
	lastCommitTS, err := s.findMaxCommitTS()
	if err != nil {
		cleanupOnSwapFail()
		return err
	}
	minRetainedTS, err := s.findMinRetainedTS()
	if err != nil {
		cleanupOnSwapFail()
		return err
	}
	pendingMinRetainedTS, err := s.findPendingMinRetainedTS()
	if err != nil {
		cleanupOnSwapFail()
		return err
	}
	s.lastCommitTS = lastCommitTS
	s.minRetainedTS = minRetainedTS
	s.pendingMinRetainedTS = pendingMinRetainedTS
	return nil
}

func (s *pebbleStore) Close() error {
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	// Acquire dbMu exclusively so that all in-flight DB operations finish
	// before we close the database.  Lock ordering: dbMu before mtx.
	s.dbMu.Lock()
	defer s.dbMu.Unlock()
	err := s.db.Close()
	// Release our creator reference on the block cache so the memory is
	// freed once pebble has also dropped its internal reference (which
	// Close does). Safe to call after a Close error: Unref is
	// idempotent-friendly in that s.cache is only nilled out here, and
	// Close() is not expected to be called twice.
	if s.cache != nil {
		s.cache.Unref()
		s.cache = nil
	}
	return errors.WithStack(err)
}

// BlockCacheCapacityBytes returns the configured maximum size of the
// underlying Pebble block cache in bytes, or 0 if the store is closed /
// mid-restore. Exported for the monitoring collector so operators can graph
// configured capacity alongside current usage.
func (s *pebbleStore) BlockCacheCapacityBytes() int64 {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	if s.cache == nil {
		return 0
	}
	return s.cache.MaxSize()
}

// Metrics returns a snapshot of the underlying Pebble DB's operational
// metrics (LSM shape, compaction debt, memtable, block cache). The
// return value is a freshly allocated *pebble.Metrics owned by the
// caller.
//
// Returns nil only before the first Open has installed a DB or after a
// failed Open left s.db unset; callers during an in-flight Restore block
// on dbMu (which Restore holds exclusively) rather than observing nil,
// and Close() does not clear s.db. Callers must still handle nil for the
// pre-Open case.
//
// Safe for concurrent use: takes the dbMu read lock to protect against
// Restore swapping the DB pointer.
func (s *pebbleStore) Metrics() *pebble.Metrics {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	if s.db == nil {
		return nil
	}
	return s.db.Metrics()
}
