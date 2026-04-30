package backup

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"syscall"

	cockroachdberr "github.com/cockroachdb/errors"
)

// Redis simple-type encoders translate raw snapshot key/value records into
// the per-adapter directory tree defined by Phase 0
// (docs/design/2026_04_29_proposed_snapshot_logical_decoder.md). This file
// covers the three "simple" Redis prefixes — strings, HLLs, and TTL scan
// index entries — that always map to ONE snapshot record per user key and
// therefore need no cross-record assembly.
//
// Hash / list / set / zset / stream prefixes carry user keys spread across
// multiple wide-column rows and ship in a follow-up PR.

// Snapshot key prefixes the encoder dispatches on. Kept in sync with
// adapter/redis_compat_types.go so a renamed prefix in the live code is
// caught here at compile time via the corresponding tests.
const (
	RedisStringPrefix = "!redis|str|"
	RedisHLLPrefix    = "!redis|hll|"
	RedisTTLPrefix    = "!redis|ttl|"

	// redisStrMagic / redisStrVersion / redisStrHasTTL / redisStrBaseHeader
	// mirror adapter/redis_compat_types.go:20-24. Re-defined here rather
	// than imported because the backup package is intentionally adapter-
	// independent (it must run as an offline tool with no live cluster).
	redisStrMagic      byte = 0xFF
	redisStrVersion    byte = 0x01
	redisStrHasTTL     byte = 0x01
	redisStrBaseHeader      = 3
	redisUint64Bytes        = 8

	redisStringsTTLFile = "strings_ttl.jsonl"
	redisHLLTTLFile     = "hll_ttl.jsonl"

	// redisJSONLBufSize is the bufio.Writer buffer for the per-database
	// TTL sidecar files. The same 64 KiB tuning as KeymapWriter — large
	// enough to amortise per-syscall cost across thousands of TTL records.
	redisJSONLBufSize = 64 << 10
)

// ErrRedisInvalidStringValue is returned when a !redis|str| value uses the
// new magic-prefix format but its declared TTL section is truncated. Legacy
// (no-magic) values are accepted as opaque raw bytes.
var ErrRedisInvalidStringValue = cockroachdberr.New("backup: invalid !redis|str| value")

// ErrRedisInvalidTTLValue is returned when a !redis|ttl| value is not the
// expected 8-byte big-endian uint64 millisecond expiry.
var ErrRedisInvalidTTLValue = cockroachdberr.New("backup: invalid !redis|ttl| value")

// redisKeyKind tracks which Redis-type prefix introduced a user key, so that
// when a later !redis|ttl|<K> record arrives we know whether to write its
// expiry into strings_ttl.jsonl, hll_ttl.jsonl, or buffer it for a wide-
// column type (hash/list/set/zset/stream).
type redisKeyKind uint8

const (
	redisKindUnknown redisKeyKind = iota
	redisKindString
	redisKindHLL
)

// RedisDB encodes one logical Redis database (`redis/db_<n>/`). All
// operations are scoped to its outRoot; the caller wires per-database
// instances when the producer supports multiple databases (today only
// db_0 is meaningful, but the encoder is wired to take any non-negative
// index so a future multi-db dump does not silently collide on db_0).
//
// Lifecycle:
//
//	r := NewRedisDB(outRoot, dbIndex)
//	for each snapshot record matching a redis prefix: r.Handle*(...)
//	r.Finalize()
//
// Handle* methods are NOT goroutine-safe; the decoder pipeline is
// inherently sequential per scope, so a mutex would only add cost.
type RedisDB struct {
	outRoot string
	dbIndex int

	// kindByKey records the Redis type each user key was first seen as.
	// Populated by HandleString and HandleHLL; consulted by HandleTTL.
	// Sized for typical clusters (millions of keys × ~50 bytes each is
	// affordable on the dump host); a follow-up PR introducing the
	// wide-column types may switch to a streamed approach if profiling
	// shows this is the binding cost.
	kindByKey map[string]redisKeyKind

	// stringsTTL / hllTTL are lazily opened on first write. Per the spec,
	// empty sidecar files are omitted from the dump.
	stringsTTL *jsonlFile
	hllTTL     *jsonlFile

	// pendingWideColumnTTL accumulates !redis|ttl| records whose user key
	// has not been claimed by HandleString / HandleHLL. These are
	// candidates for hashes/lists/sets/zsets/streams (handled in a
	// follow-up PR) — for now Finalize logs them via the warning hook
	// rather than dropping silently. Bounded by maxPendingWideColumnTTL
	// so a malformed snapshot with millions of orphan TTL records cannot
	// drive the encoder OOM; the bound is high enough that real
	// production state (where wide-column type encoders eventually
	// claim every TTL) is never affected.
	pendingWideColumnTTL []redisTTLPending

	// dirsCreated caches the per-encoder directories writeBlob and
	// appendTTL have already MkdirAll'd. Avoids the per-record syscalls
	// flagged by Gemini #218; for a 10M-key dump this saves ~10M
	// stat+mkdir(EEXIST) round-trips.
	dirsCreated map[string]struct{}

	// inlineTTLEmitted tracks string keys whose TTL was already
	// extracted from the inline magic-prefix header by HandleString and
	// written to strings_ttl.jsonl. The live Redis encoder emits BOTH
	// `!redis|str|<k>` (with inline TTL) and `!redis|ttl|<k>` (the
	// scan-index entry the sweeper consumes) for an expiring string
	// (see adapter/redis_lua_context.go stringCommitElems). Without
	// this set, HandleTTL would route the redundant `!redis|ttl|`
	// record back into the same sidecar, duplicating the entry and
	// violating the one-record-per-key contract sidecar consumers
	// rely on. Codex P1 round 5.
	inlineTTLEmitted map[string]struct{}

	// warn is the structured-warning sink. Non-nil in production
	// (fed by the decoder driver); nil in tests if the test does not
	// care about warnings.
	warn func(event string, fields ...any)
}

type redisTTLPending struct {
	UserKey    []byte
	ExpireAtMs uint64
}

// maxPendingWideColumnTTL caps the orphan-TTL buffer. 1M entries is well
// past anything a real Redis instance produces under normal operation
// (each entry is ~50 bytes, so the cap is ~50 MiB) but small enough that
// a snapshot loaded with a billion synthetic !redis|ttl| records cannot
// drive the encoder OOM. When the cap is hit, HandleTTL drops further
// orphans and surfaces a structured warning at Finalize.
const maxPendingWideColumnTTL = 1_000_000

// NewRedisDB constructs a RedisDB rooted at <outRoot>/redis/db_<n>/.
// dbIndex selects <n>; today the producer always passes 0, but accepting
// the index as a parameter prevents a future multi-db dump from silently
// colliding on db_0.
func NewRedisDB(outRoot string, dbIndex int) *RedisDB {
	if dbIndex < 0 {
		dbIndex = 0
	}
	return &RedisDB{
		outRoot:          outRoot,
		dbIndex:          dbIndex,
		kindByKey:        make(map[string]redisKeyKind),
		dirsCreated:      make(map[string]struct{}),
		inlineTTLEmitted: make(map[string]struct{}),
	}
}

// WithWarnSink wires a structured-warning sink. The sink is called with
// stable event names ("redis_orphan_ttl", etc.) and key=value pairs.
func (r *RedisDB) WithWarnSink(fn func(event string, fields ...any)) *RedisDB {
	r.warn = fn
	return r
}

// HandleString processes one !redis|str|<userKey> record. The value is the
// raw stored bytes; HandleString peels the magic-prefix TTL header (if
// present) and writes the user-visible value to strings/<encoded>.bin and
// the TTL — if any — to strings_ttl.jsonl.
func (r *RedisDB) HandleString(userKey, value []byte) error {
	r.kindByKey[string(userKey)] = redisKindString
	userValue, expireAtMs, err := decodeRedisStringValue(value)
	if err != nil {
		return err
	}
	if err := r.writeBlob("strings", userKey, userValue); err != nil {
		return err
	}
	if expireAtMs == 0 {
		return nil
	}
	// Mark the key as already emitted inline so HandleTTL can drop the
	// redundant !redis|ttl| scan-index record; otherwise the same
	// expiring string would be written to strings_ttl.jsonl twice.
	r.inlineTTLEmitted[string(userKey)] = struct{}{}
	return r.appendTTL(&r.stringsTTL, redisStringsTTLFile, userKey, expireAtMs)
}

// HandleHLL processes one !redis|hll|<userKey> record. The value is the
// raw HLL sketch bytes, written byte-for-byte to hll/<encoded>.bin. TTL
// for HLL keys lives in !redis|ttl|<userKey> and is consumed by
// HandleTTL.
func (r *RedisDB) HandleHLL(userKey, value []byte) error {
	r.kindByKey[string(userKey)] = redisKindHLL
	return r.writeBlob("hll", userKey, value)
}

// HandleTTL processes one !redis|ttl|<userKey> record. Routing depends on
// what HandleString/HandleHLL recorded for the same userKey:
//
//   - redisKindHLL    -> hll_ttl.jsonl
//   - redisKindString -> strings_ttl.jsonl (legacy strings, whose TTL
//     lives in !redis|ttl| rather than the inline magic-prefix header)
//   - redisKindUnknown -> buffered as pendingWideColumnTTL; reported via
//     the warn sink on Finalize because Phase 0a's wide-column encoders
//     have not landed yet.
func (r *RedisDB) HandleTTL(userKey, value []byte) error {
	expireAtMs, err := decodeRedisTTLValue(value)
	if err != nil {
		return err
	}
	switch r.kindByKey[string(userKey)] {
	case redisKindHLL:
		return r.appendTTL(&r.hllTTL, redisHLLTTLFile, userKey, expireAtMs)
	case redisKindString:
		// New-format strings carry TTL inline in the magic-prefix
		// header; HandleString already wrote the entry to
		// strings_ttl.jsonl. The `!redis|ttl|` scan-index record
		// the sweeper consumes is redundant for backup output. Only
		// legacy strings (no inline TTL) reach the appendTTL call.
		// Codex P1 round 5.
		if _, ok := r.inlineTTLEmitted[string(userKey)]; ok {
			return nil
		}
		return r.appendTTL(&r.stringsTTL, redisStringsTTLFile, userKey, expireAtMs)
	case redisKindUnknown:
		// Bounded to prevent OOM on a snapshot that contains a
		// runaway number of orphan TTL records (e.g., many wide-
		// column types whose meta records were dropped). After the
		// cap, additional records are tracked only as a counter via
		// the warning sink at Finalize.
		if len(r.pendingWideColumnTTL) < maxPendingWideColumnTTL {
			r.pendingWideColumnTTL = append(r.pendingWideColumnTTL, redisTTLPending{
				UserKey:    bytes.Clone(userKey),
				ExpireAtMs: expireAtMs,
			})
		}
		return nil
	}
	return nil
}

// Finalize flushes all open sidecar writers and emits warnings for any
// pending TTL records whose user key was never claimed by the wide-column
// encoders. Call exactly once after every snapshot record has been
// dispatched.
func (r *RedisDB) Finalize() error {
	var firstErr error
	if err := closeJSONL(r.stringsTTL); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := closeJSONL(r.hllTTL); err != nil && firstErr == nil {
		firstErr = err
	}
	if r.warn != nil && len(r.pendingWideColumnTTL) > 0 {
		r.warn("redis_orphan_ttl",
			"count", len(r.pendingWideColumnTTL),
			"hint", "wide-column type encoders (hash/list/set/zset/stream) have not landed yet")
	}
	return firstErr
}

// dbDir returns the per-encoder root, e.g. "<outRoot>/redis/db_0/".
// Computed once per call rather than at construction so the encoder's
// outRoot remains a plain field — easier to reason about in tests.
func (r *RedisDB) dbDir() string {
	return filepath.Join(r.outRoot, "redis", redisDBSegment(r.dbIndex))
}

func redisDBSegment(idx int) string {
	if idx < 0 {
		idx = 0
	}
	return "db_" + intToDecimal(idx)
}

// intToDecimal is a tiny zero-allocation helper for non-negative ints.
// Avoids the strconv import here just to format dbIndex.
func intToDecimal(v int) string {
	if v == 0 {
		return "0"
	}
	const maxIntDecimalDigits = 20 // covers MaxInt64
	var buf [maxIntDecimalDigits]byte
	pos := len(buf)
	for v > 0 {
		pos--
		buf[pos] = '0' + byte(v%10) //nolint:mnd // 10 == decimal radix
		v /= 10                     //nolint:mnd // 10 == decimal radix
	}
	return string(buf[pos:])
}

// ensureDir runs MkdirAll once per directory and remembers the result
// in r.dirsCreated, so repeated calls on the hot path (one per blob
// record) collapse to a map lookup.
func (r *RedisDB) ensureDir(dir string) error {
	if _, ok := r.dirsCreated[dir]; ok {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil { //nolint:mnd // 0755 == standard dir mode
		return cockroachdberr.WithStack(err)
	}
	r.dirsCreated[dir] = struct{}{}
	return nil
}

func (r *RedisDB) writeBlob(subdir string, userKey, value []byte) error {
	encoded := EncodeSegment(userKey)
	dir := filepath.Join(r.dbDir(), subdir)
	if err := r.ensureDir(dir); err != nil {
		return err
	}
	path := filepath.Join(dir, encoded+".bin")
	if err := writeFileAtomic(path, value); err != nil {
		return cockroachdberr.WithStack(err)
	}
	return nil
}

func (r *RedisDB) appendTTL(slot **jsonlFile, baseName string, userKey []byte, expireAtMs uint64) error {
	if *slot == nil {
		f, err := openJSONL(filepath.Join(r.dbDir(), baseName))
		if err != nil {
			return err
		}
		*slot = f
	}
	rec := struct {
		Key        string `json:"key"`
		ExpireAtMs uint64 `json:"expire_at_ms"`
	}{
		Key:        EncodeSegment(userKey),
		ExpireAtMs: expireAtMs,
	}
	if err := (*slot).enc.Encode(rec); err != nil {
		return cockroachdberr.WithStack(err)
	}
	return nil
}

// decodeRedisStringValue strips the redis-string magic-prefix TTL header
// (if present) from a !redis|str| value and returns (userValue,
// expireAtMs). expireAtMs == 0 means "no inline TTL"; legacy values
// always return 0 here because their TTL lives in !redis|ttl|.
func decodeRedisStringValue(value []byte) ([]byte, uint64, error) {
	if !isNewRedisStrFormat(value) {
		return value, 0, nil
	}
	if len(value) < redisStrBaseHeader {
		return nil, 0, cockroachdberr.Wrap(ErrRedisInvalidStringValue, "header truncated")
	}
	flags := value[2]
	rest := value[redisStrBaseHeader:]
	if flags&redisStrHasTTL == 0 {
		return rest, 0, nil
	}
	if len(rest) < redisUint64Bytes {
		return nil, 0, cockroachdberr.Wrap(ErrRedisInvalidStringValue, "ttl section truncated")
	}
	rawMs := binary.BigEndian.Uint64(rest[:redisUint64Bytes])
	expireAtMs := rawMs
	if expireAtMs > math.MaxInt64 {
		expireAtMs = math.MaxInt64 // mirror live decoder's clamp
	}
	return rest[redisUint64Bytes:], expireAtMs, nil
}

func isNewRedisStrFormat(raw []byte) bool {
	return len(raw) >= 2 && //nolint:mnd // 2 == magic + version length
		raw[0] == redisStrMagic && raw[1] == redisStrVersion
}

func decodeRedisTTLValue(raw []byte) (uint64, error) {
	if len(raw) != redisUint64Bytes {
		return 0, cockroachdberr.Wrapf(ErrRedisInvalidTTLValue,
			"length %d != %d", len(raw), redisUint64Bytes)
	}
	v := binary.BigEndian.Uint64(raw)
	if v > math.MaxInt64 {
		v = math.MaxInt64
	}
	return v, nil
}

// jsonlFile bundles a file handle and its bufio writer so callers can
// `f.enc.Encode(rec)` without re-creating the encoder per write.
type jsonlFile struct {
	f   *os.File
	bw  *bufio.Writer
	enc *json.Encoder
}

func openJSONL(path string) (*jsonlFile, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil { //nolint:mnd // 0755 == standard dir mode
		return nil, cockroachdberr.WithStack(err)
	}
	// Refuse to clobber a symlink at the sidecar path. os.Create
	// follows symlinks and would truncate the target outside the
	// dump tree. writeFileAtomic already defends blob writes the
	// same way; sidecar creation must mirror that boundary. Codex P2
	// round 5.
	if info, err := os.Lstat(path); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return nil, cockroachdberr.WithStack(cockroachdberr.Newf("backup: refusing to overwrite symlink at %s", path))
	}
	f, err := os.Create(path) //nolint:gosec // path is composed from output-root + fixed file name
	if err != nil {
		return nil, cockroachdberr.WithStack(err)
	}
	bw := bufio.NewWriterSize(f, redisJSONLBufSize)
	enc := json.NewEncoder(bw)
	enc.SetEscapeHTML(false)
	return &jsonlFile{f: f, bw: bw, enc: enc}, nil
}

func closeJSONL(jl *jsonlFile) error {
	if jl == nil {
		return nil
	}
	flushErr := jl.bw.Flush()
	closeErr := jl.f.Close()
	switch {
	case flushErr != nil:
		return cockroachdberr.WithStack(flushErr)
	case closeErr != nil:
		return cockroachdberr.WithStack(closeErr)
	}
	return nil
}

// writeFileAtomic writes data to path via a tmp+rename so a crash
// mid-write never leaves a partial file. Symbolic links are not followed
// (os.Create truncates a symlink target rather than the link itself; we
// reject symlinks explicitly).
func writeFileAtomic(path string, data []byte) error {
	if info, err := os.Lstat(path); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return cockroachdberr.WithStack(cockroachdberr.Newf("backup: refusing to overwrite symlink at %s", path))
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".bin.tmp-*")
	if err != nil {
		return cockroachdberr.WithStack(err)
	}
	tmpPath := tmp.Name()
	defer func() {
		// Best-effort cleanup if Rename did not consume tmpPath.
		if _, statErr := os.Stat(tmpPath); statErr == nil {
			_ = os.Remove(tmpPath)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return cockroachdberr.WithStack(err)
	}
	if err := tmp.Close(); err != nil {
		return cockroachdberr.WithStack(err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return cockroachdberr.WithStack(err)
	}
	return nil
}

// HasInlineTTL reports whether a !redis|str| value carries the new-format
// inline TTL header. Useful for tests asserting the producer's choice.
func HasInlineTTL(value []byte) bool {
	if !isNewRedisStrFormat(value) || len(value) < redisStrBaseHeader {
		return false
	}
	return value[2]&redisStrHasTTL != 0
}

// IsBlobAtomicWriteRetriable reports whether err from writeFileAtomic
// is a retriable I/O failure. Today the only retriable signal is
// io.ErrShortWrite. ENOSPC (disk full) is intentionally NOT retriable
// here — the master pipeline must surface it to the operator rather
// than spin: a backup against a full disk has no business retrying.
// IsBlobAtomicWriteOutOfSpace is the explicit out-of-space probe so
// the pipeline can choose the right alarm wording.
func IsBlobAtomicWriteRetriable(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, io.ErrShortWrite)
}

// IsBlobAtomicWriteOutOfSpace reports whether err from writeFileAtomic
// (or any os.File write the master pipeline issues) was driven by a
// full disk. Tested via syscall.ENOSPC + os.PathError unwrap, which
// matches what os.File.Write returns on POSIX and Windows.
func IsBlobAtomicWriteOutOfSpace(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, syscall.ENOSPC)
}
