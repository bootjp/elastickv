package backup

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"
)

// encode_redis.go is the Phase 0b Redis reverse encoder — the inverse
// of the RedisDB decoder in redis_*.go (design:
// docs/design/2026_05_25_proposed_snapshot_logical_encoder.md §"Redis").
// It walks a decoded redis/db_<n>/ subtree and feeds the reconstructed
// internal records to a snapshotBuilder, which MVCC-frames and writes
// them.
//
// This commit covers the simple-value families: strings, HLL, and the
// TTL handling for both. The wide-column collections (hash, list, set,
// zset, stream) land in subsequent commits on the same milestone.
//
// Format fidelity is pinned against the live adapter write path
// (adapter/redis_compat_types.go, adapter/redis.go), not just the
// decode side, so the emitted .fsm loads into a running cluster:
//
//   - String values carry TTL INLINE in the value header
//     (encodeRedisStr: [0xFF 0x01][flags][expireMs BE if has_ttl][body]).
//     The live store sets NO MVCC-level expireAt for redis writes
//     (kv.Elem has no expiry field) and writes NO !redis|ttl| scan-index
//     row for strings (buildTTLElems explicitly skips string keys).
//   - HLL values are raw sketch bytes with no inline header; their TTL
//     lives in a !redis|ttl|<userKey> scan-index row (8-byte BE ms),
//     matching buildTTLElems for non-string types.
//
// All redis entries therefore use MVCC value-header expireAt = 0
// (the redis adapter manages expiry itself); the builder's expireAt
// argument is always 0 here.

// ErrRedisEncodeMissingKeymap is returned when a strings/ or hll/ file
// name (or a TTL sidecar key) took the SHA-fallback encoding but the
// db's KEYMAP.jsonl has no matching record to recover the original
// user-key bytes. The encoder fails closed rather than emit a record
// under a truncated/hashed key the live cluster would never serve.
var ErrRedisEncodeMissingKeymap = errors.New("backup: redis encode missing KEYMAP entry for sha-fallback key")

// ErrRedisEncodeNotDir is returned when the redis/db_<n> path exists
// but is a regular file rather than a directory — a malformed dump.
// A dedicated sentinel (not ErrRedisEncodeMissingKeymap) so callers
// can distinguish "bad dump layout" from "sha-fallback key without a
// keymap entry" via errors.Is.
var ErrRedisEncodeNotDir = errors.New("backup: redis db path is not a directory")

// ErrRedisEncodeNotRegular is returned when a dump sidecar
// (KEYMAP.jsonl, strings_ttl.jsonl, ...) exists but is not a regular
// file — a symlink, FIFO, device, or directory. Reading such a path
// with plain os.Open would follow the symlink or block indefinitely on
// a reader-less FIFO; the encoder fails closed instead, matching the
// non-regular refusal walkBlobDir applies to *.bin entries (codex P2
// on PR #828).
var ErrRedisEncodeNotRegular = errors.New("backup: redis dump sidecar is not a regular file")

// RedisEncoder reconstructs the internal Redis keyspace for one logical
// database (redis/db_<n>/) from its decoded directory tree.
type RedisEncoder struct {
	inRoot  string
	dbIndex int
	keymap  map[string]KeymapRecord
}

// NewRedisEncoder constructs an encoder rooted at <inRoot>/redis/db_<n>/.
// Negative dbIndex is coerced to 0 (mirrors NewRedisDB).
func NewRedisEncoder(inRoot string, dbIndex int) *RedisEncoder {
	if dbIndex < 0 {
		dbIndex = 0
	}
	return &RedisEncoder{inRoot: inRoot, dbIndex: dbIndex}
}

func (e *RedisEncoder) dbDir() string {
	return filepath.Join(e.inRoot, "redis", redisDBSegment(e.dbIndex))
}

// Encode walks the db subtree and stages every reconstructed record on
// b. A missing db directory is not an error — there is simply nothing
// to encode for that database.
func (e *RedisEncoder) Encode(b *snapshotBuilder) error {
	dir := e.dbDir()
	info, err := os.Stat(dir)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return nil
	case err != nil:
		return errors.WithStack(err)
	case !info.IsDir():
		return errors.Wrapf(ErrRedisEncodeNotDir, "db path %q", dir)
	}
	if err := e.loadKeymap(); err != nil {
		return err
	}
	if err := e.encodeStrings(b); err != nil {
		return err
	}
	return e.encodeHLL(b)
}

// loadKeymap reads the db's KEYMAP.jsonl (if present) so sha-fallback
// encoded names can be reversed to their original key bytes. Absence is
// fine — dumps without any long/sha-fallback keys carry no KEYMAP file.
func (e *RedisEncoder) loadKeymap() error {
	f, err := openDumpSidecar(e.dbDir(), "KEYMAP.jsonl")
	if errors.Is(err, os.ErrNotExist) {
		e.keymap = map[string]KeymapRecord{}
		return nil
	}
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	m, err := LoadKeymap(f)
	if err != nil {
		return err
	}
	e.keymap = m
	return nil
}

// resolveKey turns an encoded path segment / TTL key back into the
// original Redis user-key bytes: percent/binary segments decode
// directly; SHA-fallback segments are recovered from KEYMAP.jsonl.
func (e *RedisEncoder) resolveKey(encoded string) ([]byte, error) {
	raw, err := DecodeSegment(encoded)
	if err == nil {
		return raw, nil
	}
	if !errors.Is(err, ErrShaFallbackNeedsKeymap) {
		return nil, err
	}
	rec, ok := e.keymap[encoded]
	if !ok {
		return nil, errors.Wrapf(ErrRedisEncodeMissingKeymap, "encoded %q", encoded)
	}
	original, err := rec.Original()
	if err != nil {
		return nil, errors.Wrapf(err, "encoded %q", encoded)
	}
	return original, nil
}

// encodeStrings reconstructs !redis|str| records from strings/*.bin,
// folding any strings_ttl.jsonl expiry back into the inline value
// header (the live string format). No !redis|ttl| row is emitted for
// strings — buildTTLElems skips them.
func (e *RedisEncoder) encodeStrings(b *snapshotBuilder) error {
	ttls, err := e.loadTTLMap(redisStringsTTLFile)
	if err != nil {
		return err
	}
	return e.walkBlobDir("strings", func(encoded string, rawKey, body []byte) error {
		value := encodeRedisStrInlineValue(body, ttls[encoded])
		key := append([]byte(RedisStringPrefix), rawKey...)
		return b.Add(key, value, 0)
	})
}

// encodeHLL reconstructs !redis|hll| records from hll/*.bin (raw sketch
// bytes) plus the !redis|ttl| scan-index row for any expiring HLL key,
// matching buildTTLElems for non-string types.
func (e *RedisEncoder) encodeHLL(b *snapshotBuilder) error {
	ttls, err := e.loadTTLMap(redisHLLTTLFile)
	if err != nil {
		return err
	}
	return e.walkBlobDir("hll", func(encoded string, rawKey, body []byte) error {
		key := append([]byte(RedisHLLPrefix), rawKey...)
		if err := b.Add(key, body, 0); err != nil {
			return err
		}
		expireMs, ok := ttls[encoded]
		if !ok || expireMs == 0 {
			return nil
		}
		ttlKey := append([]byte(RedisTTLPrefix), rawKey...)
		return b.Add(ttlKey, encodeRedisTTLValueMs(expireMs), 0)
	})
}

// walkBlobDir iterates <dbDir>/<subdir>/*.bin, resolves each filename
// to its original user key, reads the body, and invokes fn. A missing
// subdir is not an error. Non-.bin entries and sub-directories are
// skipped.
//
// Files are read through an os.Root rooted at the subdir, so a symlink
// inside the dump that points outside the subdir is refused — a
// crafted dump cannot make the encoder exfiltrate an arbitrary host
// file. This also keeps the read off the tainted-path G304 lint path
// without a //nolint suppression (the os.Root API is the gosec-blessed
// safe-file-access primitive).
func (e *RedisEncoder) walkBlobDir(subdir string, fn func(encoded string, rawKey, body []byte) error) error {
	dir := filepath.Join(e.dbDir(), subdir)
	root, err := os.OpenRoot(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() { _ = root.Close() }()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return errors.WithStack(err)
	}
	for _, ent := range entries {
		// Only regular .bin files. !IsRegular() skips directories,
		// symlinks, and — the point of the guard — FIFOs / sockets /
		// devices, which would otherwise reach io.ReadAll and block or
		// misbehave (parity with openSidecarFile's non-regular refusal
		// on the write side).
		if !ent.Type().IsRegular() || !strings.HasSuffix(ent.Name(), ".bin") {
			continue
		}
		encoded := strings.TrimSuffix(ent.Name(), ".bin")
		rawKey, err := e.resolveKey(encoded)
		if err != nil {
			return err
		}
		body, err := readRootFile(root, ent.Name())
		if err != nil {
			return err
		}
		if err := fn(encoded, rawKey, body); err != nil {
			return err
		}
	}
	return nil
}

// openDumpSidecar opens a fixed-name regular file at <dir>/<name> for
// reading, refusing symlinks and non-regular files (FIFO / device /
// directory) — the read-side analogue of walkBlobDir's
// os.Root + IsRegular guard for the dump's sidecar files (KEYMAP.jsonl,
// *_ttl.jsonl). Returns a wrapped os.ErrNotExist when the file is
// absent so callers can treat a missing sidecar as empty.
//
// The Lstat type check refuses a symlink (Lstat sees the link itself,
// not its target) and a reader-less FIFO BEFORE any blocking open, then
// the read goes through an os.Root so the open additionally cannot
// escape <dir>. A concurrent swap of a confirmed-regular file to a FIFO
// between Lstat and open is the only residual race and is not a
// concern for an offline tool reading a static dump tree.
func openDumpSidecar(dir, name string) (*os.File, error) {
	info, err := os.Lstat(filepath.Join(dir, name))
	if err != nil {
		return nil, errors.WithStack(err) // wraps os.ErrNotExist when absent
	}
	if !info.Mode().IsRegular() {
		return nil, errors.Wrapf(ErrRedisEncodeNotRegular, "%s (mode=%s)", name, info.Mode())
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer func() { _ = root.Close() }()
	f, err := root.Open(name)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return f, nil
}

// readRootFile reads a regular file by name within root, refusing any
// path that escapes the root (including via an in-dump symlink to an
// external target).
func readRootFile(root *os.Root, name string) ([]byte, error) {
	f, err := root.Open(name)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer func() { _ = f.Close() }()
	body, err := io.ReadAll(f)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return body, nil
}

// ttlSidecarRecord mirrors the JSONL shape appendTTL writes:
// {"key": <encoded>, "expire_at_ms": <uint64>}.
type ttlSidecarRecord struct {
	Key        string `json:"key"`
	ExpireAtMs uint64 `json:"expire_at_ms"`
}

// loadTTLMap reads a TTL sidecar (strings_ttl.jsonl / hll_ttl.jsonl)
// into encoded-key -> expire_at_ms. Absence is fine (no expiring keys).
// The map is keyed by the ENCODED segment because that is what the
// sidecar stores and what the .bin filenames share, so callers look up
// by the filename stem without re-encoding.
func (e *RedisEncoder) loadTTLMap(name string) (map[string]uint64, error) {
	f, err := openDumpSidecar(e.dbDir(), name)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]uint64{}, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	out := map[string]uint64{}
	dec := json.NewDecoder(f)
	for {
		var rec ttlSidecarRecord
		if err := dec.Decode(&rec); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, errors.Wrapf(err, "decode %s", name)
		}
		out[rec.Key] = rec.ExpireAtMs
	}
	return out, nil
}

// encodeRedisStrInlineValue reproduces adapter/redis_compat_types.go's
// encodeRedisStr for the dump path: a new-format header
// ([0xFF 0x01][flags][expireMs BE if has_ttl]) followed by the body.
// expireMs == 0 means "no TTL" (flags=0); a non-zero value sets the
// has-TTL flag and the 8-byte big-endian millis section.
//
// expireMs is written verbatim. The decode side clamps to MaxInt64
// (decodeRedisStringValue), and the value originates from
// strings_ttl.jsonl which decode already wrote post-clamp, so a
// round-tripped dump never carries a value above MaxInt64 here. A
// hand-crafted sidecar with a larger value would be silently clamped
// on the next decode — an accepted asymmetry, not a live concern
// (Unix-ms never reaches MaxInt64).
func encodeRedisStrInlineValue(body []byte, expireMs uint64) []byte {
	if expireMs == 0 {
		out := make([]byte, redisStrBaseHeader+len(body))
		out[0] = redisStrMagic
		out[1] = redisStrVersion
		out[2] = 0
		copy(out[redisStrBaseHeader:], body)
		return out
	}
	out := make([]byte, redisStrBaseHeader+redisUint64Bytes+len(body))
	out[0] = redisStrMagic
	out[1] = redisStrVersion
	out[2] = redisStrHasTTL
	binary.BigEndian.PutUint64(out[redisStrBaseHeader:redisStrBaseHeader+redisUint64Bytes], expireMs)
	copy(out[redisStrBaseHeader+redisUint64Bytes:], body)
	return out
}

// encodeRedisTTLValueMs reproduces adapter/redis_compat_types.go's
// encodeRedisTTL for the dump path: the 8-byte big-endian expiry
// millis the !redis|ttl| scan-index row carries.
func encodeRedisTTLValueMs(expireMs uint64) []byte {
	buf := make([]byte, redisUint64Bytes)
	binary.BigEndian.PutUint64(buf, expireMs)
	return buf
}
