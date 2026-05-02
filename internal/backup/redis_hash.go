package backup

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"path/filepath"
	"sort"
	"unicode/utf8"

	cockroachdberr "github.com/cockroachdb/errors"
)

// Snapshot key prefixes the hash encoder dispatches on. Mirror the live
// store/hash_helpers.go constants — a renamed prefix on the live side
// surfaces here at compile time via the dispatch tests.
const (
	RedisHashMetaPrefix      = "!hs|meta|"
	RedisHashFieldPrefix     = "!hs|fld|"
	RedisHashMetaDeltaPrefix = "!hs|meta|d|"

	// hashUserKeyLenSize is the fixed BE-uint32 width of the
	// per-key length prefix used by every wide-column key shape.
	// Mirrors store/wideColKeyLenSize.
	hashUserKeyLenSize = 4
)

// ErrRedisInvalidHashMeta is returned when the !hs|meta| value is not
// the expected 8-byte big-endian field count.
var ErrRedisInvalidHashMeta = cockroachdberr.New("backup: invalid !hs|meta| value")

// ErrRedisInvalidHashKey is returned when an !hs| key cannot be parsed
// for its userKeyLen+userKey segment (truncated, malformed, etc).
var ErrRedisInvalidHashKey = cockroachdberr.New("backup: malformed !hs| key")

// redisHashState buffers the per-userKey hash being assembled. The
// encoder accumulates fields as they arrive and flushes a single JSON
// record at Finalize time. We deliberately buffer per key (rather than
// stream) because the design's per-hash JSON shape requires the full
// field map up-front and Redis hashes are typically small.
type redisHashState struct {
	declaredLen int64
	metaSeen    bool
	fields      map[string][]byte // field-name → field-value bytes
	expireAtMs  uint64
	hasTTL      bool
}

// HandleHashMeta processes one !hs|meta|<userKey> record. The value is
// the 8-byte BE field count. We park the state for finalize-time flush
// and register the user key so a later !redis|ttl|<userKey> record
// routes back to this hash state.
//
// Delta keys (!hs|meta|d|...) share the !hs|meta| string prefix, so a
// snapshot dispatcher that routes by "starts with RedisHashMetaPrefix"
// will land delta records here too. Phase 0a's output (an array of
// observed fields) doesn't need to apply the delta arithmetic — the
// !hs|fld|... records are the source of truth — so we silently skip
// delta keys instead of returning ErrRedisInvalidHashKey. Codex P1
// round 14 (PR #725 #13).
func (r *RedisDB) HandleHashMeta(key, value []byte) error {
	if bytes.HasPrefix(key, []byte(RedisHashMetaDeltaPrefix)) {
		return nil
	}
	userKey, ok := parseHashMetaKey(key)
	if !ok {
		return cockroachdberr.Wrapf(ErrRedisInvalidHashKey, "meta key: %q", key)
	}
	if len(value) != redisUint64Bytes {
		return cockroachdberr.Wrapf(ErrRedisInvalidHashMeta,
			"length %d != %d", len(value), redisUint64Bytes)
	}
	st := r.hashState(userKey)
	st.declaredLen = int64(binary.BigEndian.Uint64(value)) //nolint:gosec // signed int64 by design
	st.metaSeen = true
	return nil
}

// HandleHashField processes one !hs|fld|<userKey><fieldName> record.
// The value is the raw field-value bytes (binary-safe).
//
// Note: Redis hash field names are binary-safe and may legitimately
// be empty — `HSET k "" v` is a valid command and the live store
// emits a key shaped exactly `!hs|fld|<len><userKey>` with no
// trailing field bytes. We deliberately do NOT reject zero-length
// field names here so backup decoding succeeds on real data created
// via HSET with empty names. Codex P1 round 13 (PR #725).
func (r *RedisDB) HandleHashField(key, value []byte) error {
	userKey, fieldName, ok := parseHashFieldKey(key)
	if !ok {
		return cockroachdberr.Wrapf(ErrRedisInvalidHashKey, "field key: %q", key)
	}
	st := r.hashState(userKey)
	st.fields[string(fieldName)] = bytes.Clone(value)
	return nil
}

// hashState lazily creates per-key state. The `kindByKey` registration
// lives here (Gemini medium PR #725 #1/#3) so every code path that
// touches a hash state — meta, field, and the TTL-routing back-edge
// from HandleTTL — agrees on the kind. Caller audit (per the loop's
// "audit semantics-changing edits" rule):
//
//   - HandleHashMeta and HandleHashField both want kindByKey set;
//     centralising here means the explicit assignment is no longer
//     needed at the call site.
//   - HandleTTL only ever calls hashState() inside the
//     `case redisKindHash:` branch, where kindByKey == redisKindHash
//     already holds; the assignment here is idempotent for that path.
//   - No other caller exists; verified via
//     `grep -n "r\.hashState(" internal/backup/`.
func (r *RedisDB) hashState(userKey []byte) *redisHashState {
	uk := string(userKey)
	if st, ok := r.hashes[uk]; ok {
		return st
	}
	st := &redisHashState{fields: make(map[string][]byte)}
	r.hashes[uk] = st
	r.kindByKey[uk] = redisKindHash
	return st
}

// parseHashMetaKey strips !hs|meta| and the 4-byte BE userKeyLen prefix.
// Returns (userKey, true) on success. Delta keys (!hs|meta|d|...)
// share the meta string prefix and would otherwise be parsed as
// base-meta with a garbage userKeyLen — refuse them at the boundary
// so a misrouted delta surfaces a parse error rather than silent
// state corruption. Callers that want delta-tolerant behavior
// (HandleHashMeta) should detect the delta prefix BEFORE calling
// this function. Codex P1 round 14 (PR #725 #13).
func parseHashMetaKey(key []byte) ([]byte, bool) {
	if bytes.HasPrefix(key, []byte(RedisHashMetaDeltaPrefix)) {
		return nil, false
	}
	rest := bytes.TrimPrefix(key, []byte(RedisHashMetaPrefix))
	if len(rest) == len(key) {
		return nil, false
	}
	return parseUserKeyLenPrefix(rest)
}

// parseHashFieldKey strips !hs|fld|, the 4-byte userKeyLen prefix, and
// returns (userKey, fieldName, true).
func parseHashFieldKey(key []byte) ([]byte, []byte, bool) {
	rest := bytes.TrimPrefix(key, []byte(RedisHashFieldPrefix))
	if len(rest) == len(key) {
		return nil, nil, false
	}
	userKey, ok := parseUserKeyLenPrefix(rest)
	if !ok {
		return nil, nil, false
	}
	fieldName := rest[hashUserKeyLenSize+len(userKey):]
	return userKey, fieldName, true
}

// parseUserKeyLenPrefix decodes the shared <len(4)><userKey> shape used
// by every wide-column !hs|/!st|/!zs| key. Returns the userKey slice
// (aliasing the input) plus a presence flag.
//
// The length comparison is done in uint64 space because on 32-bit
// architectures `int(uint32)` can wrap to a negative value when the
// high bit is set, bypassing the bounds check and causing a slice
// panic. Gemini high finding (PR #725 round 1).
func parseUserKeyLenPrefix(b []byte) ([]byte, bool) {
	if len(b) < hashUserKeyLenSize {
		return nil, false
	}
	ukLen := binary.BigEndian.Uint32(b[:hashUserKeyLenSize])
	if uint64(len(b)) < uint64(hashUserKeyLenSize)+uint64(ukLen) {
		return nil, false
	}
	return b[hashUserKeyLenSize : hashUserKeyLenSize+int(ukLen)], true //nolint:gosec // bounded above
}

// flushHashes writes one JSON file per accumulated hash to
// hashes/<encoded>.json. Empty hashes (Len=0, no fields) still emit a
// file because their existence is observable to clients (HEXISTS,
// HLEN). Mismatched declared-vs-observed length surfaces an
// `redis_hash_length_mismatch` warning.
func (r *RedisDB) flushHashes() error {
	if len(r.hashes) == 0 {
		return nil
	}
	dir := filepath.Join(r.dbDir(), "hashes")
	if err := r.ensureDir(dir); err != nil {
		return err
	}
	// Stable order across runs (Codex pattern from #716): sort by user
	// key before flushing so identical snapshots produce identical
	// dump output regardless of Go's randomised map iteration.
	userKeys := make([]string, 0, len(r.hashes))
	for k := range r.hashes {
		userKeys = append(userKeys, k)
	}
	sort.Strings(userKeys)
	for _, uk := range userKeys {
		st := r.hashes[uk]
		if r.warn != nil && st.metaSeen && int64(len(st.fields)) != st.declaredLen {
			r.warn("redis_hash_length_mismatch",
				"user_key_len", len(uk),
				"declared_len", st.declaredLen,
				"observed_fields", len(st.fields),
				"hint", "meta record's Len does not match the count of !hs|fld| keys for this user key")
		}
		if err := r.writeHashJSON(dir, []byte(uk), st); err != nil {
			return err
		}
	}
	return nil
}

func (r *RedisDB) writeHashJSON(dir string, userKey []byte, st *redisHashState) error {
	encoded := EncodeSegment(userKey)
	if err := r.recordIfFallback(encoded, userKey); err != nil {
		return err
	}
	path := filepath.Join(dir, encoded+".json")
	body, err := marshalHashJSON(st)
	if err != nil {
		return err
	}
	if err := writeFileAtomic(path, body); err != nil {
		return cockroachdberr.WithStack(err)
	}
	return nil
}

// hashFieldRecord is the dump-format projection of one Redis hash
// field. Both name and value go through the same UTF-8-or-base64
// envelope (json.RawMessage produced by marshalRedisBinaryValue) so
// arbitrary binary bytes round-trip without lossy rewrites.
//
// We deliberately emit `fields` as an ARRAY of records rather than a
// JSON object keyed on the field name, because Redis hash field
// names are binary-safe and JSON object keys are not. With a map
// shape, two distinct fields could collapse to the same JSON key
// (Codex P1 round 12 #725: a UTF-8 literal `%FF` and a single byte
// `0xFF` both percent-encoded to `%FF` would overwrite one
// another), and a >240-byte non-UTF-8 field name would route
// through EncodeSegment's SHA fallback which is non-reversible at
// this layer (no per-field KEYMAP). The array form keeps every
// (name, value) pair distinct and binary-safe.
type hashFieldRecord struct {
	Name  json.RawMessage `json:"name"`
	Value json.RawMessage `json:"value"`
}

func marshalHashJSON(st *redisHashState) ([]byte, error) {
	// Sort by raw byte order for deterministic output across runs.
	names := make([]string, 0, len(st.fields))
	for name := range st.fields {
		names = append(names, name)
	}
	sort.Strings(names)
	fields := make([]hashFieldRecord, 0, len(names))
	for _, name := range names {
		nameJSON, err := marshalRedisBinaryValue([]byte(name))
		if err != nil {
			return nil, err
		}
		valueJSON, err := marshalRedisBinaryValue(st.fields[name])
		if err != nil {
			return nil, err
		}
		fields = append(fields, hashFieldRecord{Name: nameJSON, Value: valueJSON})
	}
	type out struct {
		FormatVersion uint32            `json:"format_version"`
		Fields        []hashFieldRecord `json:"fields"`
		ExpireAtMs    *uint64           `json:"expire_at_ms"`
	}
	rec := out{FormatVersion: 1, Fields: fields}
	if st.hasTTL {
		ms := st.expireAtMs
		rec.ExpireAtMs = &ms
	}
	body, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return nil, cockroachdberr.WithStack(err)
	}
	return body, nil
}

// marshalRedisBinaryValue is the shared "binary-safe text or base64
// envelope" projection used by every Redis wide-column type for value
// bytes that may or may not be valid UTF-8. Mirrors the SQS body
// projection for restore-roundtrip determinism: a UTF-8 bytestring
// emits as a plain JSON string; non-UTF-8 emits as
// `{"base64":"<base64url>"}`.
func marshalRedisBinaryValue(b []byte) (json.RawMessage, error) {
	if utf8.Valid(b) {
		out, err := json.Marshal(string(b))
		if err != nil {
			return nil, cockroachdberr.WithStack(err)
		}
		return out, nil
	}
	envelope := struct {
		Base64 string `json:"base64"`
	}{Base64: base64.RawURLEncoding.EncodeToString(b)}
	out, err := json.Marshal(envelope)
	if err != nil {
		return nil, cockroachdberr.WithStack(err)
	}
	return out, nil
}
