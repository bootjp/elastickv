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
func (r *RedisDB) HandleHashMeta(key, value []byte) error {
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
	r.kindByKey[string(userKey)] = redisKindHash
	return nil
}

// HandleHashField processes one !hs|fld|<userKey><fieldName> record.
// The value is the raw field-value bytes (binary-safe).
func (r *RedisDB) HandleHashField(key, value []byte) error {
	userKey, fieldName, ok := parseHashFieldKey(key)
	if !ok {
		return cockroachdberr.Wrapf(ErrRedisInvalidHashKey, "field key: %q", key)
	}
	if len(fieldName) == 0 {
		return cockroachdberr.Wrapf(ErrRedisInvalidHashKey,
			"empty field name in key %q", key)
	}
	st := r.hashState(userKey)
	st.fields[string(fieldName)] = bytes.Clone(value)
	r.kindByKey[string(userKey)] = redisKindHash
	return nil
}

// hashState lazily creates per-key state.
func (r *RedisDB) hashState(userKey []byte) *redisHashState {
	if st, ok := r.hashes[string(userKey)]; ok {
		return st
	}
	st := &redisHashState{fields: make(map[string][]byte)}
	r.hashes[string(userKey)] = st
	return st
}

// parseHashMetaKey strips !hs|meta| and the 4-byte BE userKeyLen prefix.
// Returns (userKey, true) on success.
func parseHashMetaKey(key []byte) ([]byte, bool) {
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
func parseUserKeyLenPrefix(b []byte) ([]byte, bool) {
	if len(b) < hashUserKeyLenSize {
		return nil, false
	}
	ukLen := int(binary.BigEndian.Uint32(b[:hashUserKeyLenSize])) //nolint:gosec // bounded by len(b)
	if len(b) < hashUserKeyLenSize+ukLen {
		return nil, false
	}
	return b[hashUserKeyLenSize : hashUserKeyLenSize+ukLen], true
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

// hashJSONFields marshals the per-field map. Field NAMES are emitted
// as JSON object keys, which forces them into UTF-8 string territory:
// when a name is not valid UTF-8 we percent-encode it via
// EncodeSegment so the output is reversible without changing JSON
// shape. Field VALUES preserve full binary fidelity via the same
// `{"base64":...}` envelope used for SQS bodies (see sqsMessageBody).
type hashJSONFields map[string]json.RawMessage

func marshalHashJSON(st *redisHashState) ([]byte, error) {
	fields := make(hashJSONFields, len(st.fields))
	// Sort for deterministic output across runs.
	names := make([]string, 0, len(st.fields))
	for name := range st.fields {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		jsonName := name
		if !utf8.ValidString(name) {
			jsonName = EncodeSegment([]byte(name))
		}
		valueJSON, err := marshalRedisBinaryValue(st.fields[name])
		if err != nil {
			return nil, err
		}
		fields[jsonName] = valueJSON
	}
	type out struct {
		FormatVersion uint32         `json:"format_version"`
		Fields        hashJSONFields `json:"fields"`
		ExpireAtMs    *uint64        `json:"expire_at_ms"`
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
