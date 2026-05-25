package backup

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"
)

// encode_redis_coll.go is the wide-column slice of the Phase 0b Redis
// reverse encoder (M2b) — the inverse of the hash/list/set/zset/stream
// decoders. This commit covers hashes; the remaining collections land
// in follow-up commits on the same branch.
//
// Wide-column keys share the layout the live store builds
// (store/hash_helpers.go etc.): <prefix><userKeyLen(4 BE)><userKey>
// for the meta key and <prefix><userKeyLen(4 BE)><userKey><suffix> for
// per-element keys. The meta VALUE is an 8-byte big-endian element
// count (store.MarshalHashMeta). The live store also writes
// `!hs|meta|d|` length DELTAS that the read path sums onto the base;
// the encoder emits only the consolidated base meta (full count, no
// deltas), which is the post-compaction steady state the read path
// accepts — and which the decoder already drops on the way in.
//
// Collection TTL is NOT inline (unlike strings): it lives in a
// !redis|ttl|<userKey> scan-index row, matching buildTTLElems for
// non-string types.

// ErrRedisEncodeInvalidJSON is returned when a collection JSON file in
// the dump cannot be parsed into its expected shape.
var ErrRedisEncodeInvalidJSON = errors.New("backup: redis encode invalid collection JSON")

// hashJSONRecord mirrors marshalHashJSON's output: a format version, a
// fields ARRAY of {name,value} binary envelopes (object keys can't
// hold binary-safe field names), and an optional expiry.
type hashJSONRecord struct {
	FormatVersion uint32 `json:"format_version"`
	Fields        []struct {
		Name  json.RawMessage `json:"name"`
		Value json.RawMessage `json:"value"`
	} `json:"fields"`
	ExpireAtMs *uint64 `json:"expire_at_ms"`
}

// encodeHashes reconstructs !hs|meta| + !hs|fld| records from
// hashes/*.json, plus an !redis|ttl| row for any expiring hash.
func (e *RedisEncoder) encodeHashes(b *snapshotBuilder) error {
	return e.walkJSONDir("hashes", func(rawKey, body []byte) error {
		var rec hashJSONRecord
		if err := json.Unmarshal(body, &rec); err != nil {
			return errors.Wrapf(ErrRedisEncodeInvalidJSON, "hash %q: %v", rawKey, err)
		}
		// Base meta: element count = number of fields (consolidated,
		// no deltas).
		if err := b.Add(wideColMetaKey(RedisHashMetaPrefix, rawKey),
			marshalCount8BE(uint64(len(rec.Fields))), 0); err != nil {
			return err
		}
		for _, f := range rec.Fields {
			name, err := unmarshalRedisBinaryValue(f.Name)
			if err != nil {
				return errors.Wrapf(err, "hash %q field name", rawKey)
			}
			value, err := unmarshalRedisBinaryValue(f.Value)
			if err != nil {
				return errors.Wrapf(err, "hash %q field value", rawKey)
			}
			if err := b.Add(wideColElemKey(RedisHashFieldPrefix, rawKey, name), value, 0); err != nil {
				return err
			}
		}
		return e.addCollectionTTL(b, rawKey, rec.ExpireAtMs)
	})
}

// addCollectionTTL emits the !redis|ttl|<userKey> scan-index row for a
// non-string collection with an expiry. A nil expiry is a no-op.
func (e *RedisEncoder) addCollectionTTL(b *snapshotBuilder, rawKey []byte, expireMs *uint64) error {
	if expireMs == nil {
		return nil
	}
	key := append([]byte(RedisTTLPrefix), rawKey...)
	return b.Add(key, encodeRedisTTLValueMs(*expireMs), 0)
}

// walkJSONDir iterates <dbDir>/<subdir>/*.json, resolves each filename
// to its original user key, reads the body, and invokes fn. A missing
// subdir is not an error. Reads go through an os.Root so an in-dump
// symlink cannot escape the subdir (same posture as walkBlobDir).
func (e *RedisEncoder) walkJSONDir(subdir string, fn func(rawKey, body []byte) error) error {
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
		if !ent.Type().IsRegular() || !strings.HasSuffix(ent.Name(), ".json") {
			continue
		}
		encoded := strings.TrimSuffix(ent.Name(), ".json")
		rawKey, err := e.resolveKey(encoded)
		if err != nil {
			return err
		}
		body, err := readRootFile(root, ent.Name())
		if err != nil {
			return err
		}
		if err := fn(rawKey, body); err != nil {
			return err
		}
	}
	return nil
}

// wideColMetaKey builds <prefix><userKeyLen(4 BE)><userKey> — the
// meta key shape store/hash_helpers.go::HashMetaKey (and the set/zset
// equivalents) produce.
func wideColMetaKey(prefix string, userKey []byte) []byte {
	out := make([]byte, 0, len(prefix)+wideColumnUserKeyLenSize+len(userKey))
	out = append(out, prefix...)
	var kl [wideColumnUserKeyLenSize]byte
	binary.BigEndian.PutUint32(kl[:], uint32(len(userKey))) //nolint:gosec // bounded by max slice size
	out = append(out, kl[:]...)
	return append(out, userKey...)
}

// wideColElemKey builds <prefix><userKeyLen(4 BE)><userKey><suffix> —
// the per-element key shape (hash field, set member, ...).
func wideColElemKey(prefix string, userKey, suffix []byte) []byte {
	out := wideColMetaKey(prefix, userKey)
	return append(out, suffix...)
}

// marshalCount8BE encodes a collection element count as the 8-byte
// big-endian value store.MarshalHashMeta writes.
func marshalCount8BE(n uint64) []byte {
	buf := make([]byte, redisUint64Bytes)
	binary.BigEndian.PutUint64(buf, n)
	return buf
}

// unmarshalRedisBinaryValue is the inverse of marshalRedisBinaryValue:
// a plain JSON string decodes to its UTF-8 bytes; a
// {"base64":"<base64url>"} envelope decodes the raw (possibly
// non-UTF-8) bytes.
func unmarshalRedisBinaryValue(raw json.RawMessage) ([]byte, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) > 0 && trimmed[0] == '"' {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, errors.WithStack(err)
		}
		return []byte(s), nil
	}
	var env struct {
		Base64 string `json:"base64"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, errors.WithStack(err)
	}
	out, err := base64.RawURLEncoding.DecodeString(env.Base64)
	if err != nil {
		return nil, errors.Wrap(ErrRedisEncodeInvalidJSON, err.Error())
	}
	return out, nil
}
