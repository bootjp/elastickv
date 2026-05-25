package backup

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"math"
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

// setJSONRecord mirrors marshalSetJSON: members as an array of binary
// envelopes plus an optional expiry. A set member's identity is the
// key suffix; the live store writes an empty value for it
// (redis_compat_commands.go SADD path), which the encoder reproduces.
type setJSONRecord struct {
	FormatVersion uint32            `json:"format_version"`
	Members       []json.RawMessage `json:"members"`
	ExpireAtMs    *uint64           `json:"expire_at_ms"`
}

// encodeSets reconstructs !st|meta| + !st|mem| records from
// sets/*.json, plus an !redis|ttl| row for any expiring set.
func (e *RedisEncoder) encodeSets(b *snapshotBuilder) error {
	return e.walkJSONDir("sets", func(rawKey, body []byte) error {
		var rec setJSONRecord
		if err := json.Unmarshal(body, &rec); err != nil {
			return errors.Wrapf(ErrRedisEncodeInvalidJSON, "set %q: %v", rawKey, err)
		}
		if err := b.Add(wideColMetaKey(RedisSetMetaPrefix, rawKey),
			marshalCount8BE(uint64(len(rec.Members))), 0); err != nil {
			return err
		}
		for _, mRaw := range rec.Members {
			member, err := unmarshalRedisBinaryValue(mRaw)
			if err != nil {
				return errors.Wrapf(err, "set %q member", rawKey)
			}
			if err := b.Add(wideColElemKey(RedisSetMemberPrefix, rawKey, member), []byte{}, 0); err != nil {
				return err
			}
		}
		return e.addCollectionTTL(b, rawKey, rec.ExpireAtMs)
	})
}

// listJSONRecord mirrors marshalListJSON: items as an ordered array of
// binary envelopes (left-to-right list order) plus an optional expiry.
type listJSONRecord struct {
	FormatVersion uint32            `json:"format_version"`
	Items         []json.RawMessage `json:"items"`
	ExpireAtMs    *uint64           `json:"expire_at_ms"`
}

// encodeLists reconstructs !lst|meta| + !lst|itm| records from
// lists/*.json, plus an !redis|ttl| row for any expiring list.
//
// The dump records items in order but not their original seqs, so the
// encoder assigns the canonical contiguous form: Head=0, item i at
// seq i, Tail=Len. This satisfies the live store's Tail=Head+Len
// invariant and reproduces the same left-to-right order on read
// (store.ListItemKey scans items in sortable-seq order). The original
// LPUSH/RPUSH seq base is not recoverable and does not need to be — a
// restored list's subsequent prepends/appends simply extend from this
// base.
func (e *RedisEncoder) encodeLists(b *snapshotBuilder) error {
	return e.walkJSONDir("lists", func(rawKey, body []byte) error {
		var rec listJSONRecord
		if err := json.Unmarshal(body, &rec); err != nil {
			return errors.Wrapf(ErrRedisEncodeInvalidJSON, "list %q: %v", rawKey, err)
		}
		if err := b.Add(buildListMetaKey(rawKey), marshalListMetaHead0(uint64(len(rec.Items))), 0); err != nil {
			return err
		}
		for i, itemRaw := range rec.Items {
			item, err := unmarshalRedisBinaryValue(itemRaw)
			if err != nil {
				return errors.Wrapf(err, "list %q item %d", rawKey, i)
			}
			if err := b.Add(buildListItemKey(rawKey, int64(i)), item, 0); err != nil {
				return err
			}
		}
		return e.addCollectionTTL(b, rawKey, rec.ExpireAtMs)
	})
}

// buildListMetaKey builds !lst|meta|<userKey> (no length prefix —
// mirror of store.ListMetaKey).
func buildListMetaKey(userKey []byte) []byte {
	out := make([]byte, 0, len(ListMetaPrefix)+len(userKey))
	out = append(out, ListMetaPrefix...)
	return append(out, userKey...)
}

// marshalListMetaHead0 encodes the 24-byte [Head][Tail][Len] meta with
// Head=0, Len=n, Tail=n — the canonical restore form (mirror of
// store.marshalListMeta with Head=0).
func marshalListMetaHead0(n uint64) []byte {
	buf := make([]byte, listMetaBinarySize)
	binary.BigEndian.PutUint64(buf[0:8], 0)   // Head
	binary.BigEndian.PutUint64(buf[8:16], n)  // Tail = Head + Len
	binary.BigEndian.PutUint64(buf[16:24], n) // Len
	return buf
}

// buildListItemKey builds !lst|itm|<userKey><sortableInt64(seq)>
// (mirror of store.ListItemKey + encodeSortableInt64: the seq is
// sign-flipped so a forward byte scan yields ascending int64).
func buildListItemKey(userKey []byte, seq int64) []byte {
	out := make([]byte, 0, len(ListItemPrefix)+len(userKey)+listSeqBytes)
	out = append(out, ListItemPrefix...)
	out = append(out, userKey...)
	var raw [listSeqBytes]byte
	binary.BigEndian.PutUint64(raw[:], uint64(seq^math.MinInt64)) //nolint:gosec // sortable-int64 sign-flip; mirrors store.encodeSortableInt64
	return append(out, raw[:]...)
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
