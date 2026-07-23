package backup

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	gproto "google.golang.org/protobuf/proto"
)

// encode_redis_coll.go is the wide-column slice of the Phase 0b Redis
// reverse encoder (M2b) — the inverse of the hash/set/list/zset/stream
// decoders.
//
// Wide-column keys share the layout the live store builds
// (store/hash_helpers.go etc.): <prefix><userKeyLen(4 BE)><userKey>
// for the meta key and <prefix><userKeyLen(4 BE)><userKey><suffix> for
// per-element keys. This applies to hash/set/zset/stream; LISTS are the
// exception — their meta/item keys are ListMetaPrefix+userKey (no
// length prefix), so they use buildListMetaKey/buildListItemKey rather
// than wideColMetaKey (mirror of store.ListMetaKey). The meta VALUE is
// an 8-byte big-endian element
// count (store.MarshalHashMeta). The live store also writes
// `!hs|meta|d|` length DELTAS that the read path sums onto the base;
// the encoder emits only the consolidated base meta (full count, no
// deltas), which is the post-compaction steady state the read path
// accepts — and which the decoder already drops on the way in.
//
// Collection TTL is stored in two places: inline in the collection
// meta value (the live read path's source of truth) and in the
// !redis|ttl|<userKey> scan-index row used for expiry scans.

// ErrRedisEncodeInvalidJSON is returned when a collection JSON file in
// the dump cannot be parsed into its expected shape.
var ErrRedisEncodeInvalidJSON = errors.New("backup: redis encode invalid collection JSON")

// streamFieldPairWidth is the (name, value) arity of a stream entry's
// interleaved field list — XADD enforces even arity.
const streamFieldPairWidth = 2

// decodeOneJSON decodes exactly one JSON value from r into v and fails
// closed (ErrRedisEncodeInvalidJSON) if any trailing data follows it.
// A collection file (hashes/sets/lists/zsets) is a single JSON object;
// trailing bytes after it indicate a corrupt/concatenated dump that
// json.Decoder.Decode would otherwise silently ignore (codex P2 on
// PR #831).
func decodeOneJSON(r io.Reader, v any) error {
	dec := json.NewDecoder(r)
	if err := dec.Decode(v); err != nil {
		return errors.WithStack(err)
	}
	if dec.More() {
		return errors.Wrap(ErrRedisEncodeInvalidJSON, "trailing data after JSON value")
	}
	return nil
}

// redisCollectionFormatVersion is the only format_version the
// collection JSON encoders accept. A dump declaring a newer version
// (or a renamed schema) would otherwise decode with zero-valued fields
// and emit corrupt/empty rows; the encoders fail closed instead
// (codex P2 / claude on PR #831). Streams use a line-oriented JSONL
// without a top-level format_version, so they are not gated here.
const redisCollectionFormatVersion uint32 = 1

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
	return e.walkJSONDir("hashes", ".json", func(rawKey []byte, r io.Reader) error {
		var rec hashJSONRecord
		if err := decodeOneJSON(r, &rec); err != nil {
			return errors.Wrapf(ErrRedisEncodeInvalidJSON, "hash %q: %v", rawKey, err)
		}
		if rec.FormatVersion != redisCollectionFormatVersion {
			return errors.Wrapf(ErrRedisEncodeInvalidJSON, "hash %q: unsupported format_version %d", rawKey, rec.FormatVersion)
		}
		// Base meta: element count = number of fields (consolidated,
		// no deltas).
		if err := b.Add(wideColMetaKey(RedisHashMetaPrefix, rawKey),
			marshalCountMeta(uint64(len(rec.Fields)), rec.ExpireAtMs), 0); err != nil {
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
	return e.walkJSONDir("sets", ".json", func(rawKey []byte, r io.Reader) error {
		var rec setJSONRecord
		if err := decodeOneJSON(r, &rec); err != nil {
			return errors.Wrapf(ErrRedisEncodeInvalidJSON, "set %q: %v", rawKey, err)
		}
		if rec.FormatVersion != redisCollectionFormatVersion {
			return errors.Wrapf(ErrRedisEncodeInvalidJSON, "set %q: unsupported format_version %d", rawKey, rec.FormatVersion)
		}
		if err := b.Add(wideColMetaKey(RedisSetMetaPrefix, rawKey),
			marshalCountMeta(uint64(len(rec.Members)), rec.ExpireAtMs), 0); err != nil {
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
	return e.walkJSONDir("lists", ".json", func(rawKey []byte, r io.Reader) error {
		var rec listJSONRecord
		if err := decodeOneJSON(r, &rec); err != nil {
			return errors.Wrapf(ErrRedisEncodeInvalidJSON, "list %q: %v", rawKey, err)
		}
		if rec.FormatVersion != redisCollectionFormatVersion {
			return errors.Wrapf(ErrRedisEncodeInvalidJSON, "list %q: unsupported format_version %d", rawKey, rec.FormatVersion)
		}
		if err := b.Add(buildListMetaKey(rawKey), marshalListMetaHead0(uint64(len(rec.Items)), rec.ExpireAtMs), 0); err != nil {
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

// zsetJSONRecord mirrors marshalZSetJSON: members as an array of
// {member (binary envelope), score (number or "+inf"/"-inf")} plus an
// optional expiry.
type zsetJSONRecord struct {
	FormatVersion uint32 `json:"format_version"`
	Members       []struct {
		Member json.RawMessage `json:"member"`
		Score  json.RawMessage `json:"score"`
	} `json:"members"`
	ExpireAtMs *uint64 `json:"expire_at_ms"`
}

// encodeZSets reconstructs the two zset indexes from zsets/*.json:
// !zs|mem|<userKey><member> -> score (8-byte BE float bits) and the
// !zs|scr|<userKey><sortableScore(8)><member> -> empty range index,
// plus an !redis|ttl| row for an expiring zset. Both indexes are
// emitted so ZSCORE/ZRANK and ZRANGEBYSCORE both work on the restored
// set (the live write path keeps them in lockstep).
func (e *RedisEncoder) encodeZSets(b *snapshotBuilder) error {
	return e.walkJSONDir("zsets", ".json", func(rawKey []byte, r io.Reader) error {
		var rec zsetJSONRecord
		if err := decodeOneJSON(r, &rec); err != nil {
			return errors.Wrapf(ErrRedisEncodeInvalidJSON, "zset %q: %v", rawKey, err)
		}
		if rec.FormatVersion != redisCollectionFormatVersion {
			return errors.Wrapf(ErrRedisEncodeInvalidJSON, "zset %q: unsupported format_version %d", rawKey, rec.FormatVersion)
		}
		if err := b.Add(wideColMetaKey(RedisZSetMetaPrefix, rawKey),
			marshalCountMeta(uint64(len(rec.Members)), rec.ExpireAtMs), 0); err != nil {
			return err
		}
		for _, m := range rec.Members {
			member, err := unmarshalRedisBinaryValue(m.Member)
			if err != nil {
				return errors.Wrapf(err, "zset %q member", rawKey)
			}
			score, err := unmarshalRedisZSetScore(m.Score)
			if err != nil {
				return errors.Wrapf(err, "zset %q score", rawKey)
			}
			if err := b.Add(wideColElemKey(RedisZSetMemberPrefix, rawKey, member),
				marshalZSetScore8BE(score), 0); err != nil {
				return err
			}
			sortable := encodeSortableFloat64(score)
			scoreSuffix := append(sortable[:], member...)
			if err := b.Add(wideColElemKey(RedisZSetScorePrefix, rawKey, scoreSuffix), []byte{}, 0); err != nil {
				return err
			}
		}
		return e.addCollectionTTL(b, rawKey, rec.ExpireAtMs)
	})
}

// marshalZSetScore8BE encodes a float64 score as the 8-byte big-endian
// IEEE-754 bit pattern the !zs|mem| value carries (mirror of
// store.MarshalZSetScore).
func marshalZSetScore8BE(score float64) []byte {
	buf := make([]byte, redisZSetScoreSize)
	binary.BigEndian.PutUint64(buf, math.Float64bits(score))
	return buf
}

// encodeSortableFloat64 reproduces store.EncodeSortableFloat64: a
// byte-order-sortable 8-byte float encoding for the !zs|scr| range
// index (positive flips the sign bit; negative flips all bits; -0 is
// normalised to +0).
func encodeSortableFloat64(f float64) [8]byte {
	if f == 0 {
		f = 0.0
	}
	bits := math.Float64bits(f)
	if bits>>63 == 0 {
		bits ^= 0x8000000000000000
	} else {
		bits ^= 0xFFFFFFFFFFFFFFFF
	}
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], bits)
	return b
}

// unmarshalRedisZSetScore is the inverse of marshalRedisZSetScore:
// "+inf"/"-inf" decode to the IEEE infinities, any other token to a
// JSON number. NaN is rejected — the live store and decoder both
// refuse NaN scores, so the encoder fails closed too.
func unmarshalRedisZSetScore(raw json.RawMessage) (float64, error) {
	switch string(bytes.TrimSpace(raw)) {
	case `"+inf"`:
		return math.Inf(1), nil
	case `"-inf"`:
		return math.Inf(-1), nil
	}
	var f float64
	if err := json.Unmarshal(raw, &f); err != nil {
		return 0, errors.Wrap(ErrRedisEncodeInvalidJSON, err.Error())
	}
	if math.IsNaN(f) {
		return 0, errors.Wrap(ErrRedisEncodeInvalidJSON, "zset score is NaN")
	}
	return f, nil
}

// streamLineJSON parses one JSONL line, distinguishing an entry line
// (id + fields) from the trailing _meta terminator line. The decoder
// emits per-entry lines first, then exactly one {"_meta":true,...}.
type streamLineJSON struct {
	Meta   bool   `json:"_meta"`
	ID     string `json:"id"`
	Fields []struct {
		Name  json.RawMessage `json:"name"`
		Value json.RawMessage `json:"value"`
	} `json:"fields"`
	Length     int64   `json:"length"`
	LastMs     uint64  `json:"last_ms"`
	LastSeq    uint64  `json:"last_seq"`
	ExpireAtMs *uint64 `json:"expire_at_ms"`
}

// encodeStreams reconstructs !stream|meta| + !stream|entry| records
// from streams/<k>.jsonl, plus an !redis|ttl| row for an expiring
// stream. Each entry value is the magic-prefixed pb.RedisStreamEntry
// protobuf carrying the interleaved (name,value,...) field list — the
// exact format decodeStreamEntryValue consumes.
func (e *RedisEncoder) encodeStreams(b *snapshotBuilder) error {
	return e.walkJSONDir("streams", ".jsonl", func(rawKey []byte, r io.Reader) error {
		dec := json.NewDecoder(r)
		var meta *streamLineJSON
		var st streamAccum
		for {
			var line streamLineJSON
			if err := dec.Decode(&line); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return errors.Wrapf(ErrRedisEncodeInvalidJSON, "stream %q: %v", rawKey, err)
			}
			if line.Meta {
				if meta != nil {
					return errors.Wrapf(ErrRedisEncodeInvalidJSON, "stream %q: multiple _meta lines", rawKey)
				}
				m := line
				meta = &m
				continue
			}
			ms, seq, err := e.addStreamEntry(b, rawKey, line)
			if err != nil {
				return err
			}
			st.observe(ms, seq)
		}
		if err := validateStreamMeta(rawKey, meta, st); err != nil {
			return err
		}
		if err := b.Add(wideColMetaKey(RedisStreamMetaPrefix, rawKey),
			marshalStreamMeta(meta.Length, meta.LastMs, meta.LastSeq, meta.ExpireAtMs), 0); err != nil {
			return err
		}
		return e.addCollectionTTL(b, rawKey, meta.ExpireAtMs)
	})
}

// addStreamEntry emits one !stream|entry| record from a parsed JSONL
// entry line and returns the entry's (ms, seq) ID so the caller can
// track the entry count and the maximum ID for _meta consistency
// validation. The ID "ms-seq" is split back into the 16-byte key
// suffix; the fields array becomes the interleaved [name,value,...]
// slice the protobuf carries.
func (e *RedisEncoder) addStreamEntry(b *snapshotBuilder, rawKey []byte, line streamLineJSON) (ms, seq uint64, err error) {
	ms, seq, err = parseStreamID(line.ID)
	if err != nil {
		return 0, 0, err
	}
	interleaved := make([]string, 0, len(line.Fields)*streamFieldPairWidth)
	for _, f := range line.Fields {
		name, err := unmarshalRedisBinaryValue(f.Name)
		if err != nil {
			return 0, 0, errors.Wrapf(err, "stream %q entry %s field name", rawKey, line.ID)
		}
		value, err := unmarshalRedisBinaryValue(f.Value)
		if err != nil {
			return 0, 0, errors.Wrapf(err, "stream %q entry %s field value", rawKey, line.ID)
		}
		interleaved = append(interleaved, string(name), string(value))
	}
	val, err := buildStreamEntryValue(line.ID, interleaved)
	if err != nil {
		return 0, 0, errors.Wrapf(err, "stream %q entry %s", rawKey, line.ID)
	}
	if err := b.Add(buildStreamEntryKey(rawKey, ms, seq), val, 0); err != nil {
		return 0, 0, err
	}
	return ms, seq, nil
}

// streamAccum tracks the entry count and the maximum (ms, seq) ID seen
// while parsing a stream's JSONL, for validateStreamMeta.
type streamAccum struct {
	count         int64
	maxMs, maxSeq uint64
	have          bool
}

func (s *streamAccum) observe(ms, seq uint64) {
	s.count++
	if !s.have || ms > s.maxMs || (ms == s.maxMs && seq > s.maxSeq) {
		s.maxMs, s.maxSeq, s.have = ms, seq, true
	}
}

// validateStreamMeta fails closed when the _meta line is missing, has a
// negative length, disagrees with the parsed entry count, or carries a
// LastMs/LastSeq high-water mark BEHIND the maximum parsed entry ID.
// The last two would silently restore an inconsistent XLEN or let a
// future XADD '*' generate an ID that collides with (overwrites) an
// existing entry. A well-formed dump from the decoder never trips
// these (claude/codex on PR #831).
func validateStreamMeta(rawKey []byte, meta *streamLineJSON, st streamAccum) error {
	if meta == nil {
		return errors.Wrapf(ErrRedisEncodeInvalidJSON, "stream %q: missing _meta line", rawKey)
	}
	if meta.Length < 0 {
		return errors.Wrapf(ErrRedisEncodeInvalidJSON, "stream %q: negative _meta length %d", rawKey, meta.Length)
	}
	if meta.Length != st.count {
		return errors.Wrapf(ErrRedisEncodeInvalidJSON,
			"stream %q: _meta length %d disagrees with %d parsed entries", rawKey, meta.Length, st.count)
	}
	if st.have && (st.maxMs > meta.LastMs || (st.maxMs == meta.LastMs && st.maxSeq > meta.LastSeq)) {
		return errors.Wrapf(ErrRedisEncodeInvalidJSON,
			"stream %q: _meta last %d-%d is behind max entry %d-%d",
			rawKey, meta.LastMs, meta.LastSeq, st.maxMs, st.maxSeq)
	}
	return nil
}

// parseStreamID splits a Redis stream ID "ms-seq" into its two uint64
// components.
func parseStreamID(id string) (ms, seq uint64, err error) {
	msStr, seqStr, ok := strings.Cut(id, "-")
	if !ok {
		return 0, 0, errors.Wrapf(ErrRedisEncodeInvalidJSON, "stream id %q missing '-'", id)
	}
	ms, err = strconv.ParseUint(msStr, 10, 64)
	if err != nil {
		return 0, 0, errors.Wrapf(ErrRedisEncodeInvalidJSON, "stream id %q ms: %v", id, err)
	}
	seq, err = strconv.ParseUint(seqStr, 10, 64)
	if err != nil {
		return 0, 0, errors.Wrapf(ErrRedisEncodeInvalidJSON, "stream id %q seq: %v", id, err)
	}
	return ms, seq, nil
}

// streamEntryKey builds !stream|entry|<userKeyLen(4)><userKey><ms(8)>
// <seq(8)> (mirror of store.StreamEntryKey + EncodeStreamID).
func buildStreamEntryKey(userKey []byte, ms, seq uint64) []byte {
	var id [redisStreamIDBytes]byte
	binary.BigEndian.PutUint64(id[0:8], ms)
	binary.BigEndian.PutUint64(id[8:16], seq)
	return wideColElemKey(RedisStreamEntryPrefix, userKey, id[:])
}

// marshalStreamMeta encodes [Length][LastMs][LastSeq], optionally with
// trailing ExpireAtMs. Logical backups do not preserve StreamMeta's trim
// cursor because restore replays only live stream entries.
func marshalStreamMeta(length int64, lastMs, lastSeq uint64, expireMs *uint64) []byte {
	size := redisStreamMetaSize
	if expireMs != nil && *expireMs != 0 {
		size = redisStreamMetaInlineTTLSize
	}
	buf := make([]byte, size)
	binary.BigEndian.PutUint64(buf[0:8], uint64(length)) //nolint:gosec // length is a non-negative count from the dump's _meta line
	binary.BigEndian.PutUint64(buf[8:16], lastMs)
	binary.BigEndian.PutUint64(buf[16:24], lastSeq)
	if size == redisStreamMetaInlineTTLSize {
		binary.BigEndian.PutUint64(buf[24:32], *expireMs)
	}
	return buf
}

// buildStreamEntryValue produces the magic-prefixed pb.RedisStreamEntry
// protobuf the live store writes (adapter/redis_storage_codec.go
// marshalStreamEntry): redisStreamProtoPrefix + the marshaled
// {Id, Fields}. Both Id and Fields are set — the live read path
// (unmarshalStreamEntry) returns the entry from msg.GetId() and
// msg.GetFields(), so omitting Id would surface an empty stream ID on
// XRANGE/XREAD even though the key carries ms/seq. Proto3 string
// fields must be valid UTF-8; marshal fails closed otherwise, exactly
// as the live write path would (non-UTF-8 stream fields are
// unrepresentable on both sides).
func buildStreamEntryValue(id string, interleaved []string) ([]byte, error) {
	payload, err := gproto.Marshal(&pb.RedisStreamEntry{Id: id, Fields: interleaved})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// Build by append from a const-capacity, zero-length slice (magic
	// prefix then payload). The constant cap keeps the
	// `const + len(payload)` arithmetic out of make() — which CodeQL
	// flags as a potential allocation-size overflow — and the
	// zero-length start keeps makezero happy.
	out := make([]byte, 0, redisStreamProtoPrefixLen)
	out = append(out, redisStreamProtoPrefix...)
	return append(out, payload...), nil
}

// addCollectionTTL emits the !redis|ttl|<userKey> scan-index row for a
// non-string collection with an expiry. A nil expiry is a no-op.
func (e *RedisEncoder) addCollectionTTL(b *snapshotBuilder, rawKey []byte, expireMs *uint64) error {
	if expireMs == nil || *expireMs == 0 {
		return nil
	}
	key := append([]byte(RedisTTLPrefix), rawKey...)
	return b.Add(key, encodeRedisTTLValueMs(*expireMs), 0)
}

// walkJSONDir iterates <dbDir>/<subdir>/*<ext>, resolves each filename
// to its original user key, and invokes fn with an io.Reader streaming
// the file from disk — so a large stream .jsonl is decoded
// incrementally rather than slurped into one []byte (gemini high on
// PR #831). A missing subdir is not an error. Reads go through an
// os.Root so an in-dump symlink cannot escape the subdir (same posture
// as walkBlobDir). ext is ".json" for the per-key collections and
// ".jsonl" for streams.
func (e *RedisEncoder) walkJSONDir(subdir, ext string, fn func(rawKey []byte, r io.Reader) error) error {
	dir := filepath.Join(e.dbDir(), subdir)
	// Refuse a symlinked/non-directory subdir before os.OpenRoot follows
	// it outside the dump tree (same guard walkBlobDir applies; codex P2
	// on PR #828).
	if err := lstatDumpDir(dir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() { _ = root.Close() }()
	// List entries THROUGH the opened root fd (not os.ReadDir(dir),
	// which re-resolves the path and could follow a symlink swapped in
	// after OpenRoot — a TOCTOU window). Codex/gemini security finding
	// on PR #831.
	entries, err := readRootDirEntries(root)
	if err != nil {
		return errors.WithStack(err)
	}
	for _, ent := range entries {
		if err := e.handleJSONEntry(root, ent, ext, fn); err != nil {
			return err
		}
	}
	return nil
}

// handleJSONEntry processes one directory entry from walkJSONDir.
// Non-regular entries and names not ending in ext are skipped (the
// IsRegular() guard keeps a FIFO from reaching the decoder). The file
// is opened within root (symlink-escape safe), checked for hard links,
// and streamed to fn as an io.Reader; it is closed when fn returns.
func (e *RedisEncoder) handleJSONEntry(root *os.Root, ent os.DirEntry, ext string, fn func(rawKey []byte, r io.Reader) error) error {
	if !ent.Type().IsRegular() || !strings.HasSuffix(ent.Name(), ext) {
		return nil
	}
	encoded := strings.TrimSuffix(ent.Name(), ext)
	rawKey, err := e.resolveKey(encoded)
	if err != nil {
		return err
	}
	f, err := root.Open(ent.Name())
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return errors.WithStack(err)
	}
	// Re-check regularity on the OPEN fd, not the (stale) ReadDir
	// entry type: a FIFO/device swapped in between ReadDir and Open
	// would pass ent.Type().IsRegular() yet hand attacker-controlled
	// bytes (a reader-attached FIFO) to the decoder. The post-open
	// fstat is authoritative (claude review on PR #831).
	if !info.Mode().IsRegular() {
		return errors.Wrapf(ErrRedisEncodeNotRegular, "%s (mode=%s)", ent.Name(), info.Mode())
	}
	if err := refuseHardLink(info, ent.Name()); err != nil {
		return err
	}
	return fn(rawKey, f)
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

// marshalCountMeta encodes a collection count, optionally followed by
// inline ExpireAtMs. This mirrors store.MarshalHashMeta/SetMeta/ZSetMeta.
func marshalCountMeta(n uint64, expireMs *uint64) []byte {
	size := redisUint64Bytes
	if expireMs != nil && *expireMs != 0 {
		size = redisCountMetaInlineBytes
	}
	buf := make([]byte, size)
	binary.BigEndian.PutUint64(buf, n)
	if size == redisCountMetaInlineBytes {
		binary.BigEndian.PutUint64(buf[redisUint64Bytes:redisCountMetaInlineBytes], *expireMs)
	}
	return buf
}

// marshalListMetaHead0 encodes [Head][Tail][Len], optionally with
// trailing ExpireAtMs. Head=0, Len=n, Tail=n is the canonical restore form.
func marshalListMetaHead0(n uint64, expireMs *uint64) []byte {
	size := listMetaBinarySize
	if expireMs != nil && *expireMs != 0 {
		size = listMetaInlineTTLSize
	}
	buf := make([]byte, size)
	binary.BigEndian.PutUint64(buf[0:8], 0)   // Head
	binary.BigEndian.PutUint64(buf[8:16], n)  // Tail = Head + Len
	binary.BigEndian.PutUint64(buf[16:24], n) // Len
	if size == listMetaInlineTTLSize {
		binary.BigEndian.PutUint64(buf[24:32], *expireMs)
	}
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
	// A pointer distinguishes an absent "base64" key (nil) from a
	// present-but-empty one (""). A JSON object that is neither a string
	// nor a {"base64":...} envelope (e.g. {"wrong":"x"}) would otherwise
	// silently decode to empty bytes; fail closed instead.
	var env struct {
		Base64 *string `json:"base64"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, errors.WithStack(err)
	}
	if env.Base64 == nil {
		return nil, errors.Wrap(ErrRedisEncodeInvalidJSON, "binary value is neither a string nor a base64 envelope")
	}
	out, err := base64.RawURLEncoding.DecodeString(*env.Base64)
	if err != nil {
		return nil, errors.Wrap(ErrRedisEncodeInvalidJSON, err.Error())
	}
	return out, nil
}
