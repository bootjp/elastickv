package backup

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"math"
	"path/filepath"
	"sort"
	"strconv"

	pb "github.com/bootjp/elastickv/proto"
	cockroachdberr "github.com/cockroachdb/errors"
	gproto "google.golang.org/protobuf/proto"
)

// Redis stream encoder. Translates raw !stream|... snapshot records
// into the per-stream `streams/<key>.jsonl` shape defined by Phase 0
// (docs/design/2026_04_29_proposed_snapshot_logical_decoder.md, lines
// 336-344).
//
// Wire format mirrors store/stream_helpers.go and
// adapter/redis_storage_codec.go:
//   - !stream|meta|<userKeyLen(4)><userKey>
//     → BE Length(8) || LastMs(8) || LastSeq(8) [|| ExpireAtMs(8)]
//   - !stream|entry|<userKeyLen(4)><userKey><ms(8)><seq(8)>
//     → magic-prefixed pb.RedisStreamEntry protobuf with fields
//     {id string, fields []string} where Fields is the
//     interleaved (name1, value1, name2, value2, ...) XADD
//     field list.
//
// The protobuf entry value carries a magic prefix
// `0x00 'R' 'X' 'E' 0x01` (mirror of
// adapter/redis_storage_codec.go:17 storedRedisStreamEntryProtoPrefix);
// re-declared here so this package stays adapter-independent.
//
// Output is JSONL (one record per line) plus a trailing `_meta`
// terminator line that captures length, last_ms, last_seq, and TTL.
// Per the design line 336-339:
//
//	{"id":"1714400000000-0","fields":{"event":"login","user":"alice"}}
//	{"_meta":true,"length":2,"last_ms":1714400000001,"last_seq":0,
//	 "expire_at_ms":null}
//
// JSONL was chosen for streams over per-entry files because real
// streams routinely hold tens of thousands of entries and per-entry
// inode pressure would dominate `tar`/`find` runtime.
const (
	RedisStreamMetaPrefix  = "!stream|meta|"
	RedisStreamEntryPrefix = "!stream|entry|"

	// redisStreamMetaSize is the legacy on-disk size of one !stream|meta|
	// value: Length(8) || LastMs(8) || LastSeq(8).
	redisStreamMetaSize = 24
	// redisStreamMetaInlineTTLSize is the current shape with trailing ExpireAtMs(8).
	redisStreamMetaInlineTTLSize = 32

	// redisStreamIDBytes is the per-entry-key suffix size: ms(8)
	// || seq(8). Mirrors store.StreamIDBytes.
	redisStreamIDBytes = 16

	// redisStreamProtoPrefix is the magic byte prefix on the stored
	// pb.RedisStreamEntry serialization. Mirrors
	// adapter/redis_storage_codec.go:storedRedisStreamEntryProtoPrefix.
	// A live-side rename here without an accompanying backup update
	// would surface as ErrRedisInvalidStreamEntry on decode of any
	// real cluster dump — caught at the property tests.
	redisStreamProtoPrefixLen = 5
)

var redisStreamProtoPrefix = []byte{0x00, 'R', 'X', 'E', 0x01}

// ErrRedisInvalidStreamMeta is returned when an !stream|meta| value
// is not the expected metadata shape or carries a negative length.
var ErrRedisInvalidStreamMeta = cockroachdberr.New("backup: invalid !stream|meta| value")

// ErrRedisInvalidStreamEntry is returned when an !stream|entry|
// value's magic prefix is missing or its protobuf body fails to
// unmarshal.
var ErrRedisInvalidStreamEntry = cockroachdberr.New("backup: invalid !stream|entry| value")

// ErrRedisInvalidStreamKey is returned when a !stream| key cannot
// be parsed for its userKeyLen+userKey (or trailing ID) segments.
var ErrRedisInvalidStreamKey = cockroachdberr.New("backup: malformed !stream| key")

// redisStreamEntry buffers one decoded XADD entry while the encoder
// assembles the per-stream JSONL output. We keep ms+seq separately
// alongside the formatted string ID so flushStreams can sort by
// (ms, seq) deterministically; sorting by the formatted "ms-seq"
// string would put "10-0" before "2-0".
type redisStreamEntry struct {
	ms     uint64
	seq    uint64
	fields []string // interleaved (name, value) pairs, XADD order
}

// redisStreamState buffers one userKey's stream during a snapshot
// scan. Like the hash/list/set/zset encoders we accumulate per-key
// state in memory; a single stream is bounded by maxWideColumnItems
// on the live side, so this remains tractable.
type redisStreamState struct {
	metaSeen       bool
	length         int64
	lastMs         uint64
	lastSeq        uint64
	entries        []redisStreamEntry
	expireAtMs     uint64
	hasTTL         bool
	inlineTTLOwned bool
}

// HandleStreamMeta processes one !stream|meta|<userKey> record.
// Value layout: Length(8) || LastMs(8) || LastSeq(8), optionally followed
// by inline ExpireAtMs(8). The encoder uses the meta's last_ms / last_seq
// verbatim in the JSONL _meta terminator so a restorer can replay them into
// the same XADD '*' monotonicity window. Length mismatches against the
// observed entry count surface as `redis_stream_length_mismatch` at flush time.
func (r *RedisDB) HandleStreamMeta(key, value []byte) error {
	userKey, ok := parseStreamMetaKey(key)
	if !ok {
		return cockroachdberr.Wrapf(ErrRedisInvalidStreamKey, "meta key: %q", key)
	}
	if len(value) != redisStreamMetaSize && len(value) != redisStreamMetaInlineTTLSize {
		return cockroachdberr.Wrapf(ErrRedisInvalidStreamMeta,
			"length %d not in {%d,%d}", len(value), redisStreamMetaSize, redisStreamMetaInlineTTLSize)
	}
	rawLen := binary.BigEndian.Uint64(value[0:8])
	if rawLen > math.MaxInt64 {
		return cockroachdberr.Wrapf(ErrRedisInvalidStreamMeta,
			"declared length %d overflows int64", rawLen)
	}
	st := r.streamState(userKey)
	st.length = int64(rawLen) //nolint:gosec // bounded above
	st.lastMs = binary.BigEndian.Uint64(value[8:16])
	st.lastSeq = binary.BigEndian.Uint64(value[16:24])
	st.metaSeen = true
	if len(value) == redisStreamMetaInlineTTLSize {
		expireAtMs := binary.BigEndian.Uint64(value[24:32])
		st.expireAtMs = expireAtMs
		st.hasTTL = expireAtMs != 0
		st.inlineTTLOwned = true
	}
	return nil
}

// HandleStreamEntry processes one !stream|entry|<userKey><ms><seq>
// record. The ID is recovered from the trailing 16 bytes of the
// key; the value is the magic-prefixed `pb.RedisStreamEntry`
// protobuf carrying the entry's interleaved (name, value) field
// list.
func (r *RedisDB) HandleStreamEntry(key, value []byte) error {
	userKey, ms, seq, ok := parseStreamEntryKey(key)
	if !ok {
		return cockroachdberr.Wrapf(ErrRedisInvalidStreamKey, "entry key: %q", key)
	}
	fields, err := decodeStreamEntryValue(value)
	if err != nil {
		return err
	}
	st := r.streamState(userKey)
	st.entries = append(st.entries, redisStreamEntry{ms: ms, seq: seq, fields: fields})
	return nil
}

// streamState lazily creates per-key state. Mirrors the
// hash/list/set/zset kindByKey-registration pattern so HandleStreamMeta,
// HandleStreamEntry, and the HandleTTL back-edge all agree on the
// kind.
//
// On first registration we drain any pendingTTL for the user key.
// `!redis|ttl|<k>` lex-sorts BEFORE `!stream|...` (because `r` < `s`),
// so in real snapshot order the TTL arrives FIRST; HandleTTL parks
// it in pendingTTL, and this function inlines it into the stream's
// JSONL `_meta.expire_at_ms`. Without this drain step, every TTL'd
// stream would restore as permanent. Codex P1 finding on PR #791.
func (r *RedisDB) streamState(userKey []byte) *redisStreamState {
	uk := string(userKey)
	if st, ok := r.streams[uk]; ok {
		return st
	}
	st := &redisStreamState{}
	if expireAtMs, ok := r.claimPendingTTL(userKey); ok {
		st.expireAtMs = expireAtMs
		st.hasTTL = true
	}
	r.streams[uk] = st
	r.kindByKey[uk] = redisKindStream
	return st
}

// parseStreamMetaKey strips !stream|meta| and the 4-byte BE
// userKeyLen prefix. Returns (userKey, true) on success. Unlike
// the hash/set encoders there is no `!stream|meta|d|...` delta
// family — streams update meta in-place rather than via per-XADD
// deltas — so we do not need a delta-skip guard here.
func parseStreamMetaKey(key []byte) ([]byte, bool) {
	rest := bytes.TrimPrefix(key, []byte(RedisStreamMetaPrefix))
	if len(rest) == len(key) {
		return nil, false
	}
	return parseUserKeyLenPrefix(rest)
}

// parseStreamEntryKey strips !stream|entry| and the 4-byte BE
// userKeyLen prefix, then peels off the trailing 16-byte StreamID
// (ms || seq). Returns (userKey, ms, seq, true) on success.
func parseStreamEntryKey(key []byte) ([]byte, uint64, uint64, bool) {
	rest := bytes.TrimPrefix(key, []byte(RedisStreamEntryPrefix))
	if len(rest) == len(key) {
		return nil, 0, 0, false
	}
	userKey, ok := parseUserKeyLenPrefix(rest)
	if !ok {
		return nil, 0, 0, false
	}
	// After (userKeyLen(4) + userKey), exactly StreamIDBytes must remain.
	tail := rest[wideColumnUserKeyLenSize+len(userKey):]
	if len(tail) != redisStreamIDBytes {
		return nil, 0, 0, false
	}
	ms := binary.BigEndian.Uint64(tail[0:8])
	seq := binary.BigEndian.Uint64(tail[8:16])
	return userKey, ms, seq, true
}

// decodeStreamEntryValue strips the magic prefix and protobuf-decodes
// the entry payload. Returns the interleaved field list (name1,
// value1, name2, value2, ...) used by every Redis stream consumer.
func decodeStreamEntryValue(value []byte) ([]string, error) {
	if len(value) < redisStreamProtoPrefixLen ||
		!bytes.Equal(value[:redisStreamProtoPrefixLen], redisStreamProtoPrefix) {
		return nil, cockroachdberr.Wrapf(ErrRedisInvalidStreamEntry,
			"missing or corrupt magic prefix (len=%d)", len(value))
	}
	msg := &pb.RedisStreamEntry{}
	if err := gproto.Unmarshal(value[redisStreamProtoPrefixLen:], msg); err != nil {
		return nil, cockroachdberr.Wrapf(ErrRedisInvalidStreamEntry,
			"unmarshal: %v", err)
	}
	if len(msg.GetFields())%2 != 0 {
		// Live XADD enforces even arity (name/value pairs). An odd
		// field count at backup time indicates corruption that would
		// silently lose the dangling field if we accepted it — fail
		// closed.
		return nil, cockroachdberr.Wrapf(ErrRedisInvalidStreamEntry,
			"odd field count %d (XADD enforces name/value pairs)", len(msg.GetFields()))
	}
	return cloneStringSlice(msg.GetFields()), nil
}

func cloneStringSlice(src []string) []string {
	if src == nil {
		return nil
	}
	out := make([]string, len(src))
	copy(out, src)
	return out
}

// flushStreams writes one JSONL file per accumulated stream to
// streams/<encoded>.jsonl. Empty streams (Length==0, no entries)
// still emit a file when meta was seen, mirroring the wide-column
// encoders' policy: their existence is observable to clients (TYPE
// returns "stream", XLEN returns 0). Mismatched declared-vs-observed
// length surfaces an `redis_stream_length_mismatch` warning.
//
// Codex P1 (PR #791 r11): if `!stream|entry|` rows arrive WITHOUT
// a preceding `!stream|meta|` row (torn / partial / corrupt
// snapshot), the live read path (`loadStreamAt` in
// adapter/redis_compat_helpers.go:640-647) returns an EMPTY stream
// because `metaFound == false`. Emitting a streams/<key>.jsonl
// here would resurrect those orphan entries on restore as a real
// stream — a data-consistency regression specific to malformed
// snapshots. Skip the user key and surface a warning so the
// operator can investigate the source snapshot.
func (r *RedisDB) flushStreams() error {
	return flushWideColumnDir(r, r.streams, "streams", func(dir, uk string, st *redisStreamState) error {
		if !st.metaSeen {
			if r.warn != nil {
				r.warn("redis_stream_orphan_entries_without_meta",
					"user_key_len", len(uk),
					"observed_entries", len(st.entries),
					"hint", "!stream|entry| rows arrived without a !stream|meta| row; live read treats this as a non-existent stream, so the backup skips emitting a file to avoid resurrecting orphan entries on restore")
			}
			return nil
		}
		if r.warn != nil && int64(len(st.entries)) != st.length {
			r.warn("redis_stream_length_mismatch",
				"user_key_len", len(uk),
				"declared_len", st.length,
				"observed_entries", len(st.entries),
				"hint", "meta record's Length does not match the count of !stream|entry| keys for this user key")
		}
		return r.writeStreamJSONL(dir, []byte(uk), st)
	})
}

func (r *RedisDB) writeStreamJSONL(dir string, userKey []byte, st *redisStreamState) error {
	encoded := EncodeSegment(userKey)
	if err := r.recordIfFallback(encoded, userKey); err != nil {
		return err
	}
	path := filepath.Join(dir, encoded+".jsonl")
	body, err := marshalStreamJSONL(st)
	if err != nil {
		return err
	}
	if err := writeFileAtomic(path, body); err != nil {
		return cockroachdberr.WithStack(err)
	}
	return nil
}

// streamFieldJSON is the dump-format projection of one (name, value)
// pair from a stream entry. We emit a list of name/value records
// rather than a JSON object because XADD permits duplicate field
// names within one entry — e.g. `XADD s * f v1 f v2` records BOTH
// (f, v1) and (f, v2) as distinct interleaved entries in the
// stored protobuf's Fields slice. The design example at
// docs/design/2026_04_29_proposed_snapshot_logical_decoder.md:338
// showed an object shape, but that representation silently drops
// duplicates and a restore would not reproduce the original
// stream entry. Codex P1 (PR #791) — switched to a name/value
// record array.
//
// Name and Value are `json.RawMessage` populated via
// `marshalRedisBinaryValue` so non-UTF-8 bytes round-trip via the
// `{"base64":"..."}` envelope. Without this, `json.Marshal` of a
// plain `string` carrying invalid UTF-8 silently substitutes U+FFFD
// for each ill-formed byte sequence, and the restored stream entry
// would carry the replacement-character mangle instead of the
// original bytes. Redis stream field names and values are
// binary-safe (the live store keeps them as protobuf `bytes`
// despite the wire-format `repeated string` shape), so the
// projection must preserve every byte. Mirrors hashFieldRecord
// (redis_hash.go:235-238). Claude bot Critical (PR #791 round 2).
type streamFieldJSON struct {
	Name  json.RawMessage `json:"name"`
	Value json.RawMessage `json:"value"`
}

// streamEntryJSON is the dump-format projection of one stream
// entry. Fields is an ARRAY so duplicate field names within a
// single XADD round-trip correctly. The array preserves the
// interleaved insertion order from the protobuf so consumers can
// re-assemble the original XADD argv.
type streamEntryJSON struct {
	ID     string            `json:"id"`
	Fields []streamFieldJSON `json:"fields"`
}

// streamMetaJSON is the dump-format projection of the final _meta
// terminator line.
type streamMetaJSON struct {
	Meta       bool    `json:"_meta"`
	Length     int64   `json:"length"`
	LastMs     uint64  `json:"last_ms"`
	LastSeq    uint64  `json:"last_seq"`
	ExpireAtMs *uint64 `json:"expire_at_ms"`
}

// marshalStreamJSONL renders one stream state as JSONL. Entries are
// sorted by (ms, seq) so identical snapshots produce identical
// output across runs regardless of XADD insertion order. Each line
// uses encoding/json (compact, no MarshalIndent) so the format is
// stable enough for `diff -r`.
func marshalStreamJSONL(st *redisStreamState) ([]byte, error) {
	sort.Slice(st.entries, func(i, j int) bool {
		a, b := st.entries[i], st.entries[j]
		if a.ms != b.ms {
			return a.ms < b.ms
		}
		return a.seq < b.seq
	})
	var buf bytes.Buffer
	const xaddPairWidth = 2 // (name, value) — XADD enforces even arity
	for _, e := range st.entries {
		fields, err := buildStreamFieldRecords(e.fields, xaddPairWidth)
		if err != nil {
			return nil, err
		}
		rec := streamEntryJSON{
			ID:     formatStreamID(e.ms, e.seq),
			Fields: fields,
		}
		line, err := json.Marshal(rec)
		if err != nil {
			return nil, cockroachdberr.WithStack(err)
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	meta := streamMetaJSON{
		Meta:    true,
		Length:  st.length,
		LastMs:  st.lastMs,
		LastSeq: st.lastSeq,
	}
	if st.hasTTL {
		ms := st.expireAtMs
		meta.ExpireAtMs = &ms
	}
	line, err := json.Marshal(meta)
	if err != nil {
		return nil, cockroachdberr.WithStack(err)
	}
	buf.Write(line)
	buf.WriteByte('\n')
	return buf.Bytes(), nil
}

// formatStreamID emits a stream ID in Redis's "ms-seq" wire format
// (the same shape XADD/XRANGE clients exchange on the wire).
func formatStreamID(ms, seq uint64) string {
	return strconv.FormatUint(ms, 10) + "-" + strconv.FormatUint(seq, 10)
}

// buildStreamFieldRecords converts one entry's interleaved
// (name1, value1, name2, value2, ...) field slice into a
// streamFieldJSON array. Each name/value goes through
// marshalRedisBinaryValue so non-UTF-8 bytes round-trip via the
// `{"base64":"..."}` envelope. Without this projection, plain
// `string` fields would surrender every ill-formed UTF-8 byte to
// json.Marshal's silent U+FFFD substitution and the restored
// stream entry would not be byte-identical to the source. Claude
// bot Critical (PR #791 round 2).
func buildStreamFieldRecords(fields []string, pairWidth int) ([]streamFieldJSON, error) {
	out := make([]streamFieldJSON, 0, len(fields)/pairWidth)
	for i := 0; i+1 < len(fields); i += pairWidth {
		nameJSON, err := marshalRedisBinaryValue([]byte(fields[i]))
		if err != nil {
			return nil, err
		}
		valueJSON, err := marshalRedisBinaryValue([]byte(fields[i+1]))
		if err != nil {
			return nil, err
		}
		out = append(out, streamFieldJSON{Name: nameJSON, Value: valueJSON})
	}
	return out, nil
}
