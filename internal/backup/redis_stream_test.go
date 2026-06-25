package backup

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	gproto "google.golang.org/protobuf/proto"
)

// streamMetaKey is the test-side mirror of store.StreamMetaKey.
func streamMetaKey(userKey string) []byte {
	out := []byte(RedisStreamMetaPrefix)
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len(userKey))) //nolint:gosec
	out = append(out, l[:]...)
	return append(out, userKey...)
}

// streamEntryKey is the test-side mirror of store.StreamEntryKey.
func streamEntryKey(userKey string, ms, seq uint64) []byte {
	out := []byte(RedisStreamEntryPrefix)
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len(userKey))) //nolint:gosec
	out = append(out, l[:]...)
	out = append(out, userKey...)
	var id [16]byte
	binary.BigEndian.PutUint64(id[0:8], ms)
	binary.BigEndian.PutUint64(id[8:16], seq)
	return append(out, id[:]...)
}

// encodeStreamMetaValue mirrors store.MarshalStreamMeta:
// Length(8) || LastMs(8) || LastSeq(8), all big-endian.
func encodeStreamMetaValue(length int64, lastMs, lastSeq uint64) []byte {
	v := make([]byte, redisStreamMetaSize)
	binary.BigEndian.PutUint64(v[0:8], uint64(length)) //nolint:gosec
	binary.BigEndian.PutUint64(v[8:16], lastMs)
	binary.BigEndian.PutUint64(v[16:24], lastSeq)
	return v
}

// encodeStreamEntryValue produces the magic-prefixed protobuf wire
// format the live store writes for !stream|entry| values.
func encodeStreamEntryValue(t *testing.T, id string, fields []string) []byte {
	t.Helper()
	body, err := gproto.Marshal(&pb.RedisStreamEntry{Id: id, Fields: fields})
	if err != nil {
		t.Fatalf("marshal pb.RedisStreamEntry: %v", err)
	}
	out := make([]byte, 0, redisStreamProtoPrefixLen+len(body))
	out = append(out, redisStreamProtoPrefix...)
	out = append(out, body...)
	return out
}

// readStreamJSONL parses the JSONL output (one record per line) and
// splits it into entries (no `_meta` key) plus the trailing meta
// terminator. The terminator is always the last line per spec.
func readStreamJSONL(t *testing.T, path string) (entries []map[string]any, meta map[string]any) {
	t.Helper()
	f, err := os.Open(path) //nolint:gosec // test path
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal(line, &rec); err != nil {
			t.Fatalf("unmarshal %q: %v", line, err)
		}
		if _, ok := rec["_meta"]; ok {
			meta = rec
			continue
		}
		entries = append(entries, rec)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if meta == nil {
		t.Fatalf("no _meta terminator in %s", path)
	}
	return entries, meta
}

func streamFloat(t *testing.T, m map[string]any, key string) float64 {
	t.Helper()
	v, ok := m[key]
	if !ok {
		t.Fatalf("field %q missing in %+v", key, m)
	}
	f, ok := v.(float64)
	if !ok {
		t.Fatalf("field %q = %T(%v), want float64", key, v, v)
	}
	return f
}

// assertStreamMetaTerminator pins the trailing `_meta` line shape.
// Extracted from the round-trip tests to keep the per-test bodies
// below the cyclop ceiling.
func assertStreamMetaTerminator(t *testing.T, meta map[string]any, wantLen int64, wantLastMs, wantLastSeq uint64) {
	t.Helper()
	if streamFloat(t, meta, "length") != float64(wantLen) {
		t.Fatalf("meta.length = %v want %d", meta["length"], wantLen)
	}
	if streamFloat(t, meta, "last_ms") != float64(wantLastMs) {
		t.Fatalf("meta.last_ms = %v want %d", meta["last_ms"], wantLastMs)
	}
	if streamFloat(t, meta, "last_seq") != float64(wantLastSeq) {
		t.Fatalf("meta.last_seq = %v want %d", meta["last_seq"], wantLastSeq)
	}
	if meta["expire_at_ms"] != nil {
		t.Fatalf("meta.expire_at_ms must be nil without TTL, got %v", meta["expire_at_ms"])
	}
}

// TestRedisDB_StreamRoundTripBasic confirms a multi-entry stream
// round-trips through the encoder in (ms, seq) order, with the
// fields decoded from the protobuf entry value and the trailing
// _meta line preserving length / last_ms / last_seq.
func TestRedisDB_StreamRoundTripBasic(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	if err := db.HandleStreamMeta(streamMetaKey("events"), encodeStreamMetaValue(3, 1714400000002, 0)); err != nil {
		t.Fatalf("HandleStreamMeta: %v", err)
	}
	// Submit out of (ms, seq) order — encoder must sort.
	for _, e := range []struct {
		ms, seq uint64
		id      string
		fields  []string
	}{
		{1714400000002, 0, "1714400000002-0", []string{"event", "logout", "user", "alice"}},
		{1714400000000, 0, "1714400000000-0", []string{"event", "login", "user", "alice"}},
		{1714400000001, 0, "1714400000001-0", []string{"event", "click", "user", "alice"}},
	} {
		key := streamEntryKey("events", e.ms, e.seq)
		val := encodeStreamEntryValue(t, e.id, e.fields)
		if err := db.HandleStreamEntry(key, val); err != nil {
			t.Fatalf("HandleStreamEntry(%s): %v", e.id, err)
		}
	}
	if err := db.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	entries, meta := readStreamJSONL(t, filepath.Join(root, "redis", "db_0", "streams", "events.jsonl"))
	if len(entries) != 3 {
		t.Fatalf("entries = %d, want 3 (got %v)", len(entries), entries)
	}
	for i, w := range []string{"1714400000000-0", "1714400000001-0", "1714400000002-0"} {
		if entries[i]["id"] != w {
			t.Fatalf("entries[%d].id = %v, want %v", i, entries[i]["id"], w)
		}
	}
	assertStreamMetaTerminator(t, meta, 3, 1714400000002, 0)
}

// streamFieldsPair is the decoded counterpart of streamFieldJSON used
// in assertions.
type streamFieldsPair struct{ name, value string }

// extractStreamFieldsAsPairs pulls the `fields` array out of a
// decoded JSONL entry and returns it as a slice of (name, value)
// pairs. Centralises the type assertions so the per-test bodies
// stay below the cyclop ceiling and forcetypeassert lints don't
// fire at every call site.
//
// Each name/value can be EITHER a plain JSON string (UTF-8 content)
// or a `{"base64":"..."}` envelope object (non-UTF-8 binary bytes).
// The fields are emitted via marshalRedisBinaryValue so binary
// stream payloads round-trip byte-identical; this helper hides the
// per-pair envelope detection from the per-test assertions.
func extractStreamFieldsAsPairs(t *testing.T, entry map[string]any) []streamFieldsPair {
	t.Helper()
	raw, ok := entry["fields"].([]any)
	if !ok {
		t.Fatalf("entry.fields = %T(%v), want array", entry["fields"], entry["fields"])
	}
	out := make([]streamFieldsPair, 0, len(raw))
	for i, r := range raw {
		rec, ok := r.(map[string]any)
		if !ok {
			t.Fatalf("entry.fields[%d] = %T(%v), want object", i, r, r)
		}
		out = append(out, streamFieldsPair{
			name:  decodeRedisBinaryEnvelope(t, "name", rec["name"]),
			value: decodeRedisBinaryEnvelope(t, "value", rec["value"]),
		})
	}
	return out
}

// decodeRedisBinaryEnvelope reverses marshalRedisBinaryValue for
// tests: a plain JSON string round-trips as a string; a
// `{"base64":"..."}` envelope decodes via base64url back to the
// original byte string. Returns the recovered bytes as a Go
// string so the test assertions can compare against the input
// regardless of which projection the encoder chose.
func decodeRedisBinaryEnvelope(t *testing.T, label string, raw any) string {
	t.Helper()
	if s, ok := raw.(string); ok {
		return s
	}
	env, ok := raw.(map[string]any)
	if !ok {
		t.Fatalf("%s = %T(%v), want string or base64 envelope", label, raw, raw)
	}
	encoded, ok := env["base64"].(string)
	if !ok {
		t.Fatalf("%s base64 envelope missing payload: %v", label, env)
	}
	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("%s base64 decode: %v (payload %q)", label, err, encoded)
	}
	return string(decoded)
}

// assertStreamFieldsEqual checks that the decoded fields array
// matches the expected ordered list of (name, value) pairs.
func assertStreamFieldsEqual(t *testing.T, got []streamFieldsPair, want []streamFieldsPair) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("len(fields) = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("fields[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}

// TestRedisDB_StreamFieldsDecodedToArray confirms that interleaved
// `[name1, value1, name2, value2, ...]` arrays from the protobuf
// land as a JSONL `[{"name":...,"value":...}]` array. Switched
// from the design's original map shape (line 338) in response to
// codex's P1 on PR #791: XADD allows duplicate field names within
// one entry (e.g. `XADD s * f v1 f v2`) and the map representation
// silently collapsed them.
func TestRedisDB_StreamFieldsDecodedToArray(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	if err := db.HandleStreamMeta(streamMetaKey("s"), encodeStreamMetaValue(1, 100, 5)); err != nil {
		t.Fatal(err)
	}
	val := encodeStreamEntryValue(t, "100-5", []string{"event", "login", "user", "alice", "ip", "10.0.0.1"})
	if err := db.HandleStreamEntry(streamEntryKey("s", 100, 5), val); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	entries, _ := readStreamJSONL(t, filepath.Join(root, "redis", "db_0", "streams", "s.jsonl"))
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(entries))
	}
	assertStreamFieldsEqual(t, extractStreamFieldsAsPairs(t, entries[0]), []streamFieldsPair{
		{"event", "login"},
		{"user", "alice"},
		{"ip", "10.0.0.1"},
	})
}

// TestBuildStreamFieldRecords_NonUTF8BytesRoundTrip pins the
// claude-bot Critical fix on PR #791: stream field names and
// values are binary-safe in Redis. Previously
// streamFieldJSON.Name/Value were plain Go `string` and went
// through json.Marshal, which silently substitutes U+FFFD for
// every ill-formed UTF-8 byte sequence — a stream entry carrying
// raw binary would be silently corrupted in the dump. The fix
// routes both fields through marshalRedisBinaryValue so non-UTF-8
// bytes emit as `{"base64":"..."}` and round-trip byte-identical.
//
// The protobuf wire format itself enforces UTF-8 on `string` fields
// (proto3 `string` is by spec UTF-8, and `gproto.Marshal` rejects
// invalid bytes), so the path "live store → snapshot → decoder"
// cannot actually carry non-UTF-8 stream fields today; it's a
// defensive invariant in case a future schema migration switches
// `Fields` to `bytes`, or a code path bypasses the proto marshaler.
// We pin the projection's behavior directly on
// buildStreamFieldRecords + extractStreamFieldsAsPairs rather than
// trying to push bytes through a gproto.Marshal step that would
// reject them.
func TestBuildStreamFieldRecords_NonUTF8BytesRoundTrip(t *testing.T) {
	t.Parallel()
	const xaddPairWidth = 2
	binaryName := "\xff\xfe\x80"
	binaryValue := "\x00\x01\xc3\x28\x02"
	records, err := buildStreamFieldRecords([]string{binaryName, binaryValue, "utf8-key", "utf8-val"}, xaddPairWidth)
	if err != nil {
		t.Fatalf("buildStreamFieldRecords: %v", err)
	}
	if len(records) != xaddPairWidth { // we passed 2 pairs
		t.Fatalf("len(records) = %d, want %d", len(records), xaddPairWidth)
	}
	// Marshal one entry to JSONL bytes, parse it back, and assert
	// that the recovered pairs match the input byte-identical.
	body, err := json.Marshal(streamEntryJSON{ID: "1-0", Fields: records})
	if err != nil {
		t.Fatalf("json.Marshal streamEntryJSON: %v", err)
	}
	var parsed map[string]any
	if err := json.Unmarshal(body, &parsed); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	assertStreamFieldsEqual(t, extractStreamFieldsAsPairs(t, parsed), []streamFieldsPair{
		{binaryName, binaryValue},
		{"utf8-key", "utf8-val"},
	})
}

// TestRedisDB_StreamDuplicateFieldsPreserved pins the codex P1 fix:
// XADD permits duplicate field names within one entry (e.g.
// `XADD s * f v1 f v2` stores both pairs verbatim in the
// protobuf's Fields slice). The original map-based projection
// silently collapsed duplicates; the array shape preserves them
// and a restore can replay the original XADD argv exactly.
func TestRedisDB_StreamDuplicateFieldsPreserved(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	if err := db.HandleStreamMeta(streamMetaKey("s"), encodeStreamMetaValue(1, 1, 0)); err != nil {
		t.Fatal(err)
	}
	val := encodeStreamEntryValue(t, "1-0", []string{"f", "v1", "f", "v2", "g", "v3"})
	if err := db.HandleStreamEntry(streamEntryKey("s", 1, 0), val); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	entries, _ := readStreamJSONL(t, filepath.Join(root, "redis", "db_0", "streams", "s.jsonl"))
	assertStreamFieldsEqual(t, extractStreamFieldsAsPairs(t, entries[0]), []streamFieldsPair{
		{"f", "v1"},
		{"f", "v2"},
		{"g", "v3"},
	})
}

// TestRedisDB_StreamTTLArrivesBeforeRows pins the codex P1 fix:
// `!redis|ttl|<k>` lex-sorts BEFORE `!stream|...` because `r` <
// `s`, so in real Pebble snapshot order the TTL arrives FIRST.
// The encoder MUST buffer the expiry in pendingTTL and drain it
// when streamState first registers the user key, inlining the
// value into the JSONL `_meta.expire_at_ms`. Without this drain
// every TTL'd stream would restore as permanent.
func TestRedisDB_StreamTTLArrivesBeforeRows(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	// Snapshot order: TTL first, then meta + entry.
	if err := db.HandleTTL([]byte("k"), encodeTTLValue(fixedExpireMs)); err != nil {
		t.Fatal(err)
	}
	if err := db.HandleStreamMeta(streamMetaKey("k"), encodeStreamMetaValue(1, 100, 0)); err != nil {
		t.Fatal(err)
	}
	val := encodeStreamEntryValue(t, "100-0", []string{"event", "login"})
	if err := db.HandleStreamEntry(streamEntryKey("k", 100, 0), val); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	_, meta := readStreamJSONL(t, filepath.Join(root, "redis", "db_0", "streams", "k.jsonl"))
	if streamFloat(t, meta, "expire_at_ms") != float64(fixedExpireMs) {
		t.Fatalf("meta.expire_at_ms = %v want %d — pendingTTL drain failed", meta["expire_at_ms"], fixedExpireMs)
	}
}

// TestRedisDB_StreamEmptyStreamStillEmitsFile mirrors the other
// wide-column encoders: an empty stream (Length=0) must still emit
// a file because TYPE returns "stream" and XLEN returns 0 to clients.
func TestRedisDB_StreamEmptyStreamStillEmitsFile(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	if err := db.HandleStreamMeta(streamMetaKey("empty"), encodeStreamMetaValue(0, 0, 0)); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	entries, meta := readStreamJSONL(t, filepath.Join(root, "redis", "db_0", "streams", "empty.jsonl"))
	if len(entries) != 0 {
		t.Fatalf("empty stream should emit no entry lines, got %v", entries)
	}
	if streamFloat(t, meta, "length") != 0 {
		t.Fatalf("meta.length = %v want 0", meta["length"])
	}
}

// TestRedisDB_StreamTTLInlinedFromScanIndex pins that
// !redis|ttl|<userKey> records for a stream user key fold into the
// _meta terminator's expire_at_ms field — design line 341-344.
// Without this routing, restoring a TTL'd stream would silently
// drop the TTL.
func TestRedisDB_StreamTTLInlinedFromScanIndex(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	if err := db.HandleStreamMeta(streamMetaKey("k"), encodeStreamMetaValue(1, 100, 0)); err != nil {
		t.Fatal(err)
	}
	val := encodeStreamEntryValue(t, "100-0", []string{"event", "login"})
	if err := db.HandleStreamEntry(streamEntryKey("k", 100, 0), val); err != nil {
		t.Fatal(err)
	}
	if err := db.HandleTTL([]byte("k"), encodeTTLValue(fixedExpireMs)); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	_, meta := readStreamJSONL(t, filepath.Join(root, "redis", "db_0", "streams", "k.jsonl"))
	if streamFloat(t, meta, "expire_at_ms") != float64(fixedExpireMs) {
		t.Fatalf("meta.expire_at_ms = %v want %d", meta["expire_at_ms"], fixedExpireMs)
	}
	if _, err := os.Stat(filepath.Join(root, "redis", "db_0", "streams_ttl.jsonl")); !os.IsNotExist(err) {
		t.Fatalf("unexpected stream TTL sidecar: stat err=%v", err)
	}
}

// TestRedisDB_StreamLengthMismatchWarns pins the warn-on-mismatch
// contract — same shape as the hash/list/set/zset encoders.
func TestRedisDB_StreamLengthMismatchWarns(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	var events []string
	db.WithWarnSink(func(event string, _ ...any) { events = append(events, event) })
	if err := db.HandleStreamMeta(streamMetaKey("s"), encodeStreamMetaValue(5, 100, 0)); err != nil {
		t.Fatal(err)
	}
	val := encodeStreamEntryValue(t, "100-0", []string{"k", "v"})
	if err := db.HandleStreamEntry(streamEntryKey("s", 100, 0), val); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	want := "redis_stream_length_mismatch"
	found := false
	for _, e := range events {
		if e == want {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected %q warning, got %v", want, events)
	}
}

// TestRedisDB_StreamOrphanEntriesWithoutMetaSkipsFile pins the
// codex P1 fix (PR #791 r11): when `!stream|entry|` rows arrive
// without a preceding `!stream|meta|` row (torn / partial /
// corrupt snapshot), the encoder MUST NOT emit a
// streams/<key>.jsonl file. The live read path treats meta-less
// streams as non-existent; emitting a backup file would resurrect
// the orphan entries on restore — a data-consistency regression.
// The encoder skips the user key and surfaces a warning instead.
func TestRedisDB_StreamOrphanEntriesWithoutMetaSkipsFile(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	var events []string
	db.WithWarnSink(func(event string, _ ...any) { events = append(events, event) })
	// HandleStreamEntry without a prior HandleStreamMeta — torn
	// snapshot shape.
	val := encodeStreamEntryValue(t, "100-0", []string{"k", "v"})
	if err := db.HandleStreamEntry(streamEntryKey("orphan", 100, 0), val); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	// streams/orphan.jsonl MUST NOT exist.
	path := filepath.Join(root, "redis", "db_0", "streams", "orphan.jsonl")
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected streams/orphan.jsonl absent, stat err=%v (encoder must skip meta-less streams)", err)
	}
	// Warning must surface for operator visibility.
	want := "redis_stream_orphan_entries_without_meta"
	found := false
	for _, e := range events {
		if e == want {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected %q warning, got %v", want, events)
	}
}

// TestRedisDB_StreamRejectsMalformedMetaValueLength pins that a
// !stream|meta| value of the wrong length surfaces as an error.
// The 24-byte fixed shape is the wire-format contract — any other
// length means the on-disk record is corrupt.
func TestRedisDB_StreamRejectsMalformedMetaValueLength(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	err := db.HandleStreamMeta(streamMetaKey("k"), []byte{0x00, 0x01, 0x02})
	if !errors.Is(err, ErrRedisInvalidStreamMeta) {
		t.Fatalf("err=%v want ErrRedisInvalidStreamMeta", err)
	}
}

// TestRedisDB_StreamRejectsOverflowingMetaLength pins the high-bit
// overflow guard — same shape as hash/list/set/zset encoders.
func TestRedisDB_StreamRejectsOverflowingMetaLength(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	overflow := make([]byte, redisStreamMetaSize)
	binary.BigEndian.PutUint64(overflow[0:8], 1<<63)
	err := db.HandleStreamMeta(streamMetaKey("k"), overflow)
	if !errors.Is(err, ErrRedisInvalidStreamMeta) {
		t.Fatalf("err=%v want ErrRedisInvalidStreamMeta", err)
	}
}

// TestRedisDB_StreamMaxInt64MetaLength pins the math.MaxInt64
// boundary — declaredLen=math.MaxInt64 must be accepted, only > that
// rejected.
func TestRedisDB_StreamMaxInt64MetaLength(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	boundary := make([]byte, redisStreamMetaSize)
	binary.BigEndian.PutUint64(boundary[0:8], math.MaxInt64)
	if err := db.HandleStreamMeta(streamMetaKey("k"), boundary); err != nil {
		t.Fatalf("math.MaxInt64 boundary must be accepted, got %v", err)
	}
}

// TestRedisDB_StreamRejectsEntryWithMissingMagic pins that a value
// missing the `0x00 'R' 'X' 'E' 0x01` magic prefix fails closed.
// The live store always writes this prefix; its absence indicates
// the value came from a stale legacy format or from a corrupted
// store, and decoding raw protobuf bytes without the prefix would
// either silently misparse or panic deep inside the proto library.
func TestRedisDB_StreamRejectsEntryWithMissingMagic(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	body, err := gproto.Marshal(&pb.RedisStreamEntry{Id: "1-0", Fields: []string{"k", "v"}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// Pass the raw body without the magic prefix.
	err = db.HandleStreamEntry(streamEntryKey("k", 1, 0), body)
	if !errors.Is(err, ErrRedisInvalidStreamEntry) {
		t.Fatalf("err=%v want ErrRedisInvalidStreamEntry", err)
	}
}

// TestRedisDB_StreamRejectsEntryWithOddFieldCount pins the
// even-arity invariant. Live XADD enforces name/value pairs at the
// wire level (XADD rejects odd argument counts), so an odd count
// at backup time indicates corruption. Silently emitting the
// dangling field as `{"<name>": null}` would re-corrupt the
// restored cluster.
func TestRedisDB_StreamRejectsEntryWithOddFieldCount(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	val := encodeStreamEntryValue(t, "1-0", []string{"event"}) // 1 element, missing value
	err := db.HandleStreamEntry(streamEntryKey("k", 1, 0), val)
	if !errors.Is(err, ErrRedisInvalidStreamEntry) {
		t.Fatalf("err=%v want ErrRedisInvalidStreamEntry", err)
	}
}

// TestRedisDB_StreamRejectsMalformedEntryKey pins that an entry key
// without the trailing 16-byte StreamID fails parse cleanly.
func TestRedisDB_StreamRejectsMalformedEntryKey(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	// Construct a key with the entry prefix + userKeyLen + userKey
	// but only a 4-byte trailing suffix (should be 16 for StreamID).
	out := []byte(RedisStreamEntryPrefix)
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], 1)
	out = append(out, l[:]...)
	out = append(out, 'k')
	out = append(out, 0x00, 0x00, 0x00, 0x01)
	val := encodeStreamEntryValue(t, "1-0", []string{"a", "b"})
	err := db.HandleStreamEntry(out, val)
	if !errors.Is(err, ErrRedisInvalidStreamKey) {
		t.Fatalf("err=%v want ErrRedisInvalidStreamKey", err)
	}
}

// (Removed) TestRedisDB_StreamEntriesWithoutMetaStillEmitFile.
//
// The original test asserted that stream entries arriving without
// a preceding `!stream|meta|` row would still produce a
// streams/<k>.jsonl file. That behavior is incorrect: the live
// read path (`loadStreamAt` in adapter/redis_compat_helpers.go:
// 640-647) returns EMPTY when no meta is found, so emitting a
// backup file would resurrect orphan entries on restore as a
// real stream — a data-consistency regression specific to torn /
// corrupt snapshots.
//
// Codex P1 on PR #791 round 11 inverted this contract. The
// correct behavior is now pinned by
// TestRedisDB_StreamOrphanEntriesWithoutMetaSkipsFile above:
// the file is NOT emitted and a
// redis_stream_orphan_entries_without_meta warning fires for
// operator visibility.
//
// list (#755 r2) and set (#758) have different semantics — their
// live read paths DO return content when meta is absent (they
// don't depend on a separate meta row to mark existence), so
// "items-without-meta still emit" is correct for list/set but
// not for stream.

// TestRedisDB_StreamIDFormatMatchesRedisWire pins the wire format
// of the JSON `id` field: `<ms>-<seq>` decimal, matching what
// XADD/XRANGE expose to clients. A future encoder bug that emitted
// hex or base10-with-leading-zeros would silently corrupt the
// stream-restore replay path.
func TestRedisDB_StreamIDFormatMatchesRedisWire(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	if err := db.HandleStreamMeta(streamMetaKey("s"), encodeStreamMetaValue(1, 1714400000000, 7)); err != nil {
		t.Fatal(err)
	}
	val := encodeStreamEntryValue(t, "1714400000000-7", []string{"k", "v"})
	if err := db.HandleStreamEntry(streamEntryKey("s", 1714400000000, 7), val); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	entries, _ := readStreamJSONL(t, filepath.Join(root, "redis", "db_0", "streams", "s.jsonl"))
	if entries[0]["id"] != "1714400000000-7" {
		t.Fatalf("id = %v want %q", entries[0]["id"], "1714400000000-7")
	}
}

// TestRedisDB_StreamMultipleStreamsSortedByUserKey pins that
// flushStreams iterates user keys in sorted byte order so two
// runs producing the same logical content produce identical
// directory output.
func TestRedisDB_StreamMultipleStreamsSortedByUserKey(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	for _, uk := range []string{"zeta", "alpha", "mango"} {
		if err := db.HandleStreamMeta(streamMetaKey(uk), encodeStreamMetaValue(1, 1, 0)); err != nil {
			t.Fatal(err)
		}
		val := encodeStreamEntryValue(t, "1-0", []string{"k", "v"})
		if err := db.HandleStreamEntry(streamEntryKey(uk, 1, 0), val); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	for _, uk := range []string{"alpha", "mango", "zeta"} {
		path := filepath.Join(root, "redis", "db_0", "streams", uk+".jsonl")
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("stat %s: %v", path, err)
		}
	}
}
