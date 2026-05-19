package backup

import (
	"bufio"
	"bytes"
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

// TestRedisDB_StreamFieldsDecodedToObject confirms that interleaved
// `[name1, value1, name2, value2, ...]` arrays from the protobuf
// land as `{"name1": "value1", "name2": "value2"}` JSON objects per
// the design example (line 338).
func TestRedisDB_StreamFieldsDecodedToObject(t *testing.T) {
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
	fields, ok := entries[0]["fields"].(map[string]any)
	if !ok {
		t.Fatalf("entries[0].fields = %T(%v), want object", entries[0]["fields"], entries[0]["fields"])
	}
	want := map[string]any{"event": "login", "user": "alice", "ip": "10.0.0.1"}
	if len(fields) != len(want) {
		t.Fatalf("fields = %v, want %v", fields, want)
	}
	for k, v := range want {
		if fields[k] != v {
			t.Fatalf("fields[%q] = %v, want %v", k, fields[k], v)
		}
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

// TestRedisDB_StreamEntriesWithoutMetaStillEmitFile pins the
// entries-without-meta contract: stream entries may arrive before
// (or without) meta, and the encoder must still emit the JSONL
// without firing the length-mismatch warning. Mirrors the
// items-without-meta rule from list (#755 round 2) and set (#758).
func TestRedisDB_StreamEntriesWithoutMetaStillEmitFile(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	var events []string
	db.WithWarnSink(func(event string, _ ...any) { events = append(events, event) })
	val := encodeStreamEntryValue(t, "1-0", []string{"a", "b"})
	if err := db.HandleStreamEntry(streamEntryKey("s", 1, 0), val); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	entries, _ := readStreamJSONL(t, filepath.Join(root, "redis", "db_0", "streams", "s.jsonl"))
	if len(entries) != 1 {
		t.Fatalf("entries = %v, want 1", entries)
	}
	for _, e := range events {
		if e == "redis_stream_length_mismatch" {
			t.Fatalf("entries-without-meta must not fire length-mismatch warning, got events %v", events)
		}
	}
}

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
