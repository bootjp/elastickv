package backup

import (
	"bytes"
	"encoding/json"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/cockroachdb/errors"
)

// fakeRegularDirEntry is an os.DirEntry that LIES about its type,
// reporting a regular file regardless of the real on-disk object. It
// lets a unit test drive handleJSONEntry past the pre-open
// ent.Type().IsRegular() check so the post-open fstat guard can be
// exercised deterministically (simulating a ReadDir-vs-Open TOCTOU
// without an actual race).
type fakeRegularDirEntry struct{ name string }

func (f fakeRegularDirEntry) Name() string               { return f.name }
func (f fakeRegularDirEntry) IsDir() bool                { return false }
func (f fakeRegularDirEntry) Type() fs.FileMode          { return 0 } // 0 == regular
func (f fakeRegularDirEntry) Info() (fs.FileInfo, error) { return nil, errors.New("unused") }

// TestHandleJSONEntryRejectsNonRegularAfterOpen pins the post-open
// regularity guard: a ReadDir entry that claims to be a regular .json
// file but actually resolves to a directory on open is rejected with
// ErrRedisEncodeNotRegular, and the callback never runs. This is the
// deterministic stand-in for the FIFO-swap TOCTOU the guard defends
// against (a directory opens without blocking, unlike a reader-less
// FIFO).
func TestHandleJSONEntryRejectsNonRegularAfterOpen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// A directory named like a regular collection file.
	if err := os.MkdirAll(filepath.Join(dir, "x.json"), 0o755); err != nil {
		t.Fatalf("mkdir x.json: %v", err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatalf("OpenRoot: %v", err)
	}
	defer func() { _ = root.Close() }()

	e := NewRedisEncoder(dir, 0)
	called := false
	err = e.handleJSONEntry(root, fakeRegularDirEntry{name: "x.json"}, ".json",
		func(_ []byte, _ io.Reader) error { called = true; return nil })
	if !errors.Is(err, ErrRedisEncodeNotRegular) {
		t.Fatalf("handleJSONEntry err = %v, want ErrRedisEncodeNotRegular", err)
	}
	if called {
		t.Fatal("callback must not run for a non-regular open target")
	}
}

// buildHashJSON constructs a hashes/<k>.json body in the exact shape
// marshalHashJSON emits (fields as an array of binary-envelope
// {name,value} records), so the encoder reads a faithful decoder
// output. expireMs nil means no TTL.
func buildHashJSON(t *testing.T, fields map[string]string, expireMs *uint64) []byte {
	t.Helper()
	type fieldRec struct {
		Name  json.RawMessage `json:"name"`
		Value json.RawMessage `json:"value"`
	}
	names := make([]string, 0, len(fields))
	for n := range fields {
		names = append(names, n)
	}
	sort.Strings(names)
	recs := make([]fieldRec, 0, len(names))
	for _, n := range names {
		nameJSON, err := marshalRedisBinaryValue([]byte(n))
		if err != nil {
			t.Fatalf("marshal name: %v", err)
		}
		valJSON, err := marshalRedisBinaryValue([]byte(fields[n]))
		if err != nil {
			t.Fatalf("marshal value: %v", err)
		}
		recs = append(recs, fieldRec{Name: nameJSON, Value: valJSON})
	}
	out := struct {
		FormatVersion uint32     `json:"format_version"`
		Fields        []fieldRec `json:"fields"`
		ExpireAtMs    *uint64    `json:"expire_at_ms"`
	}{FormatVersion: 1, Fields: recs, ExpireAtMs: expireMs}
	body, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("marshal hash json: %v", err)
	}
	return body
}

// readHashFields parses a decoded hashes/<k>.json into a name->value
// map plus its expiry.
func readHashFields(t *testing.T, root, enc string) (map[string]string, *uint64) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, "redis", "db_0", "hashes", enc+".json"))
	if err != nil {
		t.Fatalf("read decoded hash: %v", err)
	}
	var rec hashJSONRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		t.Fatalf("unmarshal decoded hash: %v", err)
	}
	out := map[string]string{}
	for _, f := range rec.Fields {
		name, err := unmarshalRedisBinaryValue(f.Name)
		if err != nil {
			t.Fatalf("unmarshal field name: %v", err)
		}
		value, err := unmarshalRedisBinaryValue(f.Value)
		if err != nil {
			t.Fatalf("unmarshal field value: %v", err)
		}
		out[string(name)] = string(value)
	}
	return out, rec.ExpireAtMs
}

// TestRedisEncodeHashRoundTripViaDecode runs the gold-standard
// directory round-trip for a hash with fields and a TTL: the fields
// survive and the TTL (emitted as an !redis|ttl| row by the encoder)
// is recovered into expire_at_ms by the decoder.
func TestRedisEncodeHashRoundTripViaDecode(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	enc := EncodeSegment([]byte("myhash"))
	const expireMs uint64 = 1_735_689_600_000
	want := map[string]string{"name": "alice", "age": "30", "city": "tokyo"}
	exp := expireMs
	writeRedisFile(t, in, filepath.Join("hashes", enc+".json"), buildHashJSON(t, want, &exp))

	out := decodeRedisTree(t, encodeRedisTree(t, in))

	got, gotExp := readHashFields(t, out, enc)
	if len(got) != len(want) {
		t.Fatalf("got %d fields, want %d", len(got), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("field %q = %q, want %q", k, got[k], v)
		}
	}
	if gotExp == nil || *gotExp != expireMs {
		t.Fatalf("decoded expire_at_ms = %v, want %d", gotExp, expireMs)
	}
}

// TestRedisEncodeHashNoTTLRoundTripViaDecode pins the no-TTL hash path:
// fields survive and expire_at_ms is null.
func TestRedisEncodeHashNoTTLRoundTripViaDecode(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	enc := EncodeSegment([]byte("plain"))
	want := map[string]string{"k1": "v1"}
	writeRedisFile(t, in, filepath.Join("hashes", enc+".json"), buildHashJSON(t, want, nil))

	out := decodeRedisTree(t, encodeRedisTree(t, in))

	got, gotExp := readHashFields(t, out, enc)
	if got["k1"] != "v1" {
		t.Fatalf("field k1 = %q, want v1", got["k1"])
	}
	if gotExp != nil {
		t.Fatalf("decoded expire_at_ms = %v, want nil", *gotExp)
	}
}

// TestRedisEncodeHashBinaryFieldRoundTrip pins that non-UTF-8 field
// names and values survive via the base64 envelope.
func TestRedisEncodeHashBinaryFieldRoundTrip(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	enc := EncodeSegment([]byte("bin"))
	// 0xFF 0xFE are not valid UTF-8 → base64 envelope on both sides.
	want := map[string]string{"\xff\xfe": "\x00\x01\x02"}
	writeRedisFile(t, in, filepath.Join("hashes", enc+".json"), buildHashJSON(t, want, nil))

	out := decodeRedisTree(t, encodeRedisTree(t, in))

	got, _ := readHashFields(t, out, enc)
	if got["\xff\xfe"] != "\x00\x01\x02" {
		t.Fatalf("binary field round-trip failed: got %x", got["\xff\xfe"])
	}
}

// buildSetJSON constructs a sets/<k>.json body in marshalSetJSON's
// shape (members array of binary envelopes).
func buildSetJSON(t *testing.T, members []string, expireMs *uint64) []byte {
	t.Helper()
	sort.Strings(members)
	raw := make([]json.RawMessage, 0, len(members))
	for _, m := range members {
		v, err := marshalRedisBinaryValue([]byte(m))
		if err != nil {
			t.Fatalf("marshal member: %v", err)
		}
		raw = append(raw, v)
	}
	out := struct {
		FormatVersion uint32            `json:"format_version"`
		Members       []json.RawMessage `json:"members"`
		ExpireAtMs    *uint64           `json:"expire_at_ms"`
	}{FormatVersion: 1, Members: raw, ExpireAtMs: expireMs}
	body, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("marshal set json: %v", err)
	}
	return body
}

// readSetMembers parses a decoded sets/<k>.json into a member set plus
// its expiry.
func readSetMembers(t *testing.T, root, enc string) (map[string]struct{}, *uint64) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, "redis", "db_0", "sets", enc+".json"))
	if err != nil {
		t.Fatalf("read decoded set: %v", err)
	}
	var rec setJSONRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		t.Fatalf("unmarshal decoded set: %v", err)
	}
	out := map[string]struct{}{}
	for _, mRaw := range rec.Members {
		m, err := unmarshalRedisBinaryValue(mRaw)
		if err != nil {
			t.Fatalf("unmarshal member: %v", err)
		}
		out[string(m)] = struct{}{}
	}
	return out, rec.ExpireAtMs
}

// TestRedisEncodeSetRoundTripViaDecode runs the gold-standard directory
// round-trip for a set with TTL and a non-UTF-8 member.
func TestRedisEncodeSetRoundTripViaDecode(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	enc := EncodeSegment([]byte("myset"))
	const expireMs uint64 = 1_700_000_000_123
	members := []string{"red", "green", "\xff\x00"}
	exp := expireMs
	writeRedisFile(t, in, filepath.Join("sets", enc+".json"), buildSetJSON(t, members, &exp))

	out := decodeRedisTree(t, encodeRedisTree(t, in))

	got, gotExp := readSetMembers(t, out, enc)
	if len(got) != len(members) {
		t.Fatalf("got %d members, want %d", len(got), len(members))
	}
	for _, m := range members {
		if _, ok := got[m]; !ok {
			t.Fatalf("member %q missing from decoded set", m)
		}
	}
	if gotExp == nil || *gotExp != expireMs {
		t.Fatalf("decoded expire_at_ms = %v, want %d", gotExp, expireMs)
	}
}

// TestRedisEncodeSetNoTTLRoundTripViaDecode pins the no-TTL set path.
func TestRedisEncodeSetNoTTLRoundTripViaDecode(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	enc := EncodeSegment([]byte("plainset"))
	writeRedisFile(t, in, filepath.Join("sets", enc+".json"), buildSetJSON(t, []string{"a", "b"}, nil))

	out := decodeRedisTree(t, encodeRedisTree(t, in))

	got, gotExp := readSetMembers(t, out, enc)
	if _, ok := got["a"]; !ok {
		t.Fatal("member a missing")
	}
	if _, ok := got["b"]; !ok {
		t.Fatal("member b missing")
	}
	if gotExp != nil {
		t.Fatalf("decoded expire_at_ms = %v, want nil", *gotExp)
	}
}

// buildListJSON constructs a lists/<k>.json body (items array in
// left-to-right order, binary envelopes).
func buildListJSON(t *testing.T, items []string, expireMs *uint64) []byte {
	t.Helper()
	raw := make([]json.RawMessage, 0, len(items))
	for _, it := range items {
		v, err := marshalRedisBinaryValue([]byte(it))
		if err != nil {
			t.Fatalf("marshal item: %v", err)
		}
		raw = append(raw, v)
	}
	out := struct {
		FormatVersion uint32            `json:"format_version"`
		Items         []json.RawMessage `json:"items"`
		ExpireAtMs    *uint64           `json:"expire_at_ms"`
	}{FormatVersion: 1, Items: raw, ExpireAtMs: expireMs}
	body, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("marshal list json: %v", err)
	}
	return body
}

// readListItems parses a decoded lists/<k>.json into an ordered slice
// plus its expiry.
func readListItems(t *testing.T, root, enc string) ([]string, *uint64) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, "redis", "db_0", "lists", enc+".json"))
	if err != nil {
		t.Fatalf("read decoded list: %v", err)
	}
	var rec listJSONRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		t.Fatalf("unmarshal decoded list: %v", err)
	}
	out := make([]string, 0, len(rec.Items))
	for _, itRaw := range rec.Items {
		it, err := unmarshalRedisBinaryValue(itRaw)
		if err != nil {
			t.Fatalf("unmarshal item: %v", err)
		}
		out = append(out, string(it))
	}
	return out, rec.ExpireAtMs
}

// TestRedisEncodeListRoundTripViaDecode runs the gold-standard
// directory round-trip for a list, asserting ORDER is preserved (the
// defining list property) plus a TTL.
func TestRedisEncodeListRoundTripViaDecode(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	enc := EncodeSegment([]byte("mylist"))
	const expireMs uint64 = 1_700_000_000_777
	items := []string{"job-1", "job-2", "job-3", "\xff\xfe"}
	exp := expireMs
	writeRedisFile(t, in, filepath.Join("lists", enc+".json"), buildListJSON(t, items, &exp))

	out := decodeRedisTree(t, encodeRedisTree(t, in))

	got, gotExp := readListItems(t, out, enc)
	if len(got) != len(items) {
		t.Fatalf("got %d items, want %d", len(got), len(items))
	}
	for i := range items {
		if got[i] != items[i] {
			t.Fatalf("item[%d] = %q, want %q (order must be preserved)", i, got[i], items[i])
		}
	}
	if gotExp == nil || *gotExp != expireMs {
		t.Fatalf("decoded expire_at_ms = %v, want %d", gotExp, expireMs)
	}
}

// TestRedisEncodeListNoTTLRoundTripViaDecode pins the no-TTL list path
// and single-element order.
func TestRedisEncodeListNoTTLRoundTripViaDecode(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	enc := EncodeSegment([]byte("q"))
	items := []string{"only"}
	writeRedisFile(t, in, filepath.Join("lists", enc+".json"), buildListJSON(t, items, nil))

	out := decodeRedisTree(t, encodeRedisTree(t, in))

	got, gotExp := readListItems(t, out, enc)
	if len(got) != 1 || got[0] != "only" {
		t.Fatalf("got %v, want [only]", got)
	}
	if gotExp != nil {
		t.Fatalf("decoded expire_at_ms = %v, want nil", *gotExp)
	}
}

// buildZSetJSON constructs a zsets/<k>.json body (members array of
// {member envelope, score number/"+inf"/"-inf"}).
func buildZSetJSON(t *testing.T, members map[string]float64, expireMs *uint64) []byte {
	t.Helper()
	names := make([]string, 0, len(members))
	for n := range members {
		names = append(names, n)
	}
	sort.Strings(names)
	type rec struct {
		Member json.RawMessage `json:"member"`
		Score  json.RawMessage `json:"score"`
	}
	recs := make([]rec, 0, len(names))
	for _, n := range names {
		nameJSON, err := marshalRedisBinaryValue([]byte(n))
		if err != nil {
			t.Fatalf("marshal member: %v", err)
		}
		scoreJSON, err := marshalRedisZSetScore(members[n])
		if err != nil {
			t.Fatalf("marshal score: %v", err)
		}
		recs = append(recs, rec{Member: nameJSON, Score: scoreJSON})
	}
	out := struct {
		FormatVersion uint32  `json:"format_version"`
		Members       []rec   `json:"members"`
		ExpireAtMs    *uint64 `json:"expire_at_ms"`
	}{FormatVersion: 1, Members: recs, ExpireAtMs: expireMs}
	body, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("marshal zset json: %v", err)
	}
	return body
}

// readZSetMembers parses a decoded zsets/<k>.json into member->score
// plus its expiry.
func readZSetMembers(t *testing.T, root, enc string) (map[string]float64, *uint64) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, "redis", "db_0", "zsets", enc+".json"))
	if err != nil {
		t.Fatalf("read decoded zset: %v", err)
	}
	var rec zsetJSONRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		t.Fatalf("unmarshal decoded zset: %v", err)
	}
	out := map[string]float64{}
	for _, m := range rec.Members {
		member, err := unmarshalRedisBinaryValue(m.Member)
		if err != nil {
			t.Fatalf("unmarshal member: %v", err)
		}
		score, err := unmarshalRedisZSetScore(m.Score)
		if err != nil {
			t.Fatalf("unmarshal score: %v", err)
		}
		out[string(member)] = score
	}
	return out, rec.ExpireAtMs
}

// TestRedisEncodeZSetRoundTripViaDecode runs the gold-standard
// directory round-trip for a zset with fractional, negative, and
// +inf scores plus a TTL.
func TestRedisEncodeZSetRoundTripViaDecode(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	enc := EncodeSegment([]byte("myzset"))
	const expireMs uint64 = 1_700_000_000_999
	want := map[string]float64{
		"alice": 100.5,
		"bob":   -3.25,
		"max":   math.Inf(1),
	}
	exp := expireMs
	writeRedisFile(t, in, filepath.Join("zsets", enc+".json"), buildZSetJSON(t, want, &exp))

	out := decodeRedisTree(t, encodeRedisTree(t, in))

	got, gotExp := readZSetMembers(t, out, enc)
	if len(got) != len(want) {
		t.Fatalf("got %d members, want %d", len(got), len(want))
	}
	for m, s := range want {
		if got[m] != s {
			t.Fatalf("member %q score = %v, want %v", m, got[m], s)
		}
	}
	if gotExp == nil || *gotExp != expireMs {
		t.Fatalf("decoded expire_at_ms = %v, want %d", gotExp, expireMs)
	}
}

// TestRedisEncodeZSetNoTTLRoundTripViaDecode pins the no-TTL zset path.
func TestRedisEncodeZSetNoTTLRoundTripViaDecode(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	enc := EncodeSegment([]byte("zz"))
	writeRedisFile(t, in, filepath.Join("zsets", enc+".json"),
		buildZSetJSON(t, map[string]float64{"x": 1, "y": 2}, nil))

	out := decodeRedisTree(t, encodeRedisTree(t, in))

	got, gotExp := readZSetMembers(t, out, enc)
	if got["x"] != 1 || got["y"] != 2 {
		t.Fatalf("got %v, want x=1 y=2", got)
	}
	if gotExp != nil {
		t.Fatalf("decoded expire_at_ms = %v, want nil", *gotExp)
	}
}

// streamTestEntry is a test stream entry: an ID plus ordered
// (name,value) field pairs.
type streamTestEntry struct {
	id     string
	fields [][2]string
}

// buildStreamJSONL constructs a streams/<k>.jsonl body: one entry line
// per entry then the _meta terminator line, matching marshalStreamJSONL.
func buildStreamJSONL(t *testing.T, entries []streamTestEntry, lastMs, lastSeq uint64, expireMs *uint64) []byte {
	t.Helper()
	type fieldRec struct {
		Name  json.RawMessage `json:"name"`
		Value json.RawMessage `json:"value"`
	}
	var buf bytes.Buffer
	for _, e := range entries {
		recs := make([]fieldRec, 0, len(e.fields))
		for _, f := range e.fields {
			nameJSON, err := marshalRedisBinaryValue([]byte(f[0]))
			if err != nil {
				t.Fatalf("marshal name: %v", err)
			}
			valJSON, err := marshalRedisBinaryValue([]byte(f[1]))
			if err != nil {
				t.Fatalf("marshal value: %v", err)
			}
			recs = append(recs, fieldRec{Name: nameJSON, Value: valJSON})
		}
		line, err := json.Marshal(struct {
			ID     string     `json:"id"`
			Fields []fieldRec `json:"fields"`
		}{ID: e.id, Fields: recs})
		if err != nil {
			t.Fatalf("marshal entry: %v", err)
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	meta, err := json.Marshal(struct {
		Meta       bool    `json:"_meta"`
		Length     int64   `json:"length"`
		LastMs     uint64  `json:"last_ms"`
		LastSeq    uint64  `json:"last_seq"`
		ExpireAtMs *uint64 `json:"expire_at_ms"`
	}{Meta: true, Length: int64(len(entries)), LastMs: lastMs, LastSeq: lastSeq, ExpireAtMs: expireMs})
	if err != nil {
		t.Fatalf("marshal meta: %v", err)
	}
	buf.Write(meta)
	buf.WriteByte('\n')
	return buf.Bytes()
}

// readStreamEntries parses a decoded streams/<k>.jsonl into ordered
// entries plus the _meta fields.
func readStreamEntries(t *testing.T, root, enc string) ([]streamTestEntry, streamLineJSON) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, "redis", "db_0", "streams", enc+".jsonl"))
	if err != nil {
		t.Fatalf("read decoded stream: %v", err)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	var entries []streamTestEntry
	var meta streamLineJSON
	for dec.More() {
		var line streamLineJSON
		if err := dec.Decode(&line); err != nil {
			t.Fatalf("decode stream line: %v", err)
		}
		if line.Meta {
			meta = line
			continue
		}
		ent := streamTestEntry{id: line.ID}
		for _, f := range line.Fields {
			name, err := unmarshalRedisBinaryValue(f.Name)
			if err != nil {
				t.Fatalf("unmarshal field name: %v", err)
			}
			value, err := unmarshalRedisBinaryValue(f.Value)
			if err != nil {
				t.Fatalf("unmarshal field value: %v", err)
			}
			ent.fields = append(ent.fields, [2]string{string(name), string(value)})
		}
		entries = append(entries, ent)
	}
	return entries, meta
}

// TestRedisEncodeStreamRoundTripViaDecode runs the gold-standard
// directory round-trip for a stream: entries (with duplicate field
// names and a binary value) preserve ID + interleaved field order, and
// the _meta line (length, last_ms, last_seq, TTL) round-trips.
func TestRedisEncodeStreamRoundTripViaDecode(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	enc := EncodeSegment([]byte("mystream"))
	const expireMs uint64 = 1_700_000_000_321
	// Stream field values must be valid UTF-8: the live store marshals
	// them into a proto3 string field, which rejects non-UTF-8 — so a
	// real dump never carries binary stream fields. Duplicate field
	// names within one entry ARE valid (XADD s * f v1 f v2) and must
	// preserve interleaved order.
	entries := []streamTestEntry{
		{id: "1714400000000-0", fields: [][2]string{{"event", "login"}, {"user", "alice"}}},
		{id: "1714400000001-5", fields: [][2]string{{"f", "v1"}, {"f", "v2"}}},
	}
	exp := expireMs
	writeRedisFile(t, in, filepath.Join("streams", enc+".jsonl"),
		buildStreamJSONL(t, entries, 1714400000001, 5, &exp))

	out := decodeRedisTree(t, encodeRedisTree(t, in))

	got, meta := readStreamEntries(t, out, enc)
	assertStreamEntriesEqual(t, got, entries)
	if meta.Length != int64(len(entries)) || meta.LastMs != 1714400000001 || meta.LastSeq != 5 {
		t.Fatalf("meta = {len:%d lastMs:%d lastSeq:%d}, want {2 1714400000001 5}", meta.Length, meta.LastMs, meta.LastSeq)
	}
	if meta.ExpireAtMs == nil || *meta.ExpireAtMs != expireMs {
		t.Fatalf("meta.expire_at_ms = %v, want %d", meta.ExpireAtMs, expireMs)
	}
}

// assertStreamEntriesEqual checks two stream-entry slices match on ID
// and interleaved field order.
func assertStreamEntriesEqual(t *testing.T, got, want []streamTestEntry) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d entries, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].id != want[i].id {
			t.Fatalf("entry[%d] id = %q, want %q", i, got[i].id, want[i].id)
		}
		if len(got[i].fields) != len(want[i].fields) {
			t.Fatalf("entry[%d] field count = %d, want %d", i, len(got[i].fields), len(want[i].fields))
		}
		for j := range want[i].fields {
			if got[i].fields[j] != want[i].fields[j] {
				t.Fatalf("entry[%d] field[%d] = %v, want %v (order must hold)", i, j, got[i].fields[j], want[i].fields[j])
			}
		}
	}
}

// TestRedisEncodeStreamEmptyRoundTripViaDecode pins an empty-but-meta
// stream (length 0, no entries) — XLEN observable, no TTL.
func TestRedisEncodeStreamEmptyRoundTripViaDecode(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	enc := EncodeSegment([]byte("emptystream"))
	writeRedisFile(t, in, filepath.Join("streams", enc+".jsonl"),
		buildStreamJSONL(t, nil, 0, 0, nil))

	out := decodeRedisTree(t, encodeRedisTree(t, in))

	got, meta := readStreamEntries(t, out, enc)
	if len(got) != 0 {
		t.Fatalf("got %d entries, want 0", len(got))
	}
	if !meta.Meta {
		t.Fatal("decoded stream missing the _meta terminator line")
	}
	if meta.Length != 0 {
		t.Fatalf("meta.length = %d, want 0", meta.Length)
	}
	if meta.ExpireAtMs != nil {
		t.Fatalf("meta.expire_at_ms = %v, want nil", *meta.ExpireAtMs)
	}
}

// TestUnmarshalRedisBinaryValue pins both envelope shapes directly.
func TestUnmarshalRedisBinaryValue(t *testing.T) {
	t.Parallel()
	// UTF-8 plain-string form.
	plain, err := unmarshalRedisBinaryValue(json.RawMessage(`"hello"`))
	if err != nil || !bytes.Equal(plain, []byte("hello")) {
		t.Fatalf("plain decode = %q, %v; want hello", plain, err)
	}
	// base64 envelope form (0xff 0x00).
	b64, err := unmarshalRedisBinaryValue(json.RawMessage(`{"base64":"_wA"}`))
	if err != nil || !bytes.Equal(b64, []byte{0xff, 0x00}) {
		t.Fatalf("base64 decode = %x, %v; want ff00", b64, err)
	}
	// A JSON object lacking "base64" must fail closed, not silently
	// decode to empty bytes.
	if _, err := unmarshalRedisBinaryValue(json.RawMessage(`{"wrong":"x"}`)); !errors.Is(err, ErrRedisEncodeInvalidJSON) {
		t.Fatalf("unknown-object decode err = %v, want ErrRedisEncodeInvalidJSON", err)
	}
	// An empty base64 envelope is a valid empty value (not an error).
	empty, err := unmarshalRedisBinaryValue(json.RawMessage(`{"base64":""}`))
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty base64 = %x, %v; want empty/no-error", empty, err)
	}
}

// TestRedisEncodeCollectionRejectsUnknownFormatVersion pins that each
// JSON collection encoder fails closed on an unsupported
// format_version rather than decoding with zero-valued fields and
// emitting corrupt/empty rows.
func TestRedisEncodeCollectionRejectsUnknownFormatVersion(t *testing.T) {
	t.Parallel()
	for _, subdir := range []string{"hashes", "sets", "lists", "zsets"} {
		t.Run(subdir, func(t *testing.T) {
			t.Parallel()
			in := t.TempDir()
			enc := EncodeSegment([]byte("k"))
			// format_version 99 is unsupported; the guard fires right
			// after Decode, before any field is consulted.
			writeRedisFile(t, in, filepath.Join(subdir, enc+".json"), []byte(`{"format_version":99}`))
			b := newSnapshotBuilder(redisEncTS)
			err := NewRedisEncoder(in, 0).Encode(b)
			if !errors.Is(err, ErrRedisEncodeInvalidJSON) {
				t.Fatalf("%s Encode err = %v, want ErrRedisEncodeInvalidJSON", subdir, err)
			}
		})
	}
}

// TestRedisEncodeCollectionRejectsTrailingJSON pins that a collection
// file with data after the single JSON object fails closed rather than
// silently ignoring the trailing bytes.
func TestRedisEncodeCollectionRejectsTrailingJSON(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	enc := EncodeSegment([]byte("k"))
	// A valid hash object followed by a second object (trailing data).
	body := append(buildHashJSON(t, map[string]string{"f": "v"}, nil), []byte(`{"x":1}`)...)
	writeRedisFile(t, in, filepath.Join("hashes", enc+".json"), body)
	b := newSnapshotBuilder(redisEncTS)
	err := NewRedisEncoder(in, 0).Encode(b)
	if !errors.Is(err, ErrRedisEncodeInvalidJSON) {
		t.Fatalf("Encode err = %v, want ErrRedisEncodeInvalidJSON", err)
	}
}

// TestRedisEncodeStreamRejectsDuplicateMeta pins that a stream JSONL
// carrying two _meta lines fails closed rather than silently letting
// the second overwrite the first.
func TestRedisEncodeStreamRejectsDuplicateMeta(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	enc := EncodeSegment([]byte("dupmeta"))
	body := []byte(
		`{"_meta":true,"length":0,"last_ms":0,"last_seq":0,"expire_at_ms":null}` + "\n" +
			`{"_meta":true,"length":0,"last_ms":0,"last_seq":0,"expire_at_ms":null}` + "\n")
	writeRedisFile(t, in, filepath.Join("streams", enc+".jsonl"), body)
	b := newSnapshotBuilder(redisEncTS)
	err := NewRedisEncoder(in, 0).Encode(b)
	if !errors.Is(err, ErrRedisEncodeInvalidJSON) {
		t.Fatalf("Encode err = %v, want ErrRedisEncodeInvalidJSON", err)
	}
}

// TestRedisEncodeStreamRejectsMismatchedMetaLength pins that a _meta
// length disagreeing with the parsed entry count fails closed rather
// than restoring an inconsistent XLEN.
func TestRedisEncodeStreamRejectsMismatchedMetaLength(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	enc := EncodeSegment([]byte("mism"))
	// One entry line, but _meta claims length 0.
	body := []byte(`{"id":"1-0","fields":[{"name":"f","value":"v"}]}` + "\n" +
		`{"_meta":true,"length":0,"last_ms":1,"last_seq":0,"expire_at_ms":null}` + "\n")
	writeRedisFile(t, in, filepath.Join("streams", enc+".jsonl"), body)

	b := newSnapshotBuilder(redisEncTS)
	err := NewRedisEncoder(in, 0).Encode(b)
	if !errors.Is(err, ErrRedisEncodeInvalidJSON) {
		t.Fatalf("Encode err = %v, want ErrRedisEncodeInvalidJSON", err)
	}
}

// TestRedisEncodeStreamRejectsStaleLastID pins that a _meta
// last_ms/last_seq behind the maximum parsed entry ID fails closed —
// otherwise a future XADD '*' could mint an ID that collides with an
// existing entry.
func TestRedisEncodeStreamRejectsStaleLastID(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	enc := EncodeSegment([]byte("stale"))
	// Entry at 5-0 but _meta high-water mark only 3-0.
	body := []byte(`{"id":"5-0","fields":[{"name":"f","value":"v"}]}` + "\n" +
		`{"_meta":true,"length":1,"last_ms":3,"last_seq":0,"expire_at_ms":null}` + "\n")
	writeRedisFile(t, in, filepath.Join("streams", enc+".jsonl"), body)

	b := newSnapshotBuilder(redisEncTS)
	err := NewRedisEncoder(in, 0).Encode(b)
	if !errors.Is(err, ErrRedisEncodeInvalidJSON) {
		t.Fatalf("Encode err = %v, want ErrRedisEncodeInvalidJSON", err)
	}
}

// TestRedisEncodeStreamRejectsNegativeLength pins that a _meta line with
// a negative length fails closed rather than encoding a corrupt
// (uint64-wrapped) stream meta.
func TestRedisEncodeStreamRejectsNegativeLength(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	enc := EncodeSegment([]byte("badstream"))
	body := []byte(`{"_meta":true,"length":-1,"last_ms":0,"last_seq":0,"expire_at_ms":null}` + "\n")
	writeRedisFile(t, in, filepath.Join("streams", enc+".jsonl"), body)

	b := newSnapshotBuilder(redisEncTS)
	err := NewRedisEncoder(in, 0).Encode(b)
	if !errors.Is(err, ErrRedisEncodeInvalidJSON) {
		t.Fatalf("Encode err = %v, want ErrRedisEncodeInvalidJSON", err)
	}
}
