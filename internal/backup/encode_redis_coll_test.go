package backup

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

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
}
