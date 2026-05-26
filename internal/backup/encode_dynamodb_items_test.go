package backup

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
)

// writeDDBItemFile writes a raw item JSON body under
// <root>/dynamodb/<EncodeSegment(table)>/items/<rel>, creating parents.
func writeDDBItemFile(t *testing.T, root, table, rel string, body []byte) {
	t.Helper()
	path := filepath.Join(root, "dynamodb", EncodeSegment([]byte(table)), "items", rel)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("WriteFile %s: %v", rel, err)
	}
}

// collectDDBItems walks the decoded items/ tree for a table and returns
// each item's public attribute map (parsed from JSON), so assertions do
// not depend on the decoder's filename layout.
func collectDDBItems(t *testing.T, outRoot, table string) []map[string]any {
	t.Helper()
	itemsDir := filepath.Join(outRoot, "dynamodb", EncodeSegment([]byte(table)), "items")
	var out []map[string]any
	err := filepath.WalkDir(itemsDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}
		data, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		var m map[string]any
		if jerr := json.Unmarshal(data, &m); jerr != nil {
			return jerr
		}
		out = append(out, m)
		return nil
	})
	if err != nil {
		t.Fatalf("walk decoded items: %v", err)
	}
	return out
}

// reparse normalizes a JSON body through json.Unmarshal so it can be
// compared against decoded items with reflect.DeepEqual.
func reparse(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("reparse: %v", err)
	}
	return m
}

// TestDDBEncodeItemCompositeRoundTrip pins the gold-standard directory
// round-trip for a composite-key (S/S) table carrying every attribute
// type: the item survives encode -> real DecodeSnapshot -> dump byte for
// byte.
func TestDDBEncodeItemCompositeRoundTrip(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const table = "orders"
	writeDDBSchema(t, in, EncodeSegment([]byte(table)), []byte(`{"format_version":1,"table_name":"orders",`+
		`"primary_key":{"hash_key":{"name":"pk","type":"S"},"range_key":{"name":"sk","type":"S"}},`+
		`"attribute_definitions":[{"name":"pk","type":"S"},{"name":"sk","type":"S"}]}`))
	item := []byte(`{` +
		`"pk":{"S":"u1"},"sk":{"S":"2024"},` +
		`"name":{"S":"alice"},"age":{"N":"30"},"active":{"BOOL":true},"meta":{"NULL":true},` +
		`"tags":{"SS":["a","b"]},"scores":{"NS":["1","2"]},` +
		`"blob":{"B":"aGVsbG8="},"blobs":{"BS":["AAE=","Av8="]},` +
		`"items":{"L":[{"S":"x"},{"N":"5"}]},"nested":{"M":{"k":{"S":"v"}}}` +
		`}`)
	writeDDBItemFile(t, in, table, "item.json", item)

	out := decodeDDBTree(t, encodeDDBTree(t, in))
	got := collectDDBItems(t, out, table)
	if len(got) != 1 {
		t.Fatalf("decoded %d items, want 1", len(got))
	}
	want := reparse(t, item)
	if !reflect.DeepEqual(got[0], want) {
		t.Fatalf("round-tripped item mismatch:\n got = %#v\nwant = %#v", got[0], want)
	}
}

// TestDDBEncodeItemHashOnlyRoundTrip pins the hash-only (no range key)
// layout: items/<hashSeg>.json read at the top level.
func TestDDBEncodeItemHashOnlyRoundTrip(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const table = "sessions"
	writeDDBSchema(t, in, EncodeSegment([]byte(table)), []byte(`{"format_version":1,"table_name":"sessions",`+
		`"primary_key":{"hash_key":{"name":"id","type":"S"}},`+
		`"attribute_definitions":[{"name":"id","type":"S"}]}`))
	item := []byte(`{"id":{"S":"abc"},"v":{"N":"7"}}`)
	writeDDBItemFile(t, in, table, "abc.json", item)

	out := decodeDDBTree(t, encodeDDBTree(t, in))
	got := collectDDBItems(t, out, table)
	if len(got) != 1 {
		t.Fatalf("decoded %d items, want 1", len(got))
	}
	if !reflect.DeepEqual(got[0], reparse(t, item)) {
		t.Fatalf("hash-only item mismatch: got %#v", got[0])
	}
}

// TestDDBEncodeItemBinaryKeyRoundTrip pins a binary (B) hash key, whose
// raw bytes (including a 0x00 that must be escaped 0x00 0xFF in the
// ordered key) survive the round-trip.
func TestDDBEncodeItemBinaryKeyRoundTrip(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const table = "blobs"
	writeDDBSchema(t, in, EncodeSegment([]byte(table)), []byte(`{"format_version":1,"table_name":"blobs",`+
		`"primary_key":{"hash_key":{"name":"bk","type":"B"}},`+
		`"attribute_definitions":[{"name":"bk","type":"B"}]}`))
	// bk = bytes {0x00,0x01} (base64 std "AAE="), exercising 0x00 escape.
	item := []byte(`{"bk":{"B":"AAE="},"payload":{"S":"hi"}}`)
	writeDDBItemFile(t, in, table, "sub/x.json", item) // also exercises subdir descent

	out := decodeDDBTree(t, encodeDDBTree(t, in))
	got := collectDDBItems(t, out, table)
	if len(got) != 1 {
		t.Fatalf("decoded %d items, want 1", len(got))
	}
	if !reflect.DeepEqual(got[0], reparse(t, item)) {
		t.Fatalf("binary-key item mismatch: got %#v", got[0])
	}
}

// TestDDBEncodeItemNumericKeyFailsClosed pins that a numeric (N) primary
// key fails closed in this slice rather than emitting a wrongly-ordered
// key.
func TestDDBEncodeItemNumericKeyFailsClosed(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const table = "counters"
	writeDDBSchema(t, in, EncodeSegment([]byte(table)), []byte(`{"format_version":1,"table_name":"counters",`+
		`"primary_key":{"hash_key":{"name":"id","type":"N"}},`+
		`"attribute_definitions":[{"name":"id","type":"N"}]}`))
	writeDDBItemFile(t, in, table, "1.json", []byte(`{"id":{"N":"1"},"v":{"S":"x"}}`))

	b := newSnapshotBuilder(ddbEncTS)
	err := NewDynamoDBEncoder(in).Encode(b)
	if !errors.Is(err, ErrDDBEncodeNumericKeyUnsupported) {
		t.Fatalf("Encode err = %v, want ErrDDBEncodeNumericKeyUnsupported", err)
	}
}

// TestDDBEncodeItemNumericRangeKeyFailsClosed pins the fail-closed path
// for a numeric RANGE key (the hash-key case is covered separately), so
// both primary-key positions reject N until the ordered numeric encoding
// lands (M3b-2).
func TestDDBEncodeItemNumericRangeKeyFailsClosed(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const table = "events"
	writeDDBSchema(t, in, EncodeSegment([]byte(table)), []byte(`{"format_version":1,"table_name":"events",`+
		`"primary_key":{"hash_key":{"name":"pk","type":"S"},"range_key":{"name":"ts","type":"N"}},`+
		`"attribute_definitions":[{"name":"pk","type":"S"},{"name":"ts","type":"N"}]}`))
	writeDDBItemFile(t, in, table, "e.json", []byte(`{"pk":{"S":"a"},"ts":{"N":"100"}}`))

	b := newSnapshotBuilder(ddbEncTS)
	err := NewDynamoDBEncoder(in).Encode(b)
	if !errors.Is(err, ErrDDBEncodeNumericKeyUnsupported) {
		t.Fatalf("Encode err = %v, want ErrDDBEncodeNumericKeyUnsupported", err)
	}
}

// TestDDBEncodeItemRejectsDeeperNesting pins that an items/ tree deeper
// than the decoder's fixed 2 levels (items/<hash>/<range>.json) fails
// closed rather than silently skipping items — preventing a "restore
// succeeded but item count is off" surprise from a corrupt/hand-crafted
// dump (claude review on PR #837).
func TestDDBEncodeItemRejectsDeeperNesting(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const table = "t"
	writeDDBSchema(t, in, EncodeSegment([]byte(table)), []byte(`{"format_version":1,"table_name":"t",`+
		`"primary_key":{"hash_key":{"name":"pk","type":"S"},"range_key":{"name":"sk","type":"S"}},`+
		`"attribute_definitions":[{"name":"pk","type":"S"},{"name":"sk","type":"S"}]}`))
	// items/h/sub/x.json — a directory nested inside a hash directory.
	writeDDBItemFile(t, in, table, "h/sub/x.json", []byte(`{"pk":{"S":"h"},"sk":{"S":"x"}}`))

	b := newSnapshotBuilder(ddbEncTS)
	err := NewDynamoDBEncoder(in).Encode(b)
	if !errors.Is(err, ErrDDBEncodeInvalidItem) {
		t.Fatalf("Encode err = %v, want ErrDDBEncodeInvalidItem", err)
	}
}

// TestDDBEncodeItemMissingHashKeyFailsClosed pins that an item whose JSON
// lacks the schema's hash-key attribute is rejected.
func TestDDBEncodeItemMissingHashKeyFailsClosed(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const table = "t"
	writeDDBSchema(t, in, EncodeSegment([]byte(table)), []byte(`{"format_version":1,"table_name":"t",`+
		`"primary_key":{"hash_key":{"name":"id","type":"S"}},`+
		`"attribute_definitions":[{"name":"id","type":"S"}]}`))
	writeDDBItemFile(t, in, table, "x.json", []byte(`{"other":{"S":"v"}}`))

	b := newSnapshotBuilder(ddbEncTS)
	err := NewDynamoDBEncoder(in).Encode(b)
	if !errors.Is(err, ErrDDBEncodeInvalidItem) {
		t.Fatalf("Encode err = %v, want ErrDDBEncodeInvalidItem", err)
	}
}

// TestDDBEncodeItemMissingItemsDirIsNoop pins that a table with a schema
// but no items/ directory encodes its schema with no item records and no
// error.
func TestDDBEncodeItemMissingItemsDirIsNoop(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const table = "empty"
	writeDDBSchema(t, in, EncodeSegment([]byte(table)), []byte(`{"format_version":1,"table_name":"empty",`+
		`"primary_key":{"hash_key":{"name":"id","type":"S"}},`+
		`"attribute_definitions":[{"name":"id","type":"S"}]}`))
	b := newSnapshotBuilder(ddbEncTS)
	if err := NewDynamoDBEncoder(in).Encode(b); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	// schema record + gen counter, no item records.
	if b.Len() != 2 {
		t.Fatalf("entries = %d, want 2 (schema + gen)", b.Len())
	}
}

// TestDDBItemKeyBytesLayout pins the FULL ordered item key against a
// hand-computed expectation. The decoder round-trip cannot cover this
// (it reconstructs items from the proto VALUE and ignores the key's
// ordered hash/range payload), so this is the guard that the emitted key
// matches what the live adapter (dynamoItemKey, KeyEncodingVersion=V2)
// looks up — i.e. that the restored item is actually GetItem-able.
func TestDDBItemKeyBytesLayout(t *testing.T) {
	t.Parallel()
	schema := &pb.DynamoTableSchema{PrimaryKey: &pb.DynamoKeySchema{HashKey: "h", RangeKey: "r"}}
	item := &pb.DynamoItem{Attributes: map[string]*pb.DynamoAttributeValue{
		"h": {Value: &pb.DynamoAttributeValue_S{S: "a"}},
		"r": {Value: &pb.DynamoAttributeValue_B{B: []byte{0x00, 0x09}}},
	}}
	got, err := ddbItemKeyBytes("tbl", 1, schema, item)
	if err != nil {
		t.Fatalf("ddbItemKeyBytes: %v", err)
	}
	want := []byte(DDBItemPrefix)
	want = append(want, base64.RawURLEncoding.EncodeToString([]byte("tbl"))...)
	want = append(want, "|1|"...)
	want = append(want, 'a', 0x00, 0x01)              // hash "a" -> 'a' 00 01
	want = append(want, 0x00, 0xFF, 0x09, 0x00, 0x01) // range {00,09} -> 00 FF 09 00 01
	if !bytes.Equal(got, want) {
		t.Fatalf("item key = %x, want %x", got, want)
	}
}

// TestDDBEncodeItemPreservesStructuralEdgeCases pins ROUND-TRIP fidelity
// for the structural edge cases the decoder intentionally preserves
// (rather than dropping): empty sets are serialized as [] and a NULL is
// serialized with its boolean as-is. The encoder is the decoder's inverse
// and must reproduce these so a snapshot of a legacy/drifted store can be
// recovered — the encode path reconstructs internal records, it does NOT
// re-validate AWS API payload semantics (codex P1/P2 on PR #837).
func TestDDBEncodeItemPreservesStructuralEdgeCases(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const table = "edge"
	writeDDBSchema(t, in, EncodeSegment([]byte(table)), []byte(`{"format_version":1,"table_name":"edge",`+
		`"primary_key":{"hash_key":{"name":"id","type":"S"}},`+
		`"attribute_definitions":[{"name":"id","type":"S"}]}`))
	item := []byte(`{"id":{"S":"k"},` +
		`"ess":{"SS":[]},"ens":{"NS":[]},"ebs":{"BS":[]},"nf":{"NULL":false}}`)
	writeDDBItemFile(t, in, table, "k.json", item)

	out := decodeDDBTree(t, encodeDDBTree(t, in))
	got := collectDDBItems(t, out, table)
	if len(got) != 1 {
		t.Fatalf("decoded %d items, want 1", len(got))
	}
	if !reflect.DeepEqual(got[0], reparse(t, item)) {
		t.Fatalf("structural edge-case round-trip mismatch:\n got = %#v\nwant = %#v", got[0], reparse(t, item))
	}
}

// TestEncodeDDBOrderedKeySegment pins the ordered-key byte layout
// directly: 0x00 escapes to 0x00 0xFF and every segment ends 0x00 0x01.
func TestEncodeDDBOrderedKeySegment(t *testing.T) {
	t.Parallel()
	got := encodeDDBOrderedKeySegment([]byte{'a', 0x00, 'b'})
	want := []byte{'a', 0x00, 0xFF, 'b', 0x00, 0x01}
	if !bytes.Equal(got, want) {
		t.Fatalf("ordered segment = %x, want %x", got, want)
	}
	if g := encodeDDBOrderedKeySegment(nil); !bytes.Equal(g, []byte{0x00, 0x01}) {
		t.Fatalf("empty ordered segment = %x, want 0001", g)
	}
}
