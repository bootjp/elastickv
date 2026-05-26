package backup

import (
	"bytes"
	"encoding/base64"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
)

// liveDDBEntries encodes a dump tree and returns the decoded live
// (userKey,userValue) records — used to inspect the derived !ddb|gsi| rows
// directly, since the decoder's HandleGSIRow drops them (so a directory
// round-trip cannot observe them).
func liveDDBEntries(t *testing.T, inRoot string) []RoundTripEntry {
	t.Helper()
	b := newSnapshotBuilder(ddbEncTS)
	if err := NewDynamoDBEncoder(inRoot).Encode(b); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var buf bytes.Buffer
	if _, err := b.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	entries, _, err := DecodeLiveEntries(&buf)
	if err != nil {
		t.Fatalf("DecodeLiveEntries: %v", err)
	}
	return entries
}

func splitDDBEntries(entries []RoundTripEntry) (itemKeys [][]byte, gsiRows []RoundTripEntry) {
	for _, e := range entries {
		switch {
		case bytes.HasPrefix(e.UserKey, []byte(DDBItemPrefix)):
			itemKeys = append(itemKeys, e.UserKey)
		case bytes.HasPrefix(e.UserKey, []byte(DDBGSIPrefix)):
			gsiRows = append(gsiRows, e)
		}
	}
	return itemKeys, gsiRows
}

const ddbGSIOrdersSchema = `{"format_version":1,"table_name":"orders",` +
	`"primary_key":{"hash_key":{"name":"customer","type":"S"},"range_key":{"name":"order","type":"S"}},` +
	`"attribute_definitions":[{"name":"customer","type":"S"},{"name":"order","type":"S"},{"name":"region","type":"S"}],` +
	`"global_secondary_indexes":[{"name":"by-region","key_schema":{"hash_key":{"name":"region","type":"S"}},"projection":{"type":"ALL"}}]}`

// TestDDBEncodeGSIRowPointsToItem pins that an item with the index key
// attribute produces exactly one derived GSI row whose value is the base
// item's !ddb|item| key and whose key carries the
// !ddb|gsi|<table>|<gen>|<index>| prefix.
func TestDDBEncodeGSIRowPointsToItem(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const table = "orders"
	writeDDBSchema(t, in, EncodeSegment([]byte(table)), []byte(ddbGSIOrdersSchema))
	writeDDBItemFile(t, in, table, "i.json", []byte(`{"customer":{"S":"c1"},"order":{"S":"o1"},"region":{"S":"us"}}`))

	itemKeys, gsiRows := splitDDBEntries(liveDDBEntries(t, in))
	if len(itemKeys) != 1 {
		t.Fatalf("item records = %d, want 1", len(itemKeys))
	}
	if len(gsiRows) != 1 {
		t.Fatalf("GSI rows = %d, want 1", len(gsiRows))
	}
	if !bytes.Equal(gsiRows[0].UserValue, itemKeys[0]) {
		t.Fatalf("GSI value = %x, want item key %x", gsiRows[0].UserValue, itemKeys[0])
	}
	wantPrefix := []byte(DDBGSIPrefix)
	wantPrefix = append(wantPrefix, base64.RawURLEncoding.EncodeToString([]byte(table))...)
	wantPrefix = append(wantPrefix, "|1|"...)
	wantPrefix = append(wantPrefix, base64.RawURLEncoding.EncodeToString([]byte("by-region"))...)
	wantPrefix = append(wantPrefix, '|')
	if !bytes.HasPrefix(gsiRows[0].UserKey, wantPrefix) {
		t.Fatalf("GSI key %x missing prefix %x", gsiRows[0].UserKey, wantPrefix)
	}
}

// TestDDBEncodeGSISparseSkipsMissingIndexKey pins sparse indexing: an item
// lacking the index hash attribute contributes no GSI row (but still its
// base item record).
func TestDDBEncodeGSISparseSkipsMissingIndexKey(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const table = "orders"
	writeDDBSchema(t, in, EncodeSegment([]byte(table)), []byte(ddbGSIOrdersSchema))
	writeDDBItemFile(t, in, table, "with.json", []byte(`{"customer":{"S":"c1"},"order":{"S":"o1"},"region":{"S":"us"}}`))
	writeDDBItemFile(t, in, table, "without.json", []byte(`{"customer":{"S":"c2"},"order":{"S":"o2"}}`))

	itemKeys, gsiRows := splitDDBEntries(liveDDBEntries(t, in))
	if len(itemKeys) != 2 {
		t.Fatalf("item records = %d, want 2", len(itemKeys))
	}
	if len(gsiRows) != 1 {
		t.Fatalf("GSI rows = %d, want 1 (only the item carrying region)", len(gsiRows))
	}
}

// TestDDBGSIRowKeyLayout pins the full derived GSI key bytes against a
// hand-computed expectation: prefix + ordered index hash + ordered PK hash
// (no ranges), value = the item key.
func TestDDBGSIRowKeyLayout(t *testing.T) {
	t.Parallel()
	schema := &pb.DynamoTableSchema{
		PrimaryKey: &pb.DynamoKeySchema{HashKey: "ph"},
		GlobalSecondaryIndexes: map[string]*pb.DynamoGlobalSecondaryIndex{
			"g": {KeySchema: &pb.DynamoKeySchema{HashKey: "gh"}},
		},
	}
	item := &pb.DynamoItem{Attributes: map[string]*pb.DynamoAttributeValue{
		"ph": {Value: &pb.DynamoAttributeValue_S{S: "P"}},
		"gh": {Value: &pb.DynamoAttributeValue_S{S: "X"}},
	}}
	itemKey, err := ddbItemKeyBytes("t", 1, schema, item)
	if err != nil {
		t.Fatalf("ddbItemKeyBytes: %v", err)
	}
	rows, err := ddbGSIRows("t", 1, schema, item, itemKey)
	if err != nil {
		t.Fatalf("ddbGSIRows: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	want := []byte(DDBGSIPrefix)
	want = append(want, base64.RawURLEncoding.EncodeToString([]byte("t"))...)
	want = append(want, "|1|"...)
	want = append(want, base64.RawURLEncoding.EncodeToString([]byte("g"))...)
	want = append(want, '|')
	want = append(want, 'X', 0x00, 0x01) // ordered index hash
	want = append(want, 'P', 0x00, 0x01) // ordered PK hash
	if !bytes.Equal(rows[0].key, want) {
		t.Fatalf("GSI key = %x, want %x", rows[0].key, want)
	}
	if !bytes.Equal(rows[0].value, itemKey) {
		t.Fatalf("GSI value = %x, want item key %x", rows[0].value, itemKey)
	}
}

// TestDDBEncodeNoGSINoRows pins that a table without GSIs emits no
// !ddb|gsi| rows.
func TestDDBEncodeNoGSINoRows(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const table = "sessions"
	writeDDBSchema(t, in, EncodeSegment([]byte(table)), []byte(`{"format_version":1,"table_name":"sessions",`+
		`"primary_key":{"hash_key":{"name":"id","type":"S"}},`+
		`"attribute_definitions":[{"name":"id","type":"S"}]}`))
	writeDDBItemFile(t, in, table, "a.json", []byte(`{"id":{"S":"a"}}`))

	_, gsiRows := splitDDBEntries(liveDDBEntries(t, in))
	if len(gsiRows) != 0 {
		t.Fatalf("GSI rows = %d, want 0", len(gsiRows))
	}
}
