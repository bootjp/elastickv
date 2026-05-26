package backup

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/errors"
)

const ddbEncTS uint64 = 0x0001_8F1A_2B3C_00AB

// writeDDBSchema writes a _schema.json under <root>/dynamodb/<dir>/.
func writeDDBSchema(t *testing.T, root, tableDir string, body []byte) {
	t.Helper()
	path := filepath.Join(root, "dynamodb", tableDir, "_schema.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
}

// encodeDDBTree runs the DynamoDB encoder over inRoot and returns the
// EKVPBBL1 bytes.
func encodeDDBTree(t *testing.T, inRoot string) []byte {
	t.Helper()
	b := newSnapshotBuilder(ddbEncTS)
	if err := NewDynamoDBEncoder(inRoot).Encode(b); err != nil {
		t.Fatalf("DynamoDBEncoder.Encode: %v", err)
	}
	var buf bytes.Buffer
	if _, err := b.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	return buf.Bytes()
}

// decodeDDBTree decodes fsm bytes through the real decode path into a
// fresh output tree and returns its root.
func decodeDDBTree(t *testing.T, fsm []byte) string {
	t.Helper()
	out := t.TempDir()
	if _, err := DecodeSnapshot(bytes.NewReader(fsm), DecodeOptions{
		OutRoot:  out,
		Adapters: AdapterSet{DynamoDB: true},
	}); err != nil {
		t.Fatalf("DecodeSnapshot: %v", err)
	}
	return out
}

// readDDBSchema parses a decoded _schema.json for the given table.
func readDDBSchema(t *testing.T, root, tableName string) ddbPublicSchema {
	t.Helper()
	dir := EncodeSegment([]byte(tableName))
	data, err := os.ReadFile(filepath.Join(root, "dynamodb", dir, "_schema.json"))
	if err != nil {
		t.Fatalf("read decoded schema: %v", err)
	}
	var pub ddbPublicSchema
	if err := json.Unmarshal(data, &pub); err != nil {
		t.Fatalf("unmarshal decoded schema: %v", err)
	}
	return pub
}

// TestDDBEncodeSchemaRoundTripViaDecode runs the gold-standard
// directory round-trip for a composite-key table with a GSI: the
// schema (table name, primary key, attribute definitions, GSI) is
// reconstructed into the !ddb|meta|table| record and recovered by the
// decoder into an equivalent _schema.json.
func TestDDBEncodeSchemaRoundTripViaDecode(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const table = "orders"
	input := ddbPublicSchema{
		FormatVersion: 1,
		TableName:     table,
		PrimaryKey: publicKeySchema{
			HashKey:  publicKeyAttribute{Name: "customer", Type: "S"},
			RangeKey: publicKeyAttribute{Name: "ts", Type: "N"},
		},
		AttributeDefinitions: []publicAttributeDefinition{
			{Name: "customer", Type: "S"},
			{Name: "region", Type: "S"},
			{Name: "ts", Type: "N"},
		},
		GlobalSecondaryIndexes: []publicGSI{{
			Name: "by-region",
			KeySchema: publicKeySchema{
				HashKey:  publicKeyAttribute{Name: "region", Type: "S"},
				RangeKey: publicKeyAttribute{Name: "ts", Type: "N"},
			},
			Projection: publicProjection{Type: "ALL"},
		}},
	}
	body, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("marshal input schema: %v", err)
	}
	writeDDBSchema(t, in, EncodeSegment([]byte(table)), body)

	out := decodeDDBTree(t, encodeDDBTree(t, in))
	got := readDDBSchema(t, out, table)
	assertCompositeOrdersSchema(t, got, table)
}

// assertCompositeOrdersSchema checks the round-tripped composite-key
// "orders" schema (pk customer/ts, 3 attr defs, one GSI by-region/ALL).
func assertCompositeOrdersSchema(t *testing.T, got ddbPublicSchema, table string) {
	t.Helper()
	if got.TableName != table {
		t.Fatalf("table_name = %q, want %q", got.TableName, table)
	}
	assertKeyAttr(t, "hash", got.PrimaryKey.HashKey, "customer", "S")
	assertKeyAttr(t, "range", got.PrimaryKey.RangeKey, "ts", "N")
	if len(got.AttributeDefinitions) != 3 {
		t.Fatalf("attr defs = %d, want 3", len(got.AttributeDefinitions))
	}
	if len(got.GlobalSecondaryIndexes) != 1 {
		t.Fatalf("GSIs = %d, want 1", len(got.GlobalSecondaryIndexes))
	}
	gsi := got.GlobalSecondaryIndexes[0]
	if gsi.Name != "by-region" || gsi.Projection.Type != "ALL" {
		t.Fatalf("GSI = %+v, want by-region/ALL", gsi)
	}
	assertKeyAttr(t, "GSI hash", gsi.KeySchema.HashKey, "region", "S")
}

// assertKeyAttr checks one key attribute's name and type.
func assertKeyAttr(t *testing.T, label string, got publicKeyAttribute, name, typ string) {
	t.Helper()
	if got.Name != name || got.Type != typ {
		t.Fatalf("%s key = %+v, want %s/%s", label, got, name, typ)
	}
}

// TestDDBEncodeHashOnlySchemaRoundTrip pins a hash-only (no range key)
// table: the decoded schema omits the range key.
func TestDDBEncodeHashOnlySchemaRoundTrip(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const table = "sessions"
	input := ddbPublicSchema{
		FormatVersion: 1,
		TableName:     table,
		PrimaryKey: publicKeySchema{
			HashKey: publicKeyAttribute{Name: "id", Type: "S"},
		},
		AttributeDefinitions: []publicAttributeDefinition{{Name: "id", Type: "S"}},
	}
	body, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	writeDDBSchema(t, in, EncodeSegment([]byte(table)), body)

	out := decodeDDBTree(t, encodeDDBTree(t, in))
	got := readDDBSchema(t, out, table)

	if got.PrimaryKey.HashKey.Name != "id" {
		t.Fatalf("hash key = %+v, want id", got.PrimaryKey.HashKey)
	}
	if got.PrimaryKey.RangeKey.Name != "" {
		t.Fatalf("range key = %+v, want empty", got.PrimaryKey.RangeKey)
	}
}

// TestDDBEncodeRejectsUnknownSchemaFormatVersion pins the format gate.
func TestDDBEncodeRejectsUnknownSchemaFormatVersion(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	writeDDBSchema(t, in, "tbl", []byte(`{"format_version":99,"table_name":"x"}`))
	b := newSnapshotBuilder(ddbEncTS)
	err := NewDynamoDBEncoder(in).Encode(b)
	if !errors.Is(err, ErrDDBEncodeInvalidSchema) {
		t.Fatalf("Encode err = %v, want ErrDDBEncodeInvalidSchema", err)
	}
}

// TestDDBEncodeRejectsNonRegularSchema pins the pre-open guard: a
// _schema.json that is a directory (cross-platform stand-in for a
// symlink/FIFO) is refused with ErrDDBEncodeNotRegular before any
// blocking open.
func TestDDBEncodeRejectsNonRegularSchema(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	// Place a directory where _schema.json should be a regular file.
	if err := os.MkdirAll(filepath.Join(in, "dynamodb", "tbl", "_schema.json"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	b := newSnapshotBuilder(ddbEncTS)
	err := NewDynamoDBEncoder(in).Encode(b)
	if !errors.Is(err, ErrDDBEncodeNotRegular) {
		t.Fatalf("Encode err = %v, want ErrDDBEncodeNotRegular", err)
	}
}

// TestDDBEncodeRejectsDuplicateAttributeDefinition pins that a
// _schema.json with two attribute definitions sharing a name fails
// closed: publicToSchema folds AttributeDefinitions into a map, so a
// duplicate would otherwise silently overwrite an entry and lose a type
// without any error (coderabbit Major on PR #833).
func TestDDBEncodeRejectsDuplicateAttributeDefinition(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	writeDDBSchema(t, in, "tbl", []byte(`{"format_version":1,"table_name":"x",`+
		`"primary_key":{"hash_key":{"name":"id","type":"S"}},`+
		`"attribute_definitions":[{"name":"id","type":"S"},{"name":"id","type":"N"}]}`))
	b := newSnapshotBuilder(ddbEncTS)
	err := NewDynamoDBEncoder(in).Encode(b)
	if !errors.Is(err, ErrDDBEncodeInvalidSchema) {
		t.Fatalf("Encode err = %v, want ErrDDBEncodeInvalidSchema", err)
	}
}

// TestDDBEncodeRejectsDuplicateGSIName pins the same fail-closed guard
// for GlobalSecondaryIndexes, which are also folded into a map.
func TestDDBEncodeRejectsDuplicateGSIName(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	writeDDBSchema(t, in, "tbl", []byte(`{"format_version":1,"table_name":"x",`+
		`"primary_key":{"hash_key":{"name":"id","type":"S"}},`+
		`"attribute_definitions":[{"name":"id","type":"S"},{"name":"region","type":"S"}],`+
		`"global_secondary_indexes":[`+
		`{"name":"by-region","key_schema":{"hash_key":{"name":"region","type":"S"}},"projection":{"type":"ALL"}},`+
		`{"name":"by-region","key_schema":{"hash_key":{"name":"id","type":"S"}},"projection":{"type":"ALL"}}]}`))
	b := newSnapshotBuilder(ddbEncTS)
	err := NewDynamoDBEncoder(in).Encode(b)
	if !errors.Is(err, ErrDDBEncodeInvalidSchema) {
		t.Fatalf("Encode err = %v, want ErrDDBEncodeInvalidSchema", err)
	}
}

// TestDDBEncodeRejectsEmptyHashKey pins that a schema with no primary
// hash key fails closed rather than propagating into the item encoder.
func TestDDBEncodeRejectsEmptyHashKey(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	writeDDBSchema(t, in, "tbl", []byte(`{"format_version":1,"table_name":"x","primary_key":{"hash_key":{"name":""}}}`))
	b := newSnapshotBuilder(ddbEncTS)
	err := NewDynamoDBEncoder(in).Encode(b)
	if !errors.Is(err, ErrDDBEncodeInvalidSchema) {
		t.Fatalf("Encode err = %v, want ErrDDBEncodeInvalidSchema", err)
	}
}

// TestDDBEncodeMissingDirIsNoop pins that an absent dynamodb/ dir is a
// no-op (no entries, no error).
func TestDDBEncodeMissingDirIsNoop(t *testing.T) {
	t.Parallel()
	b := newSnapshotBuilder(ddbEncTS)
	if err := NewDynamoDBEncoder(t.TempDir()).Encode(b); err != nil {
		t.Fatalf("Encode on empty tree: %v", err)
	}
	if b.Len() != 0 {
		t.Fatalf("got %d entries, want 0", b.Len())
	}
}
