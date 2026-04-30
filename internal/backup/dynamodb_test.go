package backup

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	gproto "google.golang.org/protobuf/proto"
)

func encodeSchemaValue(t *testing.T, schema *pb.DynamoTableSchema) []byte {
	t.Helper()
	body, err := gproto.Marshal(schema)
	if err != nil {
		t.Fatalf("marshal schema: %v", err)
	}
	out := append([]byte{}, storedDDBSchemaMagic...)
	return append(out, body...)
}

func encodeItemValue(t *testing.T, item *pb.DynamoItem) []byte {
	t.Helper()
	body, err := gproto.Marshal(item)
	if err != nil {
		t.Fatalf("marshal item: %v", err)
	}
	out := append([]byte{}, storedDDBItemMagic...)
	return append(out, body...)
}

func sAttr(s string) *pb.DynamoAttributeValue {
	return &pb.DynamoAttributeValue{Value: &pb.DynamoAttributeValue_S{S: s}}
}

func nAttr(n string) *pb.DynamoAttributeValue {
	return &pb.DynamoAttributeValue{Value: &pb.DynamoAttributeValue_N{N: n}}
}

func bAttr(b []byte) *pb.DynamoAttributeValue {
	return &pb.DynamoAttributeValue{Value: &pb.DynamoAttributeValue_B{B: b}}
}

func boolAttr(b bool) *pb.DynamoAttributeValue {
	return &pb.DynamoAttributeValue{Value: &pb.DynamoAttributeValue_BoolValue{BoolValue: b}}
}

func newDDBEncoder(t *testing.T) (*DDBEncoder, string) {
	t.Helper()
	root := t.TempDir()
	return NewDDBEncoder(root), root
}

func readPublicSchema(t *testing.T, path string) ddbPublicSchema {
	t.Helper()
	body, err := os.ReadFile(path) //nolint:gosec // test path
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	var got ddbPublicSchema
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal schema: %v", err)
	}
	return got
}

func readItemMap(t *testing.T, path string) map[string]any {
	t.Helper()
	body, err := os.ReadFile(path) //nolint:gosec // test path
	if err != nil {
		t.Fatalf("read item: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal item: %v", err)
	}
	return got
}

func mustSubMap(t *testing.T, m map[string]any, key string) map[string]any {
	t.Helper()
	v, ok := m[key].(map[string]any)
	if !ok {
		t.Fatalf("field %q wrong shape: %v", key, m[key])
	}
	return v
}

func TestDDB_HashOnlyTableRoundTrip(t *testing.T) {
	t.Parallel()
	enc, root := newDDBEncoder(t)
	schema := &pb.DynamoTableSchema{
		TableName:            "sessions",
		PrimaryKey:           &pb.DynamoKeySchema{HashKey: "session_id"},
		AttributeDefinitions: map[string]string{"session_id": "S"},
		Generation:           1,
	}
	item := &pb.DynamoItem{Attributes: map[string]*pb.DynamoAttributeValue{
		"session_id": sAttr("sess-abc123"),
		"user_id":    sAttr("alice"),
		"flags":      boolAttr(true),
		"count":      nAttr("42"),
	}}
	if err := enc.HandleItem(EncodeDDBItemKey("sessions", 1, "sess-abc123", ""), encodeItemValue(t, item)); err != nil {
		t.Fatalf("HandleItem: %v", err)
	}
	if err := enc.HandleTableMeta(EncodeDDBTableMetaKey("sessions"), encodeSchemaValue(t, schema)); err != nil {
		t.Fatalf("HandleTableMeta: %v", err)
	}
	if err := enc.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	got := readPublicSchema(t, filepath.Join(root, "dynamodb", "sessions", "_schema.json"))
	if got.TableName != "sessions" {
		t.Fatalf("table_name = %q", got.TableName)
	}
	if got.PrimaryKey.HashKey.Name != "session_id" || got.PrimaryKey.HashKey.Type != "S" {
		t.Fatalf("primary_key = %+v", got.PrimaryKey)
	}
	if got.PrimaryKey.RangeKey.Name != "" {
		t.Fatalf("hash-only table must have empty range_key, got %+v", got.PrimaryKey.RangeKey)
	}

	asMap := readItemMap(t, filepath.Join(root, "dynamodb", "sessions", "items", "sess-abc123.json"))
	if mustSubMap(t, asMap, "session_id")["S"] != "sess-abc123" {
		t.Fatalf("session_id.S = %v", asMap["session_id"])
	}
	if mustSubMap(t, asMap, "flags")["BOOL"] != true {
		t.Fatalf("flags.BOOL = %v", asMap["flags"])
	}
}

func TestDDB_CompositeKeyTableRoundTrip(t *testing.T) {
	t.Parallel()
	enc, root := newDDBEncoder(t)
	schema := &pb.DynamoTableSchema{
		TableName: "orders",
		PrimaryKey: &pb.DynamoKeySchema{
			HashKey:  "customer_id",
			RangeKey: "order_ts",
		},
		AttributeDefinitions: map[string]string{
			"customer_id": "S",
			"order_ts":    "S",
		},
		Generation: 1,
	}
	item := &pb.DynamoItem{Attributes: map[string]*pb.DynamoAttributeValue{
		"customer_id": sAttr("customer-7421"),
		"order_ts":    sAttr("2026-04-29T12:00:00Z"),
		"total":       nAttr("129.50"),
	}}
	if err := enc.HandleItem(EncodeDDBItemKey("orders", 1, "customer-7421", "2026-04-29T12:00:00Z"), encodeItemValue(t, item)); err != nil {
		t.Fatal(err)
	}
	if err := enc.HandleTableMeta(EncodeDDBTableMetaKey("orders"), encodeSchemaValue(t, schema)); err != nil {
		t.Fatal(err)
	}
	if err := enc.Finalize(); err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(root, "dynamodb", "orders", "items", "customer-7421", "2026-04-29T12%3A00%3A00Z.json")
	if _, err := os.Stat(want); err != nil {
		t.Fatalf("expected %s, stat err=%v", want, err)
	}
}

func TestDDB_BinaryHashKeyRendersAsB64Prefix(t *testing.T) {
	t.Parallel()
	enc, root := newDDBEncoder(t)
	schema := &pb.DynamoTableSchema{
		TableName: "blobs",
		PrimaryKey: &pb.DynamoKeySchema{
			HashKey: "id",
		},
		AttributeDefinitions: map[string]string{"id": "B"},
		Generation:           1,
	}
	item := &pb.DynamoItem{Attributes: map[string]*pb.DynamoAttributeValue{
		"id":   bAttr([]byte{0x00, 0x01, 0x02}),
		"data": sAttr("v"),
	}}
	if err := enc.HandleItem(EncodeDDBItemKey("blobs", 1, "doesnt-matter", ""), encodeItemValue(t, item)); err != nil {
		t.Fatal(err)
	}
	if err := enc.HandleTableMeta(EncodeDDBTableMetaKey("blobs"), encodeSchemaValue(t, schema)); err != nil {
		t.Fatal(err)
	}
	if err := enc.Finalize(); err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(root, "dynamodb", "blobs", "items", "b64.AAEC.json")
	if _, err := os.Stat(want); err != nil {
		t.Fatalf("expected %s, stat err=%v", want, err)
	}
}

func TestDDB_OrphanItemsWithoutSchemaWarn(t *testing.T) {
	t.Parallel()
	enc, _ := newDDBEncoder(t)
	var events []string
	enc.WithWarnSink(func(event string, _ ...any) {
		events = append(events, event)
	})
	item := &pb.DynamoItem{Attributes: map[string]*pb.DynamoAttributeValue{
		"id": sAttr("orphan"),
	}}
	if err := enc.HandleItem(EncodeDDBItemKey("ghost", 1, "orphan", ""), encodeItemValue(t, item)); err != nil {
		t.Fatal(err)
	}
	if err := enc.Finalize(); err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0] != "ddb_orphan_items" {
		t.Fatalf("events = %v", events)
	}
}

func TestDDB_RejectsValueWithoutMagic(t *testing.T) {
	t.Parallel()
	t.Run("schema", func(t *testing.T) {
		enc, _ := newDDBEncoder(t)
		err := enc.HandleTableMeta(EncodeDDBTableMetaKey("t"), []byte("not-magic"))
		if !errors.Is(err, ErrDDBInvalidSchema) {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("item", func(t *testing.T) {
		enc, _ := newDDBEncoder(t)
		err := enc.HandleItem(EncodeDDBItemKey("t", 1, "h", ""), []byte("not-magic"))
		if !errors.Is(err, ErrDDBInvalidItem) {
			t.Fatalf("err=%v", err)
		}
	})
}

func TestDDB_RejectsItemMissingHashKeyAttribute(t *testing.T) {
	t.Parallel()
	enc, _ := newDDBEncoder(t)
	schema := &pb.DynamoTableSchema{
		TableName: "t", PrimaryKey: &pb.DynamoKeySchema{HashKey: "id"},
		AttributeDefinitions: map[string]string{"id": "S"},
		Generation:           1,
	}
	item := &pb.DynamoItem{Attributes: map[string]*pb.DynamoAttributeValue{
		// "id" is missing
		"other": sAttr("v"),
	}}
	if err := enc.HandleItem(EncodeDDBItemKey("t", 1, "x", ""), encodeItemValue(t, item)); err != nil {
		t.Fatal(err)
	}
	if err := enc.HandleTableMeta(EncodeDDBTableMetaKey("t"), encodeSchemaValue(t, schema)); err != nil {
		t.Fatal(err)
	}
	err := enc.Finalize()
	if !errors.Is(err, ErrDDBInvalidItem) {
		t.Fatalf("Finalize err=%v want ErrDDBInvalidItem", err)
	}
}

func TestDDB_GSIRowsIgnored(t *testing.T) {
	t.Parallel()
	enc, _ := newDDBEncoder(t)
	if err := enc.HandleGSIRow([]byte("!ddb|gsi|whatever"), []byte("opaque")); err != nil {
		t.Fatalf("HandleGSIRow should be a no-op, err=%v", err)
	}
}

func TestDDB_AllAttributeKindsRoundTripThroughJSON(t *testing.T) {
	t.Parallel()
	enc, root := newDDBEncoder(t)
	schema := &pb.DynamoTableSchema{
		TableName: "kitchensink", PrimaryKey: &pb.DynamoKeySchema{HashKey: "id"},
		AttributeDefinitions: map[string]string{"id": "S"},
		Generation:           1,
	}
	item := &pb.DynamoItem{Attributes: map[string]*pb.DynamoAttributeValue{
		"id":     sAttr("k"),
		"s":      sAttr("hi"),
		"n":      nAttr("1.5"),
		"b":      bAttr([]byte{0xff, 0x01}),
		"bool_t": boolAttr(true),
		"null_a": {Value: &pb.DynamoAttributeValue_NullValue{NullValue: true}},
		"ss":     {Value: &pb.DynamoAttributeValue_Ss{Ss: &pb.DynamoStringSet{Values: []string{"a", "b"}}}},
		"ns":     {Value: &pb.DynamoAttributeValue_Ns{Ns: &pb.DynamoNumberSet{Values: []string{"1", "2"}}}},
		"bs":     {Value: &pb.DynamoAttributeValue_Bs{Bs: &pb.DynamoBinarySet{Values: [][]byte{{0x01}, {0x02}}}}},
		"l":      {Value: &pb.DynamoAttributeValue_L{L: &pb.DynamoAttributeValueList{Values: []*pb.DynamoAttributeValue{sAttr("x"), nAttr("9")}}}},
		"m":      {Value: &pb.DynamoAttributeValue_M{M: &pb.DynamoAttributeValueMap{Values: map[string]*pb.DynamoAttributeValue{"k1": sAttr("v1")}}}},
	}}
	if err := enc.HandleItem(EncodeDDBItemKey("kitchensink", 1, "k", ""), encodeItemValue(t, item)); err != nil {
		t.Fatal(err)
	}
	if err := enc.HandleTableMeta(EncodeDDBTableMetaKey("kitchensink"), encodeSchemaValue(t, schema)); err != nil {
		t.Fatal(err)
	}
	if err := enc.Finalize(); err != nil {
		t.Fatal(err)
	}
	got := readItemMap(t, filepath.Join(root, "dynamodb", "kitchensink", "items", "k.json"))
	// Spot-check a few attributes; full per-attribute assertions live
	// in the dedicated attributeValueToPublic tests below.
	if mustSubMap(t, got, "s")["S"] != "hi" {
		t.Fatalf("s = %v", got["s"])
	}
	if mustSubMap(t, got, "bool_t")["BOOL"] != true {
		t.Fatalf("bool_t = %v", got["bool_t"])
	}
	lInner, ok := mustSubMap(t, got, "l")["L"].([]any)
	if !ok {
		t.Fatalf("l[\"L\"] wrong shape: %v", mustSubMap(t, got, "l")["L"])
	}
	if len(lInner) != 2 {
		t.Fatalf("l[\"L\"] len = %d want 2", len(lInner))
	}
}

func TestDDB_AttributeValueToPublic_EmptyOneofSurfacedAsNull(t *testing.T) {
	t.Parallel()
	got := attributeValueToPublic(&pb.DynamoAttributeValue{})
	if got["NULL"] != true {
		t.Fatalf("got %v want NULL=true", got)
	}
}

func TestDDB_BundleJSONLNotImplementedYet(t *testing.T) {
	t.Parallel()
	enc, _ := newDDBEncoder(t)
	enc.WithBundleJSONL(true)
	err := enc.Finalize()
	if err == nil {
		t.Fatalf("expected not-implemented error from Finalize on bundle mode")
	}
}

func TestDDB_StaleGenerationItemsExcludedAndWarned(t *testing.T) {
	t.Parallel()
	enc, root := newDDBEncoder(t)
	var events []string
	enc.WithWarnSink(func(e string, _ ...any) { events = append(events, e) })

	schema := &pb.DynamoTableSchema{
		TableName:            "t",
		PrimaryKey:           &pb.DynamoKeySchema{HashKey: "id"},
		AttributeDefinitions: map[string]string{"id": "S"},
		Generation:           5,
	}
	live := &pb.DynamoItem{Attributes: map[string]*pb.DynamoAttributeValue{
		"id": sAttr("alive"), "v": sAttr("active"),
	}}
	stale := &pb.DynamoItem{Attributes: map[string]*pb.DynamoAttributeValue{
		"id": sAttr("ghost"), "v": sAttr("from-prev-gen"),
	}}
	if err := enc.HandleItem(EncodeDDBItemKey("t", 5, "alive", ""), encodeItemValue(t, live)); err != nil {
		t.Fatal(err)
	}
	if err := enc.HandleItem(EncodeDDBItemKey("t", 4, "ghost", ""), encodeItemValue(t, stale)); err != nil {
		t.Fatal(err)
	}
	if err := enc.HandleTableMeta(EncodeDDBTableMetaKey("t"), encodeSchemaValue(t, schema)); err != nil {
		t.Fatal(err)
	}
	if err := enc.Finalize(); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(filepath.Join(root, "dynamodb", "t", "items", "alive.json")); err != nil {
		t.Fatalf("expected active-gen item: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "dynamodb", "t", "items", "ghost.json")); !os.IsNotExist(err) {
		t.Fatalf("stale-gen item must NOT be emitted, stat err=%v", err)
	}
	if len(events) != 1 || events[0] != "ddb_stale_generation_items" {
		t.Fatalf("events=%v want [ddb_stale_generation_items]", events)
	}
}

func TestDDB_EmptyStringSetSerializesAsEmptyArrayNotNull(t *testing.T) {
	t.Parallel()
	// Per Gemini #442 — a set attribute with no members must
	// serialize as `[]` rather than `null` so downstream tools
	// see a present-but-empty set, not a missing field.
	got := setAttributeValueToPublic(&pb.DynamoAttributeValue{
		Value: &pb.DynamoAttributeValue_Ss{Ss: &pb.DynamoStringSet{Values: nil}},
	})
	body, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != `{"SS":[]}` {
		t.Fatalf("got %s want {\"SS\":[]}", body)
	}
}

func TestDDB_ParseItemKeyExtractsGeneration(t *testing.T) {
	t.Parallel()
	enc, gen, err := parseDDBItemKey(EncodeDDBItemKey("orders", 42, "pk", "sk"))
	if err != nil {
		t.Fatal(err)
	}
	if gen != 42 {
		t.Fatalf("gen=%d want 42", gen)
	}
	want := "b3JkZXJz" // base64url("orders")
	if enc != want {
		t.Fatalf("enc=%q want %q", enc, want)
	}
}

func TestDDB_RejectsKeyWithMissingTableSegment(t *testing.T) {
	t.Parallel()
	enc, _ := newDDBEncoder(t)
	// Missing the table segment entirely.
	err := enc.HandleItem([]byte(DDBItemPrefix), []byte("ignored"))
	if !errors.Is(err, ErrDDBMalformedKey) {
		t.Fatalf("err=%v", err)
	}
}
