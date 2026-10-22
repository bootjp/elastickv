package adapter

import (
	"bytes"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"

	logicalbackup "github.com/bootjp/elastickv/internal/backup"
	"github.com/stretchr/testify/require"
)

// DynamoDB mirrors pinned here (internal/backup copy -> live counterpart):
//
//   - encode_dynamodb.go publicToSchema + marshalStoredDDBSchema and the
//     generation counter -> buildCreateTableSchema + makeCreateTableRequest
//     (encodeStoredDynamoTableSchema, dynamoTableMetaKey,
//     dynamoTableGenerationKey).
//   - encode_dynamodb_items.go ddbItemKeyBytes, ddbOrderedPrimaryKeySegments,
//     encodeDDBOrderedKeySegment, marshalStoredDDBItem ->
//     itemKeyFromAttributes (dynamoItemKey, encodeDynamoKeySegment) and
//     encodeStoredDynamoItem.
//   - encode_dynamodb_numeric.go ddbNumericKeyBytes -> encodeNumericKeyBytes.
//   - encode_dynamodb_gsi.go ddbGSIRows -> gsiEntryKeysForItem (dynamoGSIKey).
//   - dynamodb.go canonicalDDBNumber (dump file names) -> canonicalNumberString.
//
// By design the dump carries no table generation, so the restore stamps
// generation 1 into the schema, the counter, and every item and GSI key
// (Option B in the logical-encoder design). The live side is therefore the
// first CreateTable on a fresh cluster, which also allocates generation 1.

// xcheckDDBGeneration is the generation of a table created on a fresh
// cluster and of every restored table.
const xcheckDDBGeneration uint64 = 1

// xcheckDDBTable is one table fixture: the CreateTable input, the items
// PutItem writes into it, and the number of GSI rows those items project
// (pinning that the fixture really contains sparse items).
type xcheckDDBTable struct {
	name        string
	input       createTableInput
	items       []map[string]attributeValue
	wantGSIRows int
}

func TestBackupCrossCheckDynamoDB(t *testing.T) {
	t.Parallel()
	for _, tc := range xcheckDDBTables() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			schema, live := xcheckDDBLiveRecords(t, tc)
			require.Equal(t, tc.wantGSIRows, xcheckCountPrefix(live, logicalbackup.DDBGSIPrefix), "live GSI rows")

			dumpDir, restored := xcheckRoundTrip(t, logicalbackup.AdapterSet{DynamoDB: true}, live)
			xcheckRequireIdentical(t, live, restored)
			xcheckRequireDDBItemFiles(t, dumpDir, schema, tc.items)
		})
	}
}

// xcheckDDBLiveRecords returns the records the live CreateTable and PutItem
// paths write for a table fixture, plus the schema PutItem resolves.
func xcheckDDBLiveRecords(t *testing.T, tc xcheckDDBTable) (*dynamoTableSchema, []xcheckPair) {
	t.Helper()
	base, err := buildCreateTableSchema(tc.input)
	require.NoError(t, err)
	create, err := makeCreateTableRequest(base, xcheckDDBGeneration)
	require.NoError(t, err)
	live := xcheckPutPairs(t, create.Elems)

	// PutItem resolves the schema from its stored bytes, so build item keys
	// from the decoded meta record rather than from the in-memory input.
	metaValue := xcheckValueOf(t, live, dynamoTableMetaKey(tc.input.TableName))
	schema, err := decodeStoredDynamoTableSchema(metaValue)
	require.NoError(t, err)
	for _, item := range tc.items {
		itemKey, err := schema.itemKeyFromAttributes(item)
		require.NoError(t, err)
		put, _, err := buildItemWriteRequestWithSource(schema, itemKey, item, nil)
		require.NoError(t, err)
		live = append(live, xcheckPutPairs(t, put.Elems)...)
	}
	return schema, live
}

// xcheckRequireDDBItemFiles requires the dump to hold exactly one file per
// item, named by the live key-equality rules. Two spellings of one number
// are one DynamoDB key, so the decoder names number key segments by their
// canonical form, which must be the live canonicalNumberString.
func xcheckRequireDDBItemFiles(t *testing.T, dumpDir string, schema *dynamoTableSchema, items []map[string]attributeValue) {
	t.Helper()
	itemsDir := filepath.Join(dumpDir, "dynamodb", logicalbackup.EncodeSegment([]byte(schema.TableName)), "items")
	files := xcheckListFiles(t, itemsDir)
	require.Len(t, files, len(items), "item files in the dump")
	for _, item := range items {
		rel := xcheckDDBDumpSegment(item[schema.PrimaryKey.HashKey])
		if schema.PrimaryKey.RangeKey != "" {
			rel = filepath.Join(rel, xcheckDDBDumpSegment(item[schema.PrimaryKey.RangeKey]))
		}
		require.Contains(t, files, filepath.ToSlash(rel)+".json", "dump file for item key")
	}
}

// xcheckDDBDumpSegment is the dump file-name segment of a primary-key
// attribute: the canonical number for N, the binary segment for B, and the
// string itself for S.
func xcheckDDBDumpSegment(av attributeValue) string {
	switch {
	case av.N != nil:
		return logicalbackup.EncodeSegment([]byte(canonicalNumberString(*av.N)))
	case av.B != nil:
		return logicalbackup.EncodeBinarySegment(av.B)
	default:
		return logicalbackup.EncodeSegment([]byte(av.stringValue()))
	}
}

func xcheckDDBTables() []xcheckDDBTable {
	return []xcheckDDBTable{
		xcheckDDBOrdersTable(),
		xcheckDDBNumbersTable(),
		xcheckDDBNumberAliasesTable(),
		xcheckDDBBinaryTable(),
		xcheckDDBStringTable(),
	}
}

// xcheckDDBOrdersTable is a string-hash / number-range table with a
// hash+range GSI, a hash-only GSI, and a binary-hash GSI, and with sparse
// items that miss an index hash or an index range attribute.
func xcheckDDBOrdersTable() xcheckDDBTable {
	return xcheckDDBTable{
		name: "string hash, number range, three GSIs",
		input: createTableInput{
			TableName: "xcheck.orders_v1-A",
			AttributeDefinitions: xcheckDDBAttrDefs(
				"customer", "S", "seq", "N", "status", "S", "amount", "N", "region", "S", "tag", "B"),
			KeySchema: xcheckDDBKeySchema("customer", "seq"),
			GlobalSecondaryIndexes: []createTableGSI{
				{
					IndexName:  "by-status-amount",
					KeySchema:  xcheckDDBKeySchema("status", "amount"),
					Projection: createTableProjection{ProjectionType: "ALL"},
				},
				{
					IndexName:  "by-region",
					KeySchema:  xcheckDDBKeySchema("region", ""),
					Projection: createTableProjection{ProjectionType: "KEYS_ONLY"},
				},
				{
					IndexName:  "by-tag",
					KeySchema:  xcheckDDBKeySchema("tag", "customer"),
					Projection: createTableProjection{ProjectionType: "INCLUDE", NonKeyAttributes: []string{"note", "status"}},
				},
			},
		},
		items: []map[string]attributeValue{
			xcheckDDBRichItem(),
			// Sparse: no status (no by-status-amount row), no tag (no by-tag row).
			{"customer": xcheckS("alice"), "seq": xcheckN("-42"), "region": xcheckS("us-east-1")},
			// Sparse: index hash present but index range missing, and no region.
			{"customer": xcheckS("alice"), "seq": xcheckN("0"), "status": xcheckS("CLOSED"), "tag": xcheckB([]byte{0x01})},
			{
				"customer": xcheckS("bob\x00nul"), "seq": xcheckN("1e2"), "status": xcheckS("OPEN"),
				"amount": xcheckN("100"), "region": xcheckS("ap-northeast-1"), "tag": xcheckB([]byte{0x00}),
			},
			{
				"customer": xcheckS("bob\x00nul"), "seq": xcheckN("3.14159"), "status": xcheckS("OPEN"),
				"amount": xcheckN("+5"), "region": xcheckS("ap-northeast-1"),
			},
			{"customer": xcheckS("ünïcødé/|key"), "seq": xcheckN("-0.0015"), "status": xcheckS("OPEN"), "amount": xcheckN("05")},
			{"customer": xcheckS("carol"), "seq": xcheckN("123456789012345678901234567890"), "region": xcheckS("eu-west-1")},
			{"customer": xcheckS("carol"), "seq": xcheckN("1e300")},
			{"customer": xcheckS("carol"), "seq": xcheckN("-1e-130"), "status": xcheckS("OPEN"), "amount": xcheckN("1.50")},
		},
		wantGSIRows: 13,
	}
}

// xcheckDDBRichItem carries every index key and one attribute of every
// DynamoDB type, so the stored item value covers the whole proto codec.
func xcheckDDBRichItem() map[string]attributeValue {
	return map[string]attributeValue{
		"customer":   xcheckS("alice"),
		"seq":        xcheckN("1"),
		"status":     xcheckS("OPEN"),
		"amount":     xcheckN("-1.5E-3"),
		"region":     xcheckS("eu-west-1"),
		"tag":        xcheckB([]byte{0x00, 0xFF, 0x10}),
		"note":       xcheckS("<first> & \"quoted\"  "),
		"active":     xcheckBool(true),
		"archived":   xcheckBool(false),
		"deleted_at": xcheckNull(),
		"labels":     {SS: []string{"b", "a", "c"}},
		"scores":     {NS: []string{"+5", "05", "1.50", "-0"}},
		"blobs":      {BS: [][]byte{{0x00}, {0xFF, 0xFE}}},
		"history": {L: []attributeValue{
			xcheckS("x"),
			xcheckN("1e2"),
			{M: map[string]attributeValue{"k": xcheckB([]byte{0x7F})}},
		}},
		"profile": {M: map[string]attributeValue{
			"name": xcheckS("Alice"),
			"tags": {L: []attributeValue{xcheckBool(true), xcheckNull()}},
		}},
	}
}

// xcheckDDBNumbersTable is a number-hash table covering negative, zero,
// fractional, exponent, huge, and tiny keys, with a number-keyed GSI whose
// keys use non-canonical spellings.
func xcheckDDBNumbersTable() xcheckDDBTable {
	return xcheckDDBTable{
		name: "number hash with a number GSI",
		input: createTableInput{
			TableName:            "xcheck_numbers",
			AttributeDefinitions: xcheckDDBAttrDefs("n", "N", "bucket", "N"),
			KeySchema:            xcheckDDBKeySchema("n", ""),
			GlobalSecondaryIndexes: []createTableGSI{{
				IndexName:  "by-bucket",
				KeySchema:  xcheckDDBKeySchema("bucket", "n"),
				Projection: createTableProjection{ProjectionType: "ALL"},
			}},
		},
		items: []map[string]attributeValue{
			{"n": xcheckN("-1e-130"), "bucket": xcheckN("-3")},
			{"n": xcheckN("-42"), "bucket": xcheckN("1")},
			{"n": xcheckN("-1.5E-3"), "bucket": xcheckN("01")},
			{"n": xcheckN("0"), "bucket": xcheckN("1.0")},
			{"n": xcheckN("0.25")},
			{"n": xcheckN("7"), "bucket": xcheckN("1e0")},
			{"n": xcheckN("1e2")},
			{"n": xcheckN("123456789012345678901234567890"), "bucket": xcheckN("-3.0")},
			{"n": xcheckN("1e300")},
			{"n": xcheckN(" 8 ")},
			{"n": xcheckN("+0.5e1")},
		},
		wantGSIRows: 6,
	}
}

// xcheckDDBNumberAliasesTable puts several spellings of one number under
// distinct range keys. Every spelling of a value encodes to the same ordered
// hash segment and lands in the same canonical dump directory.
func xcheckDDBNumberAliasesTable() xcheckDDBTable {
	aliases := []struct{ n, spelling string }{
		{"100", "plain"}, {"1e2", "exponent"}, {"1.00E+2", "mixed-exponent"}, {" 100 ", "padded"},
		{"5", "five"}, {"+5", "plus-sign"}, {"05", "leading-zero"}, {"5.0", "trailing-zero"},
		{"5.", "trailing-dot"}, {"0.5e1", "fraction-exponent"},
		{"1.5", "fraction"}, {"1.50", "fraction-trailing-zero"}, {".15e1", "no-integer-part"},
		{"-1.5E-3", "negative-exponent"}, {"-0.0015", "negative-fraction"},
		{"0", "zero"}, {"-0", "negative-zero"}, {"0.000", "zero-fraction"}, {"0e5", "zero-exponent"},
	}
	items := make([]map[string]attributeValue, 0, len(aliases))
	for _, a := range aliases {
		items = append(items, map[string]attributeValue{"n": xcheckN(a.n), "spelling": xcheckS(a.spelling)})
	}
	return xcheckDDBTable{
		name: "number hash aliases, string range",
		input: createTableInput{
			TableName:            "xcheck_number_aliases",
			AttributeDefinitions: xcheckDDBAttrDefs("n", "N", "spelling", "S"),
			KeySchema:            xcheckDDBKeySchema("n", "spelling"),
		},
		items: items,
	}
}

// xcheckDDBBinaryTable is a binary-hash / binary-range table whose keys hold
// the 0x00 escape byte, with a number-hash / binary-range GSI.
func xcheckDDBBinaryTable() xcheckDDBTable {
	return xcheckDDBTable{
		name: "binary hash, binary range, number GSI",
		input: createTableInput{
			TableName:            "xcheck_blobs",
			AttributeDefinitions: xcheckDDBAttrDefs("pk", "B", "sk", "B", "score", "N"),
			KeySchema:            xcheckDDBKeySchema("pk", "sk"),
			GlobalSecondaryIndexes: []createTableGSI{{
				IndexName:  "by-score",
				KeySchema:  xcheckDDBKeySchema("score", "sk"),
				Projection: createTableProjection{ProjectionType: "KEYS_ONLY"},
			}},
		},
		items: []map[string]attributeValue{
			{"pk": xcheckB([]byte{0x00}), "sk": xcheckB([]byte{0x00, 0x00}), "score": xcheckN("-1")},
			{"pk": xcheckB([]byte{0x00}), "sk": xcheckB([]byte{0xFF}), "score": xcheckN("0")},
			{"pk": xcheckB([]byte{0x00, 0xFF, 0x00}), "sk": xcheckB([]byte{0x01}), "score": xcheckN("2.5e-1")},
			{"pk": xcheckB([]byte("plain")), "sk": xcheckB([]byte{0x00, 0x01})},
			{"pk": xcheckB([]byte{0xFF, 0xFF}), "sk": xcheckB([]byte{0xFE}), "score": xcheckN("1e2"), "payload": xcheckB([]byte{0xDE, 0xAD})},
		},
		wantGSIRows: 4,
	}
}

// xcheckDDBStringTable is a string-hash table without GSIs whose keys need
// escaping in the ordered encoding and in dump file names, including one
// long enough to take the SHA-fallback file name.
func xcheckDDBStringTable() xcheckDDBTable {
	ids := []string{
		"simple", "with space", "slash/and|pipe", "nul\x00byte", "ünïcødé ✓", "emoji 🚀",
		"percent%2Fencoded", "dot.dot", "a", strings.Repeat("k", 300),
	}
	items := make([]map[string]attributeValue, 0, len(ids))
	for _, id := range ids {
		items = append(items, map[string]attributeValue{"id": xcheckS(id), "v": xcheckS("value of " + id)})
	}
	return xcheckDDBTable{
		name: "string hash without GSIs",
		input: createTableInput{
			TableName:            "xcheck_strings",
			AttributeDefinitions: xcheckDDBAttrDefs("id", "S"),
			KeySchema:            xcheckDDBKeySchema("id", ""),
		},
		items: items,
	}
}

// xcheckDDBAttrDefs builds attribute definitions from name/type pairs.
func xcheckDDBAttrDefs(nameTypes ...string) []createTableAttributeDefinition {
	out := make([]createTableAttributeDefinition, 0, len(nameTypes)/2)
	for i := 0; i+1 < len(nameTypes); i += 2 {
		out = append(out, createTableAttributeDefinition{AttributeName: nameTypes[i], AttributeType: nameTypes[i+1]})
	}
	return out
}

// xcheckDDBKeySchema builds a HASH (+ optional RANGE) key schema.
func xcheckDDBKeySchema(hash, rangeKey string) []createTableKeySchemaElement {
	out := []createTableKeySchemaElement{{AttributeName: hash, KeyType: "HASH"}}
	if rangeKey != "" {
		out = append(out, createTableKeySchemaElement{AttributeName: rangeKey, KeyType: "RANGE"})
	}
	return out
}

func xcheckS(v string) attributeValue { return attributeValue{S: &v} }

func xcheckN(v string) attributeValue { return attributeValue{N: &v} }

func xcheckB(v []byte) attributeValue { return attributeValue{B: v} }

func xcheckBool(v bool) attributeValue { return attributeValue{BOOL: &v} }

func xcheckNull() attributeValue {
	null := true
	return attributeValue{NULL: &null}
}

// xcheckValueOf returns the value the live records hold for key.
func xcheckValueOf(t *testing.T, live []xcheckPair, key []byte) []byte {
	t.Helper()
	for _, p := range live {
		if bytes.Equal(p.key, key) {
			return p.value
		}
	}
	require.FailNow(t, "live record missing", "key %q", key)
	return nil
}

// xcheckCountPrefix counts the live records whose key starts with prefix.
func xcheckCountPrefix(live []xcheckPair, prefix string) int {
	n := 0
	for _, p := range live {
		if bytes.HasPrefix(p.key, []byte(prefix)) {
			n++
		}
	}
	return n
}

// xcheckListFiles lists the regular files under root as slash-separated
// paths relative to root.
func xcheckListFiles(t *testing.T, root string) []string {
	t.Helper()
	var files []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		files = append(files, filepath.ToSlash(rel))
		return nil
	})
	require.NoError(t, err)
	return files
}
