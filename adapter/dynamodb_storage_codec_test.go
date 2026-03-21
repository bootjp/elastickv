package adapter

import (
	"testing"

	json "github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

func TestStoredDynamoItemCodec_RoundTripProto(t *testing.T) {
	t.Parallel()

	boolFalse := false
	nullTrue := true
	item := map[string]attributeValue{
		"pk":     newStringAttributeValue("tenant#1"),
		"sk":     {N: stringPtr("42")},
		"blob":   {B: []byte("payload")},
		"active": {BOOL: &boolFalse},
		"gone":   {NULL: &nullTrue},
		"tags":   {SS: []string{"a", "b"}},
		"scores": {NS: []string{"1", "2"}},
		"bins":   {BS: [][]byte{[]byte("x"), []byte("y")}},
		"list": {
			L: []attributeValue{
				newStringAttributeValue("nested"),
				{NULL: &nullTrue},
			},
		},
		"map": {
			M: map[string]attributeValue{
				"flag": {BOOL: &boolFalse},
			},
		},
	}

	body, err := encodeStoredDynamoItem(item)
	require.NoError(t, err)
	require.True(t, hasStoredDynamoPrefix(body, storedDynamoItemProtoPrefix))

	decoded, err := decodeStoredDynamoItem(body)
	require.NoError(t, err)
	require.Equal(t, item, decoded)
}

func TestStoredDynamoItemCodec_LegacyJSONFallback(t *testing.T) {
	t.Parallel()

	boolTrue := true
	legacy := map[string]attributeValue{
		"pk":     newStringAttributeValue("tenant#1"),
		"active": {BOOL: &boolTrue},
	}

	body, err := json.Marshal(legacy)
	require.NoError(t, err)

	decoded, err := decodeStoredDynamoItem(body)
	require.NoError(t, err)
	require.Equal(t, legacy, decoded)
}

func TestStoredDynamoItemCodec_NormalizesNullTrue(t *testing.T) {
	t.Parallel()

	falseVal := false
	item := map[string]attributeValue{
		"gone": {NULL: &falseVal},
	}

	body, err := encodeStoredDynamoItem(item)
	require.NoError(t, err)

	decoded, err := decodeStoredDynamoItem(body)
	require.NoError(t, err)
	require.NotNil(t, decoded["gone"].NULL)
	require.True(t, *decoded["gone"].NULL)
}

func TestStoredDynamoTableSchemaCodec_LegacyJSONFallback(t *testing.T) {
	t.Parallel()

	body, err := json.Marshal(map[string]any{
		"table_name": "threads",
		"attribute_definitions": map[string]string{
			"threadId":  "S",
			"createdAt": "S",
			"status":    "S",
		},
		"primary_key": map[string]any{
			"hash_key": "threadId",
		},
		"global_secondary_indexes": map[string]any{
			"status-index": map[string]any{
				"hash_key":  "status",
				"range_key": "createdAt",
			},
		},
		"generation": 7,
	})
	require.NoError(t, err)

	schema, err := decodeStoredDynamoTableSchema(body)
	require.NoError(t, err)
	require.Equal(t, "threads", schema.TableName)
	require.Equal(t, "status", schema.GlobalSecondaryIndexes["status-index"].KeySchema.HashKey)
	require.Equal(t, "createdAt", schema.GlobalSecondaryIndexes["status-index"].KeySchema.RangeKey)
	require.Equal(t, "ALL", schema.GlobalSecondaryIndexes["status-index"].Projection.ProjectionType)
}

func stringPtr(v string) *string {
	return &v
}
