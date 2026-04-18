package adapter

import (
	"testing"

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

func stringPtr(v string) *string {
	return &v
}
