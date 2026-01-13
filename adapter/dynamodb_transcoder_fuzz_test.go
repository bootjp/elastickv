package adapter

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func FuzzDynamoDBTranscoder_PutItem(f *testing.F) {
	f.Add("test-table", "my-key", "my-value")
	f.Fuzz(func(t *testing.T, tableName, key, value string) {
		tr := newDynamoDBTranscoder()

		input := putItemInput{
			TableName: tableName,
			Item: map[string]attributeValue{
				"key":   {S: key},
				"value": {S: value},
			},
		}

		b, err := json.Marshal(input)
		require.NoError(t, err)

		got, err := tr.PutItemToRequest(b)
		if key == "" && value == "" {
			// valueAttrToOps returns error if both S and L are empty/absent
			require.Error(t, err)
			return
		}
		require.NoError(t, err)
		require.NotNil(t, got)
		require.False(t, got.IsTxn)
		require.Len(t, got.Elems, 1)
		require.Equal(t, key, string(got.Elems[0].Key))
		require.Equal(t, value, string(got.Elems[0].Value))
	})
}

func FuzzDynamoDBTranscoder_TransactWriteItems(f *testing.F) {
	f.Add("k1", "v1", "k2", "v2")
	f.Fuzz(func(t *testing.T, k1, v1, k2, v2 string) {
		tr := newDynamoDBTranscoder()

		input := transactWriteItemsInput{
			TransactItems: []transactWriteItem{
				{
					Put: &putItemInput{
						Item: map[string]attributeValue{
							"key":   {S: k1},
							"value": {S: v1},
						},
					},
				},
				{
					Put: &putItemInput{
						Item: map[string]attributeValue{
							"key":   {S: k2},
							"value": {S: v2},
						},
					},
				},
			},
		}

		b, err := json.Marshal(input)
		require.NoError(t, err)

		got, err := tr.TransactWriteItemsToRequest(b)
		if (k1 == "" && v1 == "") || (k2 == "" && v2 == "") {
			require.Error(t, err)
			return
		}
		require.NoError(t, err)
		require.NotNil(t, got)
		require.True(t, got.IsTxn)
		require.Len(t, got.Elems, 2)
	})
}
