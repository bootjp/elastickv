package adapter

import (
	"encoding/json"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

var nonEmptyString = rapid.String().Filter(func(s string) bool { return s != "" })

func TestDynamoDBTranscoder_Property_PutItem(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		tableName := rapid.String().Draw(t, "tableName")
		key := nonEmptyString.Draw(t, "key")
		value := nonEmptyString.Draw(t, "value")
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
		require.NoError(t, err)
		require.Len(t, got.Elems, 1)
		require.Equal(t, kv.Put, got.Elems[0].Op)
		require.Equal(t, key, string(got.Elems[0].Key))
		require.Equal(t, value, string(got.Elems[0].Value))
	})
}

func TestDynamoDBTranscoder_Property_TransactWrite(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		tr := newDynamoDBTranscoder()
		k1 := nonEmptyString.Draw(t, "k1")
		v1 := nonEmptyString.Draw(t, "v1")
		k2 := nonEmptyString.Draw(t, "k2")
		v2 := nonEmptyString.Draw(t, "v2")

		txInput := transactWriteItemsInput{
			TransactItems: []transactWriteItem{
				{Put: &putItemInput{Item: map[string]attributeValue{"key": {S: k1}, "value": {S: v1}}}},
				{Put: &putItemInput{Item: map[string]attributeValue{"key": {S: k2}, "value": {S: v2}}}},
			},
		}

		bTx, err := json.Marshal(txInput)
		require.NoError(t, err)

		gotTx, err := tr.TransactWriteItemsToRequest(bTx)
		require.NoError(t, err)
		require.True(t, gotTx.IsTxn)
		require.Equal(t, uint64(0), gotTx.StartTS)
		require.Len(t, gotTx.Elems, 2)

		require.Equal(t, kv.Put, gotTx.Elems[0].Op)
		require.Equal(t, k1, string(gotTx.Elems[0].Key))
		require.Equal(t, v1, string(gotTx.Elems[0].Value))
		require.Equal(t, kv.Put, gotTx.Elems[1].Op)
		require.Equal(t, k2, string(gotTx.Elems[1].Key))
		require.Equal(t, v2, string(gotTx.Elems[1].Value))
	})
}

func TestAttributeValue_UnmarshalJSON_RejectsEmptyObject(t *testing.T) {
	var a attributeValue
	err := json.Unmarshal([]byte(`{}`), &a)
	require.Error(t, err)
}

func TestAttributeValue_HasStringType_IsExplicit(t *testing.T) {
	var zero attributeValue
	require.False(t, zero.hasStringType())

	var parsed attributeValue
	err := json.Unmarshal([]byte(`{"S":""}`), &parsed)
	require.NoError(t, err)
	require.True(t, parsed.hasStringType())
}
