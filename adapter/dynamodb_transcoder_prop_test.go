package adapter

import (
	"encoding/json"
	"testing"

	"pgregory.net/rapid"
)

func TestDynamoDBTranscoder_Property_PutItem(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		tableName := rapid.String().Draw(t, "tableName")
		key := rapid.String().Filter(func(s string) bool { return s != "" }).Draw(t, "key")
		value := rapid.String().Filter(func(s string) bool { return s != "" }).Draw(t, "value")
		tr := newDynamoDBTranscoder()

		input := putItemInput{
			TableName: tableName,
			Item: map[string]attributeValue{
				"key":   {S: key},
				"value": {S: value},
			},
		}

		b, _ := json.Marshal(input)
		got, err := tr.PutItemToRequest(b)
		if err != nil {
			t.Fatalf("PutItemToRequest failed: %v", err)
		}

		if len(got.Elems) != 1 || string(got.Elems[0].Key) != key || string(got.Elems[0].Value) != value {
			t.Errorf("PutItemToRequest mapping incorrect")
		}
	})
}

func TestDynamoDBTranscoder_Property_TransactWrite(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		tr := newDynamoDBTranscoder()
		k1 := rapid.String().Filter(func(s string) bool { return s != "" }).Draw(t, "k1")
		v1 := rapid.String().Filter(func(s string) bool { return s != "" }).Draw(t, "v1")
		k2 := rapid.String().Filter(func(s string) bool { return s != "" }).Draw(t, "k2")
		v2 := rapid.String().Filter(func(s string) bool { return s != "" }).Draw(t, "v2")

		txInput := transactWriteItemsInput{
			TransactItems: []transactWriteItem{
				{Put: &putItemInput{Item: map[string]attributeValue{"key": {S: k1}, "value": {S: v1}}}},
				{Put: &putItemInput{Item: map[string]attributeValue{"key": {S: k2}, "value": {S: v2}}}},
			},
		}

		bTx, _ := json.Marshal(txInput)
		gotTx, err := tr.TransactWriteItemsToRequest(bTx)
		if err != nil {
			t.Fatalf("TransactWriteItemsToRequest failed: %v", err)
		}

		if !gotTx.IsTxn || len(gotTx.Elems) != 2 {
			t.Errorf("TransactWriteItemsToRequest should be a transaction with 2 elements")
		}
		if string(gotTx.Elems[0].Key) != k1 || string(gotTx.Elems[1].Key) != k2 {
			t.Errorf("TransactWriteItemsToRequest elements mapping incorrect")
		}
	})
}
