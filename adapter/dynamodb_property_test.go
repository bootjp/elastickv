package adapter

import (
	"bytes"
	"encoding/json"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
)

type smallString string

type smallStringList []string

func (s smallString) Generate(r *rand.Rand, size int) reflect.Value {
	length := r.Intn(16)
	chars := make([]byte, length)
	for i := range chars {
		chars[i] = byte('a' + r.Intn(26))
	}
	return reflect.ValueOf(smallString(chars))
}

func (s smallStringList) Generate(r *rand.Rand, size int) reflect.Value {
	length := r.Intn(6)
	items := make([]string, 0, length)
	for i := 0; i < length; i++ {
		items = append(items, string(smallString("").Generate(r, size).Interface().(smallString)))
	}
	return reflect.ValueOf(smallStringList(items))
}

func TestDynamoDBTranscoderScalarProperty(t *testing.T) {
	transcoder := newDynamoDBTranscoder()

	property := func(key, value smallString) bool {
		input := putItemInput{
			TableName: "table",
			Item: map[string]attributeValue{
				"key":   {S: string(key)},
				"value": {S: string(value)},
			},
		}
		payload, err := json.Marshal(input)
		if err != nil {
			return false
		}
		group, err := transcoder.PutItemToRequest(payload)
		if err != nil || group == nil {
			return false
		}
		if group.IsTxn || len(group.Elems) != 1 {
			return false
		}
		elem := group.Elems[0]
		return elem.Op == kv.Put && bytes.Equal(elem.Key, []byte(key)) && bytes.Equal(elem.Value, []byte(value))
	}

	if err := quick.Check(property, nil); err != nil {
		t.Fatal(err)
	}
}

func TestDynamoDBTranscoderListProperty(t *testing.T) {
	transcoder := newDynamoDBTranscoder()

	property := func(key smallString, values smallStringList) bool {
		if len(values) == 0 {
			return true
		}
		listAttr := make([]attributeValue, 0, len(values))
		for _, value := range values {
			listAttr = append(listAttr, attributeValue{S: value})
		}
		input := putItemInput{
			TableName: "table",
			Item: map[string]attributeValue{
				"key":   {S: string(key)},
				"value": {L: listAttr},
			},
		}
		payload, err := json.Marshal(input)
		if err != nil {
			return false
		}
		group, err := transcoder.PutItemToRequest(payload)
		if err != nil || group == nil {
			return false
		}
		if !group.IsTxn || len(group.Elems) != len(values)+1 {
			return false
		}

		metaKey := store.ListMetaKey([]byte(key))
		itemValues := make(map[string]string, len(values))
		for i, value := range values {
			itemKey := store.ListItemKey([]byte(key), int64(i))
			itemValues[string(itemKey)] = value
		}

		var metaFound bool
		seenItems := 0
		for _, elem := range group.Elems {
			switch {
			case bytes.Equal(elem.Key, metaKey):
				if elem.Op != kv.Put {
					return false
				}
				meta, err := store.UnmarshalListMeta(elem.Value)
				if err != nil {
					return false
				}
				if meta.Head != 0 || meta.Tail != int64(len(values)) || meta.Len != int64(len(values)) {
					return false
				}
				metaFound = true
			case store.IsListItemKey(elem.Key):
				expected, ok := itemValues[string(elem.Key)]
				if !ok || elem.Op != kv.Put {
					return false
				}
				if !bytes.Equal(elem.Value, []byte(expected)) {
					return false
				}
				seenItems++
			default:
				return false
			}
		}

		return metaFound && seenItems == len(values)
	}

	if err := quick.Check(property, nil); err != nil {
		t.Fatal(err)
	}
}
