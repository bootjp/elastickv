package adapter

import (
	"encoding/json"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

type dynamodbTranscoder struct{}

func newDynamoDBTranscoder() *dynamodbTranscoder { // create new transcoder
	return &dynamodbTranscoder{}
}

type attributeValue struct {
	S string           `json:"S,omitempty"`
	L []attributeValue `json:"L,omitempty"`
}

type putItemInput struct {
	TableName string                    `json:"TableName"`
	Item      map[string]attributeValue `json:"Item"`
}

type transactWriteItemsInput struct {
	TransactItems []transactWriteItem `json:"TransactItems"`
}

type transactWriteItem struct {
	Put *putItemInput `json:"Put,omitempty"`
}

func (t *dynamodbTranscoder) PutItemToRequest(b []byte) (*kv.OperationGroup[kv.OP], error) {
	var in putItemInput
	if err := json.Unmarshal(b, &in); err != nil {
		return nil, errors.WithStack(err)
	}
	keyAttr, ok := in.Item["key"]
	if !ok {
		return nil, errors.New("missing key attribute")
	}
	valAttr, ok := in.Item["value"]
	if !ok {
		return nil, errors.New("missing value attribute")
	}

	return t.valueAttrToOps([]byte(keyAttr.S), valAttr)
}

func (t *dynamodbTranscoder) TransactWriteItemsToRequest(b []byte) (*kv.OperationGroup[kv.OP], error) {
	var in transactWriteItemsInput
	if err := json.Unmarshal(b, &in); err != nil {
		return nil, errors.WithStack(err)
	}

	var elems []*kv.Elem[kv.OP]
	for _, item := range in.TransactItems {
		if item.Put == nil {
			return nil, errors.New("unsupported transact item")
		}
		keyAttr, ok := item.Put.Item["key"]
		if !ok {
			return nil, errors.New("missing key attribute")
		}
		valAttr, ok := item.Put.Item["value"]
		if !ok {
			return nil, errors.New("missing value attribute")
		}

		ops, err := t.valueAttrToOps([]byte(keyAttr.S), valAttr)
		if err != nil {
			return nil, err
		}
		elems = append(elems, ops.Elems...)
	}

	return &kv.OperationGroup[kv.OP]{
		IsTxn: true,
		Elems: elems,
	}, nil
}

func (t *dynamodbTranscoder) valueAttrToOps(key []byte, val attributeValue) (*kv.OperationGroup[kv.OP], error) {
	// List handling: only lists of scalar strings are supported.
	if len(val.L) > 0 {
		var elems []*kv.Elem[kv.OP]
		for i, item := range val.L {
			if item.S == "" {
				return nil, errors.New("only string list items are supported")
			}
			elems = append(elems, &kv.Elem[kv.OP]{
				Op:    kv.Put,
				Key:   store.ListItemKey(key, int64(i)),
				Value: []byte(item.S),
			})
		}
		meta := store.ListMeta{
			Head: 0,
			Tail: int64(len(val.L)),
			Len:  int64(len(val.L)),
		}
		b, err := json.Marshal(meta)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: store.ListMetaKey(key), Value: b})

		return &kv.OperationGroup[kv.OP]{IsTxn: true, Elems: elems}, nil
	}

	// Default: simple string
	if val.S == "" {
		return nil, errors.New("unsupported attribute type (only S or L of S)")
	}
	return &kv.OperationGroup[kv.OP]{
		IsTxn: false,
		Elems: []*kv.Elem[kv.OP]{{
			Op:    kv.Put,
			Key:   key,
			Value: []byte(val.S),
		}},
	}, nil
}
