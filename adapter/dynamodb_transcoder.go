package adapter

import (
	"encoding/json"

	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

type dynamodbTranscoder struct{}

func newDynamoDBTranscoder() *dynamodbTranscoder { // create new transcoder
	return &dynamodbTranscoder{}
}

type attributeValue struct {
	S string `json:"S"`
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
	return &kv.OperationGroup[kv.OP]{
		IsTxn: false,
		Elems: []*kv.Elem[kv.OP]{
			{
				Op:    kv.Put,
				Key:   []byte(keyAttr.S),
				Value: []byte(valAttr.S),
			},
		},
	}, nil
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
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   []byte(keyAttr.S),
			Value: []byte(valAttr.S),
		})
	}

	return &kv.OperationGroup[kv.OP]{
		IsTxn: true,
		Elems: elems,
	}, nil
}
