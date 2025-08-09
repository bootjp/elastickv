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
