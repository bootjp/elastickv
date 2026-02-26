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
	S    string                    `json:"S,omitempty"`
	N    string                    `json:"N,omitempty"`
	BOOL *bool                     `json:"BOOL,omitempty"`
	NULL *bool                     `json:"NULL,omitempty"`
	L    []attributeValue          `json:"L,omitempty"`
	M    map[string]attributeValue `json:"M,omitempty"`

	hasS bool `json:"-"`
	hasN bool `json:"-"`
	hasL bool `json:"-"`
	hasM bool `json:"-"`
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
	if !keyAttr.hasStringType() {
		return nil, errors.New("key attribute must be S")
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
		if !keyAttr.hasStringType() {
			return nil, errors.New("key attribute must be S")
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
	if val.hasListType() {
		return t.listAttributeToOps(key, val.L)
	}

	// Default: simple string (allow empty string).
	if !val.isSupportedScalarString() {
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

func (a attributeValue) hasStringType() bool {
	return (a.hasS || a.S != "") || !a.hasAnyExplicitNonStringType()
}

func (a attributeValue) hasNumberType() bool {
	return a.hasN || a.N != ""
}

func (a attributeValue) hasListType() bool {
	return a.hasL || len(a.L) > 0
}

func (a attributeValue) hasMapType() bool {
	return a.hasM || len(a.M) > 0
}

func (a attributeValue) hasAnyExplicitNonStringType() bool {
	return a.hasN || a.N != "" || a.BOOL != nil || a.NULL != nil || a.hasL || len(a.L) > 0 || a.hasM || len(a.M) > 0
}

func (a attributeValue) isSupportedScalarString() bool {
	return a.hasStringType() && !a.hasAnyExplicitNonStringType()
}

func (t *dynamodbTranscoder) listAttributeToOps(key []byte, list []attributeValue) (*kv.OperationGroup[kv.OP], error) {
	elems := make([]*kv.Elem[kv.OP], 0, len(list)+1)
	for i, item := range list {
		if !item.isSupportedScalarString() {
			return nil, errors.New("nested lists are not supported")
		}
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.ListItemKey(key, int64(i)),
			Value: []byte(item.S),
		})
	}
	meta := store.ListMeta{Head: 0, Tail: int64(len(list)), Len: int64(len(list))}
	b, err := store.MarshalListMeta(meta)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: store.ListMetaKey(key), Value: b})
	return &kv.OperationGroup[kv.OP]{IsTxn: true, Elems: elems}, nil
}

func (a attributeValue) MarshalJSON() ([]byte, error) {
	hasS := a.hasStringType()
	hasN := a.hasNumberType()
	hasBOOL := a.BOOL != nil
	hasNULL := a.NULL != nil
	hasL := a.hasListType()
	hasM := a.hasMapType()
	count := 0
	for _, present := range []bool{hasS, hasN, hasBOOL, hasNULL, hasL, hasM} {
		if present {
			count++
		}
	}
	if count != 1 {
		return nil, errors.New("invalid attribute value")
	}

	switch {
	case hasS:
		return marshalAttributeValue(map[string]string{"S": a.S})
	case hasN:
		return marshalAttributeValue(map[string]string{"N": a.N})
	case hasBOOL:
		return marshalAttributeValue(map[string]bool{"BOOL": *a.BOOL})
	case hasNULL:
		return marshalAttributeValue(map[string]bool{"NULL": *a.NULL})
	case hasL:
		return marshalAttributeValue(map[string][]attributeValue{"L": a.L})
	default:
		return marshalAttributeValue(map[string]map[string]attributeValue{"M": a.M})
	}
}

func marshalAttributeValue(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return b, nil
}

func (a *attributeValue) UnmarshalJSON(b []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return errors.WithStack(err)
	}

	*a = attributeValue{}
	if len(raw) == 0 {
		a.S = ""
		a.hasS = true
		return nil
	}
	if len(raw) != 1 {
		return errors.New("invalid attribute value")
	}

	for k, v := range raw {
		return a.unmarshalSingleAttributeValue(k, v)
	}
	return errors.New("invalid attribute value")
}

func (a *attributeValue) unmarshalSingleAttributeValue(kind string, raw json.RawMessage) error {
	switch kind {
	case "S":
		return a.unmarshalString(raw)
	case "N":
		return a.unmarshalNumber(raw)
	case "BOOL":
		return a.unmarshalBool(raw)
	case "NULL":
		return a.unmarshalNull(raw)
	case "L":
		return a.unmarshalList(raw)
	case "M":
		return a.unmarshalMap(raw)
	default:
		return errors.New("unsupported attribute value type")
	}
}

func (a *attributeValue) unmarshalString(raw json.RawMessage) error {
	if err := json.Unmarshal(raw, &a.S); err != nil {
		return errors.WithStack(err)
	}
	a.hasS = true
	return nil
}

func (a *attributeValue) unmarshalNumber(raw json.RawMessage) error {
	if err := json.Unmarshal(raw, &a.N); err != nil {
		return errors.WithStack(err)
	}
	a.hasN = true
	return nil
}

func (a *attributeValue) unmarshalBool(raw json.RawMessage) error {
	var b bool
	if err := json.Unmarshal(raw, &b); err != nil {
		return errors.WithStack(err)
	}
	a.BOOL = &b
	return nil
}

func (a *attributeValue) unmarshalNull(raw json.RawMessage) error {
	var n bool
	if err := json.Unmarshal(raw, &n); err != nil {
		return errors.WithStack(err)
	}
	a.NULL = &n
	return nil
}

func (a *attributeValue) unmarshalList(raw json.RawMessage) error {
	if err := json.Unmarshal(raw, &a.L); err != nil {
		return errors.WithStack(err)
	}
	a.hasL = true
	return nil
}

func (a *attributeValue) unmarshalMap(raw json.RawMessage) error {
	if err := json.Unmarshal(raw, &a.M); err != nil {
		return errors.WithStack(err)
	}
	a.hasM = true
	return nil
}
