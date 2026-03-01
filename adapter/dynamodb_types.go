package adapter

import (
	"bytes"
	"encoding/json"

	"github.com/cockroachdb/errors"
)

type attributeValue struct {
	S    *string                   `json:"S,omitempty"`
	N    *string                   `json:"N,omitempty"`
	BOOL *bool                     `json:"BOOL,omitempty"`
	NULL *bool                     `json:"NULL,omitempty"`
	L    []attributeValue          `json:"L,omitempty"`
	M    map[string]attributeValue `json:"M,omitempty"`
}

type putItemInput struct {
	TableName string                    `json:"TableName"`
	Item      map[string]attributeValue `json:"Item"`
}

type transactWriteItemsInput struct {
	TransactItems []transactWriteItem `json:"TransactItems"`
}

type transactWriteItem struct {
	Put            *putItemInput           `json:"Put,omitempty"`
	Update         *transactUpdateInput    `json:"Update,omitempty"`
	Delete         *transactDeleteInput    `json:"Delete,omitempty"`
	ConditionCheck *transactConditionInput `json:"ConditionCheck,omitempty"`
}

type transactUpdateInput struct {
	TableName                 string                    `json:"TableName"`
	Key                       map[string]attributeValue `json:"Key"`
	UpdateExpression          string                    `json:"UpdateExpression"`
	ConditionExpression       string                    `json:"ConditionExpression"`
	ExpressionAttributeNames  map[string]string         `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues map[string]attributeValue `json:"ExpressionAttributeValues"`
}

type transactDeleteInput struct {
	TableName                 string                    `json:"TableName"`
	Key                       map[string]attributeValue `json:"Key"`
	ConditionExpression       string                    `json:"ConditionExpression"`
	ExpressionAttributeNames  map[string]string         `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues map[string]attributeValue `json:"ExpressionAttributeValues"`
}

type transactConditionInput struct {
	TableName                 string                    `json:"TableName"`
	Key                       map[string]attributeValue `json:"Key"`
	ConditionExpression       string                    `json:"ConditionExpression"`
	ExpressionAttributeNames  map[string]string         `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues map[string]attributeValue `json:"ExpressionAttributeValues"`
}

func (a attributeValue) hasStringType() bool {
	return a.S != nil
}

func (a attributeValue) hasNumberType() bool {
	return a.N != nil
}

func (a attributeValue) hasListType() bool {
	return a.L != nil
}

func (a attributeValue) hasMapType() bool {
	return a.M != nil
}

func (a attributeValue) stringValue() string {
	if a.S == nil {
		return ""
	}
	return *a.S
}

func (a attributeValue) numberValue() string {
	if a.N == nil {
		return ""
	}
	return *a.N
}

func newStringAttributeValue(v string) attributeValue {
	return attributeValue{S: &v}
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
		return marshalAttributeValue(map[string]string{"S": a.stringValue()})
	case hasN:
		return marshalAttributeValue(map[string]string{"N": a.numberValue()})
	case hasBOOL:
		return marshalAttributeValue(map[string]bool{"BOOL": *a.BOOL})
	case hasNULL:
		// DynamoDB wire format only allows {"NULL": true}; always normalize.
		return marshalAttributeValue(map[string]bool{"NULL": true})
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
		return errors.New("invalid attribute value")
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
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return errors.WithStack(err)
	}
	a.S = &s
	return nil
}

func (a *attributeValue) unmarshalNumber(raw json.RawMessage) error {
	var n string
	if err := json.Unmarshal(raw, &n); err != nil {
		return errors.WithStack(err)
	}
	a.N = &n
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
	if !n {
		return errors.New("dynamodb NULL attribute must be true")
	}
	a.NULL = &n
	return nil
}

func (a *attributeValue) unmarshalList(raw json.RawMessage) error {
	if isJSONNull(raw) {
		return errors.New("dynamodb L attribute must be an array")
	}
	var list []attributeValue
	if err := json.Unmarshal(raw, &list); err != nil {
		return errors.WithStack(err)
	}
	a.L = list
	return nil
}

func (a *attributeValue) unmarshalMap(raw json.RawMessage) error {
	if isJSONNull(raw) {
		return errors.New("dynamodb M attribute must be an object")
	}
	var m map[string]attributeValue
	if err := json.Unmarshal(raw, &m); err != nil {
		return errors.WithStack(err)
	}
	a.M = m
	return nil
}

func isJSONNull(raw json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}
