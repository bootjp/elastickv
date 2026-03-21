package adapter

import (
	"bytes"
	"slices"

	"github.com/cockroachdb/errors"
	json "github.com/goccy/go-json"
)

type attributeValue struct {
	S    *string                   `json:"S,omitempty"`
	N    *string                   `json:"N,omitempty"`
	B    []byte                    `json:"B,omitempty"`
	BOOL *bool                     `json:"BOOL,omitempty"`
	NULL *bool                     `json:"NULL,omitempty"`
	SS   []string                  `json:"SS,omitempty"`
	NS   []string                  `json:"NS,omitempty"`
	BS   [][]byte                  `json:"BS,omitempty"`
	L    []attributeValue          `json:"L,omitempty"`
	M    map[string]attributeValue `json:"M,omitempty"`
}

type attributeValueKind string

const (
	attributeValueKindInvalid   attributeValueKind = ""
	attributeValueKindString    attributeValueKind = "S"
	attributeValueKindNumber    attributeValueKind = "N"
	attributeValueKindBinary    attributeValueKind = "B"
	attributeValueKindBool      attributeValueKind = "BOOL"
	attributeValueKindNull      attributeValueKind = "NULL"
	attributeValueKindStringSet attributeValueKind = "SS"
	attributeValueKindNumberSet attributeValueKind = "NS"
	attributeValueKindBinarySet attributeValueKind = "BS"
	attributeValueKindList      attributeValueKind = "L"
	attributeValueKindMap       attributeValueKind = "M"
)

type attributeValueKindDetector struct {
	kind  attributeValueKind
	match func(attributeValue) bool
}

type attributeValueUnmarshalFunc func(*attributeValue, json.RawMessage, int) error

const maxAttributeValueNestingDepth = 32

var attributeValueKindDetectors = []attributeValueKindDetector{
	{kind: attributeValueKindString, match: func(a attributeValue) bool { return a.hasStringType() }},
	{kind: attributeValueKindNumber, match: func(a attributeValue) bool { return a.hasNumberType() }},
	{kind: attributeValueKindBinary, match: func(a attributeValue) bool { return a.hasBinaryType() }},
	{kind: attributeValueKindBool, match: func(a attributeValue) bool { return a.BOOL != nil }},
	{kind: attributeValueKindNull, match: func(a attributeValue) bool { return a.NULL != nil }},
	{kind: attributeValueKindStringSet, match: func(a attributeValue) bool { return a.hasStringSetType() }},
	{kind: attributeValueKindNumberSet, match: func(a attributeValue) bool { return a.hasNumberSetType() }},
	{kind: attributeValueKindBinarySet, match: func(a attributeValue) bool { return a.hasBinarySetType() }},
	{kind: attributeValueKindList, match: func(a attributeValue) bool { return a.hasListType() }},
	{kind: attributeValueKindMap, match: func(a attributeValue) bool { return a.hasMapType() }},
}

var attributeValueMarshalers = map[attributeValueKind]func(attributeValue) any{
	attributeValueKindString:    func(a attributeValue) any { return map[string]string{"S": a.stringValue()} },
	attributeValueKindNumber:    func(a attributeValue) any { return map[string]string{"N": a.numberValue()} },
	attributeValueKindBinary:    func(a attributeValue) any { return map[string][]byte{"B": a.binaryValue()} },
	attributeValueKindBool:      func(a attributeValue) any { return map[string]bool{"BOOL": *a.BOOL} },
	attributeValueKindNull:      func(attributeValue) any { return map[string]bool{"NULL": true} },
	attributeValueKindStringSet: func(a attributeValue) any { return map[string][]string{"SS": cloneStringSlice(a.SS)} },
	attributeValueKindNumberSet: func(a attributeValue) any { return map[string][]string{"NS": cloneStringSlice(a.NS)} },
	attributeValueKindBinarySet: func(a attributeValue) any { return map[string][][]byte{"BS": cloneBinarySet(a.BS)} },
	attributeValueKindList:      func(a attributeValue) any { return map[string][]attributeValue{"L": cloneAttributeValueList(a.L)} },
	attributeValueKindMap: func(a attributeValue) any {
		return map[string]map[string]attributeValue{"M": cloneAttributeValueMap(a.M)}
	},
}

func (a attributeValue) hasStringType() bool {
	return a.S != nil
}

func (a attributeValue) hasNumberType() bool {
	return a.N != nil
}

func (a attributeValue) hasBinaryType() bool {
	return a.B != nil
}

func (a attributeValue) hasStringSetType() bool {
	return a.SS != nil
}

func (a attributeValue) hasNumberSetType() bool {
	return a.NS != nil
}

func (a attributeValue) hasBinarySetType() bool {
	return a.BS != nil
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

func (a attributeValue) binaryValue() []byte {
	return bytes.Clone(a.B)
}

func newStringAttributeValue(v string) attributeValue {
	return attributeValue{S: &v}
}

func (a attributeValue) MarshalJSON() ([]byte, error) {
	kind, count := detectAttributeValueKind(a)
	if count != 1 {
		return nil, errors.New("invalid attribute value")
	}
	marshal, ok := attributeValueMarshalers[kind]
	if !ok {
		return nil, errors.New("invalid attribute value")
	}
	return marshalAttributeValue(marshal(a))
}

func marshalAttributeValue(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return b, nil
}

func (a *attributeValue) UnmarshalJSON(b []byte) error {
	return a.unmarshalJSONWithDepth(b, 1)
}

func (a *attributeValue) unmarshalJSONWithDepth(b []byte, depth int) error {
	if depth > maxAttributeValueNestingDepth {
		return errors.New("attribute value nesting exceeds maximum depth")
	}

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
		return a.unmarshalSingleAttributeValue(k, v, depth)
	}
	return errors.New("invalid attribute value")
}

func (a *attributeValue) unmarshalSingleAttributeValue(kind string, raw json.RawMessage, depth int) error {
	unmarshal := attributeValueUnmarshaller(kind)
	if unmarshal == nil {
		return errors.New("unsupported attribute value type")
	}
	return unmarshal(a, raw, depth)
}

func attributeValueUnmarshaller(kind string) attributeValueUnmarshalFunc {
	return map[string]attributeValueUnmarshalFunc{
		"S":    (*attributeValue).unmarshalString,
		"N":    (*attributeValue).unmarshalNumber,
		"B":    (*attributeValue).unmarshalBinary,
		"BOOL": (*attributeValue).unmarshalBool,
		"NULL": (*attributeValue).unmarshalNull,
		"SS":   (*attributeValue).unmarshalStringSet,
		"NS":   (*attributeValue).unmarshalNumberSet,
		"BS":   (*attributeValue).unmarshalBinarySet,
		"L":    (*attributeValue).unmarshalList,
		"M":    (*attributeValue).unmarshalMap,
	}[kind]
}

func detectAttributeValueKind(a attributeValue) (attributeValueKind, int) {
	count := 0
	kind := attributeValueKindInvalid
	for _, detector := range attributeValueKindDetectors {
		if !detector.match(a) {
			continue
		}
		count++
		if count == 1 {
			kind = detector.kind
		}
	}
	return kind, count
}

func (a *attributeValue) unmarshalString(raw json.RawMessage, _ int) error {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return errors.WithStack(err)
	}
	a.S = &s
	return nil
}

func (a *attributeValue) unmarshalNumber(raw json.RawMessage, _ int) error {
	var n string
	if err := json.Unmarshal(raw, &n); err != nil {
		return errors.WithStack(err)
	}
	a.N = &n
	return nil
}

func (a *attributeValue) unmarshalBinary(raw json.RawMessage, _ int) error {
	if isJSONNull(raw) {
		return errors.New("dynamodb B attribute must be a string")
	}
	var b []byte
	if err := json.Unmarshal(raw, &b); err != nil {
		return errors.WithStack(err)
	}
	a.B = b
	return nil
}

func (a *attributeValue) unmarshalBool(raw json.RawMessage, _ int) error {
	var b bool
	if err := json.Unmarshal(raw, &b); err != nil {
		return errors.WithStack(err)
	}
	a.BOOL = &b
	return nil
}

func (a *attributeValue) unmarshalNull(raw json.RawMessage, _ int) error {
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

func (a *attributeValue) unmarshalStringSet(raw json.RawMessage, _ int) error {
	if isJSONNull(raw) {
		return errors.New("dynamodb SS attribute must be an array")
	}
	var ss []string
	if err := json.Unmarshal(raw, &ss); err != nil {
		return errors.WithStack(err)
	}
	a.SS = ss
	return nil
}

func (a *attributeValue) unmarshalNumberSet(raw json.RawMessage, _ int) error {
	if isJSONNull(raw) {
		return errors.New("dynamodb NS attribute must be an array")
	}
	var ns []string
	if err := json.Unmarshal(raw, &ns); err != nil {
		return errors.WithStack(err)
	}
	a.NS = ns
	return nil
}

func (a *attributeValue) unmarshalBinarySet(raw json.RawMessage, _ int) error {
	if isJSONNull(raw) {
		return errors.New("dynamodb BS attribute must be an array")
	}
	var bs [][]byte
	if err := json.Unmarshal(raw, &bs); err != nil {
		return errors.WithStack(err)
	}
	a.BS = bs
	return nil
}

func (a *attributeValue) unmarshalList(raw json.RawMessage, depth int) error {
	if isJSONNull(raw) {
		return errors.New("dynamodb L attribute must be an array")
	}
	var elems []json.RawMessage
	if err := json.Unmarshal(raw, &elems); err != nil {
		return errors.WithStack(err)
	}
	list := make([]attributeValue, len(elems))
	for i := range elems {
		if err := list[i].unmarshalJSONWithDepth(elems[i], depth+1); err != nil {
			return err
		}
	}
	a.L = list
	return nil
}

func (a *attributeValue) unmarshalMap(raw json.RawMessage, depth int) error {
	if isJSONNull(raw) {
		return errors.New("dynamodb M attribute must be an object")
	}
	var rawMap map[string]json.RawMessage
	if err := json.Unmarshal(raw, &rawMap); err != nil {
		return errors.WithStack(err)
	}
	m := make(map[string]attributeValue, len(rawMap))
	for key, value := range rawMap {
		var attr attributeValue
		if err := attr.unmarshalJSONWithDepth(value, depth+1); err != nil {
			return err
		}
		m[key] = attr
	}
	a.M = m
	return nil
}

func isJSONNull(raw json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

func cloneStringSlice(in []string) []string {
	if in == nil {
		return nil
	}
	return slices.Clone(in)
}

func cloneBinarySet(in [][]byte) [][]byte {
	if in == nil {
		return nil
	}
	out := make([][]byte, len(in))
	for i := range in {
		out[i] = bytes.Clone(in[i])
	}
	return out
}
