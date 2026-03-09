package adapter

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

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

	programmatic := newStringAttributeValue("")
	require.True(t, programmatic.hasStringType())
}

func TestAttributeValue_UnmarshalJSON_RejectsNullListAndMap(t *testing.T) {
	var listAttr attributeValue
	err := json.Unmarshal([]byte(`{"L":null}`), &listAttr)
	require.Error(t, err)

	var mapAttr attributeValue
	err = json.Unmarshal([]byte(`{"M":null}`), &mapAttr)
	require.Error(t, err)
}

func TestAttributeValue_UnmarshalJSON_RejectsNullFalse(t *testing.T) {
	var a attributeValue
	err := json.Unmarshal([]byte(`{"NULL":false}`), &a)
	require.Error(t, err)
}

func TestAttributeValue_MarshalJSON_NormalizesNullTrue(t *testing.T) {
	falseVal := false
	a := attributeValue{NULL: &falseVal}
	b, err := json.Marshal(a)
	require.NoError(t, err)
	require.JSONEq(t, `{"NULL":true}`, string(b))
}

func TestAttributeValue_UnmarshalJSON_RejectsExcessiveListNesting(t *testing.T) {
	var a attributeValue

	err := json.Unmarshal([]byte(nestedListAttributeJSON(maxAttributeValueNestingDepth+1)), &a)

	require.Error(t, err)
	require.Contains(t, err.Error(), "maximum depth")
}

func TestAttributeValue_UnmarshalJSON_AllowsMaxMapNestingDepth(t *testing.T) {
	var a attributeValue

	err := json.Unmarshal([]byte(nestedMapAttributeJSON(maxAttributeValueNestingDepth-1)), &a)

	require.NoError(t, err)
}

func nestedListAttributeJSON(depth int) string {
	var b strings.Builder
	for i := 0; i < depth; i++ {
		b.WriteString(`{"L":[`)
	}
	b.WriteString(`{"S":"leaf"}`)
	for i := 0; i < depth; i++ {
		b.WriteString(`]}`)
	}
	return b.String()
}

func nestedMapAttributeJSON(depth int) string {
	var b strings.Builder
	for i := 0; i < depth; i++ {
		b.WriteString(`{"M":{"child":`)
	}
	b.WriteString(`{"S":"leaf"}`)
	for i := 0; i < depth; i++ {
		b.WriteString(`}}`)
	}
	return b.String()
}
