package adapter

import (
	"encoding/json"
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
