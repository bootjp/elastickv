package adapter

import (
	"testing"

	json "github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

func TestStoredRedisHashCodec_RoundTripProto(t *testing.T) {
	t.Parallel()

	value := redisHashValue{
		"field-1": "value-1",
		"field-2": "value-2",
	}

	body, err := marshalHashValue(value)
	require.NoError(t, err)
	require.True(t, hasStoredRedisPrefix(body, storedRedisHashProtoPrefix))

	decoded, err := unmarshalHashValue(body)
	require.NoError(t, err)
	require.Equal(t, value, decoded)
}

func TestStoredRedisHashCodec_LegacyJSONFallback(t *testing.T) {
	t.Parallel()

	legacy := redisHashValue{"field": "value"}
	body, err := json.Marshal(legacy)
	require.NoError(t, err)

	decoded, err := unmarshalHashValue(body)
	require.NoError(t, err)
	require.Equal(t, legacy, decoded)
}

func TestStoredRedisSetCodec_RoundTripProto(t *testing.T) {
	t.Parallel()

	value := redisSetValue{Members: []string{"b", "a"}}

	body, err := marshalSetValue(value)
	require.NoError(t, err)
	require.True(t, hasStoredRedisPrefix(body, storedRedisSetProtoPrefix))

	decoded, err := unmarshalSetValue(body)
	require.NoError(t, err)
	require.Equal(t, redisSetValue{Members: []string{"a", "b"}}, decoded)
}

func TestStoredRedisSetCodec_LegacyJSONFallback(t *testing.T) {
	t.Parallel()

	legacy := redisSetValue{Members: []string{"b", "a"}}
	body, err := json.Marshal(legacy)
	require.NoError(t, err)

	decoded, err := unmarshalSetValue(body)
	require.NoError(t, err)
	require.Equal(t, redisSetValue{Members: []string{"a", "b"}}, decoded)
}

func TestStoredRedisZSetCodec_RoundTripProto(t *testing.T) {
	t.Parallel()

	value := redisZSetValue{
		Entries: []redisZSetEntry{
			{Member: "b", Score: 2},
			{Member: "a", Score: 1},
		},
	}

	body, err := marshalZSetValue(value)
	require.NoError(t, err)
	require.True(t, hasStoredRedisPrefix(body, storedRedisZSetProtoPrefix))

	decoded, err := unmarshalZSetValue(body)
	require.NoError(t, err)
	require.Equal(t, redisZSetValue{
		Entries: []redisZSetEntry{
			{Member: "a", Score: 1},
			{Member: "b", Score: 2},
		},
	}, decoded)
}

func TestStoredRedisZSetCodec_LegacyJSONFallback(t *testing.T) {
	t.Parallel()

	legacy := redisZSetValue{
		Entries: []redisZSetEntry{
			{Member: "b", Score: 2},
			{Member: "a", Score: 1},
		},
	}
	body, err := json.Marshal(legacy)
	require.NoError(t, err)

	decoded, err := unmarshalZSetValue(body)
	require.NoError(t, err)
	require.Equal(t, redisZSetValue{
		Entries: []redisZSetEntry{
			{Member: "a", Score: 1},
			{Member: "b", Score: 2},
		},
	}, decoded)
}

func TestStoredRedisStreamCodec_RoundTripProto(t *testing.T) {
	t.Parallel()

	value := redisStreamValue{
		Entries: []redisStreamEntry{
			newRedisStreamEntry("1001-0", []string{"field", "value"}),
			newRedisStreamEntry("1002-0", []string{"field", "value-2"}),
		},
	}

	body, err := marshalStreamValue(value)
	require.NoError(t, err)
	require.True(t, hasStoredRedisPrefix(body, storedRedisStreamProtoPrefix))

	decoded, err := unmarshalStreamValue(body)
	require.NoError(t, err)
	require.Equal(t, value, decoded)
	require.True(t, decoded.Entries[0].parsedIDValid)
}

func TestStoredRedisStreamCodec_LegacyJSONFallback(t *testing.T) {
	t.Parallel()

	legacy := redisStreamValue{
		Entries: []redisStreamEntry{
			{ID: "1001-0", Fields: []string{"field", "value"}},
			{ID: "1002-0", Fields: []string{"field", "value-2"}},
		},
	}
	body, err := json.Marshal(legacy)
	require.NoError(t, err)

	decoded, err := unmarshalStreamValue(body)
	require.NoError(t, err)
	require.Equal(t, redisStreamValue{
		Entries: []redisStreamEntry{
			newRedisStreamEntry("1001-0", []string{"field", "value"}),
			newRedisStreamEntry("1002-0", []string{"field", "value-2"}),
		},
	}, decoded)
}
