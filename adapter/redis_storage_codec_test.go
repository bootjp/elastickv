package adapter

import (
	"testing"
	"time"

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

func TestStoredRedisHLLCodec_RoundTripInlineTTL(t *testing.T) {
	t.Parallel()

	ttl := time.UnixMilli(123_456)
	body, err := encodeRedisHLL(redisSetValue{Members: []string{"b", "a"}}, &ttl)
	require.NoError(t, err)
	require.True(t, isNewRedisHLLFormat(body))

	decoded, gotTTL, embedded, err := decodeRedisHLL(body)
	require.NoError(t, err)
	require.True(t, embedded)
	require.Equal(t, redisSetValue{Members: []string{"a", "b"}}, decoded)
	require.NotNil(t, gotTTL)
	require.Equal(t, redisExpireAtMillis(ttl), redisExpireAtMillis(*gotTTL))
}

func TestStoredRedisHLLCodec_LegacySetPayloadFallback(t *testing.T) {
	t.Parallel()

	body, err := marshalSetValue(redisSetValue{Members: []string{"legacy"}})
	require.NoError(t, err)

	decoded, ttl, embedded, err := decodeRedisHLL(body)
	require.NoError(t, err)
	require.False(t, embedded)
	require.Nil(t, ttl)
	require.Equal(t, redisSetValue{Members: []string{"legacy"}}, decoded)
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
