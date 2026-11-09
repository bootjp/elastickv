package adapter

import (
	"testing"
	"time"

	pb "github.com/bootjp/elastickv/proto"
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

// marshalStreamValue encodes a stream in the legacy single-blob layout.
// Production no longer reads or writes it (PR #620 discards legacy blobs and
// deletes them on the next write); tests use it to seed that legacy data.
func marshalStreamValue(v redisStreamValue) ([]byte, error) {
	return marshalStoredRedisMessage(storedRedisStreamProtoPrefix, redisStreamValueToProto(v))
}

func redisStreamValueToProto(v redisStreamValue) *pb.RedisStreamValue {
	entries := make([]*pb.RedisStreamEntry, 0, len(v.Entries))
	for _, entry := range v.Entries {
		entries = append(entries, &pb.RedisStreamEntry{
			Id:     entry.ID,
			Fields: cloneStringSlice(entry.Fields),
		})
	}
	return &pb.RedisStreamValue{Entries: entries}
}
