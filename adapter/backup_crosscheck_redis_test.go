package adapter

import (
	"bytes"
	"strings"
	"testing"
	"time"

	logicalbackup "github.com/bootjp/elastickv/internal/backup"
	"github.com/bootjp/elastickv/kv"
	"github.com/stretchr/testify/require"
)

// Redis mirrors pinned here (internal/backup copy -> live counterpart):
//
//   - redis_string.go / encode_redis.go string value header
//     (redisStrMagic, redisStrVersion, redisStrHasTTL,
//     encodeRedisStrInlineValue) -> encodeRedisStr.
//   - redis_string.go / encode_redis.go HLL value header (redisHLLMagic,
//     redisHLLVersion, redisHLLHasTTL, encodeRedisHLLInlineValue) ->
//     encodeRedisHLL.
//   - encode_redis.go !redis|ttl| scan-index row (encodeRedisTTLValueMs) ->
//     encodeRedisTTL as staged by appendHLLScanIndexElem.
//
// A no-TTL HLL restores as the legacy raw payload, not the current
// [magic, version, flags] envelope (encode_redis.go: "No-TTL HLL restore
// keeps the legacy raw-sketch value"). The byte comparison therefore builds
// that key in the legacy state, and
// TestBackupCrossCheckRedisNoTTLHLLRestoresLegacyPayload pins that the
// current-format value restores to the same logical HLL.

// xcheckRedisExpireAtMs is the expiry every TTL'd fixture key carries.
const xcheckRedisExpireAtMs int64 = 4_102_444_800_123

// xcheckRedisHeaderSize is the [magic, version, flags] prefix of an inline
// value header without a TTL.
const xcheckRedisHeaderSize = redisHLLBaseHeader

func TestBackupCrossCheckRedis(t *testing.T) {
	t.Parallel()
	live := xcheckRedisPuts(xcheckRedisFixtureElems(t))
	// One dump of the whole database, as a backup of one Redis db takes it.
	_, restored := xcheckRoundTrip(t, logicalbackup.AdapterSet{Redis: true}, live)

	cases := []struct {
		name   string
		key    string
		family string
	}{
		{"string without TTL: value header", "plain", logicalbackup.RedisStringPrefix},
		{"string without TTL: no TTL index row", "plain", logicalbackup.RedisTTLPrefix},
		{"string with TTL: value header carries the expiry", "session:{42}", logicalbackup.RedisStringPrefix},
		{"binary key with TTL: value header", "\x00\xff|bin/key", logicalbackup.RedisStringPrefix},
		{"string whose body starts with the header magic", "magic", logicalbackup.RedisStringPrefix},
		{"empty string: header only", "empty", logicalbackup.RedisStringPrefix},
		{"long key (SHA-fallback dump name): value header", strings.Repeat("k", 300), logicalbackup.RedisStringPrefix},
		{"HLL with TTL: value header carries the expiry", "counter", logicalbackup.RedisHLLPrefix},
		{"HLL with TTL: TTL index row", "counter", logicalbackup.RedisTTLPrefix},
		{"HLL without TTL: legacy payload", "hll:legacy", logicalbackup.RedisHLLPrefix},
		{"HLL without TTL: no TTL index row", "hll:legacy", logicalbackup.RedisTTLPrefix},
	}
	compared := make(map[string]struct{}, len(cases))
	for _, tc := range cases {
		storageKey := tc.family + tc.key
		compared[storageKey] = struct{}{}
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			xcheckRequireIdentical(t, xcheckLiveWithKey(live, storageKey), xcheckRestoredWithKey(restored, storageKey))
		})
	}
	for _, e := range restored {
		require.Contains(t, compared, string(e.UserKey), "restored record no case compares")
	}
}

// TestBackupCrossCheckRedisNoTTLHLLRestoresLegacyPayload pins the documented
// no-TTL HLL asymmetry. The live writer stores the current
// [magic, version, flags=0] envelope; the restore stores the bare payload,
// which the live decoder reads as the same members with no TTL.
func TestBackupCrossCheckRedisNoTTLHLLRestoresLegacyPayload(t *testing.T) {
	t.Parallel()
	key := []byte("hll:current-format")
	elems := xcheckRedisPFAddElems(t, key, []string{"x", "y"}, nil)
	live := xcheckRedisPuts(elems)
	require.Len(t, live, 1, "a no-TTL PFADD stages only the HLL row")
	liveValue := live[0].value

	_, restored := xcheckRoundTrip(t, logicalbackup.AdapterSet{Redis: true}, live)
	require.Len(t, restored, 1, "a no-TTL HLL restores without a TTL index row")
	require.Equal(t, redisHLLKey(key), restored[0].UserKey)
	restoredValue := restored[0].UserValue

	require.Equal(t, []byte{redisHLLMagic, redisHLLVersion, 0}, liveValue[:xcheckRedisHeaderSize])
	require.Equal(t, liveValue[xcheckRedisHeaderSize:], restoredValue, "restore keeps the payload and drops the envelope")

	liveSet, liveTTL, liveEmbedded, err := decodeRedisHLL(liveValue)
	require.NoError(t, err)
	restoredSet, restoredTTL, restoredEmbedded, err := decodeRedisHLL(restoredValue)
	require.NoError(t, err)
	require.Equal(t, liveSet, restoredSet)
	require.Nil(t, liveTTL)
	require.Nil(t, restoredTTL)
	require.True(t, liveEmbedded)
	require.False(t, restoredEmbedded)
}

// xcheckRedisFixtureElems stages the fixture database: the elements the live
// write paths stage for each key.
func xcheckRedisFixtureElems(t *testing.T) []*kv.Elem[kv.OP] {
	t.Helper()
	expireAt := time.UnixMilli(xcheckRedisExpireAtMs)
	legacyPayload, err := marshalSetValue(redisSetValue{Members: []string{"p", "q"}})
	require.NoError(t, err)
	var elems []*kv.Elem[kv.OP]
	elems = append(elems, xcheckRedisSetStringElems([]byte("plain"), []byte("hello"), nil)...)
	elems = append(elems, xcheckRedisSetStringElems([]byte("session:{42}"), []byte("token-value"), &expireAt)...)
	elems = append(elems, xcheckRedisSetStringElems([]byte("\x00\xff|bin/key"), []byte{0x00, 0x01, 0xFF}, &expireAt)...)
	elems = append(elems, xcheckRedisSetStringElems([]byte("magic"), []byte{redisStrMagic, redisStrVersion, 0x00, 'x'}, nil)...)
	elems = append(elems, xcheckRedisSetStringElems([]byte("empty"), []byte{}, nil)...)
	elems = append(elems, xcheckRedisSetStringElems([]byte(strings.Repeat("k", 300)), []byte("long"), nil)...)
	elems = append(elems, xcheckRedisPFAddElems(t, []byte("counter"), []string{"c", "a", "b"}, &expireAt)...)
	// The state a no-TTL HLL restores to: the bare payload a pre-envelope
	// writer stored, with no TTL index row.
	return append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: redisHLLKey([]byte("hll:legacy")), Value: legacyPayload})
}

// xcheckRedisSetStringElems stages a string write the way MULTI/EXEC commits
// a dirty string (appendWorkingKeyElems): the value with its inline TTL
// header and, when the key expires, the !redis|ttl| scan-index row. SET
// outside a transaction (saveString, replaceWithStringReadTimestampTxn)
// stages the same rows.
func xcheckRedisSetStringElems(key, value []byte, ttl *time.Time) []*kv.Elem[kv.OP] {
	tv := &txnValue{raw: value, ttl: ttl, dirty: true}
	return (&txnContext{}).appendWorkingKeyElems(nil, redisStrKey(key), tv)
}

// xcheckRedisPFAddElems stages what PFADD commits for an HLL key
// (persistHLLMembersTxn): the members in the inline-TTL envelope plus the
// scan-index row, or its delete when the key does not expire.
func xcheckRedisPFAddElems(t *testing.T, key []byte, members []string, ttl *time.Time) []*kv.Elem[kv.OP] {
	t.Helper()
	set := make(map[string]struct{}, len(members))
	for _, m := range members {
		set[m] = struct{}{}
	}
	payload, err := encodeRedisHLL(redisSetValue{Members: sortedExactSetMembers(set)}, ttl)
	require.NoError(t, err)
	elems := []*kv.Elem[kv.OP]{{Op: kv.Put, Key: redisHLLKey(key), Value: payload}}
	return appendHLLScanIndexElem(elems, key, ttl)
}

// xcheckRedisPuts returns the records the elements leave in an empty
// keyspace. A Del of an absent key leaves nothing a snapshot reader returns.
func xcheckRedisPuts(elems []*kv.Elem[kv.OP]) []xcheckPair {
	out := make([]xcheckPair, 0, len(elems))
	for _, e := range elems {
		if e.Op == kv.Put {
			out = append(out, xcheckPair{key: bytes.Clone(e.Key), value: bytes.Clone(e.Value)})
		}
	}
	return out
}

// xcheckLiveWithKey returns the live records stored under storageKey.
func xcheckLiveWithKey(live []xcheckPair, storageKey string) []xcheckPair {
	var out []xcheckPair
	for _, p := range live {
		if string(p.key) == storageKey {
			out = append(out, p)
		}
	}
	return out
}

// xcheckRestoredWithKey returns the restored records stored under storageKey.
func xcheckRestoredWithKey(restored []logicalbackup.RoundTripEntry, storageKey string) []logicalbackup.RoundTripEntry {
	var out []logicalbackup.RoundTripEntry
	for _, e := range restored {
		if string(e.UserKey) == storageKey {
			out = append(out, e)
		}
	}
	return out
}
