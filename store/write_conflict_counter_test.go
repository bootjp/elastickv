package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClassifyWriteConflictKey(t *testing.T) {
	// Table test covering every bucket in classifyWriteConflictKey. A
	// new bucket without a test row here is intentional dead code.
	cases := []struct {
		name string
		key  []byte
		want string
	}{
		{"txn_lock", []byte("!txn|lock|user/42"), writeConflictClassTxnLock},
		{"txn_intent", []byte("!txn|int|user/42"), writeConflictClassTxnIntent},
		{"txn_commit", []byte("!txn|cmt|user/42\x00\x00\x00\x00\x00\x00\x00\x01"), writeConflictClassTxnCommit},
		{"txn_rollback", []byte("!txn|rb|user/42\x00\x00\x00\x00\x00\x00\x00\x01"), writeConflictClassTxnRollback},
		{"txn_meta", []byte("!txn|meta|primary"), writeConflictClassTxnMeta},
		{"txn_other", []byte("!txn|unknown|x"), writeConflictClassTxnOther},
		{"redis_string", []byte("!redis|str|foo"), writeConflictClassRedisString},
		{"redis_ttl", []byte("!redis|ttl|foo"), writeConflictClassRedisTTL},
		{"redis_hll", []byte("!redis|hll|foo"), writeConflictClassRedisHLL},
		{"redis_stream_routes_to_stream", []byte("!redis|stream|foo"), writeConflictClassStream},
		{"redis_hash_routes_to_hash", []byte("!redis|hash|foo"), writeConflictClassHash},
		{"redis_set_routes_to_set", []byte("!redis|set|foo"), writeConflictClassSet},
		{"redis_zset_routes_to_zset", []byte("!redis|zset|foo"), writeConflictClassZSet},
		{"redis_other", []byte("!redis|weird|foo"), writeConflictClassRedisOther},
		{"hash_prefix", []byte("!hs|meta|foo"), writeConflictClassHash},
		{"set_prefix", []byte("!st|mem|foo"), writeConflictClassSet},
		{"zset_prefix", []byte("!zs|scr|foo"), writeConflictClassZSet},
		{"list_prefix", []byte("!lst|itm|foo"), writeConflictClassList},
		{"dynamodb_prefix", []byte("!ddb|item|users/42"), writeConflictClassDynamoDB},
		{"other_fallback", []byte("user/42"), writeConflictClassOther},
		{"empty_key", []byte(""), writeConflictClassOther},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, classifyWriteConflictKey(tc.key))
		})
	}
}

func TestWriteConflictCounterRecordAndSnapshot(t *testing.T) {
	c := newWriteConflictCounter()
	require.Zero(t, c.total())

	c.record(WriteConflictKindWrite, writeConflictClassTxnRollback)
	c.record(WriteConflictKindWrite, writeConflictClassTxnRollback)
	c.record(WriteConflictKindRead, writeConflictClassRedisString)

	assert.EqualValues(t, 3, c.total())

	snap := c.snapshot()
	assert.EqualValues(t, 2, snap[EncodeWriteConflictLabel(WriteConflictKindWrite, writeConflictClassTxnRollback)])
	assert.EqualValues(t, 1, snap[EncodeWriteConflictLabel(WriteConflictKindRead, writeConflictClassRedisString)])

	// Snapshot is a copy: mutating it must not affect the counter.
	snap["bogus"] = 999
	assert.EqualValues(t, 3, c.total())
}

func TestDecodeWriteConflictLabel(t *testing.T) {
	kind, class, ok := DecodeWriteConflictLabel(EncodeWriteConflictLabel(WriteConflictKindWrite, writeConflictClassTxnLock))
	require.True(t, ok)
	assert.Equal(t, WriteConflictKindWrite, kind)
	assert.Equal(t, writeConflictClassTxnLock, class)

	_, _, ok = DecodeWriteConflictLabel("malformed")
	assert.False(t, ok)
}

func TestWriteConflictCounterNilSafe(t *testing.T) {
	var c *writeConflictCounter
	require.NotPanics(t, func() { c.record(WriteConflictKindWrite, writeConflictClassOther) })
	assert.Nil(t, c.snapshot())
	assert.Zero(t, c.total())
}
