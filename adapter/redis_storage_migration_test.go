package adapter

import (
	"bytes"
	"context"
	"testing"

	"github.com/bootjp/elastickv/store"
	json "github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

type redisFixtureWriter struct {
	t      *testing.T
	store  store.MVCCStore
	nextTS uint64
}

func newRedisFixtureWriter(t *testing.T, st store.MVCCStore) *redisFixtureWriter {
	t.Helper()
	return &redisFixtureWriter{t: t, store: st, nextTS: 1}
}

func (w *redisFixtureWriter) put(key []byte, value []byte) {
	w.t.Helper()
	require.NoError(w.t, w.store.PutAt(context.Background(), key, value, w.nextTS, 0))
	w.nextTS++
}

func newRedisStorageMigrationTestServer(t *testing.T) (*RedisServer, store.MVCCStore) {
	t.Helper()
	st := store.NewMVCCStore()
	server := NewRedisServer(nil, "", st, newLocalAdapterCoordinator(st), nil, nil)
	return server, st
}

func assertRedisReadDoesNotRewriteLegacyJSON(t *testing.T, st store.MVCCStore, storageKey, rawBefore, protoPrefix []byte) {
	t.Helper()
	rawAfterRead := mustReadRawValue(t, st, storageKey)
	require.False(t, hasStoredRedisPrefix(rawBefore, protoPrefix))
	require.False(t, hasStoredRedisPrefix(rawAfterRead, protoPrefix))
	require.True(t, bytes.Equal(rawBefore, rawAfterRead))
}

func TestRedisHashLegacyJSONReadThenRewriteToProto(t *testing.T) {
	t.Parallel()

	server, st := newRedisStorageMigrationTestServer(t)
	writer := newRedisFixtureWriter(t, st)
	key := []byte("legacy:hash")
	storageKey := redisHashKey(key)

	legacy := redisHashValue{"field": "old"}
	rawBefore, err := json.Marshal(legacy)
	require.NoError(t, err)
	writer.put(storageKey, rawBefore)

	value, err := server.loadHashAt(context.Background(), key, server.readTS())
	require.NoError(t, err)
	require.Equal(t, legacy, value)
	assertRedisReadDoesNotRewriteLegacyJSON(t, st, storageKey, rawBefore, storedRedisHashProtoPrefix)

	added, err := server.applyHashFieldPairs(key, [][]byte{[]byte("next"), []byte("new")})
	require.NoError(t, err)
	require.Equal(t, 1, added)

	rawAfterWrite := mustReadRawValue(t, st, storageKey)
	require.True(t, hasStoredRedisPrefix(rawAfterWrite, storedRedisHashProtoPrefix))

	decoded, err := unmarshalHashValue(rawAfterWrite)
	require.NoError(t, err)
	require.Equal(t, redisHashValue{"field": "old", "next": "new"}, decoded)
}

func TestRedisSetLegacyJSONReadThenRewriteToProto(t *testing.T) {
	t.Parallel()

	server, st := newRedisStorageMigrationTestServer(t)
	writer := newRedisFixtureWriter(t, st)
	key := []byte("legacy:set")
	storageKey := redisSetKey(key)

	legacy := redisSetValue{Members: []string{"b", "a"}}
	rawBefore, err := json.Marshal(legacy)
	require.NoError(t, err)
	writer.put(storageKey, rawBefore)

	value, err := server.loadSetAt(context.Background(), "set", key, server.readTS())
	require.NoError(t, err)
	require.Equal(t, redisSetValue{Members: []string{"a", "b"}}, value)
	assertRedisReadDoesNotRewriteLegacyJSON(t, st, storageKey, rawBefore, storedRedisSetProtoPrefix)

	conn := &recordingConn{}
	server.mutateExactSet(conn, "set", key, [][]byte{[]byte("c")}, true)
	require.Empty(t, conn.err)
	require.Equal(t, int64(1), conn.int)

	rawAfterWrite := mustReadRawValue(t, st, storageKey)
	require.True(t, hasStoredRedisPrefix(rawAfterWrite, storedRedisSetProtoPrefix))

	decoded, err := unmarshalSetValue(rawAfterWrite)
	require.NoError(t, err)
	require.Equal(t, redisSetValue{Members: []string{"a", "b", "c"}}, decoded)
}

func TestRedisHLLLegacyJSONReadThenRewriteToProto(t *testing.T) {
	t.Parallel()

	server, st := newRedisStorageMigrationTestServer(t)
	writer := newRedisFixtureWriter(t, st)
	key := []byte("legacy:hll")
	storageKey := redisHLLKey(key)

	legacy := redisSetValue{Members: []string{"b", "a"}}
	rawBefore, err := json.Marshal(legacy)
	require.NoError(t, err)
	writer.put(storageKey, rawBefore)

	value, err := server.loadSetAt(context.Background(), "hll", key, server.readTS())
	require.NoError(t, err)
	require.Equal(t, redisSetValue{Members: []string{"a", "b"}}, value)
	assertRedisReadDoesNotRewriteLegacyJSON(t, st, storageKey, rawBefore, storedRedisSetProtoPrefix)

	conn := &recordingConn{}
	server.mutateExactSet(conn, "hll", key, [][]byte{[]byte("c")}, true)
	require.Empty(t, conn.err)
	require.Equal(t, int64(1), conn.int)

	rawAfterWrite := mustReadRawValue(t, st, storageKey)
	require.True(t, hasStoredRedisPrefix(rawAfterWrite, storedRedisSetProtoPrefix))

	decoded, err := unmarshalSetValue(rawAfterWrite)
	require.NoError(t, err)
	require.Equal(t, redisSetValue{Members: []string{"a", "b", "c"}}, decoded)
}

func TestRedisZSetLegacyJSONReadThenRewriteToProto(t *testing.T) {
	t.Parallel()

	server, st := newRedisStorageMigrationTestServer(t)
	writer := newRedisFixtureWriter(t, st)
	key := []byte("legacy:zset")
	storageKey := redisZSetKey(key)

	legacy := redisZSetValue{
		Entries: []redisZSetEntry{
			{Member: "b", Score: 2},
			{Member: "a", Score: 1},
		},
	}
	rawBefore, err := json.Marshal(legacy)
	require.NoError(t, err)
	writer.put(storageKey, rawBefore)

	value, exists, err := server.loadZSetAt(context.Background(), key, server.readTS())
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, redisZSetValue{
		Entries: []redisZSetEntry{
			{Member: "a", Score: 1},
			{Member: "b", Score: 2},
		},
	}, value)
	assertRedisReadDoesNotRewriteLegacyJSON(t, st, storageKey, rawBefore, storedRedisZSetProtoPrefix)

	added, err := server.zaddTxn(context.Background(), key, zaddFlags{}, []zaddPair{{score: 3, member: "c"}})
	require.NoError(t, err)
	require.Equal(t, 1, added)

	rawAfterWrite := mustReadRawValue(t, st, storageKey)
	require.True(t, hasStoredRedisPrefix(rawAfterWrite, storedRedisZSetProtoPrefix))

	decoded, err := unmarshalZSetValue(rawAfterWrite)
	require.NoError(t, err)
	require.Equal(t, redisZSetValue{
		Entries: []redisZSetEntry{
			{Member: "a", Score: 1},
			{Member: "b", Score: 2},
			{Member: "c", Score: 3},
		},
	}, decoded)
}

func TestRedisStreamLegacyJSONReadThenRewriteToProto(t *testing.T) {
	t.Parallel()

	server, st := newRedisStorageMigrationTestServer(t)
	writer := newRedisFixtureWriter(t, st)
	key := []byte("legacy:stream")
	storageKey := redisStreamKey(key)

	legacy := redisStreamValue{
		Entries: []redisStreamEntry{
			{ID: "1001-0", Fields: []string{"field", "old"}},
		},
	}
	rawBefore, err := json.Marshal(legacy)
	require.NoError(t, err)
	writer.put(storageKey, rawBefore)

	value, err := server.loadStreamAt(context.Background(), key, server.readTS())
	require.NoError(t, err)
	require.Equal(t, redisStreamValue{
		Entries: []redisStreamEntry{
			newRedisStreamEntry("1001-0", []string{"field", "old"}),
		},
	}, value)
	assertRedisReadDoesNotRewriteLegacyJSON(t, st, storageKey, rawBefore, storedRedisStreamProtoPrefix)

	id, err := server.xaddTxn(context.Background(), key, xaddRequest{id: "1002-0", fields: []string{"field", "new"}})
	require.NoError(t, err)
	require.Equal(t, "1002-0", id)

	rawAfterWrite := mustReadRawValue(t, st, storageKey)
	require.True(t, hasStoredRedisPrefix(rawAfterWrite, storedRedisStreamProtoPrefix))

	decoded, err := unmarshalStreamValue(rawAfterWrite)
	require.NoError(t, err)
	require.Equal(t, redisStreamValue{
		Entries: []redisStreamEntry{
			newRedisStreamEntry("1001-0", []string{"field", "old"}),
			newRedisStreamEntry("1002-0", []string{"field", "new"}),
		},
	}, decoded)
}
