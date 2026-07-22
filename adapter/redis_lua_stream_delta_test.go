package adapter

import (
	"bytes"
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type luaStreamCountingStore struct {
	store.MVCCStore
	entryPrefix  []byte
	entryScans   []int
	entryStarts  [][]byte
	entryPuts    int
	entryDeletes int
}

type luaStreamFenceRaceCoordinator struct {
	*occAdapterCoordinator
	key      []byte
	injected bool
}

func (c *luaStreamFenceRaceCoordinator) Dispatch(
	ctx context.Context,
	req *kv.OperationGroup[kv.OP],
) (*kv.CoordinateResponse, error) {
	if req != nil && req.IsTxn && !c.injected {
		c.injected = true
		if err := c.store.PutAt(ctx, redisStrKey(c.key), encodeRedisStr([]byte("racer"), nil), req.StartTS+1, 0); err != nil {
			return nil, err
		}
	}
	return c.occAdapterCoordinator.Dispatch(ctx, req)
}

func newLuaStreamCountingStore(inner store.MVCCStore, key []byte) *luaStreamCountingStore {
	return &luaStreamCountingStore{
		MVCCStore:   inner,
		entryPrefix: store.StreamEntryScanPrefix(key),
	}
}

func (s *luaStreamCountingStore) ScanAt(
	ctx context.Context,
	start []byte,
	end []byte,
	limit int,
	ts uint64,
) ([]*store.KVPair, error) {
	if bytes.HasPrefix(start, s.entryPrefix) {
		s.entryScans = append(s.entryScans, limit)
		s.entryStarts = append(s.entryStarts, bytes.Clone(start))
	}
	return s.MVCCStore.ScanAt(ctx, start, end, limit, ts)
}

func (s *luaStreamCountingStore) PutAt(ctx context.Context, key, value []byte, commitTS, expireAt uint64) error {
	if bytes.HasPrefix(key, s.entryPrefix) {
		s.entryPuts++
	}
	return s.MVCCStore.PutAt(ctx, key, value, commitTS, expireAt)
}

func (s *luaStreamCountingStore) DeleteAt(ctx context.Context, key []byte, commitTS uint64) error {
	if bytes.HasPrefix(key, s.entryPrefix) {
		s.entryDeletes++
	}
	return s.MVCCStore.DeleteAt(ctx, key, commitTS)
}

func (s *luaStreamCountingStore) reset() {
	s.entryScans = nil
	s.entryStarts = nil
	s.entryPuts = 0
	s.entryDeletes = 0
}

func seedLuaWideStream(
	t *testing.T,
	st store.MVCCStore,
	key []byte,
	length int,
	expireAt uint64,
) {
	t.Helper()
	ctx := context.Background()
	var lastMs uint64
	for range length {
		lastMs++
		id := redisStreamID{ms: lastMs, seq: 0}
		entry := newRedisStreamEntry(formatStreamID(id.ms, id.seq), []string{"field", "value"})
		raw, err := marshalStreamEntry(entry)
		require.NoError(t, err)
		require.NoError(t, st.PutAt(ctx, store.StreamEntryKey(key, id.ms, id.seq), raw, 1, 0))
	}
	meta := store.StreamMeta{Length: int64(length), ExpireAt: expireAt}
	if length > 0 {
		meta.LastMs = lastMs
	}
	rawMeta, err := store.MarshalStreamMeta(meta)
	require.NoError(t, err)
	require.NoError(t, st.PutAt(ctx, store.StreamMetaKey(key), rawMeta, 1, 0))
}

func formatStreamID(ms, seq uint64) string {
	return strconv.FormatUint(ms, 10) + "-" + strconv.FormatUint(seq, 10)
}

func newLuaStreamDeltaTestServer(
	t *testing.T,
	key []byte,
) (*RedisServer, store.MVCCStore, *luaStreamCountingStore) {
	t.Helper()
	base := store.NewMVCCStore()
	counting := newLuaStreamCountingStore(base, key)
	server := NewRedisServer(nil, "", counting, newLocalAdapterCoordinator(counting), nil, nil)
	return server, base, counting
}

func TestLuaStreamXAddDeltaCommitAvoidsFullScanAndRewrite(t *testing.T) {
	key := []byte("lua:stream:delta")
	server, base, counting := newLuaStreamDeltaTestServer(t, key)
	seedLuaWideStream(t, base, key, 128, 0)
	counting.reset()

	conn := &recordingConn{}
	server.runLuaScript(conn, `
redis.call("XADD", KEYS[1], "129-0", "field", "a")
redis.call("XADD", KEYS[1], "130-0", "field", "b")
return redis.call("XLEN", KEYS[1])
`, [][]byte{[]byte("1"), key})
	require.Empty(t, conn.err)
	require.Equal(t, int64(130), conn.int)
	require.Empty(t, counting.entryScans, "XADD-only Lua must not scan existing stream entries")
	require.Equal(t, 2, counting.entryPuts, "only the two appended rows should be written")
	require.Zero(t, counting.entryDeletes)

	meta, found, err := server.loadStreamMetaAt(context.Background(), key, server.readTS())
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(130), meta.Length)
	require.Equal(t, uint64(130), meta.LastMs)
}

func TestLuaStreamXAddMaxLenUsesBoundedDeltaTrim(t *testing.T) {
	key := []byte("lua:stream:maxlen-delta")
	server, base, counting := newLuaStreamDeltaTestServer(t, key)
	seedLuaWideStream(t, base, key, 128, 0)
	counting.reset()

	conn := &recordingConn{}
	server.runLuaScript(conn, `
redis.call("XADD", KEYS[1], "MAXLEN", "~", 128, "129-0", "field", "a")
return redis.call("XADD", KEYS[1], "MAXLEN", "~", 128, "130-0", "field", "b")
`, [][]byte{[]byte("1"), key})
	require.Empty(t, conn.err)
	require.Equal(t, "130-0", string(conn.bulk))
	require.Equal(t, []int{2}, counting.entryScans, "two head deletions should share one bounded scan")
	require.Equal(t, 2, counting.entryPuts)
	require.Equal(t, 2, counting.entryDeletes)

	meta, found, err := server.loadStreamMetaAt(context.Background(), key, server.readTS())
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(128), meta.Length)
	require.Equal(t, uint64(130), meta.LastMs)
	require.Equal(t, uint64(2), meta.TrimmedMs)
	require.Zero(t, meta.TrimmedSeq)
}

func TestLuaStreamXAddMaxLenZeroKeepsLastIDWithoutRows(t *testing.T) {
	key := []byte("lua:stream:maxlen-zero")
	server, base, counting := newLuaStreamDeltaTestServer(t, key)
	seedLuaWideStream(t, base, key, 2, 0)
	counting.reset()

	conn := &recordingConn{}
	server.runLuaScript(conn, `
redis.call("XADD", KEYS[1], "MAXLEN", 0, "3-0", "field", "a")
local next = redis.call("XADD", KEYS[1], "MAXLEN", 0, "*", "field", "b")
return next
`, [][]byte{[]byte("1"), key})
	require.Empty(t, conn.err)
	require.Greater(t, compareRedisStreamID(string(conn.bulk), "3-0"), 0)
	require.Equal(t, []int{2}, counting.entryScans)
	require.Zero(t, counting.entryPuts, "both script-local appends are trimmed before commit")
	require.Equal(t, 2, counting.entryDeletes)

	meta, found, err := server.loadStreamMetaAt(context.Background(), key, server.readTS())
	require.NoError(t, err)
	require.True(t, found)
	require.Zero(t, meta.Length)
	require.Equal(t, string(conn.bulk), formatStreamID(meta.LastMs, meta.LastSeq))
	require.Equal(t, uint64(2), meta.TrimmedMs)
	require.Zero(t, meta.TrimmedSeq)
}

func TestLuaStreamXAddMaxLenUsesTrimCursorForNextHeadScan(t *testing.T) {
	key := []byte("lua:stream:trim-cursor")
	server, base, counting := newLuaStreamDeltaTestServer(t, key)
	seedLuaWideStream(t, base, key, 4, 0)

	conn := &recordingConn{}
	server.runLuaScript(conn, `return redis.call("XADD", KEYS[1], "MAXLEN", 4, "5-0", "field", "a")`, [][]byte{
		[]byte("1"), key,
	})
	require.Empty(t, conn.err)
	meta, found, err := server.loadStreamMetaAt(context.Background(), key, server.readTS())
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(1), meta.TrimmedMs)
	require.Zero(t, meta.TrimmedSeq)

	counting.reset()
	second := &recordingConn{}
	server.runLuaScript(second, `return redis.call("XADD", KEYS[1], "MAXLEN", 4, "6-0", "field", "b")`, [][]byte{
		[]byte("1"), key,
	})
	require.Empty(t, second.err)
	require.Equal(t, []int{1}, counting.entryScans)
	require.Len(t, counting.entryStarts, 1)
	require.Equal(t, store.StreamEntryKey(key, 1, 1), counting.entryStarts[0])

	meta, found, err = server.loadStreamMetaAt(context.Background(), key, server.readTS())
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(2), meta.TrimmedMs)
	require.Zero(t, meta.TrimmedSeq)
}

func TestTrimLuaStreamHeadMaxLenZeroAtStorageCapRemovesPendingAppend(t *testing.T) {
	st := &luaStreamState{
		baseMeta: store.StreamMeta{Length: maxWideColumnItems},
		meta:     store.StreamMeta{Length: maxWideColumnItems + 1},
		appended: []redisStreamEntry{newRedisStreamEntry("100001-0", []string{"field", "value"})},
	}

	removed := trimLuaStreamHead(st, st.meta.Length)
	require.Equal(t, int64(maxWideColumnItems+1), removed)
	require.Equal(t, int64(maxWideColumnItems), st.baseTrim)
	require.Empty(t, st.appended)
	require.Zero(t, st.meta.Length)
}

func TestLuaStreamTTLAndExplicitIDDeltaSemantics(t *testing.T) {
	key := []byte("lua:stream:ttl")
	expireAt := time.Now().Add(time.Minute)
	server, base, counting := newLuaStreamDeltaTestServer(t, key)
	seedLuaWideStream(t, base, key, 2, redisExpireAtMillis(expireAt))
	counting.reset()

	updatedTTL := 45 * time.Second
	conn := &recordingConn{}
	server.runLuaScript(conn, `
local id = redis.call("XADD", KEYS[1], "3-7", "field", "value")
redis.call("PEXPIRE", KEYS[1], ARGV[1])
return id
`, [][]byte{
		[]byte("1"), key, []byte(strconv.FormatInt(updatedTTL.Milliseconds(), 10)),
	})
	require.Empty(t, conn.err)
	require.Equal(t, "3-7", string(conn.bulk))
	require.Empty(t, counting.entryScans)

	meta, found, err := server.loadStreamMetaAt(context.Background(), key, server.readTS())
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(3), meta.Length)
	require.Equal(t, uint64(3), meta.LastMs)
	require.Equal(t, uint64(7), meta.LastSeq)
	require.WithinDuration(t, time.Now().Add(updatedTTL), *redisTimeFromMillis(meta.ExpireAt), 3*time.Second)

	rejected := &recordingConn{}
	server.runLuaScript(rejected, `return redis.call("XADD", KEYS[1], "3-6", "field", "value")`, [][]byte{
		[]byte("1"), key,
	})
	require.Contains(t, rejected.err, "equal or smaller")
}

func TestLuaStreamRangeForcesFullFallbackAndSeesScriptMutations(t *testing.T) {
	key := []byte("lua:stream:range-fallback")
	server, base, counting := newLuaStreamDeltaTestServer(t, key)
	seedLuaWideStream(t, base, key, 3, 0)
	counting.reset()

	ctx, err := newLuaScriptContext(context.Background(), server)
	require.NoError(t, err)
	defer ctx.Close()

	_, err = ctx.cmdXAdd([]string{string(key), "4-0", "field", "new"})
	require.NoError(t, err)
	length, err := ctx.cmdXLen([]string{string(key)})
	require.NoError(t, err)
	require.Equal(t, int64(4), length.integer)

	ranged, err := ctx.cmdXRange([]string{string(key), "2-0", "+", "COUNT", "2"})
	require.NoError(t, err)
	require.Len(t, ranged.array, 2)
	require.Equal(t, "2-0", ranged.array[0].array[0].text)
	require.True(t, ctx.streams[string(key)].materialized)
	reversed, err := ctx.cmdXRevRange([]string{string(key), "+", "-", "COUNT", "1"})
	require.NoError(t, err)
	require.Len(t, reversed.array, 1)
	require.Equal(t, "4-0", reversed.array[0].array[0].text)

	trimmed, err := ctx.cmdXTrim([]string{string(key), "MAXLEN", "2"})
	require.NoError(t, err)
	require.Equal(t, int64(2), trimmed.integer)
	length, err = ctx.cmdXLen([]string{string(key)})
	require.NoError(t, err)
	require.Equal(t, int64(2), length.integer)

	plan, err := ctx.streamCommitPlan(context.Background(), string(key))
	require.NoError(t, err)
	require.False(t, plan.preserveExisting, "range access must select the full fallback")
	require.Len(t, plan.elems, 3, "two final entries plus metadata")
	require.Equal(t, []int{scanStreamEntriesLimit(3)}, counting.entryScans)
}

func TestLuaStreamDeleteRecreateAndExpiredFallbackCleanOldRows(t *testing.T) {
	tests := []struct {
		name     string
		key      []byte
		expireAt uint64
		script   string
		newID    redisStreamID
		writeTTL bool
	}{
		{
			name:   "delete then recreate",
			key:    []byte("lua:stream:delete-recreate"),
			script: `redis.call("DEL", KEYS[1]); return redis.call("XADD", KEYS[1], "1-1", "field", "new")`,
			newID:  redisStreamID{ms: 1, seq: 1},
		},
		{
			name:     "expired layout",
			key:      []byte("lua:stream:expired-recreate"),
			expireAt: redisExpireAtMillis(time.UnixMilli(1)),
			script:   `return redis.call("XADD", KEYS[1], "1-1", "field", "new")`,
			newID:    redisStreamID{ms: 1, seq: 1},
			writeTTL: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server, base, _ := newLuaStreamDeltaTestServer(t, tc.key)
			seedLuaWideStream(t, base, tc.key, 3, tc.expireAt)
			if tc.writeTTL {
				require.NoError(t, base.PutAt(context.Background(), redisTTLKey(tc.key), encodeRedisTTL(time.UnixMilli(1)), 2, 0))
			}

			conn := &recordingConn{}
			server.runLuaScript(conn, tc.script, [][]byte{[]byte("1"), tc.key})
			require.Empty(t, conn.err)

			readTS := server.readTS()
			meta, found, err := server.loadStreamMetaAt(context.Background(), tc.key, readTS)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, int64(1), meta.Length)
			require.Zero(t, meta.ExpireAt)
			oldExists, err := base.ExistsAt(context.Background(), store.StreamEntryKey(tc.key, 2, 0), readTS)
			require.NoError(t, err)
			require.False(t, oldExists)
			newExists, err := base.ExistsAt(context.Background(), store.StreamEntryKey(tc.key, tc.newID.ms, tc.newID.seq), readTS)
			require.NoError(t, err)
			require.True(t, newExists)
			ttlExists, err := base.ExistsAt(context.Background(), redisTTLKey(tc.key), readTS)
			require.NoError(t, err)
			require.False(t, ttlExists)
		})
	}
}

func TestLuaStreamLegacyFallbackDiscardsBlobAndWritesDeltaLayout(t *testing.T) {
	key := []byte("lua:stream:legacy")
	server, base, _ := newLuaStreamDeltaTestServer(t, key)
	legacyRaw, err := marshalStreamValue(redisStreamValue{Entries: []redisStreamEntry{
		newRedisStreamEntry("9-0", []string{"field", "old"}),
	}})
	require.NoError(t, err)
	require.NoError(t, base.PutAt(context.Background(), redisStreamKey(key), legacyRaw, 1, 0))

	conn := &recordingConn{}
	server.runLuaScript(conn, `return redis.call("XADD", KEYS[1], "1-1", "field", "new")`, [][]byte{
		[]byte("1"), key,
	})
	require.Empty(t, conn.err)
	readTS := server.readTS()
	legacyExists, err := base.ExistsAt(context.Background(), redisStreamKey(key), readTS)
	require.NoError(t, err)
	require.False(t, legacyExists)
	meta, found, err := server.loadStreamMetaAt(context.Background(), key, readTS)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(1), meta.Length)
}

func TestLuaStreamDeltaCommitCarriesTypeAndMetaReadFences(t *testing.T) {
	key := []byte("lua:stream:read-fences")
	st := store.NewMVCCStore()
	coord := newReadKeyRecordingCoordinator(st)
	server := NewRedisServer(nil, "", st, coord, nil, nil)

	conn := &recordingConn{}
	server.runLuaScript(conn, `return redis.call("XADD", KEYS[1], "1-1", "field", "value")`, [][]byte{
		[]byte("1"), key,
	})
	require.Empty(t, conn.err)
	want := append([][]byte{}, redisTxnWideCollectionFenceKeys(key)...)
	want = append(want,
		store.StreamMetaKey(key),
		redisStrKey(key),
		redisHLLKey(key),
		key,
		store.ListMetaKey(key),
		redisHashKey(key),
		redisSetKey(key),
		redisZSetKey(key),
		redisStreamKey(key),
	)
	requireReadKeysMatch(t, coord.lastReadKeys, want)
}

func TestLuaStreamAbsentCreateReadFenceRejectsConcurrentString(t *testing.T) {
	key := []byte("lua:stream:read-fence-race")
	st := store.NewMVCCStore()
	coord := &luaStreamFenceRaceCoordinator{
		occAdapterCoordinator: newOCCAdapterCoordinator(st),
		key:                   key,
	}
	server := NewRedisServer(nil, "", st, coord, nil, nil)

	conn := &recordingConn{}
	server.runLuaScript(conn, `return redis.call("XADD", KEYS[1], "1-1", "field", "value")`, [][]byte{
		[]byte("1"), key,
	})
	require.True(t, coord.injected)
	require.Contains(t, conn.err, wrongTypeMessage)
	_, found, err := server.loadStreamMetaAt(context.Background(), key, server.readTS())
	require.NoError(t, err)
	require.False(t, found, "conflicting stream create must not leave a second logical type")
	value, _, err := server.readRedisStringAtSnapshot(key, server.readTS())
	require.NoError(t, err)
	require.Equal(t, []byte("racer"), value)
}

func TestLuaStreamXAddRetryDoesNotDuplicateEntries(t *testing.T) {
	key := []byte("lua:stream:retry")
	st := store.NewMVCCStore()
	seedLuaWideStream(t, st, key, 1, 0)
	coord := newRetryOnceCoordinator(st)
	coord.clock.Observe(1)
	server := NewRedisServer(nil, "", st, coord, nil, nil)

	conn := &recordingConn{}
	server.runLuaScript(conn, `return redis.call("XADD", KEYS[1], "2-0", "field", "value")`, [][]byte{
		[]byte("1"), key,
	})
	require.Empty(t, conn.err)
	require.Equal(t, 2, coord.dispatches)
	meta, found, err := server.loadStreamMetaAt(context.Background(), key, server.readTS())
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(2), meta.Length)
	entries, err := server.scanStreamEntriesAt(context.Background(), key, server.readTS(), meta)
	require.NoError(t, err)
	require.Len(t, entries, 2)
}

func TestLuaStreamXAddAmbiguousWireConflictFailsClosed(t *testing.T) {
	key := []byte("lua:stream:wire-conflict")
	st := store.NewMVCCStore()
	seedLuaWideStream(t, st, key, 1, 0)
	coord := newDedupTestCoordinator(st, 1, true)
	coord.wireWriteConflicts = true
	server := NewRedisServer(nil, "", st, coord, nil, nil)

	conn := &recordingConn{}
	server.runLuaScript(conn, `return redis.call("XADD", KEYS[1], "2-0", "field", "value")`, [][]byte{
		[]byte("1"), key,
	})
	require.NotEmpty(t, conn.err)
	require.Equal(t, 1, coord.dispatches, "generic Lua retry must not replay an ambiguous applied write")
	meta, found, err := server.loadStreamMetaAt(context.Background(), key, server.readTS())
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(2), meta.Length)
	entries, err := server.scanStreamEntriesAt(context.Background(), key, server.readTS(), meta)
	require.NoError(t, err)
	require.Len(t, entries, 2)
}

var _ store.MVCCStore = (*luaStreamCountingStore)(nil)
var _ kv.Coordinator = (*readKeyRecordingCoordinator)(nil)
