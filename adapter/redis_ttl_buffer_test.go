package adapter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type shutdownFlushCoordinator struct {
	stubAdapterCoordinator
	failUntil int32
	calls     atomic.Int32
}

func (c *shutdownFlushCoordinator) Dispatch(context.Context, *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	call := c.calls.Add(1)
	if call <= c.failUntil {
		return nil, errors.New("dispatch failed")
	}
	return &kv.CoordinateResponse{}, nil
}

// ────────────────────────────────────────────────────────────────
// TTLBuffer — unit tests
// ────────────────────────────────────────────────────────────────

func TestTTLBuffer_SetAndGet(t *testing.T) {
	t.Parallel()
	b := newTTLBuffer()
	expireAt := time.Now().Add(time.Minute)

	b.Set([]byte("k"), &expireAt)

	got, found := b.Get([]byte("k"))
	require.True(t, found)
	require.NotNil(t, got)
	require.Equal(t, expireAt.UnixNano(), got.UnixNano())
}

func TestTTLBuffer_GetMissReturnsFalse(t *testing.T) {
	t.Parallel()
	b := newTTLBuffer()

	got, found := b.Get([]byte("missing"))
	require.False(t, found)
	require.Nil(t, got)
}

// Set(key, nil) represents PERSIST semantics: the TTL was explicitly removed.
// Get must return (nil, true) — "found but no expiry".
func TestTTLBuffer_SetNilIsPersistFound(t *testing.T) {
	t.Parallel()
	b := newTTLBuffer()

	b.Set([]byte("k"), nil)

	got, found := b.Get([]byte("k"))
	require.True(t, found, "PERSIST entry must be found in buffer")
	require.Nil(t, got, "PERSIST entry must have nil expireAt")
}

// A second Set with a higher seq must overwrite a previous Set.
func TestTTLBuffer_LaterSetWins(t *testing.T) {
	t.Parallel()
	b := newTTLBuffer()
	t1 := time.Now().Add(10 * time.Second)
	t2 := time.Now().Add(20 * time.Second)

	b.Set([]byte("k"), &t1)
	b.Set([]byte("k"), &t2)

	got, found := b.Get([]byte("k"))
	require.True(t, found)
	require.Equal(t, t2.UnixNano(), got.UnixNano())
}

func TestTTLBuffer_Drain_ReturnsSnapshotAndClearsBuffer(t *testing.T) {
	t.Parallel()
	b := newTTLBuffer()
	expireAt := time.Now().Add(time.Minute)
	b.Set([]byte("a"), &expireAt)
	b.Set([]byte("b"), nil)

	snapshot := b.Drain()

	require.Len(t, snapshot, 2)
	require.Contains(t, snapshot, "a")
	require.Contains(t, snapshot, "b")
	require.Equal(t, 0, b.Len(), "buffer must be empty after Drain")
}

func TestTTLBuffer_Drain_EmptyReturnsNil(t *testing.T) {
	t.Parallel()
	b := newTTLBuffer()

	snapshot := b.Drain()
	require.Nil(t, snapshot)
}

// MergeBack must NOT restore a failed entry if a newer write already exists.
func TestTTLBuffer_MergeBack_NewerWriteWins(t *testing.T) {
	t.Parallel()
	b := newTTLBuffer()

	// First write — goes into the buffer.
	old := time.Now().Add(10 * time.Second)
	b.Set([]byte("k"), &old)
	snapshot := b.Drain() // grab the entry (seq == 1)

	// New write arrives while the flush (using snapshot) was in-flight.
	newer := time.Now().Add(60 * time.Second)
	b.Set([]byte("k"), &newer) // seq == 2

	// Simulate flush failure: merge the old snapshot back.
	b.MergeBack(snapshot)

	// The buffer must keep the newer value (seq=2), not the merged-back one (seq=1).
	got, found := b.Get([]byte("k"))
	require.True(t, found)
	require.Equal(t, newer.UnixNano(), got.UnixNano(),
		"MergeBack must not overwrite a newer in-buffer write")
}

// MergeBack must restore an entry when no concurrent write occurred.
func TestTTLBuffer_MergeBack_RestoresWhenNoNewerWrite(t *testing.T) {
	t.Parallel()
	b := newTTLBuffer()
	expireAt := time.Now().Add(time.Minute)
	b.Set([]byte("k"), &expireAt)
	snapshot := b.Drain()

	// No new Set — buffer is now empty.
	b.MergeBack(snapshot)

	got, found := b.Get([]byte("k"))
	require.True(t, found)
	require.Equal(t, expireAt.UnixNano(), got.UnixNano())
}

func TestTTLBuffer_MergeBack_Full_DropsAndCounts(t *testing.T) {
	t.Parallel()
	b := newTTLBufferWithMaxSize(1)
	expireAt := time.Now().Add(time.Minute)
	b.Set([]byte("live"), &expireAt)

	snapshot := map[string]ttlBufferEntry{
		"a": {expireAt: &expireAt, seq: 1},
		"b": {expireAt: nil, seq: 2},
	}
	b.MergeBack(snapshot)

	require.Equal(t, uint64(2), b.dropped.Load())
	require.Equal(t, 1, b.Len())
	_, found := b.Get([]byte("a"))
	require.False(t, found)
	_, found = b.Get([]byte("b"))
	require.False(t, found)
}

// A nil *TTLBuffer must not panic on Get.
func TestTTLBuffer_NilReceiver_GetIsNoop(t *testing.T) {
	t.Parallel()
	var b *TTLBuffer
	got, found := b.Get([]byte("k"))
	require.False(t, found)
	require.Nil(t, got)
}

// A nil *TTLBuffer must not panic on Set.
func TestTTLBuffer_NilReceiver_SetIsNoop(t *testing.T) {
	t.Parallel()
	var b *TTLBuffer
	expireAt := time.Now().Add(time.Minute)
	require.NotPanics(t, func() { b.Set([]byte("k"), &expireAt) })
}

// When the buffer is full, new (unseen) keys must be silently dropped.
// Existing keys must still be updatable.
func TestTTLBuffer_Full_DropsNewKey(t *testing.T) {
	t.Parallel()
	const testMaxSize = 8
	b := newTTLBufferWithMaxSize(testMaxSize)

	// Fill the map to exactly ttlBufferMaxSize via direct struct access so the
	// test runs in O(N) map writes without going through the seq machinery.
	b.mu.Lock()
	var seq uint64
	for i := range testMaxSize {
		seq++
		b.entries[fmt.Sprintf("slot:%d", i)] = ttlBufferEntry{seq: seq}
	}
	b.mu.Unlock()

	require.Equal(t, testMaxSize, b.Len())

	// A brand-new key must be dropped.
	expireAt := time.Now().Add(time.Minute)
	b.Set([]byte("brand-new"), &expireAt)
	_, found := b.Get([]byte("brand-new"))
	require.False(t, found, "new key must be dropped when the buffer is full")

	// An existing key must still accept updates.
	b.Set([]byte("slot:0"), &expireAt)
	got, found := b.Get([]byte("slot:0"))
	require.True(t, found, "existing key must be updatable even when buffer is full")
	require.NotNil(t, got)
}

// Concurrent Set/Get/Drain/MergeBack must not data-race.
// Run with `go test -race` to exercise the race detector.
func TestTTLBuffer_ConcurrentAccess(t *testing.T) {
	t.Parallel()
	b := newTTLBuffer()
	const goroutines = 50
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("key:%d", id%10))
			expireAt := time.Now().Add(time.Duration(id) * time.Second)
			for range opsPerGoroutine {
				b.Set(key, &expireAt)
				_, _ = b.Get(key)
				if id%5 == 0 {
					drained := b.Drain()
					b.MergeBack(drained)
				}
			}
		}(g)
	}
	wg.Wait()
}

func TestRunTTLFlusher_ShutdownRetriesUntilSuccess(t *testing.T) {
	t.Parallel()
	coord := &shutdownFlushCoordinator{failUntil: 2}
	server := &RedisServer{
		coordinator:      coord,
		ttlBuffer:        newTTLBufferWithMaxSize(8),
		ttlFlushInterval: time.Hour,
	}
	expireAt := time.Now().Add(time.Minute)
	server.ttlBuffer.Set([]byte("k"), &expireAt)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	server.runTTLFlusher(ctx)

	require.Equal(t, int32(3), coord.calls.Load())
	require.Equal(t, 0, server.ttlBuffer.Len())
}

func TestRunTTLFlusher_ShutdownStopsAfterBoundedRetries(t *testing.T) {
	t.Parallel()
	coord := &shutdownFlushCoordinator{failUntil: 100}
	server := &RedisServer{
		coordinator:      coord,
		ttlBuffer:        newTTLBufferWithMaxSize(8),
		ttlFlushInterval: time.Hour,
	}
	expireAt := time.Now().Add(time.Minute)
	server.ttlBuffer.Set([]byte("k"), &expireAt)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	server.runTTLFlusher(ctx)

	require.Equal(t, int32(ttlShutdownFlushAttempts), coord.calls.Load())
	require.Equal(t, 1, server.ttlBuffer.Len())
}

// ────────────────────────────────────────────────────────────────
// buildTTLFlushElems — unit tests
// ────────────────────────────────────────────────────────────────

func TestBuildTTLFlushElems_Empty(t *testing.T) {
	t.Parallel()
	elems := buildTTLFlushElems(nil)
	require.Empty(t, elems)
}

// nil expireAt (PERSIST) must produce a Del operation on the TTL key.
func TestBuildTTLFlushElems_NilExpireGeneratesDel(t *testing.T) {
	t.Parallel()
	entries := map[string]ttlBufferEntry{
		"mykey": {expireAt: nil, seq: 1},
	}

	elems := buildTTLFlushElems(entries)

	require.Len(t, elems, 1)
	assert.Equal(t, kv.Del, elems[0].Op)
	assert.Equal(t, redisTTLKey([]byte("mykey")), elems[0].Key)
	assert.Nil(t, elems[0].Value)
}

// non-nil expireAt must produce a Put with the encoded TTL.
func TestBuildTTLFlushElems_NonNilExpireGeneratesPut(t *testing.T) {
	t.Parallel()
	expireAt := time.UnixMilli(9_000_000_000_000) // far future
	entries := map[string]ttlBufferEntry{
		"mykey": {expireAt: &expireAt, seq: 1},
	}

	elems := buildTTLFlushElems(entries)

	require.Len(t, elems, 1)
	assert.Equal(t, kv.Put, elems[0].Op)
	assert.Equal(t, redisTTLKey([]byte("mykey")), elems[0].Key)
	require.NotNil(t, elems[0].Value)

	// Round-trip: decoded value must match the original expiry.
	decoded, err := decodeRedisTTL(elems[0].Value)
	require.NoError(t, err)
	assert.Equal(t, expireAt.UnixMilli(), decoded.UnixMilli())
}

// Keys must appear in lexicographic order for deterministic Raft log entries.
func TestBuildTTLFlushElems_KeysAreSorted(t *testing.T) {
	t.Parallel()
	exp := time.Now().Add(time.Minute)
	entries := map[string]ttlBufferEntry{
		"zzz": {expireAt: &exp, seq: 3},
		"aaa": {expireAt: &exp, seq: 1},
		"mmm": {expireAt: &exp, seq: 2},
	}

	elems := buildTTLFlushElems(entries)

	require.Len(t, elems, 3)
	// TTL key has the !redis|ttl| prefix; strip it to compare user keys.
	prefix := len(redisTTLKey(nil))
	userKeys := make([]string, len(elems))
	for i, e := range elems {
		userKeys[i] = string(e.Key[prefix:])
	}
	assert.Equal(t, []string{"aaa", "mmm", "zzz"}, userKeys)
}

// Mixed nil / non-nil in one batch.
func TestBuildTTLFlushElems_MixedDelAndPut(t *testing.T) {
	t.Parallel()
	exp := time.Now().Add(time.Minute)
	entries := map[string]ttlBufferEntry{
		"persist": {expireAt: nil, seq: 1},
		"expire":  {expireAt: &exp, seq: 2},
	}

	elems := buildTTLFlushElems(entries)
	require.Len(t, elems, 2)

	ops := map[string]kv.OP{}
	prefix := len(redisTTLKey(nil))
	for _, e := range elems {
		ops[string(e.Key[prefix:])] = e.Op
	}
	assert.Equal(t, kv.Del, ops["persist"])
	assert.Equal(t, kv.Put, ops["expire"])
}

// ────────────────────────────────────────────────────────────────
// ttlAt — buffer-first read path (unit)
// ────────────────────────────────────────────────────────────────

// When the buffer holds a TTL entry, ttlAt must return it without
// touching the Raft/MVCCStore.
func TestTTLAt_BufferHit_SkipsStore(t *testing.T) {
	t.Parallel()
	server, st := newRedisStorageMigrationTestServer(t)
	ctx := context.Background()
	key := []byte("bufhit:key")

	// Write a TTL into the buffer only — the Raft store has no entry.
	expireAt := time.Now().Add(time.Hour)
	server.ttlBuffer.Set(key, &expireAt)

	// Verify the store is indeed empty.
	_, err := st.GetAt(ctx, redisTTLKey(key), server.readTS())
	require.ErrorIs(t, err, store.ErrKeyNotFound, "store must not have a TTL entry yet")

	// ttlAt must return the buffer value.
	got, err := server.ttlAt(ctx, key, server.readTS())
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, expireAt.UnixMilli(), got.UnixMilli())
}

// When the buffer is empty, ttlAt must fall back to the Raft store.
func TestTTLAt_BufferMiss_FallsBackToStore(t *testing.T) {
	t.Parallel()
	server, st := newRedisStorageMigrationTestServer(t)
	ctx := context.Background()
	key := []byte("bufmiss:key")

	// Write TTL directly to the store, bypassing the buffer.
	expireAt := time.Now().Add(time.Hour)
	ts := server.readTS() + 1
	require.NoError(t, st.PutAt(ctx, redisTTLKey(key), encodeRedisTTL(expireAt), ts, 0))

	// Buffer is empty.
	_, found := server.ttlBuffer.Get(key)
	require.False(t, found)

	got, err := server.ttlAt(ctx, key, ts)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, expireAt.UnixMilli(), got.UnixMilli())
}

// A PERSIST entry in the buffer (nil expireAt, found=true) must return nil
// without falling back to the store, even when the store has a stale TTL.
func TestTTLAt_BufferPersist_ReturnsNil(t *testing.T) {
	t.Parallel()
	server, st := newRedisStorageMigrationTestServer(t)
	ctx := context.Background()
	key := []byte("persist:key")

	// Store has an old TTL.
	oldExpiry := time.Now().Add(time.Hour)
	ts := server.readTS() + 1
	require.NoError(t, st.PutAt(ctx, redisTTLKey(key), encodeRedisTTL(oldExpiry), ts, 0))

	// Buffer says "TTL was removed".
	server.ttlBuffer.Set(key, nil)

	got, err := server.ttlAt(ctx, key, ts)
	require.NoError(t, err)
	require.Nil(t, got, "buffer PERSIST entry must shadow the stale store TTL")
}
