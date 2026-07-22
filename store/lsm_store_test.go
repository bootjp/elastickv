package store

import (
	"bytes"
	"context"
	"math"
	"os"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func requirePebbleRetentionController(t *testing.T, st MVCCStore) RetentionController {
	t.Helper()

	retention, ok := st.(RetentionController)
	require.True(t, ok)
	return retention
}

func countPebbleVersions(t *testing.T, st MVCCStore) int {
	t.Helper()

	ps, ok := st.(*pebbleStore)
	require.True(t, ok)

	iter, err := ps.db.NewIter(nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, iter.Close()) }()

	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		if isPebbleMetaKey(iter.Key()) {
			continue
		}
		count++
	}
	require.NoError(t, iter.Error())
	return count
}

func requireChannelBlocked[T any](t *testing.T, ch <-chan T, started <-chan struct{}) {
	t.Helper()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("goroutine did not start in time")
	}

	select {
	case <-ch:
		t.Fatal("operation completed while maintenance lock was held")
	case <-time.After(50 * time.Millisecond):
	}
}

type cancelAfterErrCallsContext struct {
	context.Context
	maxNilErrCalls int
	errCalls       int
}

func (c *cancelAfterErrCallsContext) Err() error {
	if err := c.Context.Err(); err != nil {
		return err
	}
	c.errCalls++
	if c.errCalls > c.maxNilErrCalls {
		return context.Canceled
	}
	return nil
}

func TestPebbleStore_Basic(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()
	key := []byte("key1")

	// Put at TS 10
	err = s.PutAt(ctx, key, []byte("val10"), 10, 0)
	require.NoError(t, err)

	// Get at TS 10
	val, err := s.GetAt(ctx, key, 10)
	require.NoError(t, err)
	assert.Equal(t, []byte("val10"), val)

	// Get at TS 5 (should be not found)
	_, err = s.GetAt(ctx, key, 5)
	assert.Equal(t, ErrKeyNotFound, err)

	// Put at TS 20
	err = s.PutAt(ctx, key, []byte("val20"), 20, 0)
	require.NoError(t, err)

	// Get at TS 20 -> val20
	val, err = s.GetAt(ctx, key, 20)
	require.NoError(t, err)
	assert.Equal(t, []byte("val20"), val)

	// Get at TS 15 -> val10 (latest <= 15)
	val, err = s.GetAt(ctx, key, 15)
	require.NoError(t, err)
	assert.Equal(t, []byte("val10"), val)

	// Delete at TS 30
	err = s.DeleteAt(ctx, key, 30)
	require.NoError(t, err)

	// Get at TS 35 -> NotFound (Tombstone)
	_, err = s.GetAt(ctx, key, 35)
	assert.Equal(t, ErrKeyNotFound, err)

	// Get at TS 25 -> val20
	val, err = s.GetAt(ctx, key, 25)
	require.NoError(t, err)
	assert.Equal(t, []byte("val20"), val)
}

func TestPebbleStore_LastCommitTSPersistedAcrossRestart(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-last-ts-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	ctx := context.Background()

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	require.NoError(t, s.PutAt(ctx, []byte("k"), []byte("v"), 42, 0))
	require.Equal(t, uint64(42), s.LastCommitTS())
	require.NoError(t, s.Close())

	reopened, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer reopened.Close()
	require.Equal(t, uint64(42), reopened.LastCommitTS())
}

func TestPebbleStore_RebuildsStaleLastCommitTSOnOpen(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-last-ts-rebuild-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	ctx := context.Background()
	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	ps, ok := s.(*pebbleStore)
	require.True(t, ok)
	require.NoError(t, ps.PutAt(ctx, []byte("k"), []byte("v50"), 50, 0))
	require.Equal(t, uint64(50), ps.LastCommitTS())
	require.NoError(t, writePebbleUint64(ps.db, metaLastCommitTSBytes, 5, pebble.Sync))
	require.NoError(t, ps.Close())

	reopened, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer reopened.Close()
	require.Equal(t, uint64(50), reopened.LastCommitTS())

	err = reopened.ApplyMutations(ctx, []*KVPairMutation{
		{Op: OpTypePut, Key: []byte("k"), Value: []byte("new")},
	}, nil, 10, 60)
	require.ErrorIs(t, err, ErrWriteConflict)

	val, err := reopened.GetAt(ctx, []byte("k"), 55)
	require.NoError(t, err)
	require.Equal(t, []byte("v50"), val)
}

func TestPebbleStore_RebuildLastCommitTSIgnoresWriterRegistryRows(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-last-ts-registry-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	ctx := context.Background()
	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	ps, ok := s.(*pebbleStore)
	require.True(t, ok)
	require.NoError(t, ps.PutAt(ctx, []byte("k"), []byte("v50"), 50, 0))

	reg, err := WriterRegistryFor(ps)
	require.NoError(t, err)
	registryKey := encryption.RegistryKey(0xfeedbeef, 0xcafe)
	_, decodedRegistryTS := decodeKeyView(registryKey)
	require.Greater(t, decodedRegistryTS, uint64(50))
	require.NoError(t, reg.SetRegistryRow(registryKey, encryption.EncodeRegistryValue(encryption.RegistryValue{
		FullNodeID:          0x1234_5678_9abc_def0,
		FirstSeenLocalEpoch: 1,
		LastSeenLocalEpoch:  2,
	})))

	require.NoError(t, writePebbleUint64(ps.db, metaLastCommitTSBytes, 5, pebble.Sync))
	require.NoError(t, ps.Close())

	reopened, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer reopened.Close()
	require.Equal(t, uint64(50), reopened.LastCommitTS())

	kvs, err := reopened.ScanAt(ctx, nil, nil, 10, reopened.LastCommitTS())
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, []byte("k"), kvs[0].Key)
	require.Equal(t, []byte("v50"), kvs[0].Value)
}

func TestPebbleStore_UserKeyWithWriterRegistryPrefixRemainsMVCCData(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-registry-prefix-user-key-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	ctx := context.Background()
	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	ps, ok := s.(*pebbleStore)
	require.True(t, ok)

	userKey := append([]byte(nil), encryption.WriterRegistryPrefix...)
	userKey = append(userKey, []byte("tenant-visible-key")...)
	require.NoError(t, ps.PutAt(ctx, userKey, []byte("visible"), 100, 0))

	reg, err := WriterRegistryFor(ps)
	require.NoError(t, err)
	registryKey := encryption.RegistryKey(0xfeedbeef, 0xcafe)
	_, decodedRegistryTS := decodeKeyView(registryKey)
	require.Greater(t, decodedRegistryTS, uint64(100))
	require.NoError(t, reg.SetRegistryRow(registryKey, encryption.EncodeRegistryValue(encryption.RegistryValue{
		FullNodeID:          0x1234_5678_9abc_def0,
		FirstSeenLocalEpoch: 1,
		LastSeenLocalEpoch:  2,
	})))

	require.NoError(t, writePebbleUint64(ps.db, metaLastCommitTSBytes, 5, pebble.Sync))
	require.NoError(t, ps.Close())

	reopenedStore, err := NewPebbleStore(dir)
	require.NoError(t, err)
	reopened, ok := reopenedStore.(*pebbleStore)
	require.True(t, ok)
	defer reopened.Close()

	require.Equal(t, uint64(100), reopened.LastCommitTS())
	got, err := reopened.GetAt(ctx, userKey, reopened.LastCommitTS())
	require.NoError(t, err)
	require.Equal(t, []byte("visible"), got)

	kvs, err := reopened.ScanAt(ctx, nil, nil, 10, reopened.LastCommitTS())
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, userKey, kvs[0].Key)

	reverse, err := reopened.ReverseScanAt(ctx, nil, nil, 10, reopened.LastCommitTS())
	require.NoError(t, err)
	require.Len(t, reverse, 1)
	require.Equal(t, userKey, reverse[0].Key)
}

func TestPebbleStore_GetAtBatch(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-get-at-batch-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	ctx := context.Background()
	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	require.NoError(t, s.PutAt(ctx, []byte("b"), []byte("v-b"), 10, 0))
	require.NoError(t, s.PutAt(ctx, []byte("a"), []byte("v-a"), 20, 0))
	require.NoError(t, s.DeleteAt(ctx, []byte("deleted"), 30))

	ps, ok := s.(*pebbleStore)
	require.True(t, ok)
	got, err := ps.GetAtBatch(ctx, [][]byte{
		[]byte("missing"),
		[]byte("b"),
		[]byte("a"),
		[]byte("b"),
		[]byte("deleted"),
	}, ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, map[string][]byte{
		"b": []byte("v-b"),
		"a": []byte("v-a"),
	}, got)
}

func TestPebbleStore_GetAtBatchReseeksFromLaterIteratorPosition(t *testing.T) {
	dir := t.TempDir()
	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()
	require.NoError(t, s.PutAt(ctx, []byte("b"), []byte("v-b"), 20, 0))
	require.NoError(t, s.PutAt(ctx, []byte("c"), []byte("v-c"), 30, 0))

	ps, ok := s.(*pebbleStore)
	require.True(t, ok)

	iter, err := ps.db.NewIter(&pebble.IterOptions{})
	require.NoError(t, err)
	defer iter.Close()
	require.True(t, iter.SeekGE(encodeKey([]byte("c"), math.MaxUint64)))

	got, err := ps.getAtBatchWithIter(ctx, iter, [][]byte{[]byte("b")}, math.MaxUint64)
	require.NoError(t, err)
	require.Equal(t, map[string][]byte{"b": []byte("v-b")}, got)
}

func TestPebbleStore_LatestCommitTSWithIterReseeksWhenIteratorAhead(t *testing.T) {
	dir := t.TempDir()
	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()
	require.NoError(t, s.PutAt(ctx, []byte("b"), []byte("v-b"), 20, 0))
	require.NoError(t, s.PutAt(ctx, []byte("c"), []byte("v-c"), 30, 0))

	ps, ok := s.(*pebbleStore)
	require.True(t, ok)

	iter, err := ps.db.NewIter(&pebble.IterOptions{})
	require.NoError(t, err)
	defer iter.Close()
	require.True(t, iter.SeekGE(encodeKey([]byte("c"), math.MaxUint64)))

	var seekKey []byte
	var upperBound []byte
	ts, exists, err := ps.latestCommitTSWithIter(ctx, iter, []byte("b"), &seekKey, &upperBound, true, 8)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, uint64(20), ts)
}

func TestPebbleStore_MinRetainedTSPersistedAcrossRestart(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-min-retained-ts-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	ctx := context.Background()

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	require.NoError(t, s.PutAt(ctx, []byte("k"), []byte("v10"), 10, 0))
	require.NoError(t, s.PutAt(ctx, []byte("k"), []byte("v20"), 20, 0))
	requirePebbleRetentionController(t, s).SetMinRetainedTS(15)
	require.Equal(t, uint64(15), requirePebbleRetentionController(t, s).MinRetainedTS())
	require.NoError(t, s.Close())

	reopened, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer reopened.Close()

	retention := requirePebbleRetentionController(t, reopened)
	require.Equal(t, uint64(15), retention.MinRetainedTS())
	_, err = reopened.GetAt(ctx, []byte("k"), 10)
	require.ErrorIs(t, err, ErrReadTSCompacted)
}

func TestPebbleStore_ReadTSCompacted(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-read-ts-compacted-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()
	require.NoError(t, s.PutAt(ctx, []byte("k"), []byte("v10"), 10, 0))
	require.NoError(t, s.PutAt(ctx, []byte("k"), []byte("v20"), 20, 0))

	requirePebbleRetentionController(t, s).SetMinRetainedTS(15)

	_, err = s.GetAt(ctx, []byte("k"), 10)
	require.ErrorIs(t, err, ErrReadTSCompacted)

	_, err = s.ScanAt(ctx, []byte(""), nil, 10, 10)
	require.ErrorIs(t, err, ErrReadTSCompacted)

	_, err = s.ReverseScanAt(ctx, []byte(""), nil, 10, 10)
	require.ErrorIs(t, err, ErrReadTSCompacted)

	val, err := s.GetAt(ctx, []byte("k"), 20)
	require.NoError(t, err)
	require.Equal(t, []byte("v20"), val)
}

func TestPebbleStore_Scan(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-scan-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()

	// k2: v10@10
	// k1: v10@11 (aligned), v20@20
	// k3: v30@30
	require.NoError(t, s.PutAt(ctx, []byte("k2"), []byte("v10"), 10, 0))
	require.NoError(t, s.PutAt(ctx, []byte("k1"), []byte("v10"), 10, 0))
	require.NoError(t, s.PutAt(ctx, []byte("k1"), []byte("v20"), 20, 0))
	require.NoError(t, s.PutAt(ctx, []byte("k3"), []byte("v30"), 30, 0))

	// Scan at TS 25
	// Expect: k1=v20, k2=v10 (k3 is at 30, so invisible)
	pairs, err := s.ScanAt(ctx, []byte("k"), nil, 10, 25)
	require.NoError(t, err)
	assert.Len(t, pairs, 2)
	assert.Equal(t, []byte("k1"), pairs[0].Key)
	assert.Equal(t, []byte("v20"), pairs[0].Value)
	assert.Equal(t, []byte("k2"), pairs[1].Key)
	assert.Equal(t, []byte("v10"), pairs[1].Value)

	// Scan at TS 15
	// Expect: k1=v10, k2=v10
	pairs, err = s.ScanAt(ctx, []byte("k"), nil, 10, 15)
	require.NoError(t, err)
	assert.Len(t, pairs, 2)
	assert.Equal(t, []byte("k1"), pairs[0].Key)
	assert.Equal(t, []byte("v10"), pairs[0].Value)

	// Scan at TS 5
	// Expect: empty
	pairs, err = s.ScanAt(ctx, []byte("k"), nil, 10, 5)
	require.NoError(t, err)
	assert.Len(t, pairs, 0)
}

func TestPebbleStore_Scan_TombstoneMasksOlderVersions(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-scan-tombstone-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()
	require.NoError(t, s.PutAt(ctx, []byte("k1"), []byte("v1"), 10, 0))
	require.NoError(t, s.DeleteAt(ctx, []byte("k1"), 20))

	pairs, err := s.ScanAt(ctx, []byte("k"), nil, 10, 25)
	require.NoError(t, err)
	assert.Len(t, pairs, 0)
}

func TestPebbleStore_Scan_SkipsKeyWithOnlyFutureVersions(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-scan-future-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()
	require.NoError(t, s.PutAt(ctx, []byte("k1"), []byte("v100"), 100, 0))
	require.NoError(t, s.PutAt(ctx, []byte("k1"), []byte("v110"), 110, 0))
	require.NoError(t, s.PutAt(ctx, []byte("k2"), []byte("v80"), 80, 0))

	pairs, err := s.ScanAt(ctx, []byte("k"), nil, 10, 85)
	require.NoError(t, err)
	require.Len(t, pairs, 1)
	assert.Equal(t, []byte("k2"), pairs[0].Key)
	assert.Equal(t, []byte("v80"), pairs[0].Value)
}

func TestPebbleStore_Scan_IgnoresInternalMetaKey(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-scan-meta-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()
	require.NoError(t, s.PutAt(ctx, []byte("k1"), []byte("v1"), 10, 0))
	require.NoError(t, s.PutAt(ctx, []byte("k2"), []byte("v2"), 20, 0))
	requirePebbleRetentionController(t, s).SetMinRetainedTS(5)

	// Full-range scan should not decode/expose the internal Pebble metadata keys.
	pairs, err := s.ScanAt(ctx, []byte(""), nil, 10, 30)
	require.NoError(t, err)
	require.Len(t, pairs, 2)
	assert.Equal(t, []byte("k1"), pairs[0].Key)
	assert.Equal(t, []byte("k2"), pairs[1].Key)
}

func TestPebbleStore_Compact(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-compact-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()
	key := []byte("key1")
	require.NoError(t, s.PutAt(ctx, key, []byte("v10"), 10, 0))
	require.NoError(t, s.PutAt(ctx, key, []byte("v20"), 20, 0))
	require.NoError(t, s.PutAt(ctx, key, []byte("v30"), 30, 0))
	require.NoError(t, s.PutAt(ctx, key, []byte("v40"), 40, 0))
	require.Equal(t, 4, countPebbleVersions(t, s))

	require.NoError(t, s.Compact(ctx, 25))
	require.Equal(t, uint64(25), requirePebbleRetentionController(t, s).MinRetainedTS())
	require.Equal(t, 3, countPebbleVersions(t, s))

	_, err = s.GetAt(ctx, key, 15)
	require.ErrorIs(t, err, ErrReadTSCompacted)

	val, err := s.GetAt(ctx, key, 25)
	require.NoError(t, err)
	require.Equal(t, []byte("v20"), val)

	val, err = s.GetAt(ctx, key, 35)
	require.NoError(t, err)
	require.Equal(t, []byte("v30"), val)
}

func TestPebbleStore_CompactCanceledDoesNotAdvanceMinRetainedTS(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-compact-cancel-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	ctx := context.Background()

	st, err := NewPebbleStore(dir)
	require.NoError(t, err)

	key := []byte("key1")
	require.NoError(t, st.PutAt(ctx, key, []byte("v10"), 10, 0))
	require.NoError(t, st.PutAt(ctx, key, []byte("v20"), 20, 0))
	require.NoError(t, st.PutAt(ctx, key, []byte("v30"), 30, 0))

	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, st.Compact(canceledCtx, 25), context.Canceled)
	require.Equal(t, uint64(0), requirePebbleRetentionController(t, st).MinRetainedTS())

	val, err := st.GetAt(ctx, key, 15)
	require.NoError(t, err)
	require.Equal(t, []byte("v10"), val)

	require.NoError(t, st.Close())

	reopened, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer reopened.Close()

	require.Equal(t, uint64(0), requirePebbleRetentionController(t, reopened).MinRetainedTS())
	val, err = reopened.GetAt(ctx, key, 15)
	require.NoError(t, err)
	require.Equal(t, []byte("v10"), val)
}

func TestPebbleStore_CompactCanceledAfterDeleteFlushKeepsPendingMinRetainedTS(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-compact-partial-cancel-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	ctx := context.Background()

	st, err := NewPebbleStore(dir)
	require.NoError(t, err)

	key := []byte{0x01}
	value := []byte("value")
	for ts := uint64(1); ts <= compactionDeleteBatchCountLimit*2+1; ts++ {
		require.NoError(t, st.PutAt(ctx, key, value, ts, 0))
	}
	require.Equal(t, compactionDeleteBatchCountLimit*2+1, countPebbleVersions(t, st))

	cancelAfterFirstDeleteFlush := &cancelAfterErrCallsContext{
		Context:        ctx,
		maxNilErrCalls: compactionDeleteBatchCountLimit + 1,
	}
	require.ErrorIs(t, st.Compact(cancelAfterFirstDeleteFlush, compactionDeleteBatchCountLimit*3), context.Canceled)
	require.Equal(t, uint64(0), requirePebbleRetentionController(t, st).MinRetainedTS())
	require.Equal(t, compactionDeleteBatchCountLimit+1, countPebbleVersions(t, st))

	_, err = st.GetAt(ctx, key, compactionDeleteBatchCountLimit+50)
	require.ErrorIs(t, err, ErrReadTSCompacted)

	require.NoError(t, st.Close())

	reopened, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer reopened.Close()

	require.Equal(t, uint64(0), requirePebbleRetentionController(t, reopened).MinRetainedTS())
	_, err = reopened.GetAt(ctx, key, compactionDeleteBatchCountLimit+50)
	require.ErrorIs(t, err, ErrReadTSCompacted)

	require.NoError(t, reopened.Compact(ctx, compactionDeleteBatchCountLimit*3))
	require.Equal(t, uint64(compactionDeleteBatchCountLimit*3), requirePebbleRetentionController(t, reopened).MinRetainedTS())
	require.Equal(t, 1, countPebbleVersions(t, reopened))
}

func TestPebbleStore_CompactResumedPendingAdvancesToRequestedMinTS(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-compact-resume-higher-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	ctx := context.Background()

	st, err := NewPebbleStore(dir)
	require.NoError(t, err)

	key := []byte{0x01}
	value := []byte("value")
	pendingMinTS := uint64(compactionDeleteBatchCountLimit * 2)
	requestedMinTS := uint64(compactionDeleteBatchCountLimit * 3)
	for ts := uint64(1); ts <= pendingMinTS+1; ts++ {
		require.NoError(t, st.PutAt(ctx, key, value, ts, 0))
	}

	cancelAfterFirstDeleteFlush := &cancelAfterErrCallsContext{
		Context:        ctx,
		maxNilErrCalls: compactionDeleteBatchCountLimit + 10,
	}
	require.ErrorIs(t, st.Compact(cancelAfterFirstDeleteFlush, pendingMinTS), context.Canceled)
	require.Equal(t, uint64(0), requirePebbleRetentionController(t, st).MinRetainedTS())
	for ts := pendingMinTS + 2; ts <= requestedMinTS+10; ts++ {
		require.NoError(t, st.PutAt(ctx, key, value, ts, 0))
	}

	require.NoError(t, st.Close())

	reopened, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer reopened.Close()

	_, err = reopened.GetAt(ctx, key, pendingMinTS-1)
	require.ErrorIs(t, err, ErrReadTSCompacted)

	require.NoError(t, reopened.Compact(ctx, requestedMinTS))
	require.Equal(t, requestedMinTS, requirePebbleRetentionController(t, reopened).MinRetainedTS())

	_, err = reopened.GetAt(ctx, key, requestedMinTS-1)
	require.ErrorIs(t, err, ErrReadTSCompacted)

	val, err := reopened.GetAt(ctx, key, requestedMinTS+1)
	require.NoError(t, err)
	require.Equal(t, value, val)
}

func TestPebbleStore_CompactWaitsForMaintenanceLock(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-compact-lock-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	st, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer st.Close()

	ctx := context.Background()
	require.NoError(t, st.PutAt(ctx, []byte("key1"), []byte("v10"), 10, 0))
	require.NoError(t, st.PutAt(ctx, []byte("key1"), []byte("v20"), 20, 0))

	ps, ok := st.(*pebbleStore)
	require.True(t, ok)

	ps.maintenanceMu.Lock()
	done := make(chan error, 1)
	started := make(chan struct{}, 1)
	go func() {
		started <- struct{}{}
		done <- ps.Compact(ctx, 15)
	}()

	requireChannelBlocked(t, done, started)
	ps.maintenanceMu.Unlock()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for compaction")
	}
}

func TestPebbleStore_RestoreWaitsForMaintenanceLock(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-restore-lock-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	st, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer st.Close()

	ps, ok := st.(*pebbleStore)
	require.True(t, ok)

	ps.maintenanceMu.Lock()
	done := make(chan error, 1)
	started := make(chan struct{}, 1)
	go func() {
		started <- struct{}{}
		done <- ps.Restore(bytes.NewReader(nil))
	}()

	requireChannelBlocked(t, done, started)
	ps.maintenanceMu.Unlock()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for restore")
	}
}

func TestPebbleStore_SnapshotRestore(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-snap-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer func() { assert.NoError(t, s.Close()) }()

	ctx := context.Background()
	require.NoError(t, s.PutAt(ctx, []byte("k1"), []byte("v1"), 100, 0))

	// Snapshot
	buf, err := s.Snapshot()
	require.NoError(t, err)
	defer func() { assert.NoError(t, buf.Close()) }()

	var raw bytes.Buffer
	_, err = buf.WriteTo(&raw)
	require.NoError(t, err)

	// Restore to new dir
	dir2, err := os.MkdirTemp("", "pebble-restore-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir2)

	s2, err := NewPebbleStore(dir2)
	require.NoError(t, err)
	defer s2.Close()

	err = s2.Restore(bytes.NewReader(raw.Bytes()))
	require.NoError(t, err)

	val, err := s2.GetAt(ctx, []byte("k1"), 100)
	require.NoError(t, err)
	assert.Equal(t, []byte("v1"), val)

	assert.Equal(t, uint64(100), s2.LastCommitTS()) // aligned 100 -> 100 (if started from 0)
}

func TestPebbleStore_SnapshotRestorePreservesMinRetainedTS(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-retained-snap-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, s.PutAt(ctx, []byte("k1"), []byte("v10"), 10, 0))
	require.NoError(t, s.PutAt(ctx, []byte("k1"), []byte("v20"), 20, 0))
	require.NoError(t, s.Compact(ctx, 20))

	snap, err := s.Snapshot()
	require.NoError(t, err)

	var raw bytes.Buffer
	_, err = snap.WriteTo(&raw)
	require.NoError(t, err)
	require.NoError(t, snap.Close())
	require.NoError(t, s.Close())

	dir2, err := os.MkdirTemp("", "pebble-retained-restore-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir2)

	s2, err := NewPebbleStore(dir2)
	require.NoError(t, err)
	defer s2.Close()

	require.NoError(t, s2.Restore(bytes.NewReader(raw.Bytes())))
	require.Equal(t, uint64(20), requirePebbleRetentionController(t, s2).MinRetainedTS())

	_, err = s2.GetAt(ctx, []byte("k1"), 10)
	require.ErrorIs(t, err, ErrReadTSCompacted)

	val, err := s2.GetAt(ctx, []byte("k1"), 20)
	require.NoError(t, err)
	require.Equal(t, []byte("v20"), val)
}

func TestSnapshotBatchShouldFlushOnByteLimit(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-batch-flush-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opts, cache := defaultPebbleOptionsWithCache(false)
	db, err := pebble.Open(dir, opts)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, db.Close())
		cache.Unref()
	}()

	batch := db.NewBatch()
	defer func() { assert.NoError(t, batch.Close()) }()

	deferred := batch.SetDeferred(1, snapshotBatchByteLimit)
	deferred.Key[0] = 'k'
	deferred.Value[0] = 0
	require.NoError(t, deferred.Finish())

	assert.True(t, snapshotBatchShouldFlush(batch))
}

func TestPebbleStore_SnapshotRestoreLargeValues(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-large-snap-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer func() { assert.NoError(t, s.Close()) }()

	ctx := context.Background()
	largeValue := bytes.Repeat([]byte("x"), snapshotBatchByteLimit/2+1024)
	require.NoError(t, s.PutAt(ctx, []byte("k1"), largeValue, 100, 0))
	require.NoError(t, s.PutAt(ctx, []byte("k2"), largeValue, 200, 0))

	snap, err := s.Snapshot()
	require.NoError(t, err)
	defer func() { assert.NoError(t, snap.Close()) }()

	var raw bytes.Buffer
	_, err = snap.WriteTo(&raw)
	require.NoError(t, err)

	dir2, err := os.MkdirTemp("", "pebble-large-restore-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir2)

	s2, err := NewPebbleStore(dir2)
	require.NoError(t, err)
	defer func() { assert.NoError(t, s2.Close()) }()

	require.NoError(t, s2.Restore(bytes.NewReader(raw.Bytes())))

	val, err := s2.GetAt(ctx, []byte("k1"), 100)
	require.NoError(t, err)
	assert.Equal(t, largeValue, val)

	val, err = s2.GetAt(ctx, []byte("k2"), 200)
	require.NoError(t, err)
	assert.Equal(t, largeValue, val)
}

// TestPebbleStore_RestoreFromStreamingMVCC verifies that a pebbleStore can
// restore from a snapshot created by the in-memory mvccStore (streaming
// "EKVMVCC2" format). This is the migration path when upgrading from the
// in-memory FSM to the Pebble-backed FSM.
func TestPebbleStore_RestoreFromStreamingMVCC(t *testing.T) {
	ctx := context.Background()

	// Build a snapshot using the in-memory mvccStore.
	src := NewMVCCStore()
	require.NoError(t, src.PutAt(ctx, []byte("key1"), []byte("val1"), 10, 0))
	require.NoError(t, src.PutAt(ctx, []byte("key1"), []byte("val1-updated"), 20, 0))
	require.NoError(t, src.DeleteAt(ctx, []byte("key2"), 15))
	require.NoError(t, src.PutWithTTLAt(ctx, []byte("key3"), []byte("val3"), 30, 9999))

	snap, err := src.Snapshot()
	require.NoError(t, err)
	defer snap.Close()

	var buf bytes.Buffer
	_, err = snap.WriteTo(&buf)
	require.NoError(t, err)

	// Restore into a fresh pebbleStore.
	dir, err := os.MkdirTemp("", "pebble-migrate-streaming-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	dst, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer dst.Close()

	require.NoError(t, dst.Restore(bytes.NewReader(buf.Bytes())))

	// Verify data was migrated correctly.
	val, err := dst.GetAt(ctx, []byte("key1"), 20)
	require.NoError(t, err)
	assert.Equal(t, []byte("val1-updated"), val)

	val, err = dst.GetAt(ctx, []byte("key1"), 10)
	require.NoError(t, err)
	assert.Equal(t, []byte("val1"), val)

	_, err = dst.GetAt(ctx, []byte("key2"), 15)
	assert.ErrorIs(t, err, ErrKeyNotFound)

	val, err = dst.GetAt(ctx, []byte("key3"), 30)
	require.NoError(t, err)
	assert.Equal(t, []byte("val3"), val)

	assert.Equal(t, src.LastCommitTS(), dst.LastCommitTS())
}

func TestPebbleStore_RestoreFromStreamingMVCCPreservesMinRetainedTS(t *testing.T) {
	ctx := context.Background()

	src := NewMVCCStore()
	require.NoError(t, src.PutAt(ctx, []byte("key1"), []byte("val1"), 10, 0))
	require.NoError(t, src.PutAt(ctx, []byte("key1"), []byte("val2"), 20, 0))
	require.NoError(t, src.Compact(ctx, 20))

	snap, err := src.Snapshot()
	require.NoError(t, err)
	defer snap.Close()

	var buf bytes.Buffer
	_, err = snap.WriteTo(&buf)
	require.NoError(t, err)

	dir, err := os.MkdirTemp("", "pebble-migrate-streaming-retained-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	dst, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer dst.Close()

	require.NoError(t, dst.Restore(bytes.NewReader(buf.Bytes())))
	require.Equal(t, uint64(20), requirePebbleRetentionController(t, dst).MinRetainedTS())

	_, err = dst.GetAt(ctx, []byte("key1"), 10)
	require.ErrorIs(t, err, ErrReadTSCompacted)
}

// TestPebbleStore_Restore_EmptySnapshot verifies that restoring from an
// empty reader clears the DB and resets lastCommitTS to zero.
func TestPebbleStore_Restore_EmptySnapshot(t *testing.T) {
	ctx := context.Background()

	dir, err := os.MkdirTemp("", "pebble-restore-empty-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	// Pre-populate so we can verify the restore clears the data.
	require.NoError(t, s.PutAt(ctx, []byte("k"), []byte("v"), 42, 0))
	requirePebbleRetentionController(t, s).SetMinRetainedTS(21)
	require.Equal(t, uint64(42), s.LastCommitTS())

	// Restore from empty reader.
	require.NoError(t, s.Restore(bytes.NewReader(nil)))

	// DB should be empty and lastCommitTS reset to zero.
	_, err = s.GetAt(ctx, []byte("k"), 42)
	assert.ErrorIs(t, err, ErrKeyNotFound)
	assert.Equal(t, uint64(0), s.LastCommitTS())
	assert.Equal(t, uint64(0), requirePebbleRetentionController(t, s).MinRetainedTS())
}

// TestPebbleStore_Restore_TruncatedHeader verifies that a partial magic
// header (fewer than 8 bytes) is rejected as a corrupted snapshot.
func TestPebbleStore_Restore_TruncatedHeader(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-restore-trunc-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	// 4 bytes is a partial magic header (less than the 8-byte magic).
	err = s.Restore(bytes.NewReader([]byte{0x01, 0x02, 0x03, 0x04}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "truncated snapshot")
}

// TestPebbleStore_Restore_PebbleMagicMismatch verifies that a stream
// dispatched as native Pebble but containing a wrong magic header is rejected.
func TestPebbleStore_Restore_PebbleMagicMismatch(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-restore-magic-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	// Call restorePebbleNative directly with a bad magic value to test the
	// explicit in-function magic check.
	badMagic := [8]byte{'B', 'A', 'D', 'M', 'A', 'G', 'I', 'C'}
	ps, ok := s.(*pebbleStore)
	require.True(t, ok, "expected *pebbleStore")
	err = ps.restorePebbleNative(bytes.NewReader(badMagic[:]))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid pebble snapshot magic header")
}

// TestPebbleStore_Restore_NativePebbleAtomic verifies that when a native
// Pebble snapshot restore fails midway (truncated data), the existing DB
// contents are preserved and not wiped.
func TestPebbleStore_Restore_NativePebbleAtomic(t *testing.T) {
	ctx := context.Background()

	dir, err := os.MkdirTemp("", "pebble-atomic-restore-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer s.Close()

	// Pre-populate with known data.
	require.NoError(t, s.PutAt(ctx, []byte("existing"), []byte("value"), 10, 0))
	require.Equal(t, uint64(10), s.LastCommitTS())

	// Build a valid snapshot then truncate it to simulate corruption.
	snap, err := s.Snapshot()
	require.NoError(t, err)
	defer snap.Close()
	var raw bytes.Buffer
	_, err = snap.WriteTo(&raw)
	require.NoError(t, err)

	// Truncate: keep magic (8) + lastCommitTS (8) but include only a partial
	// entry framing (3 bytes of what would be an 8-byte key-length field).
	truncated := raw.Bytes()[:16+3] // partial key-length field (8 bytes expected)

	// Restore from truncated snapshot should fail.
	err = s.Restore(bytes.NewReader(truncated))
	require.Error(t, err)

	// The original data should still be accessible.
	val, getErr := s.GetAt(ctx, []byte("existing"), 10)
	require.NoError(t, getErr)
	assert.Equal(t, []byte("value"), val)
}

// TestPebbleStore_PutAt_ValueTooLarge verifies that PutAt rejects values
// exceeding maxSnapshotValueSize to ensure write and restore are consistent.
func TestPebbleStore_PutAt_ValueTooLarge(t *testing.T) {
	orig := maxSnapshotValueSize
	maxSnapshotValueSize = 100
	t.Cleanup(func() { maxSnapshotValueSize = orig })

	dir, err := os.MkdirTemp("", "pebble-value-limit-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer func() { assert.NoError(t, s.Close()) }()

	ctx := context.Background()
	oversized := make([]byte, maxSnapshotValueSize+1)
	err = s.PutAt(ctx, []byte("k"), oversized, 1, 0)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrValueTooLarge)
}

// TestPebbleStore_ApplyMutations_ValueTooLarge verifies that ApplyMutations
// rejects Put mutations with values exceeding maxSnapshotValueSize.
func TestPebbleStore_ApplyMutations_ValueTooLarge(t *testing.T) {
	orig := maxSnapshotValueSize
	maxSnapshotValueSize = 100
	t.Cleanup(func() { maxSnapshotValueSize = orig })

	dir, err := os.MkdirTemp("", "pebble-apply-value-limit-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer func() { assert.NoError(t, s.Close()) }()

	ctx := context.Background()
	oversized := make([]byte, maxSnapshotValueSize+1)
	err = s.ApplyMutations(ctx, []*KVPairMutation{
		{Op: OpTypePut, Key: []byte("k"), Value: oversized},
	}, nil, 0, 1)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrValueTooLarge)
}

// TestMVCCStore_PutAt_ValueTooLarge verifies that the in-memory mvccStore
// also rejects oversized values.
func TestMVCCStore_PutAt_ValueTooLarge(t *testing.T) {
	orig := maxSnapshotValueSize
	maxSnapshotValueSize = 100
	t.Cleanup(func() { maxSnapshotValueSize = orig })

	s := NewMVCCStore()
	defer s.Close()

	ctx := context.Background()
	oversized := make([]byte, maxSnapshotValueSize+1)
	err := s.PutAt(ctx, []byte("k"), oversized, 1, 0)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrValueTooLarge)
}

// TestMVCCStore_ApplyMutations_ValueTooLarge verifies that the in-memory
// mvccStore rejects oversized Put mutations.
func TestMVCCStore_ApplyMutations_ValueTooLarge(t *testing.T) {
	orig := maxSnapshotValueSize
	maxSnapshotValueSize = 100
	t.Cleanup(func() { maxSnapshotValueSize = orig })

	s := NewMVCCStore()
	defer s.Close()

	ctx := context.Background()
	oversized := make([]byte, maxSnapshotValueSize+1)
	err := s.ApplyMutations(ctx, []*KVPairMutation{
		{Op: OpTypePut, Key: []byte("k"), Value: oversized},
	}, nil, 0, 1)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrValueTooLarge)
}

// TestPebbleStore_SnapshotRestore_MaxSizeKey verifies that a key of exactly
// maxSnapshotKeySize bytes survives a Snapshot()+Restore() round-trip.
// The encoded on-disk Pebble key is maxSnapshotKeySize+timestampSize bytes;
// previously the restore path used maxSnapshotKeySize as the limit and would
// reject this key.
func TestPebbleStore_SnapshotRestore_MaxSizeKey(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-maxkey-snap-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer func() { assert.NoError(t, s.Close()) }()

	ctx := context.Background()
	bigKey := bytes.Repeat([]byte("k"), maxSnapshotKeySize)
	require.NoError(t, s.PutAt(ctx, bigKey, []byte("val"), 1, 0))

	snap, err := s.Snapshot()
	require.NoError(t, err)
	defer func() { assert.NoError(t, snap.Close()) }()

	var buf bytes.Buffer
	_, err = snap.WriteTo(&buf)
	require.NoError(t, err)

	dir2, err := os.MkdirTemp("", "pebble-maxkey-restore-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir2)

	s2, err := NewPebbleStore(dir2)
	require.NoError(t, err)
	defer func() { assert.NoError(t, s2.Close()) }()

	require.NoError(t, s2.Restore(bytes.NewReader(buf.Bytes())))

	got, err := s2.GetAt(ctx, bigKey, 1)
	require.NoError(t, err)
	assert.Equal(t, []byte("val"), got)
}
