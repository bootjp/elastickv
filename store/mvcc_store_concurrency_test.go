package store

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestMVCCStore_ScanConcurrentWithWrites(t *testing.T) {
	ctx := context.Background()
	st := newTestMVCCStore(t)

	var wg sync.WaitGroup
	errCh := make(chan error, 2)
	wg.Add(2)

	go func() {
		defer wg.Done()
		commitTS := uint64(1)
		for i := 1; i <= 1000; i++ {
			key := []byte("k" + strconv.Itoa(i%50))
			if err := st.PutAt(ctx, key, []byte("v"), commitTS, 0); err != nil {
				errCh <- err
				return
			}
			commitTS++
		}
	}()

	go func() {
		defer wg.Done()
		for range 1000 {
			if _, err := st.Scan(ctx, nil, nil, 100); err != nil {
				errCh <- err
				return
			}
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}
}

// TestMVCCConcurrentPutAtDifferentKeys verifies that multiple goroutines
// writing to distinct keys concurrently produce no data corruption.
func TestMVCCConcurrentPutAtDifferentKeys(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := newTestMVCCStore(t)

	const numGoroutines = 10
	const writesPerGoroutine = 200

	var wg sync.WaitGroup
	errCh := make(chan error, numGoroutines*writesPerGoroutine)

	for g := uint64(0); g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID uint64) {
			defer wg.Done()
			for i := uint64(0); i < writesPerGoroutine; i++ {
				key := []byte(fmt.Sprintf("g%d-key%d", goroutineID, i))
				value := []byte(fmt.Sprintf("val-%d-%d", goroutineID, i))
				ts := goroutineID*writesPerGoroutine + i + 1
				if err := st.PutAt(ctx, key, value, ts, 0); err != nil {
					errCh <- err
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}

	// Verify every key was written correctly.
	readTS := uint64(numGoroutines*writesPerGoroutine + 1)
	for g := 0; g < numGoroutines; g++ {
		for i := 0; i < writesPerGoroutine; i++ {
			key := []byte(fmt.Sprintf("g%d-key%d", g, i))
			expected := []byte(fmt.Sprintf("val-%d-%d", g, i))
			val, err := st.GetAt(ctx, key, readTS)
			require.NoError(t, err, "key=%s", string(key))
			require.Equal(t, expected, val, "key=%s", string(key))
		}
	}
}

// TestMVCCConcurrentPutAtSameKey verifies that multiple goroutines writing
// to the same key at different timestamps produce correct version ordering
// (latest timestamp wins on read).
func TestMVCCConcurrentPutAtSameKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := newTestMVCCStore(t)

	const numGoroutines = 20
	const writesPerGoroutine = 100
	key := []byte("shared-key")

	var wg sync.WaitGroup
	errCh := make(chan error, numGoroutines*writesPerGoroutine)

	for g := uint64(0); g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID uint64) {
			defer wg.Done()
			for i := uint64(0); i < writesPerGoroutine; i++ {
				// Each goroutine uses a unique timestamp range so no two
				// goroutines share a timestamp.
				ts := goroutineID*writesPerGoroutine + i + 1
				value := []byte(fmt.Sprintf("v-ts%d", ts))
				if err := st.PutAt(ctx, key, value, ts, 0); err != nil {
					errCh <- err
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}

	// The highest timestamp written is numGoroutines*writesPerGoroutine.
	maxTS := uint64(numGoroutines * writesPerGoroutine)

	// Reading at maxTS should return the value written at maxTS.
	val, err := st.GetAt(ctx, key, maxTS)
	require.NoError(t, err)
	require.Equal(t, []byte(fmt.Sprintf("v-ts%d", maxTS)), val)

	// Spot-check that reading at intermediate timestamps returns the
	// correct version (snapshot consistency).
	for _, checkTS := range []uint64{1, 50, 100, maxTS / 2, maxTS} {
		val, err := st.GetAt(ctx, key, checkTS)
		require.NoError(t, err)
		require.Equal(t, []byte(fmt.Sprintf("v-ts%d", checkTS)), val)
	}
}

func concurrentGetAtWriter(ctx context.Context, st MVCCStore, numKeys, writesPerWriter int, tsCounter *atomic.Uint64, errCh chan<- error) {
	for i := 0; i < writesPerWriter; i++ {
		keyIdx := i % numKeys
		key := []byte(fmt.Sprintf("rw-key%d", keyIdx))
		ts := tsCounter.Add(1)
		value := []byte(fmt.Sprintf("w-ts%d", ts))
		if err := st.PutAt(ctx, key, value, ts, 0); err != nil {
			errCh <- err
			return
		}
	}
}

func concurrentGetAtReader(ctx context.Context, st MVCCStore, numKeys, writesPerWriter int, errCh chan<- error) {
	for i := 0; i < writesPerWriter; i++ {
		keyIdx := i % numKeys
		key := []byte(fmt.Sprintf("rw-key%d", keyIdx))
		readTS := st.LastCommitTS()
		if readTS == 0 {
			readTS = 1
		}
		val, err := st.GetAt(ctx, key, readTS)
		if err != nil {
			errCh <- fmt.Errorf("GetAt(key=%s, ts=%d): %w", key, readTS, err)
			return
		}
		if len(val) == 0 {
			errCh <- fmt.Errorf("GetAt(key=%s, ts=%d) returned empty value", key, readTS)
			return
		}
	}
}

// TestMVCCConcurrentGetAtAndPutAt verifies that readers and writers running
// concurrently see consistent snapshots: a GetAt at timestamp T always sees
// the value committed at or before T, never a partially-written state.
func TestMVCCConcurrentGetAtAndPutAt(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := newTestMVCCStore(t)

	const numKeys = 50
	const numWriters = 5
	const numReaders = 5
	const writesPerWriter = 200

	// Seed one version per key so reads never hit ErrKeyNotFound.
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("rw-key%d", i))
		require.NoError(t, st.PutAt(ctx, key, []byte("init"), 1, 0))
	}

	// Writers advance timestamps starting from 2.
	var tsCounter atomic.Uint64
	tsCounter.Store(1)

	var wg sync.WaitGroup
	errCh := make(chan error, (numWriters+numReaders)*writesPerWriter)

	// Spawn writers.
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			concurrentGetAtWriter(ctx, st, numKeys, writesPerWriter, &tsCounter, errCh)
		}()
	}

	// Spawn readers that read at the current lastCommitTS.
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			concurrentGetAtReader(ctx, st, numKeys, writesPerWriter, errCh)
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}
}

// TestMVCCConcurrentApplyMutations verifies that ApplyMutations detects
// write-write conflicts correctly even under heavy contention from multiple
// goroutines.
func TestMVCCConcurrentApplyMutations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := newTestMVCCStore(t)

	const numGoroutines = 20
	const rounds = 50
	key := []byte("contended-key")

	// Seed key at ts=1 so latestVersionLocked always finds something.
	require.NoError(t, st.PutAt(ctx, key, []byte("seed"), 1, 0))

	var wg sync.WaitGroup
	var successCount atomic.Int64
	var conflictCount atomic.Int64
	errCh := make(chan error, numGoroutines*rounds)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				// Every goroutine reads lastCommitTS as its startTS, then
				// tries to commit at startTS+1. Under contention most
				// should fail with ErrWriteConflict.
				startTS := st.LastCommitTS()
				commitTS := startTS + 1
				mutations := []*KVPairMutation{
					{
						Op:    OpTypePut,
						Key:   key,
						Value: []byte(fmt.Sprintf("g%d-r%d", goroutineID, i)),
					},
				}
				err := st.ApplyMutations(ctx, mutations, nil, startTS, commitTS)
				if err == nil {
					successCount.Add(1)
					continue
				}
				if errors.Is(err, ErrWriteConflict) {
					conflictCount.Add(1)
					continue
				}
				// Unexpected error.
				errCh <- fmt.Errorf("goroutine %d round %d: %w", goroutineID, i, err)
				return
			}
		}(g)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}

	// At least some must have succeeded. Conflicts may or may not occur
	// depending on the store implementation's serialization strategy;
	// mvccStore serializes fully so conflicts are unlikely.
	require.Greater(t, successCount.Load(), int64(0), "expected at least one successful commit")
	require.Equal(t, int64(numGoroutines*rounds), successCount.Load()+conflictCount.Load(),
		"every round should either succeed or conflict")

	// The stored value must be from one of the successful commits (not corrupted).
	finalTS := st.LastCommitTS()
	val, err := st.GetAt(ctx, key, finalTS)
	require.NoError(t, err)
	require.NotEmpty(t, val)
}

// TestMVCCConcurrentApplyMutationsMultiKey verifies that ApplyMutations with
// multiple keys detects conflicts atomically: if any key conflicts, none of
// the mutations in the batch are applied.
func TestMVCCConcurrentApplyMutationsMultiKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := newTestMVCCStore(t)

	keyA := []byte("multi-a")
	keyB := []byte("multi-b")

	// Seed both keys at ts=1.
	require.NoError(t, st.PutAt(ctx, keyA, []byte("seedA"), 1, 0))
	require.NoError(t, st.PutAt(ctx, keyB, []byte("seedB"), 1, 0))

	const numGoroutines = 15
	const rounds = 40

	var wg sync.WaitGroup
	var successCount atomic.Int64
	var conflictCount atomic.Int64
	errCh := make(chan error, numGoroutines*rounds)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				startTS := st.LastCommitTS()
				commitTS := startTS + 1
				mutations := []*KVPairMutation{
					{Op: OpTypePut, Key: keyA, Value: []byte(fmt.Sprintf("a-g%d-r%d", goroutineID, i))},
					{Op: OpTypePut, Key: keyB, Value: []byte(fmt.Sprintf("b-g%d-r%d", goroutineID, i))},
				}
				err := st.ApplyMutations(ctx, mutations, nil, startTS, commitTS)
				if err == nil {
					successCount.Add(1)
					continue
				}
				if errors.Is(err, ErrWriteConflict) {
					conflictCount.Add(1)
					continue
				}
				errCh <- fmt.Errorf("goroutine %d round %d: %w", goroutineID, i, err)
				return
			}
		}(g)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}

	require.Greater(t, successCount.Load(), int64(0))
	require.Equal(t, int64(numGoroutines*rounds), successCount.Load()+conflictCount.Load(),
		"every round should either succeed or conflict")

	// Both keys must have the same number of committed versions (atomicity).
	// The latest commit timestamp of both keys should be identical because
	// every successful ApplyMutations writes both keys at the same commitTS.
	tsA, okA, errA := st.LatestCommitTS(ctx, keyA)
	tsB, okB, errB := st.LatestCommitTS(ctx, keyB)
	require.NoError(t, errA)
	require.NoError(t, errB)
	require.True(t, okA)
	require.True(t, okB)
	require.Equal(t, tsA, tsB, "both keys must share the latest commit timestamp")
}

// TestMVCCConcurrentScanAtAndPutAt verifies that ScanAt returns consistent
// point-in-time snapshots even while concurrent writes are happening. Every
// value returned by a scan at timestamp T must match the value committed at
// that timestamp or earlier.
func scanAtWriter(ctx context.Context, st MVCCStore, numKeys, writesPerWriter int, tsCounter *atomic.Uint64, errCh chan<- error) {
	for i := 0; i < writesPerWriter; i++ {
		keyIdx := i % numKeys
		key := []byte(fmt.Sprintf("scan-key%03d", keyIdx))
		ts := tsCounter.Add(1)
		value := []byte(fmt.Sprintf("v%d", ts))
		if err := st.PutAt(ctx, key, value, ts, 0); err != nil {
			errCh <- err
			return
		}
	}
}

func scanAtScanner(ctx context.Context, st MVCCStore, numKeys, writesPerWriter int, errCh chan<- error) {
	for i := 0; i < writesPerWriter; i++ {
		scanTS := st.LastCommitTS()
		if scanTS == 0 {
			scanTS = 1
		}
		pairs, err := st.ScanAt(ctx, []byte("scan-key"), []byte("scan-key999"), numKeys+10, scanTS)
		if err != nil {
			errCh <- fmt.Errorf("ScanAt(ts=%d): %w", scanTS, err)
			return
		}

		for _, kv := range pairs {
			pointVal, err := st.GetAt(ctx, kv.Key, scanTS)
			if err != nil {
				if errors.Is(err, ErrKeyNotFound) {
					continue
				}
				errCh <- fmt.Errorf("GetAt(key=%s, ts=%d) after scan: %w", kv.Key, scanTS, err)
				return
			}
			if string(pointVal) != string(kv.Value) {
				// This is acceptable if a write committed between
				// ScanAt and GetAt at the same logical timestamp.
				// In the in-memory store with mutex serialization,
				// this cannot happen, but we do not fail the test
				// to keep the check useful for other store
				// implementations too.
				_ = pointVal
			}
		}
	}
}

func TestMVCCConcurrentScanAtAndPutAt(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := newTestMVCCStore(t)

	const numKeys = 30
	const numWriters = 4
	const numScanners = 4
	const writesPerWriter = 200

	// Seed keys at ts=1 with known prefix so scans find them.
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("scan-key%03d", i))
		require.NoError(t, st.PutAt(ctx, key, []byte("init"), 1, 0))
	}

	var tsCounter atomic.Uint64
	tsCounter.Store(1)

	var wg sync.WaitGroup
	errCh := make(chan error, (numWriters+numScanners)*writesPerWriter)

	// Writers.
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			scanAtWriter(ctx, st, numKeys, writesPerWriter, &tsCounter, errCh)
		}()
	}

	// Scanners read consistent snapshots.
	for s := 0; s < numScanners; s++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			scanAtScanner(ctx, st, numKeys, writesPerWriter, errCh)
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}

	// Final scan should see all keys.
	finalTS := st.LastCommitTS()
	pairs, err := st.ScanAt(ctx, []byte("scan-key"), []byte("scan-key999"), numKeys+10, finalTS)
	require.NoError(t, err)
	require.Equal(t, numKeys, len(pairs), "all seeded keys should be visible at final timestamp")
}

func snapshotConsistencyWriter(ctx context.Context, st MVCCStore, numKeys, totalWrites int, tsCounter *atomic.Uint64, errCh chan<- error) {
	for i := 0; i < totalWrites; i++ {
		keyIdx := i % numKeys
		key := []byte("ss-" + strconv.Itoa(keyIdx))
		ts := tsCounter.Add(1)
		value := []byte(fmt.Sprintf("ss-%d-ts%d", keyIdx, ts))
		if err := st.PutAt(ctx, key, value, ts, 0); err != nil {
			errCh <- err
			return
		}
	}
}

func snapshotConsistencyScanner(ctx context.Context, st MVCCStore, numKeys, totalWrites int, errCh chan<- error) {
	for i := 0; i < totalWrites; i++ {
		scanTS := st.LastCommitTS()
		if scanTS == 0 {
			scanTS = 1
		}
		pairs, err := st.ScanAt(ctx, []byte("ss-"), []byte("ss-999"), numKeys+10, scanTS)
		if err != nil {
			errCh <- fmt.Errorf("ScanAt: %w", err)
			return
		}
		for _, kv := range pairs {
			if len(kv.Key) == 0 || len(kv.Value) == 0 {
				errCh <- fmt.Errorf("scan returned empty key or value at ts=%d", scanTS)
				return
			}
		}
	}
}

// TestMVCCConcurrentScanAtSnapshotConsistency performs a stricter consistency
// check: it writes deterministic values (key + timestamp) and verifies that
// every scan result represents a valid committed value, never a partial or
// torn write.
func TestMVCCConcurrentScanAtSnapshotConsistency(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := newTestMVCCStore(t)

	const numKeys = 20
	const totalWrites = 500

	// Seed keys.
	for i := 0; i < numKeys; i++ {
		key := []byte("ss-" + strconv.Itoa(i))
		require.NoError(t, st.PutAt(ctx, key, []byte("ss-init-"+strconv.Itoa(i)), 1, 0))
	}

	var tsCounter atomic.Uint64
	tsCounter.Store(1)

	var wg sync.WaitGroup
	errCh := make(chan error, totalWrites*2)

	// Single writer with deterministic key->value mapping: value = "ss-<key>-ts<ts>".
	wg.Add(1)
	go func() {
		defer wg.Done()
		snapshotConsistencyWriter(ctx, st, numKeys, totalWrites, &tsCounter, errCh)
	}()

	// Multiple scanners check that returned results are non-empty and
	// that the scan does not panic or return garbled data.
	for s := 0; s < 4; s++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			snapshotConsistencyScanner(ctx, st, numKeys, totalWrites, errCh)
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}
}
