package adapter

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

// TestConcurrentRPush_EventualSuccess verifies that concurrent RPUSH
// operations on the same list key eventually succeed. Item key conflicts
// may cause retries, but all pushes should complete with retries.
func TestConcurrentRPush_EventualSuccess(t *testing.T) {
	t.Parallel()

	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	server := nodes[0].redisServer
	ctx := context.Background()
	key := []byte("concurrent-push")

	const goroutines = 5
	const pushesPerGoroutine = 3

	var wg sync.WaitGroup
	var totalPushed atomic.Int64
	wg.Add(goroutines)

	for g := range goroutines {
		go func(id int) {
			defer wg.Done()
			for i := range pushesPerGoroutine {
				val := fmt.Sprintf("g%d-v%d", id, i)
				// Retry on write conflict (item key collision from stale seq).
				for attempt := range 10 {
					_, err := server.listRPush(ctx, key, [][]byte{[]byte(val)})
					if err == nil {
						totalPushed.Add(1)
						break
					}
					if attempt == 9 {
						t.Logf("RPUSH failed after retries: %v", err)
					}
				}
			}
		}(g)
	}
	wg.Wait()

	// All pushes should eventually succeed.
	require.Equal(t, int64(goroutines*pushesPerGoroutine), totalPushed.Load())

	// Verify total count via resolveListMeta.
	readTS := server.readTS()
	meta, exists, err := server.resolveListMeta(ctx, key, readTS)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, int64(goroutines*pushesPerGoroutine), meta.Len)
}

// TestConcurrentRPush_ThenCompact verifies that data survives compaction.
func TestConcurrentRPush_ThenCompact(t *testing.T) {
	t.Parallel()

	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	server := nodes[0].redisServer
	ctx := context.Background()
	key := []byte("push-then-compact")

	const pushCount = 20
	for i := range pushCount {
		_, err := server.listRPush(ctx, key, [][]byte{[]byte(fmt.Sprintf("v%d", i))})
		require.NoError(t, err)
	}

	// Verify delta count.
	readTS := server.readTS()
	prefix := store.ListMetaDeltaScanPrefix(key)
	deltas, err := server.store.ScanAt(ctx, prefix, store.PrefixScanEnd(prefix), 1000, readTS)
	require.NoError(t, err)
	require.Equal(t, pushCount, len(deltas))

	// Compact.
	compactor := NewListDeltaCompactor(server.store, server.coordinator,
		WithListCompactorMaxDeltaCount(1),
	)
	require.NoError(t, compactor.Tick(ctx))

	// After compaction, deltas should be gone and base meta correct.
	readTS = server.readTS()
	deltas, err = server.store.ScanAt(ctx, prefix, store.PrefixScanEnd(prefix), 1000, readTS)
	require.NoError(t, err)
	require.Empty(t, deltas)

	meta, exists, err := server.resolveListMeta(ctx, key, readTS)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, int64(pushCount), meta.Len)

	// Verify data integrity: all items readable.
	values, err := server.listValuesAt(ctx, key, readTS)
	require.NoError(t, err)
	require.Len(t, values, pushCount)
}

// TestConcurrentLPush_EventualSuccess verifies concurrent LPUSH operations
// on the same key eventually succeed with retries on item key conflicts.
func TestConcurrentLPush_EventualSuccess(t *testing.T) {
	t.Parallel()

	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	server := nodes[0].redisServer
	ctx := context.Background()
	key := []byte("concurrent-lpush")

	const goroutines = 5
	const pushesPerGoroutine = 3

	var wg sync.WaitGroup
	var totalPushed atomic.Int64
	wg.Add(goroutines)

	for g := range goroutines {
		go func(id int) {
			defer wg.Done()
			for i := range pushesPerGoroutine {
				val := fmt.Sprintf("g%d-v%d", id, i)
				for attempt := range 10 {
					_, err := server.listLPush(ctx, key, [][]byte{[]byte(val)})
					if err == nil {
						totalPushed.Add(1)
						break
					}
					if attempt == 9 {
						t.Logf("LPUSH failed after retries: %v", err)
					}
				}
			}
		}(g)
	}
	wg.Wait()

	require.Equal(t, int64(goroutines*pushesPerGoroutine), totalPushed.Load())

	readTS := server.readTS()
	meta, exists, err := server.resolveListMeta(ctx, key, readTS)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, int64(goroutines*pushesPerGoroutine), meta.Len)
}

// TestResolveListMeta_ScalesWithDeltaCount verifies that resolveListMeta
// produces correct results across different delta accumulation levels.
func TestResolveListMeta_ScalesWithDeltaCount(t *testing.T) {
	t.Parallel()

	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	server := nodes[0].redisServer
	ctx := context.Background()

	for _, count := range []int{1, 10, 50, 100} {
		key := []byte(fmt.Sprintf("scale-%d", count))
		for range count {
			_, err := server.listRPush(ctx, key, [][]byte{[]byte("x")})
			require.NoError(t, err)
		}

		readTS := server.readTS()
		meta, exists, err := server.resolveListMeta(ctx, key, readTS)
		require.NoError(t, err)
		require.True(t, exists)
		require.Equal(t, int64(count), meta.Len, "delta count %d", count)
	}
}
