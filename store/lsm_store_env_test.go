package store

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

// setPebbleCacheBytesForTest swaps the package-level cache capacity for one
// test. defaultPebbleOptionsWithCache replaces the process cache lazily when
// it observes a different capacity, so callers retain the production sharing
// and reference-counting path.
func setPebbleCacheBytesForTest(t *testing.T, capacity int64) {
	t.Helper()
	prev := pebbleCacheBytes
	pebbleCacheBytes = capacity
	t.Cleanup(func() { pebbleCacheBytes = prev })
}

func setSmallPebbleCacheForTest(t *testing.T) {
	t.Helper()
	setPebbleCacheBytesForTest(t, 16<<20)
}

// TestPebbleCacheEnvOverride covers the absolute and percentage-based cache
// sizing contract directly against resolvePebbleCacheBytes, which is what
// init() calls. We deliberately avoid mutating os.Environ here so parallel
// test binaries remain isolated.
func TestPebbleCacheEnvOverride(t *testing.T) {
	const memoryBudgetBytes int64 = 4000 << 20

	t.Run("valid value is parsed and converted to bytes", func(t *testing.T) {
		got := resolvePebbleCacheBytes("64", "", memoryBudgetBytes)
		require.Equal(t, int64(64)<<20, got)
	})

	t.Run("empty string uses twenty five percent of memory budget", func(t *testing.T) {
		require.Equal(t, int64(1000)<<20, resolvePebbleCacheBytes("", "", memoryBudgetBytes))
	})

	t.Run("explicit percentage is applied", func(t *testing.T) {
		require.Equal(t, int64(400)<<20, resolvePebbleCacheBytes("", "10", memoryBudgetBytes))
	})

	t.Run("absolute override wins over percentage", func(t *testing.T) {
		require.Equal(t, int64(64)<<20, resolvePebbleCacheBytes("64", "10", memoryBudgetBytes))
	})

	t.Run("garbage absolute input falls back to percentage default", func(t *testing.T) {
		require.Equal(t, int64(1000)<<20, resolvePebbleCacheBytes("garbage", "", memoryBudgetBytes))
	})

	t.Run("garbage percentage falls back to twenty five percent", func(t *testing.T) {
		require.Equal(t, int64(1000)<<20, resolvePebbleCacheBytes("", "garbage", memoryBudgetBytes))
	})

	t.Run("percentage boundaries are enforced", func(t *testing.T) {
		require.Equal(t, int64(40)<<20, resolvePebbleCacheBytes("", "1", memoryBudgetBytes))
		require.Equal(t, int64(3600)<<20, resolvePebbleCacheBytes("", "90", memoryBudgetBytes))
		require.Equal(t, int64(1000)<<20, resolvePebbleCacheBytes("", "0", memoryBudgetBytes))
		require.Equal(t, int64(1000)<<20, resolvePebbleCacheBytes("", "91", memoryBudgetBytes))
	})

	t.Run("zero is rejected and percentage default is applied", func(t *testing.T) {
		// "0" is below the 8 MiB floor, so we fall back to the default
		// rather than silently clamping. Documented behaviour: only
		// values inside [8, 65536] MiB are accepted; everything else
		// falls back.
		require.Equal(t, int64(1000)<<20, resolvePebbleCacheBytes("0", "", memoryBudgetBytes))
	})

	t.Run("below floor falls back to percentage default", func(t *testing.T) {
		require.Equal(t, int64(1000)<<20, resolvePebbleCacheBytes("4", "", memoryBudgetBytes))
	})

	t.Run("at floor is accepted", func(t *testing.T) {
		require.Equal(t, int64(8)<<20, resolvePebbleCacheBytes("8", "", memoryBudgetBytes))
	})

	t.Run("at ceiling is accepted", func(t *testing.T) {
		require.Equal(t, int64(65536)<<20, resolvePebbleCacheBytes("65536", "", memoryBudgetBytes))
	})

	t.Run("above ceiling falls back to percentage default", func(t *testing.T) {
		require.Equal(t, int64(1000)<<20, resolvePebbleCacheBytes("65537", "", memoryBudgetBytes))
	})

	t.Run("negative falls back to percentage default", func(t *testing.T) {
		require.Equal(t, int64(1000)<<20, resolvePebbleCacheBytes("-1", "", memoryBudgetBytes))
	})

	t.Run("unknown memory budget uses fixed fallback", func(t *testing.T) {
		require.Equal(t, defaultPebbleCacheBytes, resolvePebbleCacheBytes("", "", 0))
	})

	t.Run("percentage result observes cache floor", func(t *testing.T) {
		require.Equal(t, int64(8)<<20, resolvePebbleCacheBytes("", "", 16<<20))
	})
}

func TestMemoryBudgetHelpers(t *testing.T) {
	require.Equal(t, int64(512), minPositiveMemoryBytes(0, 1024, -1, 512, 2048))
	require.Equal(t, int64(0), minPositiveMemoryBytes(0, -1))
	require.Equal(t, int64(1024), parseMemoryLimitBytes([]byte("1024\n")))
	require.Equal(t, int64(0), parseMemoryLimitBytes([]byte("max\n")))
	require.Equal(t, int64(0), parseMemoryLimitBytes([]byte("invalid")))
	require.Equal(t, []string{
		"/sys/fs/cgroup/system.slice/elastickv.service/memory.max",
		"/sys/fs/cgroup/system.slice/memory.max",
		"/sys/fs/cgroup/memory.max",
	}, cgroupMemoryLimitPaths([]byte("0::/system.slice/elastickv.service\n")))
	require.Equal(t, []string{
		"/sys/fs/cgroup/memory/docker/abc/memory.limit_in_bytes",
		"/sys/fs/cgroup/memory/docker/memory.limit_in_bytes",
		"/sys/fs/cgroup/memory/memory.limit_in_bytes",
	}, cgroupMemoryLimitPaths([]byte("5:cpu,memory:/docker/abc\n")))
	require.Equal(t, []string{
		"/sys/fs/cgroup/kubepods/pod-1/container-1/memory.max",
		"/sys/fs/cgroup/kubepods/pod-1/memory.max",
		"/sys/fs/cgroup/kubepods/memory.max",
		"/sys/fs/cgroup/memory.max",
	}, cgroupAncestorLimitPaths("/sys/fs/cgroup", "kubepods/pod-1/container-1", "memory.max"))
	require.Empty(t, cgroupAncestorLimitPaths("/sys/fs/cgroup", "../escape", "memory.max"))
}

// TestSetSmallPebbleCacheForTestRestores verifies the helper reinstates the
// previous value via t.Cleanup so tests that tweak pebbleCacheBytes do not
// leak state to later tests in the package.
func TestSetSmallPebbleCacheForTestRestores(t *testing.T) {
	before := pebbleCacheBytes
	t.Run("inner", func(t *testing.T) {
		setSmallPebbleCacheForTest(t)
		require.Equal(t, int64(16)<<20, pebbleCacheBytes)
	})
	require.Equal(t, before, pebbleCacheBytes)
}

// TestDefaultPebbleOptionsCarriesCache sanity-checks that the options
// constructor wires the process-shared cache through at the configured size
// and that Unref is safe for each borrowed store/open reference.
func TestDefaultPebbleOptionsCarriesCache(t *testing.T) {
	setSmallPebbleCacheForTest(t)
	opts, cache := defaultPebbleOptionsWithCache(false)
	require.NotNil(t, cache)
	require.Same(t, cache, opts.Cache)
	require.Equal(t, int64(16)<<20, cache.MaxSize())
	cache.Unref()
}

func TestDefaultPebbleOptionsSharesProcessCache(t *testing.T) {
	setSmallPebbleCacheForTest(t)
	opts1, cache1 := defaultPebbleOptionsWithCache(false)
	defer cache1.Unref()
	opts2, cache2 := defaultPebbleOptionsWithCache(false)
	defer cache2.Unref()

	require.Same(t, cache1, cache2)
	require.Same(t, cache1, opts1.Cache)
	require.Same(t, cache1, opts2.Cache)
}

func TestDefaultPebbleOptionsSharesProcessCacheConcurrently(t *testing.T) {
	setSmallPebbleCacheForTest(t)

	const borrowers = 16
	caches := make(chan *pebble.Cache, borrowers)
	var wg sync.WaitGroup
	wg.Add(borrowers)
	for range borrowers {
		go func() {
			defer wg.Done()
			_, cache := defaultPebbleOptionsWithCache(false)
			caches <- cache
		}()
	}
	wg.Wait()
	close(caches)

	var shared *pebble.Cache
	for cache := range caches {
		if shared == nil {
			shared = cache
		}
		require.Same(t, shared, cache)
		cache.Unref()
	}
}

func TestNewPebbleStoreSharesProcessCache(t *testing.T) {
	setSmallPebbleCacheForTest(t)

	s1, err := NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	closed1 := false
	t.Cleanup(func() {
		if !closed1 {
			_ = s1.Close()
		}
	})
	ps1, ok := s1.(*pebbleStore)
	require.True(t, ok)

	s2, err := NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	defer s2.Close()
	ps2, ok := s2.(*pebbleStore)
	require.True(t, ok)

	require.NotNil(t, ps1.cache)
	require.Same(t, ps1.cache, ps2.cache)
	require.Equal(t, int64(16)<<20, ps1.BlockCacheCapacityBytes())
	require.Equal(t, ps1.BlockCacheCapacityBytes(), ps2.BlockCacheCapacityBytes())

	require.NoError(t, s1.Close())
	closed1 = true
	require.NoError(t, s2.PutAt(context.Background(), []byte("still-open"), []byte("value"), 1, 0))
	value, err := s2.GetAt(context.Background(), []byte("still-open"), 1)
	require.NoError(t, err)
	require.Equal(t, []byte("value"), value)
}

func TestPebbleStoreRestoreKeepsProcessCache(t *testing.T) {
	setSmallPebbleCacheForTest(t)
	ctx := context.Background()

	src := NewMVCCStore()
	require.NoError(t, src.PutAt(ctx, []byte("key"), []byte("value"), 1, 0))
	snapshot, err := src.Snapshot()
	require.NoError(t, err)
	var streaming bytes.Buffer
	_, err = snapshot.WriteTo(&streaming)
	require.NoError(t, err)
	require.NoError(t, snapshot.Close())

	dst, err := NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	defer dst.Close()
	ps, ok := dst.(*pebbleStore)
	require.True(t, ok)
	cache := ps.cache

	require.NoError(t, dst.Restore(bytes.NewReader(streaming.Bytes())))
	require.Same(t, cache, ps.cache, "streaming restore must reopen on the process cache")

	nativeSnapshot, err := dst.Snapshot()
	require.NoError(t, err)
	var native bytes.Buffer
	_, err = nativeSnapshot.WriteTo(&native)
	require.NoError(t, err)
	require.NoError(t, nativeSnapshot.Close())
	require.NoError(t, dst.Restore(bytes.NewReader(native.Bytes())))
	require.Same(t, cache, ps.cache, "native restore must reopen on the process cache")

	require.NoError(t, dst.Restore(bytes.NewReader(nil)))
	require.Same(t, cache, ps.cache, "empty restore must reopen on the process cache")
}

// newPebbleStoreWithFSMApplyWriteOptsForTest constructs a pebbleStore
// (not the MVCCStore interface) with an explicit *pebble.WriteOptions
// and sync-mode label for the FSM commit path, bypassing the
// ELASTICKV_FSM_SYNC_MODE env resolution. Tests use this to exercise
// both sync and nosync modes deterministically without mutating
// os.Environ (which would leak into parallel test binaries).
//
// The store is created via NewPebbleStore and then the relevant
// fields are overridden; this keeps the full init path (cache,
// metadata scans, etc.) identical to production while only swapping
// the write-options that govern commit-time durability.
func newPebbleStoreWithFSMApplyWriteOptsForTest(t *testing.T, dir string, opts *pebble.WriteOptions, label string) *pebbleStore {
	t.Helper()
	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	ps, ok := s.(*pebbleStore)
	require.True(t, ok, "NewPebbleStore returned non-*pebbleStore type")
	ps.fsmApplyWriteOpts = opts
	ps.fsmApplySyncModeLabel = label
	return ps
}

// TestFSMApplySyncModeEnvOverride covers the ELASTICKV_FSM_SYNC_MODE
// parsing contract directly against resolveFSMApplyWriteOpts, which is
// what NewPebbleStore calls. Mirrors the approach taken for
// ELASTICKV_PEBBLE_CACHE_MB to avoid mutating os.Environ at runtime.
func TestFSMApplySyncModeEnvOverride(t *testing.T) {
	t.Run("empty string uses sync default", func(t *testing.T) {
		opts, label := resolveFSMApplyWriteOpts("")
		require.Same(t, pebble.Sync, opts)
		require.Equal(t, fsmSyncModeSync, label)
	})

	t.Run("explicit sync is accepted", func(t *testing.T) {
		opts, label := resolveFSMApplyWriteOpts("sync")
		require.Same(t, pebble.Sync, opts)
		require.Equal(t, fsmSyncModeSync, label)
	})

	t.Run("explicit nosync is accepted", func(t *testing.T) {
		opts, label := resolveFSMApplyWriteOpts("nosync")
		require.Same(t, pebble.NoSync, opts)
		require.Equal(t, fsmSyncModeNoSync, label)
	})

	t.Run("mixed-case nosync is accepted", func(t *testing.T) {
		opts, label := resolveFSMApplyWriteOpts("NoSync")
		require.Same(t, pebble.NoSync, opts)
		require.Equal(t, fsmSyncModeNoSync, label)
	})

	t.Run("whitespace is trimmed", func(t *testing.T) {
		opts, label := resolveFSMApplyWriteOpts("  nosync\n")
		require.Same(t, pebble.NoSync, opts)
		require.Equal(t, fsmSyncModeNoSync, label)
	})

	t.Run("unknown value falls back to sync", func(t *testing.T) {
		// "batch" was considered in the design discussion but never
		// implemented; the resolver must not crash or silently enable
		// NoSync if an operator sets an unsupported value.
		opts, label := resolveFSMApplyWriteOpts("batch")
		require.Same(t, pebble.Sync, opts)
		require.Equal(t, fsmSyncModeSync, label)
	})

	t.Run("garbage falls back to sync", func(t *testing.T) {
		opts, label := resolveFSMApplyWriteOpts("garbage")
		require.Same(t, pebble.Sync, opts)
		require.Equal(t, fsmSyncModeSync, label)
	})
}

// TestFSMApplySyncModeLabelAccessor verifies that a constructed
// pebbleStore exposes its resolved sync-mode label via the per-instance
// accessor, so monitoring can read the mode off a concrete store
// instead of a package global.
func TestFSMApplySyncModeLabelAccessor(t *testing.T) {
	t.Run("nosync", func(t *testing.T) {
		ps := newPebbleStoreWithFSMApplyWriteOptsForTest(t, t.TempDir(), pebble.NoSync, fsmSyncModeNoSync)
		defer ps.Close()
		require.Equal(t, fsmSyncModeNoSync, ps.FSMApplySyncModeLabel())
	})
	t.Run("sync", func(t *testing.T) {
		ps := newPebbleStoreWithFSMApplyWriteOptsForTest(t, t.TempDir(), pebble.Sync, fsmSyncModeSync)
		defer ps.Close()
		require.Equal(t, fsmSyncModeSync, ps.FSMApplySyncModeLabel())
	})
}
