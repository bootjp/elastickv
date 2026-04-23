package store

import (
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

// setPebbleCacheBytesForTest swaps the package-level pebbleCacheBytes value
// for the duration of a single test and restores it during t.Cleanup. The
// real override happens in init() from ELASTICKV_PEBBLE_CACHE_MB; tests use
// this helper to exercise specific cache sizes without relying on process
// env state.
func setPebbleCacheBytesForTest(t *testing.T, n int64) {
	t.Helper()
	prev := pebbleCacheBytes
	pebbleCacheBytes = n
	t.Cleanup(func() { pebbleCacheBytes = prev })
}

// TestPebbleCacheEnvOverride covers the ELASTICKV_PEBBLE_CACHE_MB parsing
// contract directly against resolvePebbleCacheBytes, which is what init()
// calls. We deliberately avoid mutating os.Environ here so parallel test
// binaries remain isolated.
func TestPebbleCacheEnvOverride(t *testing.T) {
	t.Run("valid value is parsed and converted to bytes", func(t *testing.T) {
		got := resolvePebbleCacheBytes("64")
		require.Equal(t, int64(64)<<20, got)
	})

	t.Run("empty string falls back to default", func(t *testing.T) {
		require.Equal(t, defaultPebbleCacheBytes, resolvePebbleCacheBytes(""))
	})

	t.Run("garbage input falls back to default", func(t *testing.T) {
		require.Equal(t, defaultPebbleCacheBytes, resolvePebbleCacheBytes("garbage"))
	})

	t.Run("zero is rejected and default is applied", func(t *testing.T) {
		// "0" is below the 8 MiB floor, so we fall back to the default
		// rather than silently clamping. Documented behaviour: only
		// values inside [8, 65536] MiB are accepted; everything else
		// falls back.
		require.Equal(t, defaultPebbleCacheBytes, resolvePebbleCacheBytes("0"))
	})

	t.Run("below floor falls back to default", func(t *testing.T) {
		require.Equal(t, defaultPebbleCacheBytes, resolvePebbleCacheBytes("4"))
	})

	t.Run("at floor is accepted", func(t *testing.T) {
		require.Equal(t, int64(8)<<20, resolvePebbleCacheBytes("8"))
	})

	t.Run("at ceiling is accepted", func(t *testing.T) {
		require.Equal(t, int64(65536)<<20, resolvePebbleCacheBytes("65536"))
	})

	t.Run("above ceiling falls back to default", func(t *testing.T) {
		require.Equal(t, defaultPebbleCacheBytes, resolvePebbleCacheBytes("65537"))
	})

	t.Run("negative falls back to default", func(t *testing.T) {
		require.Equal(t, defaultPebbleCacheBytes, resolvePebbleCacheBytes("-1"))
	})
}

// TestSetPebbleCacheBytesForTestRestores verifies the helper reinstates the
// previous value via t.Cleanup so tests that tweak pebbleCacheBytes do not
// leak state to later tests in the package.
func TestSetPebbleCacheBytesForTestRestores(t *testing.T) {
	before := pebbleCacheBytes
	t.Run("inner", func(t *testing.T) {
		setPebbleCacheBytesForTest(t, 16<<20)
		require.Equal(t, int64(16)<<20, pebbleCacheBytes)
	})
	require.Equal(t, before, pebbleCacheBytes)
}

// TestDefaultPebbleOptionsCarriesCache sanity-checks that the options
// constructor wires a cache through at the configured size and that Unref
// is safe to call after closing the DB (the primary lifecycle path).
func TestDefaultPebbleOptionsCarriesCache(t *testing.T) {
	setPebbleCacheBytesForTest(t, 16<<20)
	opts, cache := defaultPebbleOptionsWithCache()
	require.NotNil(t, cache)
	require.Same(t, cache, opts.Cache)
	require.Equal(t, int64(16)<<20, cache.MaxSize())
	cache.Unref()
}

// setFSMApplyWriteOptsForTest swaps the package-level fsmApplyWriteOpts
// and fsmApplySyncModeLabel for the duration of a single test and restores
// both during t.Cleanup. Tests use this instead of mutating os.Environ so
// parallel binaries remain isolated.
func setFSMApplyWriteOptsForTest(t *testing.T, opts *pebble.WriteOptions, label string) {
	t.Helper()
	prevOpts := fsmApplyWriteOpts
	prevLabel := fsmApplySyncModeLabel
	fsmApplyWriteOpts = opts
	fsmApplySyncModeLabel = label
	t.Cleanup(func() {
		fsmApplyWriteOpts = prevOpts
		fsmApplySyncModeLabel = prevLabel
	})
}

// TestFSMApplySyncModeEnvOverride covers the ELASTICKV_FSM_SYNC_MODE
// parsing contract directly against resolveFSMApplyWriteOpts, which is
// what init() calls. Mirrors the approach taken for
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

// TestFSMApplySyncModeLabelAccessor verifies the read-only public
// accessor returns the current label (so monitoring never reads the
// package var directly) and tracks the test helper.
func TestFSMApplySyncModeLabelAccessor(t *testing.T) {
	before := FSMApplySyncModeLabel()
	t.Run("nosync", func(t *testing.T) {
		setFSMApplyWriteOptsForTest(t, pebble.NoSync, fsmSyncModeNoSync)
		require.Equal(t, fsmSyncModeNoSync, FSMApplySyncModeLabel())
	})
	// Cleanup via t.Cleanup should restore the prior label.
	require.Equal(t, before, FSMApplySyncModeLabel())
}
