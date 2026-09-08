package encryption_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/kek"
	"github.com/stretchr/testify/require"
)

// countingKEK records how many real unwraps reached the provider.
type countingKEK struct {
	mu       sync.Mutex
	unwraps  int
	wraps    int
	failNext error
}

func (k *countingKEK) Name() string { return "counting" }

func (k *countingKEK) Wrap(dek []byte) ([]byte, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.wraps++
	return append([]byte("w:"), dek...), nil
}

func (k *countingKEK) Unwrap(wrapped []byte) ([]byte, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.unwraps++
	if k.failNext != nil {
		err := k.failNext
		k.failNext = nil
		return nil, err
	}
	out := make([]byte, encryption.KeySize)
	copy(out, wrapped)
	return out, nil
}

func (k *countingKEK) count() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.unwraps
}

// TestStartupUnwrapCacheCollapsesTheDuplicateStartupUnwrap is the point
// of the whole type: the §9.1 guards and HydrateKeystoreFromSidecar
// each unwrap every wrapped DEK, which is a doubled KMS round-trip per
// DEK now that Stage 9B shipped the network providers.
func TestStartupUnwrapCacheCollapsesTheDuplicateStartupUnwrap(t *testing.T) {
	t.Parallel()

	inner := &countingKEK{}
	cache := encryption.NewStartupUnwrapCache(inner)
	require.NotNil(t, cache)

	wrapped := [][]byte{[]byte("wrapped-dek-1"), []byte("wrapped-dek-2"), []byte("wrapped-dek-3")}

	// Guard phase.
	first := make([][]byte, 0, len(wrapped))
	for _, w := range wrapped {
		dek, err := cache.Unwrap(w)
		require.NoError(t, err)
		first = append(first, dek)
	}
	require.Equal(t, len(wrapped), inner.count())

	// Hydration phase over the same sidecar.
	for i, w := range wrapped {
		dek, err := cache.Unwrap(w)
		require.NoError(t, err)
		require.Equal(t, first[i], dek, "a cached unwrap must return identical key material")
	}
	require.Equal(t, len(wrapped), inner.count(),
		"the second pass over the same wrapped DEKs must not reach the provider")
}

// TestStartupUnwrapCacheReturnsANilInterfaceWithoutAKEK guards the
// typed-nil trap. Startup decides whether encryption mutators may run
// from `kekWrapper != nil`, so returning a typed nil pointer would
// make a node with NO KEK configured report that it has one.
func TestStartupUnwrapCacheReturnsANilInterfaceWithoutAKEK(t *testing.T) {
	t.Parallel()

	var absent kek.Wrapper
	// The assignment to an interface-typed variable is the whole test.
	// require.Nil is reflection-based and accepts a typed nil pointer,
	// and so does `got == nil` when got is the concrete pointer type —
	// both pass even when the bug is present. Only an interface-typed
	// comparison distinguishes "no wrapper" from "a wrapper that
	// happens to be nil inside", which is what startup branches on.
	got := asKEKWrapper(encryption.NewStartupUnwrapCache(absent))
	require.True(t, got == nil, //nolint:testifylint // the interface comparison IS the property under test.
		"must be a nil interface, not a typed nil pointer: startup reads kekWrapper != nil to decide whether encryption mutators may run")
}

// TestStartupUnwrapCacheDoesNotCacheFailures pins that a transient
// provider error is retried. Caching it would turn one flaky KMS call
// into a permanent startup refusal.
func TestStartupUnwrapCacheDoesNotCacheFailures(t *testing.T) {
	t.Parallel()

	boom := errors.New("kms timeout")
	inner := &countingKEK{failNext: boom}
	cache := encryption.NewStartupUnwrapCache(inner)

	_, err := cache.Unwrap([]byte("wrapped"))
	require.ErrorIs(t, err, boom)

	dek, err := cache.Unwrap([]byte("wrapped"))
	require.NoError(t, err, "a failed unwrap must not be remembered as a failure")
	require.Len(t, dek, encryption.KeySize)
	require.Equal(t, 2, inner.count())
}

// TestStartupUnwrapCacheIsolatesCallersFromEachOther covers a caller
// that zeroes its DEK after use: the cached entry must survive intact
// for the next reader.
func TestStartupUnwrapCacheIsolatesCallersFromEachOther(t *testing.T) {
	t.Parallel()

	cache := encryption.NewStartupUnwrapCache(&countingKEK{})

	first, err := cache.Unwrap([]byte("wrapped"))
	require.NoError(t, err)
	want := append([]byte(nil), first...)

	// The caller wipes its copy.
	for i := range first {
		first[i] = 0
	}

	second, err := cache.Unwrap([]byte("wrapped"))
	require.NoError(t, err)
	require.Equal(t, want, second, "a caller zeroing its DEK must not corrupt the cache")
}

// TestStartupUnwrapCacheResetDropsKeyMaterial pins that the cache does
// not hold a second copy of every DEK for the process lifetime.
func TestStartupUnwrapCacheResetDropsKeyMaterial(t *testing.T) {
	t.Parallel()

	inner := &countingKEK{}
	// Routed through asKEKWrapper so this test still compiles if the
	// constructor's return type changes, leaving the typed-nil test
	// free to fail on its own assertion rather than on a build error.
	cache := asKEKWrapper(encryption.NewStartupUnwrapCache(inner))
	concrete, ok := cache.(*encryption.StartupUnwrapCache)
	require.True(t, ok)

	_, err := cache.Unwrap([]byte("wrapped"))
	require.NoError(t, err)
	require.Equal(t, 1, concrete.Len())

	concrete.Reset()
	require.Zero(t, concrete.Len())

	// After a reset the provider is consulted again.
	_, err = cache.Unwrap([]byte("wrapped"))
	require.NoError(t, err)
	require.Equal(t, 2, inner.count())
}

// TestStartupUnwrapCacheDelegatesWrapAndName pins that the decorator
// stays transparent: Wrap must not be memoized, because providers may
// add fresh randomness per call.
func TestStartupUnwrapCacheDelegatesWrapAndName(t *testing.T) {
	t.Parallel()

	inner := &countingKEK{}
	cache := encryption.NewStartupUnwrapCache(inner)

	require.Equal(t, "counting", cache.Name())
	for range 3 {
		_, err := cache.Wrap([]byte("dek"))
		require.NoError(t, err)
	}
	inner.mu.Lock()
	defer inner.mu.Unlock()
	require.Equal(t, 3, inner.wraps, "Wrap must never be memoized")
}

// asKEKWrapper forces the interface conversion the production wiring
// performs when it assigns the constructor's result to a kek.Wrapper.
// The conversion is what the typed-nil test observes, so it has to
// happen through a declared interface type rather than by inference.
func asKEKWrapper(w kek.Wrapper) kek.Wrapper { return w }
