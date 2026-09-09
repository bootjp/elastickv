package encryption

import (
	"crypto/subtle"
	"sync"

	"github.com/bootjp/elastickv/internal/encryption/kek"
	"github.com/cockroachdb/errors"
)

// StartupUnwrapCache memoizes KEK unwraps across the startup phase.
//
// Startup unwraps every wrapped DEK twice: once in the §9.1 guards, to
// prove the configured KEK actually matches the sidecar, and again in
// HydrateKeystoreFromSidecar to populate the keystore. With the
// file-mode KEK that duplication was a local AES operation and cost
// nothing, which is why it was deferred — but Stage 9B shipped the AWS
// KMS, GCP KMS and Vault providers, where every unwrap is a network
// round-trip. The condition that deferral was waiting on has arrived,
// so a node with N wrapped DEKs now makes 2N KMS calls to boot.
//
// The mapping from wrapped bytes to plaintext DEK is deterministic and
// stable, so caching it cannot change a result; it only removes the
// second call.
//
// The cache holds plaintext DEKs, which is why Reset exists: the
// keystore already retains every unretired DEK for the process
// lifetime (historical versions need them), so this adds no new class
// of exposure, but there is no reason to keep a second copy alive past
// hydration.
type StartupUnwrapCache struct {
	inner kek.Wrapper

	mu      sync.Mutex
	entries map[string][]byte
	// sealed stops memoizing once startup is done. Without it the
	// cache is not merely holding stale entries — it keeps growing,
	// retaining a plaintext copy of every DEK a later rotation
	// unwraps, for the process lifetime.
	sealed bool
}

// NewStartupUnwrapCache wraps inner, and returns a genuinely nil
// kek.Wrapper when inner is nil so the caller's wiring stays a single
// unconditional call.
//
// The return type is the INTERFACE, not *StartupUnwrapCache. Returning
// a typed nil pointer here would produce a non-nil kek.Wrapper holding
// a nil pointer, and startup decides whether encryption mutators may
// run from `kekWrapper != nil` — a node with no KEK configured would
// report one.
//
// Order matters at the call site: the cache belongs OUTSIDE any
// latency instrumentation, so a cache hit is not recorded as a
// zero-duration KMS round-trip and does not flatten the unwrap
// histogram.
func NewStartupUnwrapCache(inner kek.Wrapper) kek.Wrapper {
	if inner == nil {
		return nil
	}
	return &StartupUnwrapCache{inner: inner, entries: make(map[string][]byte)}
}

// Unwrap returns the memoized plaintext when this exact wrapped blob
// has been unwrapped before, and otherwise delegates.
//
// A failed unwrap is deliberately NOT cached: the failure may be
// transient (a KMS timeout), and caching it would turn one flaky call
// into a permanent startup refusal.
func (c *StartupUnwrapCache) Unwrap(wrapped []byte) ([]byte, error) {
	if c == nil {
		return nil, ErrKEKNotConfigured
	}
	if len(wrapped) == 0 {
		// Never cache the empty blob: sidecar entries with no wrapped
		// material are skipped by the guards, and an empty key would
		// collide across purposes.
		return c.unwrapUncached(wrapped)
	}

	if hit, ok := c.load(wrapped); ok {
		return hit, nil
	}
	dek, err := c.unwrapUncached(wrapped)
	if err != nil {
		return nil, err
	}
	c.store(wrapped, dek)
	// Hand back a copy so a caller that zeroes or mutates its DEK
	// cannot corrupt the cached entry for the next reader.
	return append([]byte(nil), dek...), nil
}

// unwrapUncached delegates to the provider. It wraps the error with
// the provider name so a KMS failure at startup names its source;
// errors.Wrapf preserves Is/As, so the §9.1 guards still match
// ErrKEKMismatch through this decorator.
func (c *StartupUnwrapCache) unwrapUncached(wrapped []byte) ([]byte, error) {
	dek, err := c.inner.Unwrap(wrapped)
	if err != nil {
		return nil, errors.Wrapf(err, "kek %s: unwrap", c.inner.Name())
	}
	return dek, nil
}

func (c *StartupUnwrapCache) load(wrapped []byte) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	dek, ok := c.entries[string(wrapped)]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), dek...), true
}

// store memoizes an unwrap unless the cache has been sealed. After
// sealing, later unwraps (rotation applies) go straight to the
// provider and leave no plaintext behind here.
func (c *StartupUnwrapCache) store(wrapped, dek []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sealed {
		return
	}
	c.entries[string(wrapped)] = append([]byte(nil), dek...)
}

// Wrap delegates. Wrapping is not memoized: providers may introduce
// fresh randomness per call, so two Wraps of the same DEK legitimately
// differ and a cache would be wrong rather than merely wasteful.
func (c *StartupUnwrapCache) Wrap(dek []byte) ([]byte, error) {
	wrapped, err := c.inner.Wrap(dek)
	if err != nil {
		return nil, errors.Wrapf(err, "kek %s: wrap", c.inner.Name())
	}
	return wrapped, nil
}

// Name reports the underlying provider so logs and the status RPC keep
// showing the real KEK source.
func (c *StartupUnwrapCache) Name() string { return c.inner.Name() }

// Seal zeroes every cached DEK and stops further memoization. Call it
// once startup has hydrated the keystore.
//
// Clearing alone would not be enough: the same wrapper is retained by
// every applier for the process lifetime, so a cache that kept
// memoizing would accumulate a plaintext copy of every DEK a later
// rotation unwraps. Sealing bounds the window to startup, which is the
// only place the duplicate unwrap it exists to remove occurs.
func (c *StartupUnwrapCache) Seal() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sealed = true
	for key, dek := range c.entries {
		zeroBytes(dek)
		delete(c.entries, key)
	}
}

// SealStartupUnwrapCache seals w when it is a StartupUnwrapCache, and
// is a no-op otherwise. Lets the caller seal without knowing whether
// the KEK source was decorated.
func SealStartupUnwrapCache(w kek.Wrapper) {
	if cache, ok := w.(*StartupUnwrapCache); ok {
		cache.Seal()
	}
}

// Len reports the number of cached entries. Test-facing.
func (c *StartupUnwrapCache) Len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// zeroBytes overwrites key material in place. subtle.ConstantTimeCopy
// is used so the compiler cannot elide the write as dead.
func zeroBytes(b []byte) {
	if len(b) == 0 {
		return
	}
	subtle.ConstantTimeCopy(1, b, make([]byte, len(b)))
}
