package main

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/kek"
	"github.com/bootjp/elastickv/monitoring"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

// wiringEncryptionObserver records the §9.2 observations the production
// storage-envelope path emits.
type wiringEncryptionObserver struct {
	mu       sync.Mutex
	failures []string
	writes   []uint32
}

func (o *wiringEncryptionObserver) ObserveEncryptionDecryptFailure(reason string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.failures = append(o.failures, reason)
}

func (o *wiringEncryptionObserver) ObserveEncryptionWrite(keyID uint32, _, _ int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.writes = append(o.writes, keyID)
}

func (o *wiringEncryptionObserver) writeCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.writes)
}

// TestEncryptionObserverReachesStoreThroughProductionWiring drives the
// real main.go topology — buildEncryptionWriteWiring → pebbleOptions()
// → store.NewPebbleStore — and proves the §9.2 observer survives it.
//
// The store-level tests in store/ build their own option list, so they
// would stay green even if production never wired the observer at all.
// This test is the one that fails in that case.
func TestEncryptionObserverReachesStoreThroughProductionWiring(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	sidecarPath := dir + "/keys.json"
	pebbleDir := dir + "/fsm.db"

	obs := &wiringEncryptionObserver{}
	keystore := encryption.NewKeystore()
	encWiring, err := buildEncryptionWriteWiring(
		true, "n1", sidecarPath, wiringFakeKEK{}, keystore, []groupSpec{{id: 1}}, obs)
	if err != nil {
		t.Fatalf("buildEncryptionWriteWiring: %v", err)
	}

	st, err := store.NewPebbleStore(pebbleDir, encWiring.pebbleOptions()...)
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	reg, err := store.WriterRegistryFor(st)
	if err != nil {
		t.Fatalf("WriterRegistryFor: %v", err)
	}
	applier, err := encryption.NewApplier(reg,
		encryption.WithKEK(wiringFakeKEK{}),
		encryption.WithKeystore(keystore),
		encryption.WithSidecarPath(sidecarPath),
		encryption.WithStateCache(encWiring.cache),
	)
	if err != nil {
		t.Fatalf("NewApplier: %v", err)
	}

	// Pre-cutover write stays cleartext: no envelope, nothing to count.
	mustPut(t, ctx, st, "before", "plain-before", 100)
	if got := obs.writeCount(); got != 0 {
		t.Fatalf("cleartext write emitted %d envelope observations, want 0", got)
	}

	// Drive the production rollout to Phase 1 so writes are encrypted.
	applyE2EBootstrap(t, applier)
	applyE2ECutover(t, applier)
	encWiring.cache.MarkRegistered(e2eStorageDEKID)

	mustPut(t, ctx, st, "after", "plain-after", 120)

	obs.mu.Lock()
	defer obs.mu.Unlock()
	if len(obs.writes) != 1 {
		t.Fatalf("post-cutover encrypted write produced %d observations, want 1", len(obs.writes))
	}
	if obs.writes[0] != e2eStorageDEKID {
		t.Fatalf("observation carried key_id=%d, want the active storage DEK %d",
			obs.writes[0], e2eStorageDEKID)
	}
	if len(obs.failures) != 0 {
		t.Fatalf("healthy rollout recorded decrypt failures: %v", obs.failures)
	}
}

// TestMetricsRegistryEncryptionObserverSatisfiesStoreInterface pins the
// production type: main.go passes metricsRegistry.EncryptionObserver()
// where a store.EncryptionObserver is required, so the monitoring
// implementation must satisfy the store's interface. A compile-time
// assertion here fails loudly if either side's method set drifts.
func TestMetricsRegistryEncryptionObserverSatisfiesStoreInterface(t *testing.T) {
	t.Parallel()

	var obs store.EncryptionObserver = monitoring.NewRegistry("n1", "127.0.0.1:1").EncryptionObserver()
	if obs == nil {
		t.Fatal("registry returned a nil encryption observer")
	}
	obs.ObserveEncryptionWrite(1, 10, 42)
	obs.ObserveEncryptionDecryptFailure(encryption.DecryptFailureReasonTagMismatch)
}

// countingKEKObserver records how many unwraps were timed.
type countingKEKObserver struct {
	mu    sync.Mutex
	count int
}

func (o *countingKEKObserver) ObserveEncryptionKEKUnwrap(time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.count++
}

func (o *countingKEKObserver) total() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.count
}

// TestTimedKEKUnwrapperCoversStartupPreflightUnwraps pins the
// decoration ORDER. loadKEKAndRunStartupGuards runs CheckStartupGuards
// and kek.VerifyWrapper, both of which perform real unwrap round trips
// — on a fresh node the preflight unwrap can be the only one that ever
// happens. Decorating after those guards would leave
// elastickv_encryption_kek_unwrap_seconds empty despite completed KMS
// calls, so the decorator must wrap the source before they run.
func TestTimedKEKUnwrapperCoversStartupPreflightUnwraps(t *testing.T) {
	t.Parallel()

	obs := &countingKEKObserver{}
	timed := monitoring.NewTimedKEKUnwrapper(preflightKEK{}, obs)
	require.NotNil(t, timed)

	// Both preflight paths unwrap through the decorated source.
	require.NoError(t, kek.VerifyWrapper(timed))
	require.Positive(t, obs.total(),
		"the startup preflight unwrap must be timed, not bypass the decorator")
}

// preflightKEK is a minimal kek.Wrapper for the decoration-order test.
type preflightKEK struct{}

func (preflightKEK) Name() string { return "preflight-fake" }

func (preflightKEK) Wrap(dek []byte) ([]byte, error) {
	return append([]byte("w:"), dek...), nil
}

func (preflightKEK) Unwrap(wrapped []byte) ([]byte, error) {
	return bytes.TrimPrefix(wrapped, []byte("w:")), nil
}
