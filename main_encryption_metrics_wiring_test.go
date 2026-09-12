package main

import (
	"context"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/monitoring"
	"github.com/bootjp/elastickv/store"
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
