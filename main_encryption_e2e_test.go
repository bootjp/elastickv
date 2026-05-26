package main

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/store"
)

// TestEncryption_E2E_BootstrapCutoverPutReadback is the Stage 6D-6c-3b
// end-to-end test: it drives the *production* main.go encryption wiring
// (buildEncryptionWriteWiring → store.WithEncryption + the shared
// StateCache) through the full §7.1 rollout — Bootstrap → cutover →
// Put → read-back — and proves that post-cutover writes land encrypted
// at rest while every version still reads back as correct plaintext.
//
// Bootstrap and the cutover are applied via the Applier (sharing the
// same keystore + StateCache the wiring built), which is exactly the
// FSM-apply effect of the BootstrapEncryption / EnableStorageEnvelope
// RPCs. The gRPC RPC + §4 capability fan-out path is covered by the
// 6D-6c-3a unit tests and the adapter EncryptionAdmin tests; this test
// validates the data path that 6D-6c-1 (shared cache) and 6D-6c-2
// (cipher/gate wiring) enable.
const (
	e2eStorageDEKID uint32 = 7
	e2eRaftDEKID    uint32 = 8
)

// e2eNodeID is the proposer's full node id used for the writer-registry
// rows. It is derived from the same raftID ("n1") the fixture passes to
// buildEncryptionWriteWiring, so the registry identity matches the
// DeterministicNonceFactory's node_id (NodeID16(DeriveNodeID("n1"))).
// Hardcoding an unrelated constant here would pass today (the applier
// validates DEKID, not FullNodeID) but mislead the Stage 7
// registration-before-first-write gate, which must confirm the nonce
// factory's node_id has a registry row.
var e2eNodeID = etcdraftengine.DeriveNodeID("n1")

func TestEncryption_E2E_BootstrapCutoverPutReadback(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	sidecarPath := dir + "/keys.json"
	pebbleDir := dir + "/fsm.db"

	st, applier, cache, closeStore := newE2EEncryptionFixture(t, sidecarPath, pebbleDir)

	// Phase 0 (pre-bootstrap): no active DEK, gate off → cleartext.
	assertActiveStorageKeyID(t, cache, 0, false)
	mustPut(t, ctx, st, "before", "plain-before", 100)

	// Bootstrap: installs the storage DEK into the shared keystore,
	// writes the sidecar, and refreshes the StateCache so
	// ActiveStorageKeyID flips — but the envelope gate stays off.
	applyE2EBootstrap(t, applier)
	assertActiveStorageKeyID(t, cache, e2eStorageDEKID, true)
	assertEnvelopeActive(t, cache, false)
	// Phase 0→1 window: DEK active but cutover not fired → still cleartext.
	mustPut(t, ctx, st, "mid", "plain-mid", 110)

	// Cutover (EnableStorageEnvelope apply effect): flips the gate.
	applyE2ECutover(t, applier)
	assertEnvelopeActive(t, cache, true)
	// Stage 7a-2 §4.1: with the envelope active, the DIRECT write path
	// (PutAt) fails closed until this load's writer registration has
	// committed AND was armed. In production main.go's registration
	// goroutine arms the gate (propose branch) and calls MarkRegistered
	// once the barrier closes; here we drive the Applier directly, so
	// reproduce both steps to exercise the armed+registered gate-pass
	// path. Without them the Phase-1 PutAt below would return
	// ErrWriterNotRegistered (armed) — see TestStateCache_StorageRegistrationSatisfied.
	cache.ArmStorageRegistration()
	cache.MarkRegistered(e2eStorageDEKID)
	// Phase 1: gate armed + active DEK + registered → encrypted at rest.
	mustPut(t, ctx, st, "after", "plain-after", 120)

	// Every version reads back as correct plaintext through the wired
	// cipher, regardless of whether it was written cleartext or
	// enveloped (the read path dispatches on each version's stored
	// encryption_state).
	assertGet(t, ctx, st, "before", "plain-before", 100)
	assertGet(t, ctx, st, "mid", "plain-mid", 110)
	assertGet(t, ctx, st, "after", "plain-after", 120)

	// Release the Pebble lock before the cipher-less reopen. closeStore
	// is idempotent (sync.Once) and also registered via t.Cleanup, so
	// an early t.Fatal above still closes the store.
	closeStore()
	assertEncryptedAtRest(t, ctx, pebbleDir)
}

// newE2EEncryptionFixture builds the production encryption write wiring
// (buildEncryptionWriteWiring) over a fresh pre-bootstrap sidecar, opens
// a PebbleStore with its options, and constructs an Applier that shares
// the same keystore + StateCache — exactly the main.go wiring topology.
// The returned closeStore is idempotent (sync.Once) and registered via
// t.Cleanup, so the Pebble store is closed exactly once whether the
// test closes it explicitly (before the cipher-less reopen) or exits
// early via t.Fatal.
func newE2EEncryptionFixture(t *testing.T, sidecarPath, pebbleDir string) (store.MVCCStore, *encryption.Applier, *encryption.StateCache, func()) {
	t.Helper()
	keystore := encryption.NewKeystore()
	encWiring, err := buildEncryptionWriteWiring(true, "n1", sidecarPath, wiringFakeKEK{}, keystore)
	if err != nil {
		t.Fatalf("buildEncryptionWriteWiring: %v", err)
	}
	if encWiring.cipher == nil || encWiring.nonceFactory == nil {
		t.Fatal("expected cipher + nonce factory wired when encryption enabled")
	}
	st, err := store.NewPebbleStore(pebbleDir, encWiring.pebbleOptions()...)
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	var closeOnce sync.Once
	closeStore := func() {
		closeOnce.Do(func() {
			if err := st.Close(); err != nil {
				t.Errorf("close encrypted store: %v", err)
			}
		})
	}
	t.Cleanup(closeStore)
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
	return st, applier, encWiring.cache, closeStore
}

func applyE2EBootstrap(t *testing.T, applier *encryption.Applier) {
	t.Helper()
	if err := applier.ApplyBootstrap(1, fsmwire.BootstrapPayload{
		StorageDEKID:   e2eStorageDEKID,
		WrappedStorage: []byte("wrapped-storage-dek"),
		RaftDEKID:      e2eRaftDEKID,
		WrappedRaft:    []byte("wrapped-raft-dek-distinct"),
		BatchRegistry: []fsmwire.RegistrationPayload{
			{DEKID: e2eStorageDEKID, FullNodeID: e2eNodeID, LocalEpoch: 0},
		},
	}); err != nil {
		t.Fatalf("ApplyBootstrap: %v", err)
	}
}

func applyE2ECutover(t *testing.T, applier *encryption.Applier) {
	t.Helper()
	if err := applier.ApplyRotation(2, fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubEnableStorageEnvelope,
		DEKID:                e2eStorageDEKID,
		Purpose:              fsmwire.PurposeStorage,
		Wrapped:              []byte{},
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: e2eStorageDEKID, FullNodeID: e2eNodeID, LocalEpoch: 1},
	}); err != nil {
		t.Fatalf("ApplyRotation cutover: %v", err)
	}
}

// assertEncryptedAtRest reopens the Pebble dir with a cipher-LESS store
// and proves the post-cutover version is a genuine on-disk §4.1
// envelope (the cleartext read path refuses it) while the pre-cutover
// versions remain plaintext-readable.
func assertEncryptedAtRest(t *testing.T, ctx context.Context, pebbleDir string) {
	t.Helper()
	plainStore, err := store.NewPebbleStore(pebbleDir)
	if err != nil {
		t.Fatalf("reopen cipher-less store: %v", err)
	}
	defer func() {
		if err := plainStore.Close(); err != nil {
			t.Errorf("close cipher-less store: %v", err)
		}
	}()

	switch _, err := plainStore.GetAt(ctx, []byte("after"), 120); {
	case err == nil:
		t.Error("cipher-less read of post-cutover key succeeded; value was not encrypted at rest")
	case !strings.Contains(err.Error(), "no cipher configured"):
		t.Errorf("cipher-less read of post-cutover key: unexpected error %v", err)
	}
	assertGet(t, ctx, plainStore, "before", "plain-before", 100)
	assertGet(t, ctx, plainStore, "mid", "plain-mid", 110)
}

func assertActiveStorageKeyID(t *testing.T, cache *encryption.StateCache, wantID uint32, wantOK bool) {
	t.Helper()
	if id, ok := cache.ActiveStorageKeyID(); id != wantID || ok != wantOK {
		t.Fatalf("ActiveStorageKeyID = (%d,%v), want (%d,%v)", id, ok, wantID, wantOK)
	}
}

func assertEnvelopeActive(t *testing.T, cache *encryption.StateCache, want bool) {
	t.Helper()
	if got := cache.StorageEnvelopeActive(); got != want {
		t.Fatalf("StorageEnvelopeActive = %v, want %v", got, want)
	}
}

func mustPut(t *testing.T, ctx context.Context, st store.MVCCStore, key, val string, ts uint64) {
	t.Helper()
	if err := st.PutAt(ctx, []byte(key), []byte(val), ts, 0); err != nil {
		t.Fatalf("PutAt %q@%d: %v", key, ts, err)
	}
}

func assertGet(t *testing.T, ctx context.Context, st store.MVCCStore, key, want string, ts uint64) {
	t.Helper()
	got, err := st.GetAt(ctx, []byte(key), ts)
	if err != nil {
		t.Fatalf("GetAt %q@%d: %v", key, ts, err)
	}
	if string(got) != want {
		t.Errorf("GetAt %q@%d = %q, want %q", key, ts, got, want)
	}
}
