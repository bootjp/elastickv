package main

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"golang.org/x/sync/errgroup"
)

func TestRegistrationEntry_RoundTrips(t *testing.T) {
	t.Parallel()
	entry := registrationEntry(7, 0xABCD, 3)
	if entry[0] != fsmwire.OpRegistration {
		t.Fatalf("opcode = 0x%02x, want OpRegistration 0x%02x", entry[0], fsmwire.OpRegistration)
	}
	p, err := fsmwire.DecodeRegistration(entry[1:])
	if err != nil {
		t.Fatalf("DecodeRegistration: %v", err)
	}
	if p.DEKID != 7 || p.FullNodeID != 0xABCD || p.LocalEpoch != 3 {
		t.Errorf("decoded payload = %+v, want {7, 0xABCD, 3}", p)
	}
}

func TestRegistrationRequest_Fields(t *testing.T) {
	t.Parallel()
	req := registrationRequest(7, 0xABCD, 3)
	if req.GetDekId() != 7 {
		t.Errorf("DekId = %d, want 7", req.GetDekId())
	}
	if len(req.GetWriters()) != 1 {
		t.Fatalf("Writers = %d, want 1", len(req.GetWriters()))
	}
	w := req.GetWriters()[0]
	if w.GetFullNodeId() != 0xABCD || w.GetLocalEpoch() != 3 {
		t.Errorf("writer = {%d, %d}, want {0xABCD, 3}", w.GetFullNodeId(), w.GetLocalEpoch())
	}
}

func newRegistrationTestStore(t *testing.T) store.MVCCStore {
	t.Helper()
	st, err := store.NewPebbleStore(t.TempDir() + "/fsm.db")
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func writeRegistryRow(t *testing.T, st store.MVCCStore, dekID uint32, fullNodeID uint64, lastSeen uint16) {
	t.Helper()
	reg, err := store.WriterRegistryFor(st)
	if err != nil {
		t.Fatalf("WriterRegistryFor: %v", err)
	}
	val := encryption.EncodeRegistryValue(encryption.RegistryValue{
		FullNodeID: fullNodeID, FirstSeenLocalEpoch: lastSeen, LastSeenLocalEpoch: lastSeen,
	})
	if err := reg.SetRegistryRow(encryption.RegistryKey(dekID, encryption.NodeID16(fullNodeID)), val); err != nil {
		t.Fatalf("SetRegistryRow: %v", err)
	}
}

func TestReadWriterRegistryLastSeen(t *testing.T) {
	t.Parallel()
	st := newRegistrationTestStore(t)
	const dekID uint32 = 7
	fullNodeID := etcdraftengine.DeriveNodeID("n1")

	// No row → 0.
	got, err := readWriterRegistryLastSeen(st, dekID, fullNodeID)
	if err != nil {
		t.Fatalf("readWriterRegistryLastSeen (no row): %v", err)
	}
	if got != 0 {
		t.Errorf("no-row last_seen = %d, want 0", got)
	}

	// After writing last_seen=5 → 5.
	writeRegistryRow(t, st, dekID, fullNodeID, 5)
	got, err = readWriterRegistryLastSeen(st, dekID, fullNodeID)
	if err != nil {
		t.Fatalf("readWriterRegistryLastSeen (with row): %v", err)
	}
	if got != 5 {
		t.Errorf("with-row last_seen = %d, want 5", got)
	}
}

// wiringFor builds an encryptionWriteWiring with a non-nil cipher and a
// StateCache reflecting the given (activeDEK, envelopeActive) state.
func wiringFor(t *testing.T, activeDEK uint32, envelopeActive bool, epoch uint16) encryptionWriteWiring {
	t.Helper()
	cipher, err := encryption.NewCipher(encryption.NewKeystore())
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}
	cache := encryption.NewStateCache()
	sc := &encryption.Sidecar{Version: encryption.SidecarVersion, StorageEnvelopeActive: envelopeActive}
	sc.Active.Storage = activeDEK
	cache.RefreshFromSidecar(sc)
	return encryptionWriteWiring{cache: cache, cipher: cipher, epoch: epoch}
}

func TestBuildProcessStartRegistrationGate_NilGateBranches(t *testing.T) {
	t.Parallel()
	fullNodeID := etcdraftengine.DeriveNodeID("n1")
	for _, tc := range []struct {
		name string
		w    encryptionWriteWiring
		seed func(t *testing.T, st store.MVCCStore)
	}{
		{name: "encryption_off", w: encryptionWriteWiring{cache: encryption.NewStateCache()}}, // cipher nil
		{name: "phase0_envelope_inactive", w: wiringFor(t, 7, false, 1)},
		{name: "not_bootstrapped", w: wiringFor(t, 0, true, 1)},
		{
			name: "already_registered",
			w:    wiringFor(t, 7, true, 3),
			seed: func(t *testing.T, st store.MVCCStore) { writeRegistryRow(t, st, 7, fullNodeID, 3) },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := newRegistrationTestStore(t)
			if tc.seed != nil {
				tc.seed(t, st)
			}
			eg, _ := errgroup.WithContext(context.Background())
			gate, err := buildProcessStartRegistrationGate(
				context.Background(), eg, &kv.ShardedCoordinator{},
				&kv.ShardGroup{Store: st}, tc.w, "n1")
			if err != nil {
				t.Fatalf("buildProcessStartRegistrationGate: %v", err)
			}
			if gate != nil {
				t.Errorf("%s: expected nil (ungated) gate, got %+v", tc.name, gate)
			}
		})
	}
}

func TestBuildProcessStartRegistrationGate_ProposeBranchArmsBarrier(t *testing.T) {
	t.Parallel()
	st := newRegistrationTestStore(t)
	fullNodeID := etcdraftengine.DeriveNodeID("n1")
	// Registry behind the bumped epoch (last_seen=2 < epoch=3) → propose.
	writeRegistryRow(t, st, 7, fullNodeID, 2)

	ctx, cancel := context.WithCancel(context.Background())
	eg, egCtx := errgroup.WithContext(ctx)
	// The propose goroutine uses egCtx; cancel stops it (a zero
	// coordinator reports not-leader + no leader address, so it just
	// retries until ctx ends).
	gate, err := buildProcessStartRegistrationGate(
		egCtx, eg, &kv.ShardedCoordinator{},
		&kv.ShardGroup{Store: st}, wiringFor(t, 7, true, 3), "n1")
	if err != nil {
		t.Fatalf("buildProcessStartRegistrationGate: %v", err)
	}
	if gate == nil {
		t.Fatal("propose branch should return a non-nil gate")
	}
	if gate.Barrier == nil {
		t.Error("propose branch gate must carry an open barrier")
	}
	if gate.StorageEnvelopeActive == nil || gate.ActiveStorageKeyID == nil {
		t.Error("gate predicates must be wired")
	}
	// Barrier must still be open (registration cannot commit against the
	// zero coordinator): a non-blocking read should not succeed.
	select {
	case <-gate.Barrier:
		t.Error("barrier closed unexpectedly without a committed registration")
	default:
	}
	cancel()
	_ = eg.Wait()
}
