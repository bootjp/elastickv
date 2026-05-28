package main

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

// TestRunWriterRegistration_VerifyCommittedClosesBarrier pins codex
// P2 #6: when the registration is already durably committed (e.g. a
// prior attempt timed out but Raft applied it), the verify-before-
// propose check closes the barrier without needing a successful
// propose — so it never reaches proposeWriterRegistration (nil
// coordinator/engine are safe here precisely because verify short-
// circuits first).
func TestRunWriterRegistration_VerifyCommittedClosesBarrier(t *testing.T) {
	t.Parallel()
	barrier := make(chan struct{})
	verify := func() (bool, error) { return true, nil } // already committed
	// A cache with the storage DEK active so Registered() is meaningful;
	// releaseBarrier (via the verify path) must MarkRegistered it.
	cache := encryption.NewStateCache()
	sc := &encryption.Sidecar{Version: encryption.SidecarVersion, StorageEnvelopeActive: true}
	sc.Active.Storage = testRegDEKID
	cache.RefreshFromSidecar(sc)
	done := make(chan struct{})
	go func() {
		runWriterRegistration(context.Background(), nil, nil, cache, testRegDEKID, nil,
			registrationRequest(testRegDEKID, 1, 3), barrier, verify)
		close(done)
	}()
	select {
	case <-barrier:
	case <-time.After(2 * time.Second):
		t.Fatal("barrier not closed when registration already committed (verify path)")
	}
	<-done
	// The verify-before-propose close site must seed the Stage 7a-2
	// direct-path gate, not only the propose-success site (claude P1).
	if !cache.Registered() {
		t.Error("verify-committed path closed the barrier but did not MarkRegistered (Registered() = false)")
	}
}

// TestRetryUntilRegistered covers the §2.3 empty-catalog + active-
// envelope bootstrap edge that setupDistributionCatalog relies on: the
// helper must retry while fn returns ErrWriterNotRegistered, then return
// fn's result once it converges; return any non-gate error immediately;
// and surface a context.Canceled-chained error on clean shutdown
// (claude review on PR #847).
func TestRetryUntilRegistered(t *testing.T) {
	t.Parallel()

	t.Run("converges after transient ErrWriterNotRegistered", func(t *testing.T) {
		t.Parallel()
		calls := 0
		err := retryUntilRegistered(context.Background(), "test", func() error {
			calls++
			if calls < 3 {
				return errors.WithStack(store.ErrWriterNotRegistered)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("retryUntilRegistered: %v", err)
		}
		if calls != 3 {
			t.Errorf("fn called %d times, want 3 (2 gate failures + 1 success)", calls)
		}
	})

	t.Run("returns non-gate error immediately", func(t *testing.T) {
		t.Parallel()
		sentinel := errors.New("some other failure")
		calls := 0
		err := retryUntilRegistered(context.Background(), "test", func() error {
			calls++
			return sentinel
		})
		if !errors.Is(err, sentinel) {
			t.Fatalf("got %v, want the sentinel error", err)
		}
		if calls != 1 {
			t.Errorf("fn called %d times, want 1 (no retry on non-gate error)", calls)
		}
	})

	t.Run("immediate success", func(t *testing.T) {
		t.Parallel()
		calls := 0
		err := retryUntilRegistered(context.Background(), "test", func() error {
			calls++
			return nil
		})
		if err != nil || calls != 1 {
			t.Errorf("got err=%v calls=%d, want nil/1", err, calls)
		}
	})

	t.Run("cancellation surfaces context.Canceled", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // already cancelled → first gate failure then ctx.Done()
		err := retryUntilRegistered(ctx, "test", func() error {
			return errors.WithStack(store.ErrWriterNotRegistered)
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want a context.Canceled-chained error", err)
		}
	})
}

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

// testRegDEKID is the storage DEK id used across the registration
// tests (the wiringFor fixtures activate the same id).
const testRegDEKID uint32 = 7

func writeRegistryRow(t *testing.T, st store.MVCCStore, fullNodeID uint64, lastSeen uint16) {
	t.Helper()
	reg, err := store.WriterRegistryFor(st)
	if err != nil {
		t.Fatalf("WriterRegistryFor: %v", err)
	}
	val := encryption.EncodeRegistryValue(encryption.RegistryValue{
		FullNodeID: fullNodeID, FirstSeenLocalEpoch: lastSeen, LastSeenLocalEpoch: lastSeen,
	})
	if err := reg.SetRegistryRow(encryption.RegistryKey(testRegDEKID, encryption.NodeID16(fullNodeID)), val); err != nil {
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
	writeRegistryRow(t, st, fullNodeID, 5)
	got, err = readWriterRegistryLastSeen(st, dekID, fullNodeID)
	if err != nil {
		t.Fatalf("readWriterRegistryLastSeen (with row): %v", err)
	}
	if got != 5 {
		t.Errorf("with-row last_seen = %d, want 5", got)
	}
}

// TestRegistrationCommittedAtEpoch locks down the codex P1 round-1
// fix (PR #853): registrationCommittedAtEpoch returns true only on an
// exact (full_node_id, epoch) match. Higher lastSeen, lower lastSeen,
// missing row, and FullNodeID mismatch all must return false so the
// verifyRegistered short-circuit cannot fail-OPEN the storage gate on
// a stale-sidecar load or a §6.1 uint16-collision.
func TestRegistrationCommittedAtEpoch(t *testing.T) {
	t.Parallel()
	fullNodeID := etcdraftengine.DeriveNodeID("n1")
	for _, tc := range []struct {
		name       string
		writeLast  *uint16 // nil → no row
		queryEpoch uint16
		want       bool
	}{
		{name: "missing_row", writeLast: nil, queryEpoch: 3, want: false},
		{name: "exact_match", writeLast: u16ptr(3), queryEpoch: 3, want: true},
		// lastSeen > epoch: the codex P1 round-1 fail-OPEN case — must
		// be false so verifyRegistered's short-circuit cannot bypass a
		// stale-sidecar load's required propose.
		{name: "lastSeen_above_epoch", writeLast: u16ptr(5), queryEpoch: 3, want: false},
		{name: "lastSeen_below_epoch", writeLast: u16ptr(2), queryEpoch: 3, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := newRegistrationTestStore(t)
			if tc.writeLast != nil {
				writeRegistryRow(t, st, fullNodeID, *tc.writeLast)
			}
			assertRegistrationCommitted(t, st, fullNodeID, tc.queryEpoch, tc.want)
		})
	}
	t.Run("full_node_id_mismatch", func(t *testing.T) {
		t.Parallel()
		assertFullNodeIDMismatchReturnsFalse(t)
	})
}

func u16ptr(v uint16) *uint16 { return &v }

func assertRegistrationCommitted(t *testing.T, st store.MVCCStore, fullNodeID uint64, epoch uint16, want bool) {
	t.Helper()
	reg, err := store.WriterRegistryFor(st)
	if err != nil {
		t.Fatalf("WriterRegistryFor: %v", err)
	}
	got, err := registrationCommittedAtEpoch(reg, testRegDEKID, fullNodeID, epoch)
	if err != nil {
		t.Fatalf("registrationCommittedAtEpoch: %v", err)
	}
	if got != want {
		t.Errorf("registrationCommittedAtEpoch(epoch=%d) = %v, want %v", epoch, got, want)
	}
}

// assertFullNodeIDMismatchReturnsFalse writes a registry row under
// FullNodeID A, then queries with a different FullNodeID B that
// truncates to the same uint16 (§6.1 collision). The strict helper
// must return false so the applier's case-4 halt-apply path runs
// instead of fail-OPENing the storage gate.
func assertFullNodeIDMismatchReturnsFalse(t *testing.T) {
	t.Helper()
	st := newRegistrationTestStore(t)
	reg, err := store.WriterRegistryFor(st)
	if err != nil {
		t.Fatalf("WriterRegistryFor: %v", err)
	}
	stored := uint64(0xAAAA_BBBB_CCCC_1234)
	val := encryption.EncodeRegistryValue(encryption.RegistryValue{
		FullNodeID: stored, FirstSeenLocalEpoch: 3, LastSeenLocalEpoch: 3,
	})
	key := encryption.RegistryKey(testRegDEKID, encryption.NodeID16(stored))
	if err := reg.SetRegistryRow(key, val); err != nil {
		t.Fatalf("SetRegistryRow: %v", err)
	}
	// Same low 16 bits, different upper bits → §6.1 collision query.
	colliding := (uint64(0x1111_2222_3333) << 16) | uint64(encryption.NodeID16(stored))
	if colliding == stored {
		t.Fatalf("colliding FullNodeID accidentally equals stored: %#x", stored)
	}
	ok, err := registrationCommittedAtEpoch(reg, testRegDEKID, colliding, 3)
	if err != nil {
		t.Fatalf("registrationCommittedAtEpoch: %v", err)
	}
	if ok {
		t.Error("full_node_id_mismatch: want false (§6.1 uint16-collision must not short-circuit), got true")
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
		// expectRegistered is the Stage 7a-2 direct-path gate state after
		// the ungated branch returns. Only the already-registered restart
		// seeds Registered() true (codex P1 on PR #843); the other ungated
		// branches leave it false (and the gate condition is false there
		// anyway, so encryptForKey never consults it).
		expectRegistered bool
	}{
		{name: "encryption_off", w: encryptionWriteWiring{cache: encryption.NewStateCache()}}, // cipher nil
		{name: "phase0_envelope_inactive", w: wiringFor(t, 7, false, 1)},
		{name: "not_bootstrapped", w: wiringFor(t, 0, true, 1)},
		{
			name:             "already_registered",
			w:                wiringFor(t, 7, true, 3),
			seed:             func(t *testing.T, st store.MVCCStore) { writeRegistryRow(t, st, fullNodeID, 3) },
			expectRegistered: true,
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
			// Ungated == a non-nil gate with a nil Barrier (the
			// awaitRegistration short-circuit), so no encrypted write
			// ever blocks.
			if gate == nil || gate.Barrier != nil {
				t.Errorf("%s: expected ungated gate (nil Barrier), got %+v", tc.name, gate)
			}
			if got := tc.w.cache.Registered(); got != tc.expectRegistered {
				t.Errorf("%s: Registered() = %v, want %v", tc.name, got, tc.expectRegistered)
			}
		})
	}
}

// TestRuntimeRegistrationTick_AlreadyRegisteredIsNoOp pins that the
// Stage 7b watcher tick body does nothing when the cache reports
// Registered() == true (e.g., the process-start propose path already
// marked it).
func TestRuntimeRegistrationTick_AlreadyRegisteredIsNoOp(t *testing.T) {
	t.Parallel()
	st := newRegistrationTestStore(t)
	w := wiringFor(t, testRegDEKID, true, 5)
	w.cache.MarkRegistered(testRegDEKID) // pre-marked
	var lastLogged uint32
	runtimeRegistrationTick(context.Background(), &kv.ShardedCoordinator{},
		&kv.ShardGroup{Store: st}, w, testRegDEKID, etcdraftengine.DeriveNodeID("n1"), &lastLogged)
	if lastLogged != 0 {
		t.Errorf("lastLogged unexpectedly mutated: %d", lastLogged)
	}
	if !w.cache.Registered() {
		t.Error("pre-marked Registered() flipped to false")
	}
}

// TestRuntimeRegistrationTick_EnvelopeInactiveIsNoOp pins the Phase-0
// short-circuit (envelope still inactive → gate not consulted → no
// propose needed).
func TestRuntimeRegistrationTick_EnvelopeInactiveIsNoOp(t *testing.T) {
	t.Parallel()
	st := newRegistrationTestStore(t)
	w := wiringFor(t, testRegDEKID, false /*envelope inactive*/, 5)
	var lastLogged uint32
	runtimeRegistrationTick(context.Background(), &kv.ShardedCoordinator{},
		&kv.ShardGroup{Store: st}, w, testRegDEKID, etcdraftengine.DeriveNodeID("n1"), &lastLogged)
	if w.cache.Registered() {
		t.Error("envelope inactive but Registered() flipped true")
	}
}

// TestRuntimeRegistrationTick_NotBootstrappedIsNoOp pins the
// pre-bootstrap-with-no-active-DEK short-circuit (ActiveStorageKeyID
// returns !ok).
func TestRuntimeRegistrationTick_NotBootstrappedIsNoOp(t *testing.T) {
	t.Parallel()
	st := newRegistrationTestStore(t)
	// Envelope on but no active DEK (impossible in production but
	// captures the not-bootstrapped guard explicitly).
	w := wiringFor(t, 0 /*no active DEK*/, true, 0)
	var lastLogged uint32
	runtimeRegistrationTick(context.Background(), &kv.ShardedCoordinator{},
		&kv.ShardGroup{Store: st}, w, 0, etcdraftengine.DeriveNodeID("n1"), &lastLogged)
	if w.cache.Registered() {
		t.Error("no active DEK but Registered() flipped true")
	}
}

// TestRuntimeRegistrationTick_CutoverInScopeProposes pins the Branch A
// (Phase-0 boot → runtime cutover) path: bootDEK==activeDEK,
// envelope just flipped active, registry pre-populated so
// verifyRegistered short-circuits — the tick marks Registered() true.
func TestRuntimeRegistrationTick_CutoverInScopeProposes(t *testing.T) {
	t.Parallel()
	st := newRegistrationTestStore(t)
	fullNodeID := etcdraftengine.DeriveNodeID("n1")
	const epoch uint16 = 5
	// Pre-populate the registry so verifyRegistered returns true and
	// runWriterRegistration short-circuits to MarkRegistered without
	// needing a real coordinator/engine.
	writeRegistryRow(t, st, fullNodeID, epoch)

	w := wiringFor(t, testRegDEKID, true /*envelope active*/, epoch)
	var lastLogged uint32
	runtimeRegistrationTick(context.Background(), &kv.ShardedCoordinator{},
		&kv.ShardGroup{Store: st}, w, testRegDEKID /*bootDEK==activeDEK*/, fullNodeID, &lastLogged)
	if !w.cache.Registered() {
		t.Error("cutover branch did not MarkRegistered (Registered=false)")
	}
}

// TestRuntimeRegistrationTick_PreBootstrapInScopeProposes pins the
// Branch B (pre-bootstrap boot → runtime bootstrap+cutover) path:
// bootDEK==0, activeDEK=X (newly bootstrapped), w.epoch==0; the tick
// proposes for (X, node, 0). This is the codex P1 round-2 fix:
// bootDEKID==0 keeps the skip-condition false so the watcher does NOT
// treat the new DEK as a rotation.
func TestRuntimeRegistrationTick_PreBootstrapInScopeProposes(t *testing.T) {
	t.Parallel()
	st := newRegistrationTestStore(t)
	fullNodeID := etcdraftengine.DeriveNodeID("n1")
	// w.epoch==0 (pre-bootstrap load), active DEK X just installed.
	writeRegistryRow(t, st, fullNodeID, 0) // pre-populated → verify short-circuits

	w := wiringFor(t, testRegDEKID /*activeDEK=X*/, true, 0 /*w.epoch=0*/)
	var lastLogged uint32
	runtimeRegistrationTick(context.Background(), &kv.ShardedCoordinator{},
		&kv.ShardGroup{Store: st}, w, 0 /*bootDEK=0*/, fullNodeID, &lastLogged)
	if !w.cache.Registered() {
		t.Error("pre-bootstrap branch did not MarkRegistered (Registered=false)")
	}
}

// TestRuntimeRegistrationTick_DeferredRotationLogsOnceAndSkips pins the
// 7b' deferral: bootDEK != 0 AND activeDEK != bootDEK → log once per
// unique rotated-into DEK, do NOT propose. A second tick at the same
// activeDEK does NOT re-log. A subsequent rotation to yet another DEK
// re-logs correctly.
func TestRuntimeRegistrationTick_DeferredRotationLogsOnceAndSkips(t *testing.T) {
	t.Parallel()
	st := newRegistrationTestStore(t)
	fullNodeID := etcdraftengine.DeriveNodeID("n1")
	// boot DEK 7, runtime rotation to DEK 8.
	w := wiringFor(t, 8 /*activeDEK changed*/, true, 5)
	var lastLogged uint32

	runtimeRegistrationTick(context.Background(), &kv.ShardedCoordinator{},
		&kv.ShardGroup{Store: st}, w, 7 /*bootDEK*/, fullNodeID, &lastLogged)
	if w.cache.Registered() {
		t.Error("deferred rotation branch unexpectedly MarkRegistered")
	}
	if lastLogged != 8 {
		t.Errorf("lastLogged = %d, want 8 (first skip should record the DEK)", lastLogged)
	}

	// Second tick at same activeDEK: still no propose, lastLogged unchanged.
	runtimeRegistrationTick(context.Background(), &kv.ShardedCoordinator{},
		&kv.ShardGroup{Store: st}, w, 7, fullNodeID, &lastLogged)
	if lastLogged != 8 {
		t.Errorf("after repeat tick lastLogged = %d, want 8 (no re-log)", lastLogged)
	}

	// Rotate to DEK 9: re-log, lastLogged updates.
	sc := &encryption.Sidecar{Version: encryption.SidecarVersion, StorageEnvelopeActive: true}
	sc.Active.Storage = 9
	w.cache.RefreshFromSidecar(sc)
	runtimeRegistrationTick(context.Background(), &kv.ShardedCoordinator{},
		&kv.ShardGroup{Store: st}, w, 7, fullNodeID, &lastLogged)
	if lastLogged != 9 {
		t.Errorf("after rotation-to-9 lastLogged = %d, want 9 (re-log on new DEK)", lastLogged)
	}
}

// TestBuildProcessStartRegistrationGate_Phase0BootDoesNotMarkRegistered
// documents the design §2.3 fail-closed posture: a node that boots in
// Phase 0 (envelope inactive) skips registration without seeding
// Registered(), so the per-DEK gate is NOT registered for the active
// DEK. The Phase-0 boot itself emits no encrypted writes (envelope
// inactive → cleartext, gate unconsulted); a runtime EnableStorageEnvelope
// would then fail closed until the node (re)registers — the deferred
// runtime-registration follow-on. This is the safe posture (no fail-OPEN
// per §2.3), and benign today since the only direct write is the startup
// catalog bootstrap Save (covered by retryUntilRegistered).
func TestBuildProcessStartRegistrationGate_Phase0BootDoesNotMarkRegistered(t *testing.T) {
	t.Parallel()
	st := newRegistrationTestStore(t)
	w := wiringFor(t, testRegDEKID, false /*envelope inactive: Phase 0*/, 1)

	eg, _ := errgroup.WithContext(context.Background())
	gate, err := buildProcessStartRegistrationGate(
		context.Background(), eg, &kv.ShardedCoordinator{},
		&kv.ShardGroup{Store: st}, w, "n1")
	if err != nil {
		t.Fatalf("buildProcessStartRegistrationGate: %v", err)
	}
	if gate == nil || gate.Barrier != nil {
		t.Fatalf("Phase 0 should be ungated (nil Barrier), got %+v", gate)
	}
	// Phase-0 boot did not seed Registered(): the node is not registered
	// for the active DEK, so a later runtime cutover fail-closes the gate
	// (deferred runtime-registration). The gate is correctly per-DEK and
	// fail-closed rather than fail-OPEN (§2.3).
	if w.cache.Registered() {
		t.Error("Phase-0 boot unexpectedly marked Registered() — must not seed registration")
	}
}

// TestBuildProcessStartRegistrationGate_BehindEpochFailsClosed pins
// codex P1: a strictly-behind epoch (registry last_seen > bumped epoch)
// must fail closed (error) rather than skip ungated. The §9.1 rollback
// guard should catch this earlier, but if a stale sidecar slips past,
// the intent step must refuse rather than serve with a behind epoch.
func TestBuildProcessStartRegistrationGate_BehindEpochFailsClosed(t *testing.T) {
	t.Parallel()
	st := newRegistrationTestStore(t)
	fullNodeID := etcdraftengine.DeriveNodeID("n1")
	// Registry ahead of the bumped epoch (last_seen=5 > epoch=3).
	writeRegistryRow(t, st, fullNodeID, 5)

	eg, _ := errgroup.WithContext(context.Background())
	gate, err := buildProcessStartRegistrationGate(
		context.Background(), eg, &kv.ShardedCoordinator{},
		&kv.ShardGroup{Store: st}, wiringFor(t, 7, true, 3), "n1")
	if err == nil {
		t.Fatal("expected fail-closed error on behind epoch, got nil")
	}
	if gate != nil {
		t.Errorf("behind-epoch should return nil gate, got %+v", gate)
	}
}

func TestBuildProcessStartRegistrationGate_ProposeBranchArmsBarrier(t *testing.T) {
	t.Parallel()
	st := newRegistrationTestStore(t)
	fullNodeID := etcdraftengine.DeriveNodeID("n1")
	// Registry behind the bumped epoch (last_seen=2 < epoch=3) → propose.
	writeRegistryRow(t, st, fullNodeID, 2)

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
