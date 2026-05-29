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
	runtimeRegistrationTick(context.Background(), &kv.ShardedCoordinator{},
		&kv.ShardGroup{Store: st}, w, testRegDEKID, etcdraftengine.DeriveNodeID("n1"))
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
	runtimeRegistrationTick(context.Background(), &kv.ShardedCoordinator{},
		&kv.ShardGroup{Store: st}, w, testRegDEKID, etcdraftengine.DeriveNodeID("n1"))
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
	runtimeRegistrationTick(context.Background(), &kv.ShardedCoordinator{},
		&kv.ShardGroup{Store: st}, w, 0, etcdraftengine.DeriveNodeID("n1"))
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
	runtimeRegistrationTick(context.Background(), &kv.ShardedCoordinator{},
		&kv.ShardGroup{Store: st}, w, testRegDEKID /*bootDEK==activeDEK*/, fullNodeID)
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
	runtimeRegistrationTick(context.Background(), &kv.ShardedCoordinator{},
		&kv.ShardGroup{Store: st}, w, 0 /*bootDEK=0*/, fullNodeID)
	if !w.cache.Registered() {
		t.Error("pre-bootstrap branch did not MarkRegistered (Registered=false)")
	}
}

// TestRuntimeRegistrationTick_PreBootstrapRotateBeforeCutoverProposes
// pins design §5 item 6b (PR #853): the bootstrap→rotate→cutover
// sub-path. A node that booted in Phase 0 (bootDEKID==0) and then
// observes a runtime Bootstrap followed by a rotation to a different
// DEK *before* its cutover (activeDEK = Y ≠ original-bootstrapped DEK,
// w.epoch == 0) MUST still propose for Y — the scope check's rotation
// branch (`bootDEKID != 0 && activeDEK != bootDEKID`) is short-circuited
// by `bootDEKID == 0`. Without this test the guard is one operand swap
// away from a regression that would silently leave the watcher fail-
// closed indefinitely.
//
// After 7b' replaces the deferred-skip branch with an in-scope propose,
// the `bootDEKID==0` short-circuit remains the cutover/pre-bootstrap
// route that 7b' did not touch.
func TestRuntimeRegistrationTick_PreBootstrapRotateBeforeCutoverProposes(t *testing.T) {
	t.Parallel()
	st := newRegistrationTestStore(t)
	fullNodeID := etcdraftengine.DeriveNodeID("n1")
	// activeDEK Y=99 (≠ original bootstrap DEK), w.epoch=0, bootDEK=0.
	const rotatedDEK uint32 = 99
	// Pre-populate the registry under DEK 99 so verifyRegistered
	// short-circuits without needing a real coordinator/engine.
	preSeedRegistryRow(t, st, rotatedDEK, fullNodeID, 0)

	w := wiringFor(t, rotatedDEK, true, 0 /*w.epoch=0*/)
	runtimeRegistrationTick(context.Background(), &kv.ShardedCoordinator{},
		&kv.ShardGroup{Store: st}, w, 0 /*bootDEK=0*/, fullNodeID)
	if !w.cache.Registered() {
		t.Error("bootstrap→rotate→cutover sub-path did not MarkRegistered (Registered=false); bootDEKID==0 should keep the rotation branch off")
	}
}

// TestRuntimeRegistrationTick_RotationInScopeVerifyShortCircuits pins
// Stage 7b' §3.2's in-scope rotation propose path. A node that booted
// with bootDEKID=X then observes a runtime RotateDEK to Y MUST propose
// (Y, node, w.epoch) — applyRotateDEK wrote Keys[Y].LocalEpoch=w.epoch
// per 7b' §3.1 so the §9.1 startup guard sees a monotone advance on
// next restart. The test pre-populates a registry row at (Y, node,
// w.epoch) so verifyRegistered short-circuits and the success-side
// MarkRegistered fires.
func TestRuntimeRegistrationTick_RotationInScopeVerifyShortCircuits(t *testing.T) {
	t.Parallel()
	st := newRegistrationTestStore(t)
	fullNodeID := etcdraftengine.DeriveNodeID("n1")
	const bootDEK, rotatedDEK uint32 = 7, 8
	const wepoch uint16 = 5

	// Pre-populate (Y, node, 5) so verifyRegistered's exact-match
	// check returns true and runWriterRegistration short-circuits to
	// MarkRegistered without needing a real coordinator/engine.
	preSeedRegistryRow(t, st, rotatedDEK, fullNodeID, wepoch)

	w := wiringFor(t, rotatedDEK, true, wepoch)
	runtimeRegistrationTick(context.Background(), &kv.ShardedCoordinator{},
		&kv.ShardGroup{Store: st}, w, bootDEK, fullNodeID)
	if !w.cache.Registered() {
		t.Error("7b' rotation in-scope path did not MarkRegistered (Registered=false); applyRotateDEK + verifyRegistered short-circuit + MarkRegistered should flip the cache")
	}
}

// TestRuntimeRegistrationTick_RotationAlreadyRegisteredIsNoOp pins
// 7b' §3.2's check ordering step (1): once cache.Registered() is true
// for the active DEK (a prior tick MarkRegistered'd it), a subsequent
// tick is a no-op — runtimeRegistrationInScope's first guard returns
// before re-proposing.
func TestRuntimeRegistrationTick_RotationAlreadyRegisteredIsNoOp(t *testing.T) {
	t.Parallel()
	st := newRegistrationTestStore(t)
	fullNodeID := etcdraftengine.DeriveNodeID("n1")
	const bootDEK, rotatedDEK uint32 = 7, 8
	const wepoch uint16 = 5
	preSeedRegistryRow(t, st, rotatedDEK, fullNodeID, wepoch)

	w := wiringFor(t, rotatedDEK, true, wepoch)
	// First tick: registers.
	runtimeRegistrationTick(context.Background(), &kv.ShardedCoordinator{},
		&kv.ShardGroup{Store: st}, w, bootDEK, fullNodeID)
	if !w.cache.Registered() {
		t.Fatalf("first tick failed prerequisite: Registered=false")
	}
	// Second tick: no-op via cache.Registered() short-circuit (step 1).
	// To assert "no propose happened", we pass a nil coordinator —
	// runtimeRegistrationInScope must return (_, false) before
	// runWriterRegistration is reached. If the short-circuit fails,
	// the tick would attempt to read from a nil coordinator and panic.
	runtimeRegistrationTick(context.Background(), nil, /*nil coordinator pins the short-circuit*/
		&kv.ShardGroup{Store: st}, w, bootDEK, fullNodeID)
	if !w.cache.Registered() {
		t.Error("second tick unexpectedly cleared Registered()")
	}
}

// TestRuntimeRegistrationTick_RotationToYetAnotherDEKReProposes pins
// 7b' §3.2 fresh-rotation propose: after registering for Y, a
// subsequent rotation to Z triggers a fresh propose because
// cache.Registered() returns false (single-DEK StateCache: active=Z
// while registered=Y, mismatch). On success cache.Registered() flips
// true for Z.
func TestRuntimeRegistrationTick_RotationToYetAnotherDEKReProposes(t *testing.T) {
	t.Parallel()
	st := newRegistrationTestStore(t)
	fullNodeID := etcdraftengine.DeriveNodeID("n1")
	const bootDEK, rotateY, rotateZ uint32 = 7, 8, 9
	const wepoch uint16 = 5
	preSeedRegistryRow(t, st, rotateY, fullNodeID, wepoch)
	preSeedRegistryRow(t, st, rotateZ, fullNodeID, wepoch)

	w := wiringFor(t, rotateY, true, wepoch)
	// Register Y.
	runtimeRegistrationTick(context.Background(), &kv.ShardedCoordinator{},
		&kv.ShardGroup{Store: st}, w, bootDEK, fullNodeID)
	if !w.cache.Registered() {
		t.Fatalf("first tick (Y) failed: Registered=false")
	}
	// Rotation Y→Z: refresh sidecar to flip activeStorageDEKID.
	sc := &encryption.Sidecar{Version: encryption.SidecarVersion, StorageEnvelopeActive: true}
	sc.Active.Storage = rotateZ
	w.cache.RefreshFromSidecar(sc)
	if w.cache.Registered() {
		t.Fatalf("after rotation Y→Z: cache.Registered() unexpectedly true; the single-DEK cache should report mismatch")
	}
	// Second tick on Z: re-proposes, MarkRegistered(Z) flips cache.
	runtimeRegistrationTick(context.Background(), &kv.ShardedCoordinator{},
		&kv.ShardGroup{Store: st}, w, bootDEK, fullNodeID)
	if !w.cache.Registered() {
		t.Error("rotation-to-Z tick did not MarkRegistered")
	}
}

// TestRuntimeRegistrationTick_RotationOscillationBtoCtoBReProposes
// pins 7b' §3.2.2 documented steady-state: B → C → B oscillation.
// After registering B then C, the single-DEK StateCache holds C; a
// rotation back to B leaves cache.Registered() == false (active=B,
// registered=C mismatch). The watcher re-proposes (safe via §4.1
// case-2 idempotent on the existing B row); MarkRegistered(B) flips
// the cache back.
func TestRuntimeRegistrationTick_RotationOscillationBtoCtoBReProposes(t *testing.T) {
	t.Parallel()
	st := newRegistrationTestStore(t)
	fullNodeID := etcdraftengine.DeriveNodeID("n1")
	const bootDEK, dekB, dekC uint32 = 7, 8, 9
	const wepoch uint16 = 5
	preSeedRegistryRow(t, st, dekB, fullNodeID, wepoch)
	preSeedRegistryRow(t, st, dekC, fullNodeID, wepoch)

	w := wiringFor(t, dekB, true, wepoch)
	// Register B.
	runtimeRegistrationTick(context.Background(), &kv.ShardedCoordinator{},
		&kv.ShardGroup{Store: st}, w, bootDEK, fullNodeID)
	if !w.cache.Registered() {
		t.Fatalf("register B failed: Registered=false")
	}
	// Rotate B → C; tick registers C.
	scC := &encryption.Sidecar{Version: encryption.SidecarVersion, StorageEnvelopeActive: true}
	scC.Active.Storage = dekC
	w.cache.RefreshFromSidecar(scC)
	runtimeRegistrationTick(context.Background(), &kv.ShardedCoordinator{},
		&kv.ShardGroup{Store: st}, w, bootDEK, fullNodeID)
	if !w.cache.Registered() {
		t.Fatalf("register C failed: Registered=false")
	}
	// Rotate C → B: §3.2.2 — cache holds C; Registered() == false
	// when active flips to B → watcher re-proposes; success path
	// MarkRegisters B.
	scB := &encryption.Sidecar{Version: encryption.SidecarVersion, StorageEnvelopeActive: true}
	scB.Active.Storage = dekB
	w.cache.RefreshFromSidecar(scB)
	if w.cache.Registered() {
		t.Fatalf("after rotation C→B: cache.Registered() unexpectedly true; single-DEK cache should mismatch")
	}
	runtimeRegistrationTick(context.Background(), &kv.ShardedCoordinator{},
		&kv.ShardGroup{Store: st}, w, bootDEK, fullNodeID)
	if !w.cache.Registered() {
		t.Error("rotation C→B re-propose did not MarkRegistered (Registered=false)")
	}
}

// TestRuntimeRegistrationTick_StaleMarkRegisteredRecoveryReProposes
// pins the gemini CRITICAL fix on PR #864: even if a stale
// MarkRegistered(oldDEK) from the 7a process-start goroutine lands
// AFTER 7b' has already MarkRegistered(newDEK) — making
// cache.Registered() return false for activeDEK=newDEK — the next
// watcher tick MUST re-propose for newDEK. The earlier draft's
// lastRegisteredDEK gate caused a permanent fail-closed in this race
// by short-circuiting the rotation branch on activeDEK ==
// lastRegisteredDEK. The fix is to let cache.Registered() be the
// sole gate; §4.1 case-2 idempotent acceptance makes the recovery
// re-propose a no-op apply.
func TestRuntimeRegistrationTick_StaleMarkRegisteredRecoveryReProposes(t *testing.T) {
	t.Parallel()
	st := newRegistrationTestStore(t)
	fullNodeID := etcdraftengine.DeriveNodeID("n1")
	const bootDEK, newDEK, oldDEK uint32 = 7, 8, 5
	const wepoch uint16 = 5
	preSeedRegistryRow(t, st, newDEK, fullNodeID, wepoch)

	w := wiringFor(t, newDEK, true, wepoch)
	// 7b' tick registers for the new DEK.
	runtimeRegistrationTick(context.Background(), &kv.ShardedCoordinator{},
		&kv.ShardGroup{Store: st}, w, bootDEK, fullNodeID)
	if !w.cache.Registered() {
		t.Fatalf("initial registration failed: Registered=false")
	}
	// Simulate a stale MarkRegistered(oldDEK) from a long-running
	// 7a process-start goroutine that was racing with the cluster's
	// rotation. This overwrites registeredStorageDEKID; the cache
	// now reports Registered() == false (active=newDEK != oldDEK).
	w.cache.MarkRegistered(oldDEK)
	if w.cache.Registered() {
		t.Fatalf("after stale MarkRegistered(%d): cache.Registered() should be false (active=%d != registered=%d)", oldDEK, newDEK, oldDEK)
	}
	// Next watcher tick MUST recover by re-proposing for newDEK.
	// With the earlier lastRegisteredDEK gate this would have been a
	// permanent fail-closed (lastRegisteredDEK == newDEK ==
	// activeDEK → scope returns (_, false)). With the fix, cache.
	// Registered() == false → step (1) falls through → rotation
	// branch propose → MarkRegistered(newDEK) → Registered() true.
	runtimeRegistrationTick(context.Background(), &kv.ShardedCoordinator{},
		&kv.ShardGroup{Store: st}, w, bootDEK, fullNodeID)
	if !w.cache.Registered() {
		t.Error("recovery tick did not re-MarkRegistered after stale MarkRegistered race; gemini CRITICAL on PR #864 fail-closed regression")
	}
}

// preSeedRegistryRow seeds a writer-registry row at
// (dekID, node, epoch) so verifyRegistered short-circuits and
// runWriterRegistration's MarkRegistered side effect fires without a
// real coordinator/engine. The hardcoded writeRegistryRow helper uses
// testRegDEKID; this variant takes the DEK id so 7b' rotation tests
// can target the rotated DEK.
func preSeedRegistryRow(t *testing.T, st store.MVCCStore, dekID uint32, fullNodeID uint64, epoch uint16) {
	t.Helper()
	reg, err := store.WriterRegistryFor(st)
	if err != nil {
		t.Fatalf("WriterRegistryFor: %v", err)
	}
	val := encryption.EncodeRegistryValue(encryption.RegistryValue{
		FullNodeID: fullNodeID, FirstSeenLocalEpoch: epoch, LastSeenLocalEpoch: epoch,
	})
	if err := reg.SetRegistryRow(encryption.RegistryKey(dekID, encryption.NodeID16(fullNodeID)), val); err != nil {
		t.Fatalf("SetRegistryRow(dek=%d, epoch=%d): %v", dekID, epoch, err)
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
