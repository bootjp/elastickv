package store

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/cockroachdb/errors"
)

// regGateFixture wires an encrypted PebbleStore with the §7.1 cutover
// gate ALWAYS active and the Stage 7a-2 registration gate driven by a
// caller-toggled `registered` pointer, so a test can exercise the
// pre-registration (false) vs post-registration (true) direct-write
// behaviour without re-opening the store.
type regGateFixture struct {
	mvcc       MVCCStore
	registered *bool
}

// regGateDEKID is the storage DEK id used across the registration-gate
// fixtures.
const regGateDEKID uint32 = 7

func newRegGateStore(t *testing.T, registered *bool) *regGateFixture {
	t.Helper()
	ks := encryption.NewKeystore()
	dek := make([]byte, encryption.KeySize)
	dek[0] = 0xCC
	if err := ks.Set(regGateDEKID, dek); err != nil {
		t.Fatalf("Keystore.Set: %v", err)
	}
	c, err := encryption.NewCipher(ks)
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}
	dir := filepath.Join(t.TempDir(), "pebble")
	mvcc, err := NewPebbleStore(dir,
		WithEncryption(c,
			NewCounterNonceFactory(0x00AA, 0x0005),
			func() (uint32, bool) { return regGateDEKID, true }, // DEK always provisioned
		),
		WithStorageEnvelopeGate(func() bool { return true }), // cutover fired
		WithStorageRegistrationGate(func() bool { return *registered }),
	)
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	t.Cleanup(func() {
		if cerr := mvcc.Close(); cerr != nil {
			t.Errorf("cleanup close pebble: %v", cerr)
		}
	})
	return &regGateFixture{mvcc: mvcc, registered: registered}
}

// TestRegistrationGate_DirectPathFailsClosedBeforeRegistration pins
// the Stage 7a-2 §4.1 enforcement: with the envelope active and an
// active DEK, the DIRECT write paths (PutAt, ExpireAt, ApplyMutations)
// refuse to emit a nonce until the writer registration commits,
// returning ErrWriterNotRegistered. After registration the same write
// succeeds.
func TestRegistrationGate_DirectPathFailsClosedBeforeRegistration(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("PutAt", func(t *testing.T) {
		t.Parallel()
		registered := false
		f := newRegGateStore(t, &registered)
		err := f.mvcc.PutAt(ctx, []byte("k"), []byte("v"), 100, 0)
		if !errors.Is(err, ErrWriterNotRegistered) {
			t.Fatalf("PutAt pre-registration: got %v, want ErrWriterNotRegistered", err)
		}
		registered = true
		if err := f.mvcc.PutAt(ctx, []byte("k"), []byte("v"), 200, 0); err != nil {
			t.Fatalf("PutAt post-registration: %v", err)
		}
		mustGet(t, f.mvcc, []byte("k"), 250, "v")
	})

	t.Run("ApplyMutations", func(t *testing.T) {
		t.Parallel()
		registered := false
		f := newRegGateStore(t, &registered)
		muts := []*KVPairMutation{{Op: OpTypePut, Key: []byte("a"), Value: []byte("1")}}
		err := f.mvcc.ApplyMutations(ctx, muts, nil, 0, 100)
		if !errors.Is(err, ErrWriterNotRegistered) {
			t.Fatalf("ApplyMutations pre-registration: got %v, want ErrWriterNotRegistered", err)
		}
		registered = true
		if err := f.mvcc.ApplyMutations(ctx, muts, nil, 0, 200); err != nil {
			t.Fatalf("ApplyMutations post-registration: %v", err)
		}
		mustGet(t, f.mvcc, []byte("a"), 250, "1")
	})

	t.Run("ExpireAt", func(t *testing.T) {
		t.Parallel()
		// ExpireAt re-encrypts the latest value, so seed a value first
		// (post-registration), then flip back to exercise the gate on the
		// ExpireAt direct-write itself.
		registered := true
		f := newRegGateStore(t, &registered)
		if err := f.mvcc.PutAt(ctx, []byte("e"), []byte("v"), 100, 0); err != nil {
			t.Fatalf("seed PutAt: %v", err)
		}
		registered = false
		err := f.mvcc.ExpireAt(ctx, []byte("e"), 5000, 200)
		if !errors.Is(err, ErrWriterNotRegistered) {
			t.Fatalf("ExpireAt pre-registration: got %v, want ErrWriterNotRegistered", err)
		}
		registered = true
		if err := f.mvcc.ExpireAt(ctx, []byte("e"), 5000, 300); err != nil {
			t.Fatalf("ExpireAt post-registration: %v", err)
		}
	})
}

// TestRegistrationGate_FSMApplyPathNeverGated pins design §1: the
// FSM-apply path (ApplyMutationsRaft) must NOT fail closed on
// not-registered — replicated apply stays deterministic and may
// legitimately encrypt before this node's own registration entry
// commits. With registered=false it still encrypts and round-trips.
func TestRegistrationGate_FSMApplyPathNeverGated(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	registered := false // never registered
	f := newRegGateStore(t, &registered)
	muts := []*KVPairMutation{{Op: OpTypePut, Key: []byte("raft"), Value: []byte("applied")}}
	if err := f.mvcc.ApplyMutationsRaft(ctx, muts, nil, 0, 100); err != nil {
		t.Fatalf("ApplyMutationsRaft must not be gated, got: %v", err)
	}
	mustGet(t, f.mvcc, []byte("raft"), 150, "applied")
}

func TestRegistrationGate_PromoteVersionsNeverGated(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	registered := true
	f := newRegGateStore(t, &registered)
	stage := func(raw string) []byte {
		return append([]byte("stage|"), []byte(raw)...)
	}
	targetKey := func(staged []byte) ([]byte, bool) {
		return bytes.TrimPrefix(staged, []byte("stage|")), bytes.HasPrefix(staged, []byte("stage|"))
	}

	if err := f.mvcc.PutAt(ctx, stage("promote"), []byte("value"), 100, 0); err != nil {
		t.Fatalf("seed staged PutAt: %v", err)
	}
	registered = false
	if err := f.mvcc.PutAt(ctx, []byte("direct"), []byte("blocked"), 110, 0); !errors.Is(err, ErrWriterNotRegistered) {
		t.Fatalf("direct PutAt pre-registration: got %v, want ErrWriterNotRegistered", err)
	}
	promoter, ok := f.mvcc.(MigrationPromoter)
	if !ok {
		t.Fatalf("expected MigrationPromoter, got %T", f.mvcc)
	}

	result, err := promoter.PromoteVersions(ctx, PromoteVersionsOptions{
		JobID:       11,
		StartKey:    []byte("stage|"),
		EndKey:      PrefixScanEnd([]byte("stage|")),
		MaxVersions: 10,
		TargetKey:   targetKey,
	})
	if err != nil {
		t.Fatalf("PromoteVersions pre-registration: %v", err)
	}
	if !result.Done || result.PromotedRows != 1 {
		t.Fatalf("PromoteVersions result = %+v, want done with one promoted row", result)
	}
	mustGet(t, f.mvcc, []byte("promote"), 150, "value")
	if _, err := f.mvcc.GetAt(ctx, stage("promote"), 150); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("staged version after promotion: got %v, want ErrKeyNotFound", err)
	}
}

// TestRegistrationGate_NotEncryptingIsUngated confirms the gate is only
// consulted when the write WOULD encrypt. When the cutover gate is off
// (envelope inactive) the direct path writes cleartext and never
// consults Registered(), so an unregistered node is not blocked.
func TestRegistrationGate_NotEncryptingIsUngated(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ks := encryption.NewKeystore()
	dek := make([]byte, encryption.KeySize)
	dek[0] = 0xDD
	if err := ks.Set(7, dek); err != nil {
		t.Fatalf("Keystore.Set: %v", err)
	}
	c, err := encryption.NewCipher(ks)
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}
	dir := filepath.Join(t.TempDir(), "pebble")
	registered := false
	mvcc, err := NewPebbleStore(dir,
		WithEncryption(c,
			NewCounterNonceFactory(0x00BB, 0x0006),
			func() (uint32, bool) { return 7, true },
		),
		WithStorageEnvelopeGate(func() bool { return false }), // cutover NOT fired
		WithStorageRegistrationGate(func() bool { return registered }),
	)
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	t.Cleanup(func() {
		if cerr := mvcc.Close(); cerr != nil {
			t.Errorf("cleanup close: %v", cerr)
		}
	})
	// Envelope inactive → cleartext write → gate not consulted → no error
	// despite registered=false.
	if err := mvcc.PutAt(ctx, []byte("k"), []byte("v"), 100, 0); err != nil {
		t.Fatalf("PutAt with envelope inactive must not be gated: %v", err)
	}
	mustGet(t, mvcc, []byte("k"), 150, "v")
}

// TestRegistrationGate_UnwiredIsNoOp confirms backward compatibility:
// a store wired with WithEncryption + the cutover gate but WITHOUT
// WithStorageRegistrationGate keeps the pre-7a-2 posture — the direct
// path emits envelopes without consulting any registration state.
func TestRegistrationGate_UnwiredIsNoOp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ks := encryption.NewKeystore()
	dek := make([]byte, encryption.KeySize)
	dek[0] = 0xEE
	if err := ks.Set(7, dek); err != nil {
		t.Fatalf("Keystore.Set: %v", err)
	}
	c, err := encryption.NewCipher(ks)
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}
	dir := filepath.Join(t.TempDir(), "pebble")
	mvcc, err := NewPebbleStore(dir,
		WithEncryption(c,
			NewCounterNonceFactory(0x00CC, 0x0007),
			func() (uint32, bool) { return 7, true },
		),
		WithStorageEnvelopeGate(func() bool { return true }),
		// No WithStorageRegistrationGate.
	)
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	t.Cleanup(func() {
		if cerr := mvcc.Close(); cerr != nil {
			t.Errorf("cleanup close: %v", cerr)
		}
	})
	if err := mvcc.PutAt(ctx, []byte("k"), []byte("v"), 100, 0); err != nil {
		t.Fatalf("PutAt with unwired registration gate must succeed: %v", err)
	}
	mustGet(t, mvcc, []byte("k"), 150, "v")
}
