package store

import (
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

	t.Run("ImportVersions", func(t *testing.T) {
		t.Parallel()
		registered := false
		f := newRegGateStore(t, &registered)
		opts := ImportVersionsOptions{
			JobID:     1,
			BracketID: 1,
			BatchSeq:  1,
			Cursor:    []byte("cursor"),
			Versions: []MVCCVersion{
				{Key: []byte("import"), CommitTS: 100, Value: []byte("v")},
			},
		}
		_, err := f.mvcc.ImportVersions(ctx, opts)
		if !errors.Is(err, ErrWriterNotRegistered) {
			t.Fatalf("ImportVersions pre-registration: got %v, want ErrWriterNotRegistered", err)
		}
		registered = true
		if _, err := f.mvcc.ImportVersions(ctx, opts); err != nil {
			t.Fatalf("ImportVersions post-registration: %v", err)
		}
		mustGet(t, f.mvcc, []byte("import"), 150, "v")
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

	_, err := f.mvcc.ImportVersionsRaft(ctx, ImportVersionsOptions{
		JobID:     2,
		BracketID: 1,
		BatchSeq:  1,
		Cursor:    []byte("cursor"),
		Versions: []MVCCVersion{
			{Key: []byte("raft-import"), CommitTS: 200, Value: []byte("imported")},
		},
	})
	if err != nil {
		t.Fatalf("ImportVersionsRaft must not be gated, got: %v", err)
	}
	mustGet(t, f.mvcc, []byte("raft-import"), 250, "imported")
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
